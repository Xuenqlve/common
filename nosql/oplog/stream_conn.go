package oplog

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xuenqlve/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	QueryTs    = "ts"
	QueryGid   = "g"
	QueryOpGT  = "$gt"
	QueryOpGTE = "$gte"
	localDB    = "local"
)

const (
	contextTimeout      = 60 * time.Second
	changeStreamTimeout = 24 // hours
	Int32max            = (int64(1) << 32) - 1

	DBRefRef = "$ref"
	DBRefId  = "$id"
	DBRefDb  = "$db"

	OplogNS                      = "oplog.rs"
	ReadWriteConcernDefault      = ""
	ReadWriteConcernLocal        = "local"
	ReadWriteConcernAvailable    = "available" // for >= 3.6
	ReadWriteConcernMajority     = "majority"
	ReadWriteConcernLinearizable = "linearizable"
)

type StreamConn struct {
	ctx       context.Context
	client    *mongo.Client
	csHandler *mongo.ChangeStream
}

const (
	DefaultStreamDataSource     = "stream"
	DefaultReaderFetchBatchSize = 1024
)

func MongoDBStreamConn(ctx context.Context, client *mongo.Client, batchSize int32, watchStartTime any) (conn *mongo.ChangeStream, err error) {
	waitTime := changeStreamTimeout * time.Hour // hours
	ops := &options.ChangeStreamOptions{
		MaxAwaitTime: &waitTime,
		BatchSize:    &batchSize,
	}
	if watchStartTime != nil {
		if val, ok := watchStartTime.(int64); ok {
			if (val >> 32) > 1 {
				startTime := &primitive.Timestamp{
					T: uint32(val >> 32),
					I: uint32(val & Int32max),
				}
				ops.SetStartAtOperationTime(startTime)
			}
		} else {
			var sourceDbVersion string
			if sourceDbVersion, err = GetDBVersion(ctx, client); err != nil {
				return
			}
			var normalized bson.Raw
			if normalized, err = normalizeResumeToken(watchStartTime); err != nil {
				return nil, err
			}
			if val_ver, _ := GetAndCompareVersion(client, "4.2.0", sourceDbVersion); val_ver {
				ops.SetStartAfter(normalized)
			} else {
				ops.SetResumeAfter(normalized)
			}
		}
	}
	ops.SetFullDocument(options.UpdateLookup)
	csHandler, err := client.Watch(ctx, mongo.Pipeline{}, ops)
	if err != nil {
		return
	}
	return csHandler, nil
}

func (c *StreamConn) GetNext() (bool, []byte) {
	if ok := c.csHandler.Next(c.ctx); !ok {
		return false, nil
	}
	return true, c.csHandler.Current
}

func (c *StreamConn) TryNext() (bool, []byte) {
	if ok := c.csHandler.TryNext(c.ctx); !ok {
		return false, nil
	}
	return true, c.csHandler.Current
}

func (c *StreamConn) ResumeToken() interface{} {
	out := c.csHandler.ResumeToken()
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c *StreamConn) Close() {
	if c.csHandler != nil {
		if err := c.csHandler.Close(c.ctx); err != nil {
			log.Errorf("mongodb Change Stream close err:%v", err)
		}
		c.csHandler = nil
	}

	// 关闭ChangeStream时，同时关闭NewMongoCommunityConn连接
	if c.client != nil {
		if err := c.client.Disconnect(c.ctx); err != nil {
			log.Errorf("mongodb client disconnect err:%v", err)
		}
		c.client = nil
	}
}

type Event struct {
	Id                bson.M              `bson:"_id" json:"_id"`
	OperationType     string              `bson:"operationType" json:"operationType"`
	FullDocument      bson.D              `bson:"fullDocument,omitempty" json:"fullDocument,omitempty"` // exists on "insert", "replace", "delete", "update"
	Ns                bson.M              `bson:"ns" json:"ns"`
	To                bson.M              `bson:"to,omitempty" json:"to,omitempty"`
	DocumentKey       bson.D              `bson:"documentKey,omitempty" json:"documentKey,omitempty"` // exists on "insert", "replace", "delete", "update"
	UpdateDescription bson.M              `bson:"updateDescription,omitempty" json:"updateDescription,omitempty"`
	ClusterTime       primitive.Timestamp `bson:"clusterTime,omitempty" json:"clusterTime,omitempty"`
	TxnNumber         *int64              `bson:"txnNumber,omitempty" json:"txnNumber,omitempty"`
	LSID              bson.Raw            `bson:"lsid,omitempty" json:"lsid,omitempty"`
}

func (e *Event) String() string {
	if ret, err := json.Marshal(e); err != nil {
		return err.Error()
	} else {
		return string(ret)
	}
}

func ConverteventOplog(input []byte, fullDoc bool) (*PartialLog, error) {
	event := new(Event)
	if err := bson.Unmarshal(input, event); err != nil {
		return nil, fmt.Errorf("unmarshal raw bson[%s] failed: %v", input, err)
	}

	oplog := new(PartialLog)
	// ts
	oplog.Timestamp = event.ClusterTime
	// transaction number
	oplog.TxnNumber = event.TxnNumber
	// lsid
	oplog.LSID = event.LSID
	// documentKey
	if len(event.DocumentKey) > 0 {
		oplog.DocumentKey = event.DocumentKey
	}

	ns := event.Ns

	// do nothing for "g", "uk", "fromMigrate"

	// handle different operation type
	switch event.OperationType {
	case "insert":
		oplog.Namespace = fmt.Sprintf("%s.%s", event.Ns["db"], event.Ns["coll"])
		oplog.Operation = "i"
		oplog.Object = event.FullDocument
	case "delete":
		oplog.Namespace = fmt.Sprintf("%s.%s", ns["db"], ns["coll"])
		oplog.Operation = "d"
		oplog.Object = event.DocumentKey
	case "replace":
		oplog.Namespace = fmt.Sprintf("%s.%s", ns["db"], ns["coll"])
		oplog.Operation = "u"
		oplog.Query = event.DocumentKey
		oplog.Object = bson.D{{"$set", event.FullDocument}}
	case "update":
		oplog.Namespace = fmt.Sprintf("%s.%s", ns["db"], ns["coll"])
		oplog.Operation = "u"
		oplog.Query = event.DocumentKey

		if fullDoc && event.FullDocument != nil && len(event.FullDocument) > 0 {
			oplog.Object = bson.D{{"$set", event.FullDocument}}
		} else {
			oplog.Object = make(bson.D, 0, 2)
			if updatedFields, ok := event.UpdateDescription["updatedFields"]; ok && len(updatedFields.(bson.M)) > 0 {
				oplog.Object = append(oplog.Object, primitive.E{
					Key:   "$set",
					Value: updatedFields,
				})
			}
			if removedFields, ok := event.UpdateDescription["removedFields"]; ok && len(removedFields.(primitive.A)) > 0 {
				removedFieldsMap := make(bson.M)
				for _, ele := range removedFields.(primitive.A) {
					removedFieldsMap[ele.(string)] = 1
				}
				oplog.Object = append(oplog.Object, primitive.E{
					Key:   "$unset",
					Value: removedFieldsMap,
				})
			}
		}

	case "drop":
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.Operation = "c"
		oplog.Object = bson.D{
			primitive.E{
				Key:   "drop",
				Value: ns["coll"],
			},
		}
		// ignore o2
	case "rename":
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.Operation = "c"
		oplog.Object = bson.D{ // should enable drop_database option on the replayer by default
			primitive.E{
				Key:   "renameCollection",
				Value: fmt.Sprintf("%s.%s", ns["db"], ns["coll"]),
			},
			primitive.E{
				Key:   "to",
				Value: fmt.Sprintf("%s.%s", event.To["db"], event.To["coll"]),
			},
		}
	case "dropDatabase":
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.Operation = "c"
		oplog.Object = bson.D{
			primitive.E{
				Key:   "dropDatabase",
				Value: 1,
			},
		}
	case "invalidate":
		return nil, fmt.Errorf("invalidate event happen, should be handle manually: %s", event)
	default:
		return nil, fmt.Errorf("unknown event type[%v] org_event[%v]", event.OperationType, event)
	}

	if oplog.Object == nil {
		oplog.Object = bson.D{}
	}
	if oplog.Query == nil {
		oplog.Query = bson.D{}
	}

	return oplog, nil
}

func GetAndCompareVersion(conn *mongo.Client, threshold string, compare string) (bool, error) {
	var err error
	if compare == "" {
		if compare, err = GetDBVersion(context.Background(), conn); err != nil {
			return false, err
		}
	}
	return CompareVersion(threshold, compare)
}

func GetDBVersion(ctx context.Context, conn *mongo.Client) (string, error) {
	res, err := conn.Database("admin").
		RunCommand(ctx, bson.D{{"buildInfo", 1}}).Raw()
	if err != nil {
		return "", err
	}
	ver, ok := res.Lookup("version").StringValueOK()
	if !ok {
		return "", fmt.Errorf("buildInfo do not have version")
	}
	return ver, nil
}

func CompareVersion(threshold string, compare string) (bool, error) {
	if compare == "" {
		return false, nil
	}
	compareArr := strings.Split(compare, ".")
	thresholdArr := strings.Split(threshold, ".")
	if len(compareArr) < 2 || len(thresholdArr) < 2 {
		return false, nil
	}

	for i := 0; i < 2; i++ {
		compareEle, errC := strconv.Atoi(compareArr[i])
		thresholdEle, errT := strconv.Atoi(thresholdArr[i])
		if errC != nil || errT != nil {
			return false, fmt.Errorf("errC:[%v], errT:[%v]", errC, errT)
		}

		if compareEle > thresholdEle {
			return true, nil
		} else if compareEle < thresholdEle {
			return false, fmt.Errorf("compare[%v] < threshold[%v]", compare, threshold)
		}
	}
	return true, nil
}

// get newest oplog
func GetNewestTimestampByConn(conn *mongo.Client) (int64, error) {
	return getOplogTimestamp(conn, -1)
}

// get oldest oplog
func GetOldestTimestampByConn(conn *mongo.Client) (int64, error) {
	return getOplogTimestamp(conn, 1)
}

func getOplogTimestamp(conn *mongo.Client, sortType int) (int64, error) {
	var result bson.M
	opts := options.FindOne().SetSort(bson.D{{"$natural", sortType}})
	err := conn.Database(localDB).Collection(OplogNS).FindOne(nil, bson.M{}, opts).Decode(&result)
	if err != nil {
		return 0, err
	}

	return TimeStampToInt64(result["ts"].(primitive.Timestamp)), nil
}

func normalizeResumeToken(origin any) (bson.Raw, error) {
	switch v := origin.(type) {
	case nil:
		return nil, nil
	case bson.Raw:
		return v, nil
	case []byte:
		return bson.Raw(v), nil
	case string:
		data, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, err
		}
		return bson.Raw(data), nil
	case map[string]any:
		data, err := bson.Marshal(v)
		if err != nil {
			return nil, err
		}
		return bson.Raw(data), nil
	default:
		return nil, fmt.Errorf("unsupported resume token type %T", v)
	}
}
