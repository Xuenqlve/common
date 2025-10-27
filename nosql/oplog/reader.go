package oplog

import (
	"context"
	"fmt"
	"time"

	"github.com/xuenqlve/common/data_source/mongodb"
	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/log"
	"go.mongodb.org/mongo-driver/mongo"

	//input_metrics "github.com/xuenqlve/timburr/pkg/metrics/input"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ReaderConfig struct {
	Host             []string `mapstructure:"urls" json:"urls" toml:"urls" yaml:"urls"`
	ReplicaSet       string   `mapstructure:"replica-set" json:"replica-set" toml:"replica-set" yaml:"replica-set"`
	Username         string   `mapstructure:"username" json:"username" toml:"username" yaml:"username"`
	Password         string   `mapstructure:"password" json:"password" toml:"password" yaml:"password"`
	AuthSource       string   `mapstructure:"auth-source" json:"auth-source" toml:"auth-source" yaml:"auth-source"`
	StartPosition    Position `mapstructure:"start-position" yaml:"start-position" toml:"start-position"`
	CommitInterval   int64    `mapstructure:"commit-interval" json:"commit-interval" yaml:"commit-interval" toml:"commit-interval"`
	CommitCount      int64    `mapstructure:"commit-count" json:"commit-count" yaml:"commit-count" toml:"commit-count"`
	BufferCapacity   int      `mapstructure:"buffer-capacity" yaml:"buffer-capacity" json:"buffer-capacity"`
	ReaderBufferTime int      `mapstructure:"reader-buffer-time" yaml:"reader-buffer-time" toml:"reader-buffer-time"`
}

func (c *ReaderConfig) connect() (*mongo.Client, error) {
	cfg := mongodb.Config{
		Host:       c.Host,
		ReplicaSet: c.ReplicaSet,
		Username:   c.Username,
		Password:   c.Password,
		AuthSource: c.AuthSource,
	}
	if err := cfg.ValidateAndSetDefault(); err != nil {
		return nil, err
	}
	return cfg.Connect()
}

type Reader struct {
	ctx      context.Context
	pipeline string
	cfg      ReaderConfig

	eventReader  *EventReader
	eventHandler EventHandler

	currentPosition Position
	// 用于按时间间隔提交位点
	lastCommitTime time.Time
	// 用于按消息数量提交位点
	messageCount int64
	// 用于按事务维度提交位点
	currentTxnID primitive.ObjectID
}

func NewOplogReader(ctx context.Context, pipeline string, cfg ReaderConfig) (reader *Reader, err error) {
	reader = &Reader{
		ctx:             ctx,
		pipeline:        pipeline,
		cfg:             cfg,
		eventReader:     NewEventReader(ctx, cfg),
		currentPosition: cfg.StartPosition,
		lastCommitTime:  time.Now(),
		messageCount:    0,
		currentTxnID:    primitive.NilObjectID,
	}
	return reader, nil
}

const (
	insertOperation  = "insert"
	deleteOperation  = "delete"
	replaceOperation = "replace"
	updateOperation  = "update"

	createOperation        = "create"
	createIndexesOperation = "createIndexes"
	dropOperation          = "drop"
	dropDatabaseOperation  = "dropDatabase"
	dropIndexesOperation   = "dropIndexes"

	renameOperation     = "rename"
	modifyOperation     = "modify"
	invalidateOperation = "invalidate"
)

func (r *Reader) SetEventHandler(h EventHandler) {
	r.eventHandler = h
}
func (r *Reader) Run() error {
	if r.eventHandler == nil {
		return fmt.Errorf("event hander is nil")
	}
	if err := r.eventHandler.OnPosSynced(r.currentPosition, false); err != nil {
		return errors.Trace(err)
	}
	r.eventReader.SetQueryTimestampOnEmpty(r.currentPosition)
	r.eventReader.Start()
	for {
		rowlogs, err := r.eventReader.Next()
		if err != nil {
			return errors.Trace(err)
		}
		event := Event{}
		if err = bson.Unmarshal(rowlogs, &event); err != nil {
			return errors.Trace(err)
		}

		// 检查是否需要按事务维度提交位点
		if event.LSID != nil && event.TxnNumber != nil {
			// 提取事务ID
			var lsid struct {
				ID primitive.ObjectID `bson:"id"`
			}
			if err = bson.Unmarshal(event.LSID, &lsid); err == nil && !lsid.ID.IsZero() {
				// 如果事务ID发生变化，则提交位点
				if r.currentTxnID != primitive.NilObjectID && r.currentTxnID != lsid.ID {
					if err = r.commitPosition(event); err != nil {
						return errors.Trace(err)
					}
				}
				r.currentTxnID = lsid.ID
			}
		}

		r.eventHandler.SyncedTimestamp(event.ClusterTime.T)
		// 处理各种操作类型的事件
		switch event.OperationType {
		case insertOperation:
			var database, collection string
			if database, collection, err = SplitNamespace(event.Ns); err != nil {
				return errors.Trace(err)
			}
			if err = r.eventHandler.OnInsertEvent(database, collection, event.FullDocument); err != nil {
				return errors.Trace(err)
			}
		case deleteOperation:
			var database, collection string
			if database, collection, err = SplitNamespace(event.Ns); err != nil {
				return errors.Trace(err)
			}
			if err = r.eventHandler.OnDeleteEvent(database, collection, event.DocumentKey); err != nil {
				return errors.Trace(err)
			}
		case replaceOperation:
			var database, collection string
			if database, collection, err = SplitNamespace(event.Ns); err != nil {
				return errors.Trace(err)
			}
			log.Infof("event:%v", event)
			object := bson.D{{"$set", event.FullDocument}}
			if err = r.eventHandler.OnUpdateEvent(database, collection, event.DocumentKey, object); err != nil {
				return errors.Trace(err)
			}
		case updateOperation:
			object := bson.D{}
			var database, collection string
			if database, collection, err = SplitNamespace(event.Ns); err != nil {
				return errors.Trace(err)
			}
			if event.FullDocument != nil {
				object = bson.D{{"$set", event.FullDocument}}
			} else {
				object = make(bson.D, 0, 2)
				if updatedFields, ok := event.UpdateDescription["updatedFields"]; ok && len(updatedFields.(bson.M)) > 0 {
					object = append(object, primitive.E{
						Key:   "$set",
						Value: updatedFields,
					})
				}
				if removedFields, ok := event.UpdateDescription["removedFields"]; ok && len(removedFields.(primitive.A)) > 0 {
					removedFieldsMap := make(bson.M)
					for _, ele := range removedFields.(primitive.A) {
						removedFieldsMap[ele.(string)] = 1
					}
					object = append(object, primitive.E{
						Key:   "$unset",
						Value: removedFieldsMap,
					})
				}
			}
			if err = r.eventHandler.OnUpdateEvent(database, collection, event.DocumentKey, object); err != nil {
				return errors.Trace(err)
			}
		case dropOperation, dropDatabaseOperation, renameOperation:
			if err = r.eventHandler.OnDDLEvent(event); err != nil {
				return errors.Trace(err)
			}
			// DDL操作立即提交位点
			if err = r.commitPosition(event); err != nil {
				return errors.Trace(err)
			}
		case createOperation, createIndexesOperation, dropIndexesOperation:
			return fmt.Errorf("unknown event type[%v] org_event[%v]", event.OperationType, event)
		case invalidateOperation:
			return fmt.Errorf("invalidate event happen, should be handle manually: %s", event)
		default:
			return fmt.Errorf("unknown event type[%v] org_event[%v]", event.OperationType, event)
		}

		// 递增消息计数器
		r.messageCount++

		// 按消息数量提交位点
		if r.cfg.CommitCount > 0 && r.messageCount >= r.cfg.CommitCount {
			if err = r.commitPosition(event); err != nil {
				return errors.Trace(err)
			}
		}

		// 按时间间隔提交位点
		if r.cfg.CommitInterval > 0 && time.Since(r.lastCommitTime).Milliseconds() >= r.cfg.CommitInterval {
			if err = r.commitPosition(event); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// commitPosition 提交当前位点并重置计数器
func (r *Reader) commitPosition(event Event) error {
	token, err := r.eventReader.ResumeToken()
	if err != nil {
		return errors.Trace(err)
	}

	currentPos := Position{
		Token:     token,
		Timestamp: TimeStampToInt64(event.ClusterTime),
	}
	r.currentPosition = currentPos

	// 提交位点
	if err = r.eventHandler.OnPosSynced(currentPos, true); err != nil {
		return errors.Trace(err)
	}

	// 重置计数器和时间
	r.messageCount = 0
	r.lastCommitTime = time.Now()

	return nil
}

const (
	EventNsDBKey       = "db"
	EventCollectionKey = "coll"
)

func SplitNamespace(ns bson.M) (string, string, error) {
	database, ok := ns[EventNsDBKey]
	if !ok {
		return "", "", fmt.Errorf("not found database in event.ns:%v", ns)
	}

	table, ok := ns[EventCollectionKey]
	if !ok {
		return "", "", fmt.Errorf("not found table in event.ns:%v", ns)
	}
	return fmt.Sprintf("%v", database), fmt.Sprintf("%s", table), nil
}
