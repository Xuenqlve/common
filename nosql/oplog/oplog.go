package oplog

import (
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"strings"
)

const ObjectIdColumn = "_id"

const (
	PrimaryKey = ObjectIdColumn
)

type GenericOplog struct {
	Raw    []byte
	Parsed *PartialLog
}

type ParsedLog struct {
	Timestamp     primitive.Timestamp `bson:"ts" json:"ts"`
	Term          *int64              `bson:"t" json:"t"`
	Hash          *int64              `bson:"h" json:"h"`
	Version       int                 `bson:"v" json:"v"`
	Operation     string              `bson:"op" json:"op"`
	Gid           string              `bson:"g,omitempty" json:"g,omitempty"`
	Namespace     string              `bson:"ns" json:"ns"`
	DB            string              `bson:"db" json:"db"`
	Coll          string              `bson:"coll" json:"coll"`
	Object        bson.D              `bson:"o" json:"o"`
	Query         bson.D              `bson:"o2" json:"o2"`                                       // update condition
	UniqueIndexes bson.M              `bson:"uk,omitempty" json:"uk,omitempty"`                   //
	LSID          bson.Raw            `bson:"lsid,omitempty" json:"lsid,omitempty"`               // mark the session id, used in transaction
	FromMigrate   bool                `bson:"fromMigrate,omitempty" json:"fromMigrate,omitempty"` // move chunk
	TxnNumber     *int64              `bson:"txnNumber,omitempty" json:"txnNumber,omitempty"`     // transaction number in session
	DocumentKey   bson.D              `bson:"documentKey,omitempty" json:"documentKey,omitempty"` // exists when source collection is sharded, only including shard key and _id
	PrevOpTime    bson.Raw            `bson:"prevOpTime,omitempty"`
	UI            *primitive.Binary   `bson:"ui,omitempty" json:"ui,omitempty"` // do not enable currently
}

type PartialLog struct {
	ParsedLog
	UniqueIndexesUpdates bson.M // generate by CollisionMatrix
	RawSize              int    // generate by Decorator
	SourceId             int    // generate by Validator
}

func (partialLog *PartialLog) String() string {
	if ret, err := json.Marshal(partialLog.ParsedLog); err != nil {
		return err.Error()
	} else {
		return string(ret)
	}
}

func (partialLog *PartialLog) Dump(keys map[string]struct{}, all bool) bson.D {
	var out bson.D
	logType := reflect.TypeOf(partialLog.ParsedLog)
	for i := 0; i < logType.NumField(); i++ {
		if tagNameWithOption, ok := logType.Field(i).Tag.Lookup("bson"); ok {
			value := reflect.ValueOf(partialLog.ParsedLog).Field(i).Interface()
			tagName := strings.Split(tagNameWithOption, ",")[0]
			if !all {
				if _, ok := keys[tagName]; !ok {
					continue
				}
			}
			out = append(out, primitive.E{tagName, value})
		}
	}
	return out
}

func NewPartialLog(data bson.M) *PartialLog {
	parsedLog := new(ParsedLog)
	logType := reflect.TypeOf(*parsedLog)
	for i := 0; i < logType.NumField(); i++ {
		tagNameWithOption := logType.Field(i).Tag.Get("bson")
		tagName := strings.Split(tagNameWithOption, ",")[0]
		if v, ok := data[tagName]; ok {
			reflect.ValueOf(parsedLog).Elem().Field(i).Set(reflect.ValueOf(v))
		}
	}
	return &PartialLog{
		ParsedLog: *parsedLog,
	}
}
