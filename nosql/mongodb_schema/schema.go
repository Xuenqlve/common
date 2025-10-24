package mongodb_schema

import (
	"context"
	"github.com/xuenqlve/common/schema_store"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type Schema struct {
	conn *mongo.Client
}

func (s *Schema) LoadSchema(key schema_store.SchemaKey) (any, error) {
	index := key.(*Index)
	return s.GetTable(index.Database, index.Table)
}

func (s *Schema) GetTable(database, table string) (*Table, error) {
	var err error
	doc := &Table{
		Database:      database,
		Table:         table,
		PrimaryIndex:  []string{},
		UniqueIndex:   map[string][]string{},
		scanColumns:   []string{},
		scanCondition: "",
	}
	doc.PrimaryIndex, doc.UniqueIndex, err = s.getUniqueKeys(database, table)
	if err != nil {
		return nil, err
	}
	doc.indexes, err = s.indexes(database, table)
	if err != nil {
		return nil, err
	}
	if err = doc.InitScanColumns(); err != nil {
		return nil, err
	}
	return doc, err
}

const (
	primaryIndexName = "_id_"
	PrimaryId        = "_id"
)

func (s *Schema) getUniqueKeys(database, table string) (primary []string, unique map[string][]string, err error) {
	list, err := s.conn.Database(database).Collection(table).Indexes().ListSpecifications(context.Background())
	if err != nil {
		return
	}
	unique = make(map[string][]string)

	for _, index := range list {
		// 解析KeysDocument获取索引字段
		elements, err := index.KeysDocument.Elements()
		if err != nil {
			continue
		}

		var indexFields []string
		for _, elem := range elements {
			indexFields = append(indexFields, elem.Key())
		}

		// MongoDB中_id字段默认是主键
		if index.Name == primaryIndexName {
			primary = indexFields
			unique[index.Name] = indexFields
		}
		if index.Unique != nil && *index.Unique {
			// 其他唯一索引
			unique[index.Name] = indexFields
		}
	}

	return primary, unique, nil
}

func (s *Schema) indexes(database, table string) ([]bson.D, error) {
	cursor, err := s.conn.Database(database).Collection(table).Indexes().List(context.Background())
	if err != nil {
		return nil, err
	}
	indexes := make([]bson.D, 0)
	if err = cursor.All(context.Background(), &indexes); err != nil {
		return nil, err
	}
	result := make([]bson.D, 0, len(indexes))
	for _, index := range indexes {
		if HaveIdIndexKey(index) {
			continue
		}
		newIndex := bson.D{}
		for _, v := range index {
			if v.Key == "ns" || v.Key == "v" || v.Key == "background" {
				continue
			}
			newIndex = append(newIndex, v)
		}
		newIndex = append(newIndex, primitive.E{
			Key:   "background",
			Value: true,
		})
		result = append(result, newIndex)
	}
	return result, nil
}

func (s *Schema) Close() error {
	return s.conn.Disconnect(context.Background())
}

func NewSchema(conn *mongo.Client) *Schema {
	return &Schema{
		conn: conn,
	}
}

func HaveIdIndexKey(obj bson.D) bool {
	for _, ele := range obj {
		if ele.Key != "key" {
			continue
		}

		keyValue, ok := ele.Value.(bson.D)
		if !ok {
			continue
		}
		if len(keyValue) > 1 {
			continue
		}

		for _, fieldEle := range keyValue {
			if fieldEle.Key == "_id" {
				return true
			}
		}
	}

	return false
}
