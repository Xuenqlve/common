package mongodb_schema

import (
	"fmt"
	"strings"

	"github.com/xuenqlve/common/ddl_parser"
	"github.com/xuenqlve/common/log"
	"github.com/xuenqlve/common/nosql/oplog"
	"github.com/xuenqlve/common/schema_store"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type oplogDDLLoader struct {
}

func (l *oplogDDLLoader) Parse(ddl ddl_parser.DDL) ([]ddl_parser.Statement, error) {
	event, ok := ddl.SQL.(oplog.Event)
	if !ok {
		return nil, fmt.Errorf("sql:%s failed to parse ddl_parser statement", ddl.SQL)
	}
	return l.makeStatement(event)
}
func (l *oplogDDLLoader) makeStatement(event oplog.Event) ([]ddl_parser.Statement, error) {
	switch event.OperationType {
	//case schema_store.Create.String():
	//	database, table, err := oplog.SplitNamespace(event.Ns)
	//	if err != nil {
	//		return nil, err
	//	}
	//	md := ddl_parser.Metadata{
	//		Database: fmt.Sprintf("%v", database),
	//		Table:    fmt.Sprintf("%v", table),
	//	}
	//	return []ddl_parser.Statement{
	//		ddl_parser.NewTableStatement(newOplogDDLStatement(schema_store.Create, event.FullDocument, md), md.Database, false, md.Table),
	//	}, nil
	case schema_store.Create_Indexes.String():
		database, table, err := oplog.SplitNamespace(event.Ns)
		if err != nil {
			return nil, err
		}
		md := ddl_parser.Metadata{
			Database: fmt.Sprintf("%v", database),
			Table:    fmt.Sprintf("%v", table),
		}
		return []ddl_parser.Statement{
			ddl_parser.NewTableStatement(newOplogDDLStatement(schema_store.Create_Indexes, event.FullDocument, md), md.Database, false, md.Table),
		}, nil
	case schema_store.Drop.String():
		database, table, err := oplog.SplitNamespace(event.Ns)
		if err != nil {
			return nil, err
		}
		md := ddl_parser.Metadata{
			Database: fmt.Sprintf("%v", database),
			Table:    fmt.Sprintf("%v", table),
		}
		return []ddl_parser.Statement{
			ddl_parser.NewTableStatement(newOplogDDLStatement(schema_store.Drop, event.FullDocument, md), md.Database, false, md.Table),
		}, nil
	case schema_store.Drop_Database.String():
		database, ok := event.Ns[oplog.EventNsDBKey]
		if !ok {
			return nil, fmt.Errorf("not found database in event.ns:%v", event.Ns)
		}
		md := ddl_parser.Metadata{
			Database: fmt.Sprintf("%v", database),
		}
		return []ddl_parser.Statement{
			ddl_parser.NewDatabaseStatement(newOplogDDLStatement(schema_store.Drop_Database, event.FullDocument, md), md.Database, false),
		}, nil
	case schema_store.Drop_Indexes.String():
		database, table, err := oplog.SplitNamespace(event.Ns)
		if err != nil {
			return nil, err
		}
		md := ddl_parser.Metadata{
			Database: fmt.Sprintf("%v", database),
			Table:    fmt.Sprintf("%v", table),
		}
		return []ddl_parser.Statement{
			ddl_parser.NewTableStatement(newOplogDDLStatement(schema_store.Drop_Indexes, event.FullDocument, md), md.Database, false, md.Table),
		}, nil
	case schema_store.Rename.String():
		database, table, err := oplog.SplitNamespace(event.Ns)
		if err != nil {
			return nil, err
		}
		oldDoc := ddl_parser.Table{
			Database: fmt.Sprintf("%v", database),
			Table:    fmt.Sprintf("%v", table),
		}
		toDb, toDoc, err := oplog.SplitNamespace(event.To)
		if err != nil {
			return nil, err
		}
		newDoc := ddl_parser.Table{
			Database: fmt.Sprintf("%v", toDb),
			Table:    fmt.Sprintf("%v", toDoc),
		}
		if newDoc.Database == "" {
			newDoc.Database = database
		}
		md := ddl_parser.Metadata{
			Database:       oldDoc.Database,
			Table:          oldDoc.Table,
			RenameDatabase: newDoc.Database,
			RenameTable:    newDoc.Table,
		}
		return []ddl_parser.Statement{
			ddl_parser.NewRenameTableStatement(newOplogDDLStatement(schema_store.Rename, event.FullDocument, md), oldDoc, newDoc, false),
		}, nil
	default:
		log.Warnf("not supported statement type: %T", event.OperationType)
		return []ddl_parser.Statement{}, nil
	}
}

type OplogDDLLoader struct {
	oplogDDLLoader
}

func NewOplogDDLLoader() *OplogDDLLoader {
	return &OplogDDLLoader{
		oplogDDLLoader{},
	}
}

func (s *OplogDDLLoader) Parse(event oplog.Event) ([]DDLStatement, error) {
	stmts, err := s.oplogDDLLoader.Parse(ddl_parser.DDL{SQL: event})
	if err != nil {
		return nil, err
	}
	list := make([]DDLStatement, 0, len(stmts))
	for _, stmt := range stmts {
		list = append(list, DDLStatement{
			Statement: stmt,
		})
	}
	return list, nil
}

type DDLLoader struct {
}

func NewDDLLoader() *DDLLoader {
	return &DDLLoader{}
}

func (l *DDLLoader) Parse(ddl ddl_parser.DDL) ([]ddl_parser.Statement, error) {
	event, ok := ddl.SQL.(bson.D)
	if !ok {
		return nil, fmt.Errorf("sql:%s failed to parse ddl_parser statement", ddl.SQL)
	}
	return l.makeStatement(ddl.Schema, event)
}

func (l *DDLLoader) makeStatement(schema string, stmt bson.D) ([]ddl_parser.Statement, error) {
	if len(stmt) == 0 {
		return nil, fmt.Errorf("stmt:%v len() == 0  ", stmt)
	}
	opType := stmt[0].Key
	statementSet := []ddl_parser.Statement{}
	switch opType {
	case schema_store.Create_Indexes.String():
		md := ddl_parser.Metadata{
			Database: schema,
			Table:    fmt.Sprintf("%v", stmt[0].Value),
		}
		for _, v := range stmt {
			switch v.Key {
			case "indexes":
				indexType := ddl_parser.IndexTypeConstraint
				indexName := ""
				columns := []string{}

				// 先尝试转换为[]bson.D类型
				if indexes, ok := v.Value.([]bson.D); ok {
					// 原有逻辑
					for _, index := range indexes {
						for _, item := range index {
							switch item.Key {
							case "key":
								bsonKeys, ok := item.Value.(bson.D)
								if !ok {
									return nil, fmt.Errorf("stmt:%v indexes key type error", stmt)
								}
								for _, key := range bsonKeys {
									columns = append(columns, key.Key)
								}
							case "name":
								indexName = item.Value.(string)
							case "unique":
								unique := item.Value.(bool)
								if unique {
									indexType = ddl_parser.IndexTypeUnique
								}
							}
						}
						statement := bson.D{primitive.E{Key: "createIndexes", Value: md.Table}}
						statement = append(statement, index...)
						statementSet = append(statementSet, ddl_parser.NewTableConstraintsStatement(newOplogDDLStatement(schema_store.Create_Indexes, statement, md), md.Database, false, md.Table, indexName, columns, indexType))
					}
				} else if indexesA, ok := v.Value.(primitive.A); ok {
					// 处理primitive.A类型
					for _, indexVal := range indexesA {
						if index, ok := indexVal.(primitive.D); ok {
							indexType = ddl_parser.IndexTypeConstraint
							indexName = ""
							columns = []string{}

							for _, item := range index {
								switch item.Key {
								case "key":
									if bsonKeys, ok := item.Value.(primitive.D); ok {
										for _, key := range bsonKeys {
											columns = append(columns, key.Key)
										}
									} else {
										return nil, fmt.Errorf("stmt:%v indexes key type error", stmt)
									}
								case "name":
									indexName = item.Value.(string)
								case "unique":
									unique := item.Value.(bool)
									if unique {
										indexType = ddl_parser.IndexTypeUnique
									}
								}
							}
							statement := bson.D{primitive.E{Key: "createIndexes", Value: md.Table}}
							statement = append(statement, index...)
							statementSet = append(statementSet, ddl_parser.NewTableConstraintsStatement(newOplogDDLStatement(schema_store.Create_Indexes, statement, md), md.Database, false, md.Table, indexName, columns, indexType))
						}
					}
				} else {
					return nil, fmt.Errorf("stmt:%v indexes type error, got %T", stmt, v.Value)
				}
			}
		}
		return statementSet, nil
	case schema_store.Drop.String():
		md := ddl_parser.Metadata{
			Database: schema,
			Table:    fmt.Sprintf("%v", stmt[0].Value),
		}
		return []ddl_parser.Statement{
			ddl_parser.NewTableStatement(newOplogDDLStatement(schema_store.Drop, stmt, md), md.Database, false, md.Table),
		}, nil
	case schema_store.Drop_Database.String():
		md := ddl_parser.Metadata{
			Database: schema,
		}
		return []ddl_parser.Statement{
			ddl_parser.NewDatabaseStatement(newOplogDDLStatement(schema_store.Drop_Database, stmt, md), md.Database, false),
		}, nil
	case schema_store.Drop_Indexes.String():
		md := ddl_parser.Metadata{
			Database: schema,
			Table:    fmt.Sprintf("%v", stmt[0].Value),
		}
		statementSet = make([]ddl_parser.Statement, 0)
		for _, v := range stmt {
			if v.Key == "index" {
				indexName := fmt.Sprintf("%v", v.Value)
				statementSet = append(statementSet, ddl_parser.NewTableConstraintsStatement(newOplogDDLStatement(schema_store.Drop_Indexes, stmt, md), md.Database, false, md.Table, indexName, []string{}, ddl_parser.IndexTypeConstraint))
			}
		}
		if len(statementSet) == 0 {
			return nil, fmt.Errorf("no index name found in DROP_INDEXES statement: %v", stmt)
		}
		return statementSet, nil
	case schema_store.Rename.String():
		md := ddl_parser.Metadata{}
		oldDoc, newDoc := ddl_parser.Table{}, ddl_parser.Table{}
		for _, v := range stmt {
			switch v.Key {
			case "renameCollection":
				database, table, err := renameCollectionSplit(v.Value)
				if err != nil {
					return nil, err
				}
				md.Database = database
				md.Table = table
				oldDoc.Database = database
				oldDoc.Table = table
			case "to":
				database, table, err := renameCollectionSplit(v.Value)
				if err != nil {
					return nil, err
				}
				md.RenameDatabase = database
				md.RenameTable = table
				newDoc.Database = database
				newDoc.Table = table
			}
		}
		return []ddl_parser.Statement{
			ddl_parser.NewRenameTableStatement(newOplogDDLStatement(schema_store.Rename, stmt, md), oldDoc, newDoc, false),
		}, nil
	default:
		log.Warnf("not supported statement type: %T", opType)
		return []ddl_parser.Statement{}, nil
	}
}

func renameCollectionSplit(value any) (database, table string, err error) {
	ns, ok := value.(string)
	if !ok {
		return "", "", fmt.Errorf("value is not string type: %T", value)
	}
	namespace := strings.Split(ns, ".")
	if len(namespace) != 2 {
		return "", "", fmt.Errorf("invalid namespace: %s", ns)
	}
	return namespace[0], namespace[1], nil
}

type DDLStatement struct {
	ddl_parser.Statement
}

func (s *DDLStatement) GenerateSQL() (bson.D, error) {
	sql, err := s.Statement.GenerateSQL()
	if err != nil {
		return bson.D{}, nil
	}
	data, ok := sql.(bson.D)
	if !ok {
		return bson.D{}, nil
	}
	return data, nil
}

type OplogDDLStatement struct {
	ddlType     schema_store.DDL
	changeEvent map[int]any
	stmt        bson.D
	metadata    ddl_parser.Metadata
}

func newOplogDDLStatement(ddlType schema_store.DDL, stmt bson.D, metadata ddl_parser.Metadata) *OplogDDLStatement {
	return &OplogDDLStatement{
		ddlType:     ddlType,
		stmt:        stmt,
		metadata:    metadata,
		changeEvent: map[int]any{},
	}
}

func NewDDLStatement(ddlType schema_store.DDL, stmt bson.D, metadata ddl_parser.Metadata) *OplogDDLStatement {
	return &OplogDDLStatement{
		ddlType:     ddlType,
		stmt:        stmt,
		metadata:    metadata,
		changeEvent: map[int]any{},
	}
}

func (s *OplogDDLStatement) DDLType() schema_store.DDL {
	return s.ddlType
}

func (s *OplogDDLStatement) SubmitModification(event int, parameter any) {
	switch event {
	case ddl_parser.ReplaceDatabase:
		s.metadata.Database = parameter.(string)
	case ddl_parser.ReplaceTable:
		s.metadata.Table = parameter.(string)
	case ddl_parser.ReplaceRenameTableSchemaMap:
		// 对于RENAME_TABLE，parameter是map[Table]Table类型
		if mapping, ok := parameter.(map[ddl_parser.Table]ddl_parser.Table); ok {
			// 取出映射中的第一个（也是唯一一个）键值对
			for oldTable, newTable := range mapping {
				// 直接使用修改选项应用后的最终值
				s.metadata.Database = oldTable.Database
				s.metadata.Table = oldTable.Table
				s.metadata.RenameDatabase = newTable.Database
				s.metadata.RenameTable = newTable.Table
				break // 只处理第一个映射
			}
		}
	case ddl_parser.ReplaceTableColumnMap:
		// 处理列名重命名
		if columnMap, ok := parameter.(map[string]string); ok && len(columnMap) > 0 {
			// 对每个BSON元素应用列名重命名
			for i, elem := range s.stmt {
				newValue := applyColumnRenaming(elem.Value, columnMap)
				s.stmt[i] = primitive.E{Key: elem.Key, Value: newValue}
			}
		}
	}
}

// applyColumnRenaming 应用列名重命名到BSON值
func applyColumnRenaming(value interface{}, columnMap map[string]string) interface{} {
	// 对于字符串值，检查是否需要重命名
	if str, ok := value.(string); ok {
		// 对于形如 "database.table" 的字符串，不进行列名重命名
		return str
	}

	// 对于primitive.D类型（索引键）
	if keyDoc, ok := value.(primitive.D); ok {
		newKeyDoc := make(primitive.D, 0, len(keyDoc))
		for _, keyElem := range keyDoc {
			// 检查列名是否需要重命名
			newColumnName := keyElem.Key
			if renamed, exists := columnMap[keyElem.Key]; exists {
				newColumnName = renamed
			}
			newKeyDoc = append(newKeyDoc, primitive.E{Key: newColumnName, Value: keyElem.Value})
		}
		return newKeyDoc
	}

	// 对于BSON文档数组（如indexes字段）
	if bsonArray, ok := value.([]bson.D); ok {
		newArray := make([]bson.D, 0, len(bsonArray))
		for _, doc := range bsonArray {
			newDoc := make(bson.D, 0, len(doc))
			for _, elem := range doc {
				if elem.Key == "key" {
					// 处理索引键的重命名
					if keyDoc, ok := elem.Value.(bson.D); ok {
						newKeyDoc := make(bson.D, 0, len(keyDoc))
						for _, keyElem := range keyDoc {
							// 检查列名是否需要重命名
							newColumnName := keyElem.Key
							if renamed, exists := columnMap[keyElem.Key]; exists {
								newColumnName = renamed
							}
							newKeyDoc = append(newKeyDoc, primitive.E{Key: newColumnName, Value: keyElem.Value})
						}
						newDoc = append(newDoc, primitive.E{Key: elem.Key, Value: newKeyDoc})
					} else {
						newDoc = append(newDoc, elem)
					}
				} else {
					newDoc = append(newDoc, elem)
				}
			}
			newArray = append(newArray, newDoc)
		}
		return newArray
	}

	// 对于其他类型，直接返回原值
	return value
}

func (s *OplogDDLStatement) Metadata() ddl_parser.Metadata {
	return s.metadata
}

func (s *OplogDDLStatement) GenerateSQL() (any, error) {
	switch s.ddlType {
	case schema_store.Create_Indexes:
		var innerBsonD, indexes bson.D
		for i, ele := range s.stmt {
			if i == 0 {
				if ele.Key != "createIndexes" {
					return nil, fmt.Errorf("s.stmt:%v when ele.Name != 'createIndexes'", s.stmt)
				}
			} else {
				if ele.Key == "key" {
					if columnMap, ok := s.changeEvent[ddl_parser.ReplaceTableColumnMap]; ok {
						if replaceMap, ok := columnMap.(map[string]string); ok {
							if keyValue, ok := ele.Value.(bson.D); ok {
								newKeyValue := bson.D{}
								for _, keyEle := range keyValue {
									newKey := keyEle.Key
									if newName, exists := replaceMap[keyEle.Key]; exists {
										newKey = newName
									}
									newKeyValue = append(newKeyValue, primitive.E{
										Key:   newKey,
										Value: keyEle.Value,
									})
								}
								ele = primitive.E{Key: "key", Value: newKeyValue}
							}
						}
					}
				}
				innerBsonD = append(innerBsonD, ele)
			}
		}

		tableName := s.metadata.Table
		indexes = append(indexes, primitive.E{Key: "createIndexes", Value: tableName})
		indexes = append(indexes, primitive.E{
			Key: "indexes",
			Value: []bson.D{ // only has 1 bson.D
				innerBsonD,
			},
		})
		return indexes, nil
	case schema_store.Drop:
		stmt := bson.D{
			primitive.E{
				Key:   "drop",
				Value: s.metadata.Table,
			},
			primitive.E{
				Key:   "db",
				Value: s.metadata.Database,
			},
		}
		return stmt, nil
	case schema_store.Drop_Database:
		stmt := bson.D{
			primitive.E{
				Key:   "dropDatabase",
				Value: 1,
			},
			primitive.E{
				Key:   "db",
				Value: s.metadata.Database,
			},
		}
		return stmt, nil
	case schema_store.Rename:
		stmt := bson.D{
			primitive.E{
				Key:   "renameCollection",
				Value: fmt.Sprintf("%s.%s", s.metadata.Database, s.metadata.Table),
			},
			primitive.E{
				Key:   "to",
				Value: fmt.Sprintf("%s.%s", s.metadata.RenameDatabase, s.metadata.RenameTable),
			},
		}
		return stmt, nil
	default:
		return s.stmt, nil
	}
}
