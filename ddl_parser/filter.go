package ddl_parser

import (
	"github.com/xuenqlve/common/log"
	"github.com/xuenqlve/common/match"
	"github.com/xuenqlve/common/schema_store"
	"github.com/xuenqlve/common/transform"
)

type FilterOption interface {
	Apply(Statement) bool
}

// 满足任意 filter 即通过
func Filter(stmt Statement, opts ...FilterOption) bool {
	for _, opt := range opts {
		flag := opt.Apply(stmt)

		if flag {
			return true
		}
	}
	return false
}

// 满足全部 filter 即通过
func FilterAll(stmt Statement, opts ...FilterOption) bool {
	for _, opt := range opts {
		flag := opt.Apply(stmt)
		if !flag {
			return false
		}
	}
	return true
}

func FilterOR(stmt Statement, opts ...[]FilterOption) bool {
	for _, opt := range opts {
		if FilterAll(stmt, opt...) {
			return true
		}
	}
	return false
}

//func IgnoreSQLHint(sqlHint string, ddlType []string) FilterOption {
//	ddlTypeMap := utils.StringSliceToMap(ddlType)
//	return &ignoreSQLHint{
//		ddlType: ddlTypeMap,
//		sqlHint: sqlHint,
//	}
//}
//
//type ignoreSQLHint struct {
//	ddlType map[string]struct{}
//	sqlHint string
//}
//
//// 命中 及 忽略
//func (t *ignoreSQLHint) Apply(stmt Statement) bool {
//	if !strings.Contains(stmt.SQLHint(), t.sqlHint) {
//		return true
//	}
//	_, ok := t.ddlType[stmt.DDLType().String()]
//	return !ok
//}
//
//func AcceptSQLHint(sqlHint string, ddlType []string) FilterOption {
//	ddlTypeMap := utils.StringSliceToMap(ddlType)
//	return &acceptSQLHint{
//		ddlType: ddlTypeMap,
//		sqlHint: sqlHint,
//	}
//}
//
//type acceptSQLHint struct {
//	ddlType map[string]struct{}
//	sqlHint string
//}

//// 命中 及 通过
//func (t *acceptSQLHint) Apply(stmt Statement) bool {
//	if !strings.Contains(stmt.SQLHint(), t.sqlHint) {
//		return false
//	}
//	_, ok := t.ddlType[stmt.DDLType().String()]
//	return ok
//}

func AcceptDDLType(ddlType []string) FilterOption {
	ddlTypeMap := transform.StringSliceToMap(ddlType)
	return &acceptDDLType{
		ddlType: ddlTypeMap,
	}
}

type acceptDDLType struct {
	ddlType map[string]struct{}
}

func (t *acceptDDLType) Apply(stmt Statement) bool {
	_, ok := t.ddlType[stmt.DDLType().String()]
	return ok
}

func AcceptDatabase(database string) FilterOption {
	return &acceptDatabaseOption{
		database: database,
	}
}

type acceptDatabaseOption struct {
	database string
}

func (o *acceptDatabaseOption) Apply(s Statement) bool {
	if ds, ok := s.(databaseStatement); ok {
		return ds.Database() == o.database
	}

	if ds, ok := s.(renameTableStatement); ok {

		oldDatabase, _ := ds.OldTable()
		newDatabase, _ := ds.NewTable()
		if oldDatabase != newDatabase {
			log.Warnf("不支持跨库rename操作，自动将其过滤 old database: %s, new database: %s", oldDatabase, newDatabase)
			return false
		}
		return oldDatabase == o.database
	}

	log.Warnf("unable to determine if Database %v is acceptable ddl type:%v", o.database, s.DDLType())
	return false
}

func MatchDatabase(databasePattern string) FilterOption {
	return &matchDatabaseOption{
		pattern: databasePattern,
	}
}

type matchDatabaseOption struct {
	pattern string
}

func (o *matchDatabaseOption) Apply(s Statement) bool {
	if ds, ok := s.(databaseStatement); ok {
		return match.Glob(o.pattern, ds.Database())
	}
	if ds, ok := s.(renameTableStatement); ok {
		database, _ := ds.OldTable()
		return match.Glob(o.pattern, database)
	}
	log.Warnf("unable to determine if Database pattern %v is acceptable ddl type:%v", o.pattern, s.DDLType())
	return false
}

func AcceptTable(database string, tables []string) FilterOption {
	tablesMap := transform.StringSliceToMap(tables)
	return &acceptTableOption{
		database: database,
		tables:   tablesMap,
	}
}

type acceptTableOption struct {
	database string
	tables   map[string]struct{}
}

func (o *acceptTableOption) Apply(s Statement) bool {
	if ds, ok := s.(tableStatement); ok {
		if ds.Database() != o.database {
			return false
		}
		if _, ok := o.tables[ds.Table()]; ok {
			return true
		}
		return false
	}

	if ds, ok := s.(renameTableStatement); ok {
		oldDatabase, oldTable := ds.OldTable()
		newDatabase, _ := ds.NewTable()
		if oldDatabase != newDatabase {
			log.Warnf("不支持跨库rename操作，自动将其过滤 old database: %s, new database: %s", oldDatabase, newDatabase)
			return false
		}
		if oldDatabase != o.database {
			return false
		}
		if _, ok := o.tables[oldTable]; ok {
			return true
		}
		return false
	}
	return false
}

type matchTableOption struct {
	database      string
	tablesPattern []string
}

func MatchTable(database string, tablesPattern []string) FilterOption {
	return &matchTableOption{
		database:      database,
		tablesPattern: tablesPattern,
	}
}

func (o *matchTableOption) Apply(s Statement) bool {
	if s.DDLType() == schema_store.CREATE_DATABASE || s.DDLType() == schema_store.DROP_DATABASE {
		ds := s.(databaseStatement)
		if ds.Database() == o.database {
			return true
		}
	}
	if ds, ok := s.(tableStatement); ok {
		if ds.Database() != o.database {
			return false
		}
		for _, pattern := range o.tablesPattern {
			if match.Glob(pattern, ds.Table()) {
				return true
			}
		}
		return false
	}

	if ds, ok := s.(renameTableStatement); ok {
		oldDatabase, oldTable := ds.OldTable()
		newDatabase, _ := ds.NewTable()
		if oldDatabase != newDatabase {
			log.Warnf("不支持跨库rename操作，自动将其过滤 old database: %s, new database: %s", oldDatabase, newDatabase)
			return false
		}
		if oldDatabase != o.database {
			return false
		}
		for _, pattern := range o.tablesPattern {
			if match.Glob(pattern, oldTable) {
				return true
			}
		}
		return false
	}
	return false
}

func IgnoreTable(database string, tablesPattern []string) FilterOption {
	return &ignoreTableOption{
		database:      database,
		tablesPattern: tablesPattern,
	}
}

type ignoreTableOption struct {
	database      string
	tablesPattern []string
}

func (o *ignoreTableOption) Apply(s Statement) bool {
	if s.DDLType() == schema_store.CREATE_DATABASE || s.DDLType() == schema_store.DROP_DATABASE {
		ds := s.(databaseStatement)
		if ds.Database() == o.database {
			return true
		}
	}

	if ds, ok := s.(tableStatement); ok {
		if ds.Database() != o.database {
			return false
		}
		for _, pattern := range o.tablesPattern {
			if match.Glob(pattern, ds.Table()) {
				return false
			}
		}
		return true
	}

	if ds, ok := s.(renameTableStatement); ok {
		oldDatabase, oldTable := ds.OldTable()
		newDatabase, _ := ds.NewTable()
		if oldDatabase != newDatabase {
			log.Warnf("不支持跨库rename操作，自动将其过滤 old database: %s, new database: %s", oldDatabase, newDatabase)
			return false
		}
		if oldDatabase != o.database {
			return false
		}
		for _, pattern := range o.tablesPattern {
			if match.Glob(pattern, oldTable) {
				return false
			}
		}
		return true
	}

	return false
}

//func FilterRemoveColumn(database, table, column string) FilterOption {
//	return &filterRemoveColumnOption{
//		database: database,
//		table:    table,
//		column:   column,
//	}
//}
//
//type filterRemoveColumnOption struct {
//	database string
//	table    string
//	column   string
//}
//
//func (o *filterRemoveColumnOption) Apply(s Statement) bool {
//	if ds, ok := s.(alterTableStatement); ok {
//		if ds.Database() == o.database && ds.Table() == o.table {
//			return ds.FilterRemoveColumn(o.column)
//		}
//	}
//	return false
//}
