package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/xuenqlve/common/errors"
	sql_tool "github.com/xuenqlve/common/sql"
)

type Index struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

func (i *Index) UniqueID() string {
	return sql_tool.UniqueID(i.Database, i.Table)
}

type Tables []Table

type Table struct {
	Database     string
	Table        string
	Columns      []Column
	ColumnMap    map[string]Column
	PrimaryIndex []string
	UniqueIndex  map[string][]string

	columnTypes    []*sql.ColumnType
	scanCondition  string
	scanColumns    []string
	once           sync.Once
	createTableSql string
}

func (t *Table) Schema() (database string, table string) {
	return t.Database, t.Table
}

func (t *Table) ScanColumns() []string {
	return t.scanColumns
}

func (t *Table) SetScanColumns(scanColumns []string) {
	t.scanColumns = scanColumns
}

func (t *Table) ColumnTypes() []*sql.ColumnType {
	return t.columnTypes
}

func (t *Table) SetColumnTypes(ct []*sql.ColumnType) {
	t.columnTypes = ct
}

func (t *Table) GenerateTableName() string {
	return sql_tool.GenerateTableName(t.Database, t.Table)
}

func (t *Table) CreateTableSql() string {
	return t.createTableSql
}

func (t *Table) SetCreateTableSql(createTableSql string) {
	t.createTableSql = createTableSql
}

func (t *Table) ScanCondition() string {
	return t.scanCondition
}

func (t *Table) SetScanCondition(scanCondition string) {
	t.scanCondition = scanCondition
}

func (t *Table) ScanIndexes() (scanIndexes map[string]int, err error) {
	scanIndexes = map[string]int{}
	columnIndexMap := map[string]int{}
	for i := range t.Columns {
		columnIndexMap[t.Columns[i].Name] = i
	}
	for _, column := range t.ScanColumns() {
		if index, ok := columnIndexMap[column]; ok {
			scanIndexes[column] = index
		} else {
			err = errors.Errorf("%s cannot find column:%s scan index", t.UniqueIndex, column)
			return
		}
	}
	return scanIndexes, nil
}

func (t *Table) Index() Index {
	return Index{
		Database: t.Database,
		Table:    t.Table,
	}
}

func (t *Table) Column(col string) (c Column, ok bool) {
	t.once.Do(func() {
		if t.ColumnMap == nil {
			t.ColumnMap = make(map[string]Column)
			for _, c := range t.Columns {
				t.ColumnMap[c.Name] = c
			}
		}
	})
	c, ok = t.ColumnMap[col]
	return
}

func (t *Table) InitScanColumns() error {
	if len(t.PrimaryIndex) != 0 {
		t.scanColumns = t.PrimaryIndex
	} else if len(t.UniqueIndex) != 0 {
		scanColumn := []string{}
		for _, v := range t.UniqueIndex {
			if len(scanColumn) == 0 || len(scanColumn) > len(v) {
				scanColumn = v
			}
		}
		t.scanColumns = scanColumn
	} else {
		return errors.Errorf("no scan column can be found automatically for %s.%s", t.Database, t.Table)
	}
	return nil
}

func GetTablesByDatabases(conn *sql.DB, database []string) (map[string][]string, error) {
	allSchema := make(map[string][]string)
	sql := fmt.Sprintf("SELECT distinct TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA in ('%s') AND TABLE_TYPE = 'BASE TABLE';", strings.Join(database, "','"))
	rows, err := conn.Query(sql)
	if err != nil {
		err = fmt.Errorf("failed to get schema_store and table names, err %v", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	for rows.Next() {
		var schemaName, tableName string
		err = rows.Scan(&schemaName, &tableName)
		if err != nil {
			err = fmt.Errorf("failed to scan, err: %v", errors.Trace(err))
			return nil, errors.Trace(err)
		}
		if _, ok := allSchema[schemaName]; !ok {
			allSchema[schemaName] = []string{}
		}
		allSchema[schemaName] = append(allSchema[schemaName], tableName)

	}
	if err = rows.Err(); err != nil {
		err = fmt.Errorf("[scanner_server] query error: %s", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	if err = rows.Close(); err != nil {
		err = fmt.Errorf("[scanner_server] query error: %s", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	return allSchema, nil
}

func GetTablesByDatabase(conn *sql.DB, database string) ([]string, error) {
	tables := []string{}
	rows, err := conn.Query("SELECT distinct TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE';", database)
	if err != nil {
		err = fmt.Errorf("failed to get schema_store and table names, err %v", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			err = fmt.Errorf("failed to scan, err: %v", errors.Trace(err))
			return nil, errors.Trace(err)
		}
		tables = append(tables, tableName)
	}
	if err = rows.Err(); err != nil {
		err = fmt.Errorf("[scanner_server] query error: %s", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	if err = rows.Close(); err != nil {
		err = fmt.Errorf("[scanner_server] query error: %s", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	return tables, nil
}
