package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/log"
	"github.com/xuenqlve/common/schema_store"
	sql_tool "github.com/xuenqlve/common/sql"
)

type Schema struct {
	conn *sql.DB
}

func (s *Schema) LoadSchema(key schema_store.SchemaKey) (any, error) {
	index := key.(*Index)
	return s.GetTableDef(index.Database, index.Table)
}

func (s *Schema) GetTableDef(database, table string) (*Table, error) {
	var err error
	tableDef := &Table{
		Database:     database,
		Table:        table,
		Columns:      make([]Column, 0),
		PrimaryIndex: make([]string, 0),
		UniqueIndex:  map[string][]string{},
		ColumnMap:    map[string]Column{},
	}
	tableDef.UniqueIndex, err = s.getUniqueKeys(database, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableDef.PrimaryIndex, tableDef.Columns, tableDef.ColumnMap, err = s.getColumns(database, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	columnTypes := []*sql.ColumnType{}
	columnTypes, err = s.getColumnType(database, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableDef.SetColumnTypes(columnTypes)
	if err = tableDef.InitScanColumns(); err != nil {
		return nil, errors.Trace(err)
	}

	return tableDef, nil
}

func (s *Schema) GetCreateTableSQL(database, table string) (string, error) {
	row := s.conn.QueryRowContext(context.Background(), fmt.Sprintf("SHOW CREATE TABLE %s", sql_tool.GenerateTableName(database, table)))
	var tableName, createTable string
	err := row.Scan(&tableName, &createTable)
	if err != nil {
		log.Errorf("get create table sql failed err:%v", err)
		return "", err
	}
	return createTable, nil
}

func (s *Schema) getColumnType(database, table string) ([]*sql.ColumnType, error) {
	statement := fmt.Sprintf("SELECT * from %s LIMIT 1", sql_tool.GenerateTableName(database, table))
	rows, err := s.conn.QueryContext(context.Background(), statement)
	defer func() {
		if rows != nil {
			if closeErr := rows.Close(); closeErr != nil {
				log.Errorf("close rows error:%v", closeErr)
			}
		}
	}()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rows.ColumnTypes()
}

func (s *Schema) getColumns(database, table string) ([]string, []Column, map[string]Column, error) {
	var columnName string
	var rawType string
	var columnKey string
	var isNullableString string
	var defaultVal sql.NullString
	var extra sql.NullString

	var columns = make([]Column, 0)
	var columnsMap = map[string]Column{}
	var primaryKeyColumns = make([]string, 0)

	columnsMap, err := s.improveColumn(database, table)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	stmt := fmt.Sprintf("show columns from `%s`.`%s`", database, table)
	rows, err := s.conn.Query(stmt)
	if err != nil {
		return nil, nil, nil, errors.Annotatef(err, "error %s", stmt)
	}
	defer func() {
		if err = rows.Close(); err != nil {
			log.Errorf("error closing stmt:%s err:%v", stmt, err)
		}
	}()
	for rows.Next() {
		err = rows.Scan(&columnName, &rawType, &isNullableString, &columnKey, &defaultVal, &extra)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		column, ok := columnsMap[columnName]
		if !ok {
			column = Column{Name: columnName}
		}
		column.Type = ExtractColumnType(rawType)
		column.RawType = rawType
		column.DefaultVal.IsNull = !defaultVal.Valid
		column.DefaultVal.ValueString = defaultVal.String
		if isNullableString == "NO" {
			column.IsNullable = false
		} else {
			column.IsNullable = true
		}

		if strings.Contains(rawType, "unsigned") {
			column.IsUnsigned = true
		} else {
			column.IsUnsigned = false
		}

		if extra.Valid && (strings.Contains(strings.ToUpper(extra.String), "VIRTUAL GENERATED") ||
			strings.Contains(strings.ToUpper(extra.String), "STORED GENERATED")) {
			column.IsGenerated = true
		}

		if columnKey == "PRI" {
			primaryKeyColumns = append(primaryKeyColumns, column.Name)
			column.IsPrimaryKey = true
		} else {
			column.IsPrimaryKey = false
		}

		columns = append(columns, column)
		columnsMap[column.Name] = column
	}

	return primaryKeyColumns, columns, columnsMap, nil
}

func (s *Schema) improveColumn(database, table string) (map[string]Column, error) {
	var columnName, dataType, columnType, columnKey string
	var ordinalPosition int
	stmt := fmt.Sprintf("select column_name,data_type,column_type,column_key,ordinal_position from information_schema.columns where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' order by ordinal_position asc ", database, table)
	rows, err := s.conn.Query(stmt)
	if err != nil {
		return nil, errors.Annotatef(err, "error %s", stmt)
	}
	defer rows.Close()
	result := map[string]Column{}
	for rows.Next() {
		err = rows.Scan(&columnName, &dataType, &columnType, &columnKey, &ordinalPosition)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result[columnName] = Column{
			Name:            columnName,
			DataType:        dataType,
			ColumnKey:       columnKey,
			OrdinalPosition: ordinalPosition,
		}
	}
	return result, nil
}

func (s *Schema) getUniqueKeys(database, table string) (map[string][]string, error) {
	rows, err := s.conn.Query(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s` WHERE Non_unique = 0", database, table))
	defer func() {
		if rows != nil {
			if rowsErr := rows.Close(); err != nil {
				log.Errorf("get unique keys rows close error: %s", rowsErr.Error())
			}
		}

	}()
	if err != nil {
		return nil, errors.Trace(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}
	var resultRows [][]sql.NullString
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err = rows.Scan(scanArgs...); err != nil {
			return nil, errors.Trace(err)
		}
		resultRows = append(resultRows, values)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	uniqueKeyMap := map[string][]string{}
	for _, value := range resultRows {
		keyName := value[2].String
		columnName := value[4].String
		if uColumns, ok := uniqueKeyMap[keyName]; ok {
			uniqueKeyMap[keyName] = append(uColumns, columnName)
		} else {
			uniqueKeyMap[keyName] = []string{columnName}
		}
	}
	return uniqueKeyMap, nil
}

func (s *Schema) Close() error {
	return s.conn.Close()
}

func NewSchema(conn *sql.DB) *Schema {
	return &Schema{
		conn: conn,
	}
}
