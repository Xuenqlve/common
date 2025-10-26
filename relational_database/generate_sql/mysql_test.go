package generate_sql

import (
	"testing"

	"github.com/xuenqlve/common/relational_database/mysql"
)

func mockTable() *mysql.Table {
	id := "id"
	name := "name"
	age := "age"
	createTime := "create_time"

	idColumn := mysql.Column{
		Name:     id,
		DataType: "int",
	}
	nameColumn := mysql.Column{
		Name:     name,
		DataType: "varchar",
	}
	ageColumn := mysql.Column{
		Name:     age,
		DataType: "int",
	}
	timeColumn := mysql.Column{
		Name:     createTime,
		DataType: "datetime",
	}

	tableDef := &mysql.Table{
		Database: "test",
		Table:    "test",
		Columns: []mysql.Column{
			idColumn, nameColumn, ageColumn, timeColumn,
		},
		ColumnMap: map[string]mysql.Column{
			id:         idColumn,
			name:       nameColumn,
			age:        ageColumn,
			createTime: timeColumn,
		},
		PrimaryIndex: []string{id},
		UniqueIndex: map[string][]string{
			"idx_name": {name},
		},
	}
	tableDef.SetScanColumns([]string{id})
	return tableDef
}

func TestGenerateSQL(t *testing.T) {
	tableDef := mockTable()
	t.Run("Complex Delete", func(t *testing.T) {
		tableDef.SetScanColumns([]string{"id", "name"})
		sql, args, err := GenerateDeleteSQL([]mysql.RowData{
			{GuideKeys: map[string]any{"id": 1, "name": "xxx1"}},
			{GuideKeys: map[string]any{"id": 2, "name": "xxx2"}},
			{GuideKeys: map[string]any{"id": 3, "name": "xxx3"}},
			{GuideKeys: map[string]any{"id": 4, "name": "xxx4"}},
		}, tableDef)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("sql:%v args:%v", sql, args)
	})
	t.Run("Single Delete", func(t *testing.T) {

		sql, args, err := GenerateDeleteSQL([]mysql.RowData{
			{GuideKeys: map[string]any{"id": 1}},
			{GuideKeys: map[string]any{"id": 2}},
			{GuideKeys: map[string]any{"id": 3}},
			{GuideKeys: map[string]any{"id": 4}},
		}, tableDef)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("sql:%v args:%v", sql, args)
	})

	t.Run("insert", func(t *testing.T) {
		sql, args, err := GenerateInsertSQL([]mysql.RowData{
			{GuideKeys: map[string]any{"id": 1}, Data: map[string]any{"id": 1, "name": "xx1", "age": 1, "create_time": "2022-01-01T00:00:00Z"}},
			{GuideKeys: map[string]any{"id": 2}, Data: map[string]any{"id": 2, "name": "xx2", "age": 2, "create_time": "2022-01-01T00:00:00Z"}},
			{GuideKeys: map[string]any{"id": 3}, Data: map[string]any{"id": 3, "name": "xx3", "age": 3, "create_time": "2022-01-01T00:00:00Z"}},
			{GuideKeys: map[string]any{"id": 4}, Data: map[string]any{"id": 4, "name": "xx4", "age": 4, "create_time": "2022-01-01T00:00:00Z"}},
		}, tableDef)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("sql:%v args:%v", sql, args)
	})

	t.Run("inset section", func(t *testing.T) {
		sql, args, err := GenerateInsertSectionSQL([]mysql.RowData{
			{GuideKeys: map[string]any{"id": 1}, Data: map[string]any{"id": 1, "name": "xx1", "age": 1}},
			{GuideKeys: map[string]any{"id": 2}, Data: map[string]any{"id": 2, "name": "xx2", "age": 2}},
			{GuideKeys: map[string]any{"id": 3}, Data: map[string]any{"id": 3, "name": "xx3", "age": 3}},
			{GuideKeys: map[string]any{"id": 4}, Data: map[string]any{"id": 4, "name": "xx4", "age": 4}},
		}, tableDef)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("sql:%v args:%v", sql, args)
	})

	t.Run("insert ignore", func(t *testing.T) {
		sql, args, err := GenerateInsertIgnoreSQL([]mysql.RowData{
			{GuideKeys: map[string]any{"id": 1}, Data: map[string]any{"id": 1, "name": "xx1", "age": 1, "create_time": "2022-01-01T00:00:00Z"}},
			{GuideKeys: map[string]any{"id": 2}, Data: map[string]any{"id": 2, "name": "xx2", "age": 2, "create_time": "2022-01-01T00:00:00Z"}},
			{GuideKeys: map[string]any{"id": 3}, Data: map[string]any{"id": 3, "name": "xx3", "age": 3, "create_time": "2022-01-01T00:00:00Z"}},
			{GuideKeys: map[string]any{"id": 4}, Data: map[string]any{"id": 4, "name": "xx4", "age": 4, "create_time": "2022-01-01T00:00:00Z"}},
		}, tableDef)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("sql:%v args:%v", sql, args)
	})

	t.Run("update", func(t *testing.T) {
		GenerateInsertOnDuplicateKeyUpdateSQL()
	})
}
