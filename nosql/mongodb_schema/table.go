package mongodb_schema

import (
	"fmt"

	"github.com/xuenqlve/common/errors"
	sql_tool "github.com/xuenqlve/common/sql"
	"go.mongodb.org/mongo-driver/bson"
)

type Index struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

func (i *Index) UniqueID() string {
	return sql_tool.UniqueID(i.Database, i.Table)
}

type Table struct {
	Database      string
	Table         string
	PrimaryIndex  []string
	UniqueIndex   map[string][]string
	scanColumns   []string
	scanCondition string
	indexes       []bson.D
}

func (t *Table) Schema() (database string, table string) {
	return t.Database, t.Table
}

func (t *Table) ScanColumns() []string {
	return t.scanColumns
}

func (t *Table) Ns() string {
	return fmt.Sprintf("%s.%s", t.Database, t.Table)
}

func (t *Table) Indexes() []bson.D {
	return t.indexes
}

func (t *Table) SetScanColumns(scanColumns []string) {
	t.scanColumns = scanColumns
}

func (t *Table) ScanCondition() string {
	return t.scanCondition
}

func (t *Table) SetScanCondition(scanCondition string) {
	t.scanCondition = scanCondition
}

func (t *Table) KeyPattern() bson.M {
	options := bson.M{}
	for _, col := range t.ScanColumns() {
		options[col] = 1
	}
	return options
}

func (t *Table) GenerateTableName() string {
	return sql_tool.GenerateTableName(t.Database, t.Table)
}

func (t *Table) SortOptions(desc bool) map[string]any {
	options := map[string]any{}
	sort := 1
	if desc {
		sort = -1
	}
	for _, col := range t.ScanColumns() {
		options[col] = sort
	}
	return options
}

func (t *Table) Index() Index {
	return Index{
		Database: t.Database,
		Table:    t.Table,
	}
}

func (t *Table) InitScanColumns() error {
	if len(t.PrimaryIndex) != 0 {
		t.SetScanColumns(t.PrimaryIndex)
	} else if len(t.UniqueIndex) != 0 {
		scanColumn := []string{}
		for _, v := range t.UniqueIndex {
			if len(scanColumn) == 0 || len(scanColumn) > len(v) {
				scanColumn = v
			}
		}
		t.SetScanColumns(scanColumn)
	} else {
		return errors.Errorf("no scan column can be found automatically for %s.%s", t.Database, t.Table)
	}
	return nil
}
