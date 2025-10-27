package mysql

import "fmt"

type RowData struct {
	Key       string         `json:"key" c:"key"`
	Data      map[string]any `json:"data" c:"data"`
	Old       map[string]any `json:"old" c:"old,omitempty"`
	GuideKeys map[string]any `json:"guide_keys" c:"guide_keys"`
}

func MakeRowKey(database, table, scanKeys string) string {
	return fmt.Sprintf("%s.%s.%s", database, table, scanKeys)
}

type defaultStruct struct{}

func (d *RowData) IsColumnSetDefault(columnName string) bool {
	data, ok := d.Data[columnName]
	if !ok {
		return false
	}
	_, ok = data.(defaultStruct)
	return ok
}

func (d *RowData) SetColumnDefault(column string) {
	d.Data[column] = defaultStruct{}
}
