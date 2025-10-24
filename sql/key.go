package sql

import (
	"fmt"
	"strings"

	"github.com/xuenqlve/common/errors"
)

func GenerateTableName(database, table string) string {
	return UniqueID(database, table)
}

func UniqueID(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

func ColumnName(column string) string {
	return fmt.Sprintf("`%s`", escapeName(column))
}

func ScanKey(scanColumns []string, rows map[string]any) string {
	keys := []string{}
	for _, column := range scanColumns {
		if pk, ok := rows[column]; ok {
			keys = append(keys, fmt.Sprintf("%s.%v", column, pk))
		}
	}
	return strings.Join(keys, "-")
}

const (
	MinPrefix = "min"
	MaxPrefix = "max"
)

func MinMaxScanKey(scanColumns []string, min, max map[string]any) string {
	minKeys, maxKeys := make([]string, 0, len(scanColumns)), make([]string, 0, len(scanColumns))
	for _, column := range scanColumns {
		if value, ok := min[column]; ok {
			minKeys = append(minKeys, fmt.Sprintf("%s.%v", column, value))
		}
		if value, ok := max[column]; ok {
			maxKeys = append(maxKeys, fmt.Sprintf("%s.%v", column, value))
		}
	}
	return fmt.Sprintf("%s:%s-%s:%s", MinPrefix, strings.Join(minKeys, "-"), MaxPrefix, strings.Join(maxKeys, "-"))
}

func GenerateGuideKeys(guideColumns []string, rowData map[string]any) (map[string]any, string, error) {
	pks := make(map[string]any)
	pksList := make([]string, 0, len(guideColumns))
	for i := 0; i < len(guideColumns); i++ {
		pkName := guideColumns[i]
		pks[pkName] = rowData[pkName]
		if pks[pkName] == nil {
			return nil, "", errors.Errorf("primary key nil, pkName: %v, data: %v", pkName, rowData)
		}
		pksList = append(pksList, fmt.Sprintf("%s.%v", pkName, pks[pkName]))
	}
	return pks, strings.Join(pksList, "-"), nil
}
