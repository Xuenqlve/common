package mysql_schema

import (
	"github.com/xuenqlve/common/sql"
)

type ColumnType = int

type ColumnValueString struct {
	ValueString string `json:"value"`
	IsNull      bool   `json:"is_null"`
}

type Column struct {
	Name            string            `json:"name"`
	Type            ColumnType        `json:"type"`
	RawType         string            `json:"raw_type"`
	DefaultVal      ColumnValueString `json:"default_value_string"`
	IsNullable      bool              `json:"is_nullable"`
	IsUnsigned      bool              `json:"is_unsigned"`
	IsPrimaryKey    bool              `json:"is_primary_key"`
	IsGenerated     bool              `json:"is_generated"`
	DataType        string            `json:"data_type"`
	ColumnKey       string            `json:"column_key"`
	OrdinalPosition int               `json:"ordinal_position"`
}

const maxMediumintUnsigned int32 = 16777215

func Deserialize(raw any, column Column) any {
	if raw == nil {
		return nil
	}
	ret := raw
	if column.Type == TypeString || column.Type == TypeJson {
		_, ok := raw.([]uint8)
		if ok {
			ret = string(raw.([]uint8))
		}
	} else if column.IsUnsigned {
		// https://github.com/siddontang/go-mysql/issues/338
		// binlog itself doesn't specify whether it's signed or not
		switch t := raw.(type) {
		case int8:
			ret = uint8(t)
		case int16:
			ret = uint16(t)
		case int32:
			if column.Type == TypeMediumInt {
				// problem with mediumint is that it's a 3-byte type. There is no compatible golang type to match that.
				// So to convert from negative to positive we'd need to convert the value manually
				if t >= 0 {
					ret = uint32(t)
				} else {
					ret = uint32(maxMediumintUnsigned + t + 1)
				}
			} else {
				ret = uint32(t)
			}
		case int64:
			ret = uint64(t)
		case int:
			ret = uint(t)
		default:
			// nothing to do
		}
	} else if column.Type == TypeTimestamp || column.Type == TypeDatetime || column.Type == TypeDate {
		v, flag := ParseTime(ret, column.Type)
		if flag {
			return v
		}
	}
	return ret
}

func GenerateGuideKeys(guideColumns []string, rowData map[string]any) (map[string]any, string, error) {
	return sql.GenerateGuideKeys(guideColumns, rowData)
}

type DefaultStruct struct{}

func IsColumnSetDefault(v any) bool {
	_, ok := v.(DefaultStruct)
	return ok
}
