package mysql

import (
	"strings"
	"time"

	"github.com/xuenqlve/common/sql"
)

const (
	TypeNumber    ColumnType = iota + 1 // tinyint, smallint, int, bigint, year
	TypeMediumInt                       // medium int
	TypeFloat                           // float
	TypeDouble                          // double
	TypeEnum                            // enum
	TypeSet                             // set
	TypeString                          // other
	TypeDatetime                        // datetime
	TypeTimestamp                       // timestamp
	TypeDate                            // date
	TypeTime                            // time
	TypeBit                             // bit
	TypeJson                            // json
	TypeDecimal                         // decimal
)

func ExtractColumnType(rawType string) ColumnType {
	if strings.HasPrefix(rawType, "float") {
		return TypeFloat
	} else if strings.HasPrefix(rawType, "double") {
		return TypeDouble
	} else if strings.HasPrefix(rawType, "decimal") {
		return TypeDecimal
	} else if strings.HasPrefix(rawType, "enum") {
		return TypeEnum
	} else if strings.HasPrefix(rawType, "set") {
		return TypeSet
	} else if strings.HasPrefix(rawType, "datetime") {
		return TypeDatetime
	} else if strings.HasPrefix(rawType, "timestamp") {
		return TypeTimestamp
	} else if strings.HasPrefix(rawType, "time") {
		return TypeTime
	} else if "date" == rawType {
		return TypeDate
	} else if strings.HasPrefix(rawType, "bit") {
		return TypeBit
	} else if strings.HasPrefix(rawType, "json") {
		return TypeJson
	} else if strings.Contains(rawType, "mediumint") {
		return TypeMediumInt
	} else if strings.Contains(rawType, "int") || strings.HasPrefix(rawType, "year") {
		return TypeNumber
	} else {
		return TypeString
	}
}

func IsNumberType(tp ColumnType) bool {
	switch tp {
	case TypeNumber, TypeMediumInt:
		return true
	default:
		return false
	}
}

func IsFloatType(tp ColumnType) bool {
	switch tp {
	case TypeFloat, TypeDouble, TypeDecimal:
		return true
	default:
		return false
	}
}

var columnTypeMap = map[ColumnType]string{
	TypeDate:      sql.DATE,
	TypeDatetime:  sql.DATETIME,
	TypeTimestamp: sql.TIMESTAMP,
}

func ParseTime(timeStr any, columnType ColumnType) (time.Time, bool) {
	tStr := ""
	switch t := timeStr.(type) {
	case time.Time:
		return t, true
	case string:
		tStr = t
	default:
		return time.Time{}, false
	}
	ti, err := sql.ParseTime(columnTypeMap[columnType], tStr)
	if err != nil {
		return time.Time{}, false
	}
	return ti, true
}
