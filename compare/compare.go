package compare

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/xuenqlve/common/errors"
)

const (
	Greater = 1
	Less    = -1
	Equal   = 0
)

// Compare 比较两个值的大小，返回比较结果和可能的错误
// 如果 left > right 返回 Greater
// 如果 left == right 返回 Equal
// 如果 left < right 返回 Less

func Compare(left, right any) (int, error) {
	// 处理nil值
	if left == nil && right == nil {
		return Equal, nil
	}
	if left == nil {
		return Less, nil
	}
	if right == nil {
		return Greater, nil
	}

	// 直接比较相同类型，避免不必要的类型转换
	switch lv := left.(type) {
	case uint:
		if rv, ok := right.(uint); ok {
			return compareOrdered(lv, rv), nil
		}
	case uint8:
		if rv, ok := right.(uint8); ok {
			return compareOrdered(lv, rv), nil
		}
	case uint16:
		if rv, ok := right.(uint16); ok {
			return compareOrdered(lv, rv), nil
		}
	case uint32:
		if rv, ok := right.(uint32); ok {
			return compareOrdered(lv, rv), nil
		}
	case uint64:
		if rv, ok := right.(uint64); ok {
			return compareOrdered(lv, rv), nil
		}
	case int:
		if rv, ok := right.(int); ok {
			return compareOrdered(lv, rv), nil
		}
	case int8:
		if rv, ok := right.(int8); ok {
			return compareOrdered(lv, rv), nil
		}
	case int16:
		if rv, ok := right.(int16); ok {
			return compareOrdered(lv, rv), nil
		}
	case int32:
		if rv, ok := right.(int32); ok {
			return compareOrdered(lv, rv), nil
		}
	case int64:
		if rv, ok := right.(int64); ok {
			return compareOrdered(lv, rv), nil
		}
	case float32:
		if rv, ok := right.(float32); ok {
			return compareOrdered(lv, rv), nil
		}
	case float64:
		if rv, ok := right.(float64); ok {
			return compareOrdered(lv, rv), nil
		}
	case string:
		if rv, ok := right.(string); ok {
			return strings.Compare(lv, rv), nil
		}
	case []byte:
		if rv, ok := right.([]byte); ok {
			return compareBytes(lv, rv), nil
		}
	case bool:
		if rv, ok := right.(bool); ok {
			return compareBool(lv, rv), nil
		}
	case time.Time:
		if rv, ok := right.(time.Time); ok {
			return compareTime(lv, rv), nil
		}
	}

	// 尝试进行数值类型之间的转换比较
	leftNum, leftIsNum := toFloat64(left)
	rightNum, rightIsNum := toFloat64(right)
	if leftIsNum && rightIsNum {
		return compareOrdered(leftNum, rightNum), nil
	}

	// 尝试转换为通用类型后比较
	left = convertType(left)
	right = convertType(right)
	aType := reflect.TypeOf(left)
	bType := reflect.TypeOf(right)
	if aType != bType {
		return 0, errors.Errorf("[CompareKey] type error left:%v type %v , right:%v type %v", left, aType, right, bType)
	}

	// 按照通用类型比较
	switch v := left.(type) {
	case uint64:
		rightInt, ok := right.(uint64)
		if !ok {
			return 0, errors.Errorf("right.(uint64) error type:%s", reflect.TypeOf(right))
		}
		return CompareUInt(v, rightInt), nil
	case int64:
		rightInt, ok := right.(int64)
		if !ok {
			return 0, errors.Errorf("right.(int64) error type:%s", reflect.TypeOf(right))
		}
		return CompareInt(v, rightInt), nil
	case string:
		return strings.Compare(v, right.(string)), nil
	default:
		// 最后尝试使用反射比较
		return compareWithReflect(left, right)
	}
}

func CompareKey(left, right any) (int, error) {
	left = convertType(left)
	right = convertType(right)
	aType := reflect.TypeOf(left)
	bType := reflect.TypeOf(right)
	if aType != bType {
		return 0, errors.Errorf("[CompareKey] type error left:%v type %v , right:%v type %v", left, aType, right, bType)
	}
	switch v := left.(type) {
	case uint64:
		rightInt, ok := right.(uint64)
		if !ok {
			return 0, errors.Errorf("right.(int) error type:%s", reflect.TypeOf(right))
		}
		return CompareUInt(v, rightInt), nil
	case int64:
		rightInt, ok := right.(int64)
		if !ok {
			return 0, errors.Errorf("right.(int) error type:%s", reflect.TypeOf(right))
		}
		return CompareInt(v, rightInt), nil
	case string:
		return strings.Compare(v, right.(string)), nil
	case float64:
		rightFloat, ok := right.(float64)
		if !ok {
			return 0, errors.Errorf("right.(float64) error type:%s", reflect.TypeOf(right))
		}
		return CompareFloat(v, rightFloat), nil
	case bool:
		rightBool, ok := right.(bool)
		if !ok {
			return 0, errors.Errorf("right.(bool) error type:%s", reflect.TypeOf(right))
		}
		return CompareBool(v, rightBool), nil
	default:
		return 0, errors.Errorf("不支持的类型 left Type:%s right Type:%s", reflect.TypeOf(left), reflect.TypeOf(right))
	}
}

// compareOrdered 比较可排序的值
func compareOrdered[T ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64](a, b T) int {
	if a > b {
		return Greater
	} else if a < b {
		return Less
	}
	return Equal
}

// compareBytes 比较字节数组
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return Less
			}
			return Greater
		}
	}

	if len(a) < len(b) {
		return Less
	} else if len(a) > len(b) {
		return Greater
	}
	return Equal
}

// compareBool 比较布尔值
func compareBool(a, b bool) int {
	if a == b {
		return Equal
	}
	if a {
		return Greater
	}
	return Less
}

// compareTime 比较时间
func compareTime(a, b time.Time) int {
	if a.Equal(b) {
		return Equal
	} else if a.Before(b) {
		return Less
	}
	return Greater
}

// toFloat64 尝试将值转换为float64
func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// compareWithReflect 使用反射比较复杂类型
func compareWithReflect(left, right any) (int, error) {
	// 转换为字符串进行比较
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	return strings.Compare(leftStr, rightStr), nil
}

func CompareInt(a, b int64) int {
	if a > b {
		return Greater
	} else if a == b {
		return Equal
	} else {
		return Less
	}
}

func CompareUInt(a, b uint64) int {
	if a > b {
		return Greater
	} else if a == b {
		return Equal
	} else {
		return Less
	}
}

func convertType(a interface{}) interface{} {
	// TODO 防止传入负整数
	switch v := a.(type) {
	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	// 处理 SQL null 类型
	case sql.NullString:
		if v.Valid {
			return v.String
		}
		return ""
	case sql.NullInt64:
		if v.Valid {
			return uint64(v.Int64)
		}
		return uint64(0)
	case sql.NullInt32:
		if v.Valid {
			return uint64(v.Int32)
		}
		return uint64(0)
	case sql.NullInt16:
		if v.Valid {
			return uint64(v.Int16)
		}
		return uint64(0)
	case sql.NullByte:
		if v.Valid {
			return uint64(v.Byte)
		}
		return uint64(0)
	case sql.NullFloat64:
		if v.Valid {
			return v.Float64
		}
		return float64(0)
	case sql.NullBool:
		if v.Valid {
			return v.Bool
		}
		return false
	default:
		return a
	}
}

func CompareFloat(a, b float64) int {
	if a > b {
		return Greater
	} else if a == b {
		return Equal
	} else {
		return Less
	}
}

func CompareBool(a, b bool) int {
	if a == b {
		return Equal
	} else if a {
		return Greater // true > false
	} else {
		return Less
	}
}
