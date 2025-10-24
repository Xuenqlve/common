package where

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func whereFilter(src map[string]interface{}, where string) bool {
	if where == "" {
		return true
	}

	// 支持的操作符，按长度排序以优先匹配长操作符
	operators := []string{"<=", ">=", "==", "!=", "<", ">"}

	// 查找操作符
	var operator string
	var operatorIndex int = -1

	for _, op := range operators {
		index := strings.Index(where, op)
		if index != -1 {
			operator = op
			operatorIndex = index
			break
		}
	}

	if operatorIndex == -1 {
		return false // 没有找到有效的操作符
	}

	// 提取字段名和值
	fieldName := strings.TrimSpace(where[:operatorIndex])
	valueStr := strings.TrimSpace(where[operatorIndex+len(operator):])

	// 从 src 中获取字段值
	fieldValue, exists := src[fieldName]
	if !exists {
		return false // 字段不存在
	}

	// 解析比较值
	compareValue, err := parseValue(valueStr)
	if err != nil {
		return false
	}

	// 执行比较
	return compare(fieldValue, operator, compareValue)
}

// parseValue 解析字符串值为适当的类型
func parseValue(valueStr string) (interface{}, error) {
	// 去除引号（如果有）
	valueStr = strings.Trim(valueStr, `"'`)

	// 尝试解析为数字
	if intVal, err := strconv.Atoi(valueStr); err == nil {
		return intVal, nil
	}

	if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return floatVal, nil
	}

	// 尝试解析为时间
	if timeVal, err := parseTimeValue(valueStr); err == nil {
		return timeVal, nil
	}

	// 解析布尔值
	if boolVal, err := strconv.ParseBool(valueStr); err == nil {
		return boolVal, nil
	}

	// 默认作为字符串处理
	return valueStr, nil
}

// compare 执行比较操作
func compare(fieldValue interface{}, operator string, compareValue interface{}) bool {
	// 将值转换为可比较的类型
	fVal := convertToComparable(fieldValue)
	cVal := convertToComparable(compareValue)

	// 处理nil值的特殊情况（数据库NULL字段）
	if fVal == nil || cVal == nil {
		switch operator {
		case "==":
			return fVal == cVal // 都为nil时返回true，否则false
		case "!=":
			return fVal != cVal // 都为nil时返回false，否则true
		default:
			// 对于比较操作符（<, <=, >, >=），如果有一个值为nil，返回false
			return false
		}
	}

	// 特殊处理时间类型（即使类型匹配也需要特殊处理）
	if isTimeType(fVal) && isTimeType(cVal) {
		return compareTimeValues(fVal, operator, cVal)
	}

	// 类型不匹配时的处理
	if reflect.TypeOf(fVal) != reflect.TypeOf(cVal) {
		// 特殊处理时间类型
		if isTimeType(fVal) || isTimeType(cVal) {
			return compareTimeValues(fVal, operator, cVal)
		}
		// 尝试将两个值都转换为字符串进行比较
		fStr := fmt.Sprintf("%v", fVal)
		cStr := fmt.Sprintf("%v", cVal)
		return compareStrings(fStr, operator, cStr)
	}

	switch operator {
	case "==":
		return fVal == cVal
	case "!=":
		return fVal != cVal
	case "<":
		return compareValues(fVal, cVal) < 0
	case "<=":
		return compareValues(fVal, cVal) <= 0
	case ">":
		return compareValues(fVal, cVal) > 0
	case ">=":
		return compareValues(fVal, cVal) >= 0
	default:
		return false
	}
}

// convertToComparable 将接口值转换为可比较的具体类型
// 针对DataPtrsVal方法的输出类型进行优化处理
func convertToComparable(value interface{}) interface{} {
	// 处理nil值（数据库NULL字段）
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	// DataPtrsVal直接返回的整数类型
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		// 注意：uint64可能超出int64范围，但为了统一比较，还是转换
		return int64(v)
	case int:
		return int64(v)
	// DataPtrsVal直接返回的浮点类型
	case float32:
		return float64(v)
	case float64:
		return v
	// DataPtrsVal直接返回的其他类型
	case string:
		return v
	case bool:
		return v
	case time.Time:
		return v
	// DataPtrsVal从sql.RawBytes转换的[]byte类型
	case []byte:
		// 将[]byte转换为字符串进行比较
		return string(v)
	default:
		// 对于其他未知类型，转换为字符串
		return fmt.Sprintf("%v", v)
	}
}

// compareValues 比较两个相同类型的值，返回 -1, 0, 1
func compareValues(a, b interface{}) int {
	switch va := a.(type) {
	case int64:
		vb := b.(int64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case float64:
		vb := b.(float64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		vb := b.(string)
		return strings.Compare(va, vb)
	case bool:
		vb := b.(bool)
		if va == vb {
			return 0
		} else if !va && vb {
			return -1
		}
		return 1
	case time.Time:
		vb := b.(time.Time)
		if va.Before(vb) {
			return -1
		} else if va.After(vb) {
			return 1
		}
		return 0
	default:
		return strings.Compare(fmt.Sprintf("%v", va), fmt.Sprintf("%v", b))
	}
}

// compareStrings 字符串比较
func compareStrings(a, operator, b string) bool {
	switch operator {
	case "==":
		return a == b
	case "!=":
		return a != b
	case "<":
		return strings.Compare(a, b) < 0
	case "<=":
		return strings.Compare(a, b) <= 0
	case ">":
		return strings.Compare(a, b) > 0
	case ">=":
		return strings.Compare(a, b) >= 0
	default:
		return false
	}
}

// parseTimeValue 解析时间字符串，支持多种常见格式
func parseTimeValue(timeStr string) (time.Time, error) {
	// 支持的时间格式列表，按常用程度排序
	timeFormats := []string{
		"2006-01-02 15:04:05",  // 2025-07-10 10:00:00
		"2006-01-02 15:04",     // 2025-07-10 10:00
		"2006-01-02",           // 2025-07-10
		"2006/01/02 15:04:05",  // 2025/07/10 10:00:00
		"2006/01/02 15:04",     // 2025/07/10 10:00
		"2006/01/02",           // 2025/07/10
		"01/02/2006 15:04:05",  // 07/10/2025 10:00:00
		"01/02/2006 15:04",     // 07/10/2025 10:00
		"01/02/2006",           // 07/10/2025
		"2006-01-02T15:04:05Z", // ISO 8601
		"2006-01-02T15:04:05",  // ISO 8601 without timezone
		time.RFC3339,           // RFC3339
		time.RFC3339Nano,       // RFC3339 with nanoseconds
	}

	for _, format := range timeFormats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("无法解析时间格式: %s", timeStr)
}

// isTimeType 检查值是否为时间类型
func isTimeType(value interface{}) bool {
	_, ok := value.(time.Time)
	return ok
}

// compareTimeValues 比较时间值，支持不同类型之间的转换
func compareTimeValues(a interface{}, operator string, b interface{}) bool {
	var timeA, timeB time.Time
	var err error

	// 转换 a 为时间
	if t, ok := a.(time.Time); ok {
		timeA = t
	} else {
		timeA, err = parseTimeValue(fmt.Sprintf("%v", a))
		if err != nil {
			return false
		}
	}

	// 转换 b 为时间
	if t, ok := b.(time.Time); ok {
		timeB = t
	} else {
		timeB, err = parseTimeValue(fmt.Sprintf("%v", b))
		if err != nil {
			return false
		}
	}

	// 执行比较
	switch operator {
	case "==":
		// 对于日期比较，如果其中一个只有日期信息，则只比较日期部分
		if hasOnlyDate(timeA) || hasOnlyDate(timeB) {
			return timeA.Format("2006-01-02") == timeB.Format("2006-01-02")
		}
		return timeA.Equal(timeB)
	case "!=":
		return !timeA.Equal(timeB)
	case "<":
		return timeA.Before(timeB)
	case "<=":
		return timeA.Before(timeB) || timeA.Equal(timeB)
	case ">":
		return timeA.After(timeB)
	case ">=":
		return timeA.After(timeB) || timeA.Equal(timeB)
	default:
		return false
	}
}

// hasOnlyDate 检查时间是否只包含日期信息（年月日）
func hasOnlyDate(t time.Time) bool {
	return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}
