package transform

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

func ToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return parsed, nil
		}
		// 支持 'true'/'false' 字符串
		if v == "true" {
			return 1, nil
		} else if v == "false" {
			return 0, nil
		}
		return 0, err
	case []byte:
		return ToFloat64(string(v))
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// ToInt 支持 MySQL/PG/SQLServer 常见 data_type: TINYINT, SMALLINT, INT, BIGINT, DECIMAL, NUMERIC, BIT, BOOLEAN, CHAR, VARCHAR, TEXT, DATE, TIME, TIMESTAMP, YEAR, ...
func ToInt(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		const maxInt64 = int64(^uint64(0) >> 1)
		if v > uint64(maxInt64) {
			return 0, fmt.Errorf("uint64 value %d overflows int64", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		// 支持 'true'/'false' 字符串
		if v == "true" {
			return 1, nil
		} else if v == "false" {
			return 0, nil
		}
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f), nil
		}
		return 0, fmt.Errorf("cannot convert string '%s' to int64", v)
	case []byte:
		return ToInt(string(v))
	case time.Time:
		// 返回Unix时间戳
		return v.Unix(), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

func ToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%v", v), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case []byte:
		return string(v), nil
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

func ToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		iv, err := ToInt(v)
		if err != nil {
			return false, err
		}
		return iv != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		iv, err := ToInt(v)
		if err != nil {
			return false, err
		}
		return iv != 0, nil
	case float32, float64:
		fv, err := ToFloat64(v)
		if err != nil {
			return false, err
		}
		return fv != 0, nil
	case string:
		if v == "true" || v == "1" {
			return true, nil
		} else if v == "false" || v == "0" {
			return false, nil
		}
		return false, fmt.Errorf("cannot convert string '%s' to bool", v)
	case []byte:
		return ToBool(string(v))
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// ToTime 支持 MySQL/PG/SQLServer 常见 data_type: DATE, TIME, DATETIME, TIMESTAMP, DATETIME2, SMALLDATETIME, DATETIMEOFFSET, YEAR, ...
func ToTime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case string:
		// 常见时间格式
		formats := []string{
			time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02", "2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05Z07:00", "15:04:05", "2006-01-02 15:04:05.999999", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02T15:04:05.999999999Z07:00",
		}
		for _, f := range formats {
			if t, err := time.Parse(f, v); err == nil {
				return t, nil
			}
		}
		// 尝试解析为Unix时间戳
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.Unix(i, 0), nil
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			return time.Unix(sec, nsec), nil
		}
		return time.Time{}, fmt.Errorf("cannot parse string '%s' to time.Time", v)
	case time.Time:
		return v, nil
	case int64:
		return time.Unix(v, 0), nil
	case float64:
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)
		return time.Unix(sec, nsec), nil
	case int:
		return time.Unix(int64(v), 0), nil
	case uint:
		return time.Unix(int64(v), 0), nil
	case []byte:
		return ToTime(string(v))
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func ToBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case string:
		// 先尝试base64，失败则当作普通字符串
		if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
			return decoded, nil
		}
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}
