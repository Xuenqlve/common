package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/log"
	"github.com/xuenqlve/common/transform"
)

// GetColumnType 获取数据库表的列类型信息
func GetColumnType(ctx context.Context, conn *sql.DB, database, table string) ([]*sql.ColumnType, error) {
	// 添加参数验证
	if conn == nil {
		return nil, errors.New("database connection is nil")
	}
	if database == "" || table == "" {
		return nil, errors.New("database name and table name cannot be empty")
	}
	statement := fmt.Sprintf("SELECT * from %s LIMIT 1", GenerateTableName(database, table))
	rows, err := conn.QueryContext(ctx, statement)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		closeErr := rows.Close()
		log.Warnf("rows close err:%v", closeErr)
	}()
	return rows.ColumnTypes()
}

// BatchDataPtrMap 批量创建数据指针映射
// 为批量数据处理创建多个列名到数据指针的映射
func BatchDataPtrMap(columnTypes []*sql.ColumnType, batch int) []map[string]any {
	// 添加参数验证
	if batch <= 0 {
		return nil
	}
	if len(columnTypes) == 0 {
		return make([]map[string]any, 0)
	}

	ret := make([]map[string]any, batch)
	for batchIdx := 0; batchIdx < batch; batchIdx++ {
		ret[batchIdx] = DataPtrMap(columnTypes)
	}
	return ret
}

// DataPtrMap 创建列名到数据指针的映射
// 为每一列创建对应的数据指针，用于数据库扫描操作
func DataPtrMap(columnTypes []*sql.ColumnType) map[string]any {
	// 添加参数验证
	if len(columnTypes) == 0 {
		return make(map[string]any)
	}
	vPtrs := make(map[string]any, len(columnTypes)) // 预分配容量
	for columnIdx := range columnTypes {
		column := columnTypes[columnIdx].Name()
		scanType := GetScanType(columnTypes[columnIdx])
		vptr := reflect.New(scanType)
		vPtrs[column] = vptr.Interface()
	}
	return vPtrs
}

// BatchDataPtrs 批量创建数据指针切片
// 为批量数据处理创建多个数据指针切片
func BatchDataPtrs(columnTypes []*sql.ColumnType, batch int) [][]any {
	// 添加参数验证
	if batch <= 0 {
		return nil
	}
	if len(columnTypes) == 0 {
		return make([][]any, 0)
	}

	ret := make([][]any, batch)
	for batchIdx := 0; batchIdx < batch; batchIdx++ {
		ret[batchIdx] = DataPtrs(columnTypes)
	}
	return ret
}

// DataPtrs 创建数据指针切片
// 为每一列创建对应的数据指针，按列顺序排列
func DataPtrs(columnTypes []*sql.ColumnType) []any {
	// 添加参数验证
	if len(columnTypes) == 0 {
		return make([]any, 0)
	}

	vPtrs := make([]any, len(columnTypes))
	for columnIdx := range columnTypes {
		scanType := GetScanType(columnTypes[columnIdx])
		vptr := reflect.New(scanType)
		vPtrs[columnIdx] = vptr.Interface()
	}
	return vPtrs
}

// GetScanType 获取列的扫描类型
// 根据数据库类型确定合适的Go类型用于数据扫描
func GetScanType(columnType *sql.ColumnType) reflect.Type {
	// 添加参数验证
	if columnType == nil {
		return reflect.TypeOf(sql.NullString{}) // 默认返回NullString类型
	}
	if isColumnSQLString(columnType.DatabaseTypeName()) {
		return reflect.TypeOf(sql.NullString{})
	} else if isColumnFloat(columnType.DatabaseTypeName()) {
		return reflect.TypeOf(sql.NullFloat64{})
	} else {
		return columnType.ScanType()
	}
}

// isColumnString 判断列是否为字符串类型
// 包括TEXT、CHAR、JSON、DATETIME、TIMESTAMP、DATE等类型
func isColumnSQLString(columnType string) bool {
	// 添加参数验证
	if columnType == "" {
		return false
	}
	upperType := strings.ToUpper(columnType) // 转换为大写以支持大小写不敏感的比较
	return strings.Contains(upperType, "TEXT") ||
		strings.Contains(upperType, "CHAR") ||
		strings.Contains(upperType, "JSON") ||
		strings.Contains(upperType, "DATETIME") ||
		strings.Contains(upperType, "TIMESTAMP") ||
		strings.Contains(upperType, "DATE")
}

// isColumnTime 判断列是否为时间类型
// 包括DATETIME、TIMESTAMP、DATE、TIME、YEAR类型
func isColumnTime(columnType string) bool {
	if columnType == "" {
		return false
	}
	upperType := strings.ToUpper(columnType)
	return strings.Contains(upperType, "DATETIME") ||
		strings.Contains(upperType, "TIMESTAMP") ||
		strings.Contains(upperType, "DATE") ||
		strings.Contains(upperType, "TIME")
	//strings.Contains(upperType, "YEAR")
}

// columnTimeType 获取时间列的具体类型
// 返回标准化的时间类型名称
func columnTimeType(columnType string) string {
	// 添加参数验证
	if columnType == "" {
		return ""
	}
	upperType := strings.ToUpper(columnType) // 转换为大写以支持大小写不敏感的比较
	if strings.Contains(upperType, "DATETIME") {
		return "DATETIME"
	}
	if strings.Contains(upperType, "TIMESTAMP") {
		return "TIMESTAMP"
	}
	if strings.Contains(upperType, "DATE") {
		return "DATE"
	}
	return ""
}

// isColumnFloat 判断列是否为浮点数类型
// 包括DECIMAL、FLOAT、DOUBLE、REAL、NUMERIC类型
func isColumnFloat(columnType string) bool {
	if columnType == "" {
		return false
	}
	upperType := strings.ToUpper(columnType)
	return strings.Contains(upperType, "DECIMAL") ||
		strings.Contains(upperType, "FLOAT") ||
		strings.Contains(upperType, "DOUBLE") ||
		strings.Contains(upperType, "REAL") ||
		strings.Contains(upperType, "NUMERIC")
}

// isBooleanColumn 判断列是否为布尔类型
// 包括BOOLEAN、BOOL、TINYINT(1)
func isBooleanColumn(columnType string) bool {
	if columnType == "" {
		return false
	}
	upperType := strings.ToUpper(columnType)
	return strings.Contains(upperType, "BOOLEAN") ||
		strings.Contains(upperType, "BOOL") ||
		(upperType == "TINYINT(1)")
}

// isColumnInt 判断列是否为整数类型
// 包括TINYINT、SMALLINT、MEDIUMINT、INT、BIGINT
func isColumnInt(columnType string) bool {
	if columnType == "" {
		return false
	}
	upperType := strings.ToUpper(columnType)
	return strings.Contains(upperType, "TINYINT") ||
		strings.Contains(upperType, "SMALLINT") ||
		strings.Contains(upperType, "MEDIUMINT") ||
		strings.Contains(upperType, "INT") ||
		strings.Contains(upperType, "BIGINT")
}

// isColumnBinary 判断列是否为二进制类型
// 包括BINARY、VARBINARY、BLOB、TINYBLOB、MEDIUMBLOB、LONGBLOB
func isColumnBinary(columnType string) bool {
	if columnType == "" {
		return false
	}
	upperType := strings.ToUpper(columnType)
	return strings.Contains(upperType, "BINARY") ||
		strings.Contains(upperType, "BLOB")
}

// isColumnJSON 判断列是否为JSON类型
func isColumnJSON(columnType string) bool {
	if columnType == "" {
		return false
	}
	upperType := strings.ToUpper(columnType)
	return strings.Contains(upperType, "JSON")
}

// isColumnEnumSet 判断列是否为ENUM或SET类型
func isColumnEnumSet(columnType string) bool {
	if columnType == "" {
		return false
	}
	upperType := strings.ToUpper(columnType)
	return strings.Contains(upperType, "ENUM") || strings.Contains(upperType, "SET")
}

func isColumnString(columnType string) bool {
	upperType := strings.ToUpper(columnType) // 转换为大写以支持大小写不敏感的比较
	return strings.Contains(upperType, "TEXT") ||
		strings.Contains(upperType, "CHAR")
}

// SQLDataPtrsVal 将数据指针切片转换为列名到值的映射
// 处理数据库扫描后的数据指针，提取实际值
func SQLDataPtrsVal(dataPtrs []any, columnTypes []*sql.ColumnType) (ret map[string]any, err error) {
	// 添加参数验证
	if len(dataPtrs) == 0 || len(columnTypes) == 0 {
		return make(map[string]any), nil
	}
	if len(dataPtrs) != len(columnTypes) {
		return nil, errors.Errorf("dataPtrs length (%d) != columnTypes length (%d)", len(dataPtrs), len(columnTypes))
	}

	ret = make(map[string]any, len(columnTypes)) // 预分配容量
	for i := range dataPtrs {
		var value any
		columnName := columnTypes[i].Name()
		value, err = DataPtrsVal(dataPtrs[i])
		if err != nil {
			return nil, err
		}
		ret[columnName] = value
	}
	return ret, nil
}

// DataPtrsVal 从数据指针中提取实际值
// 处理各种Go类型和SQL Null类型，返回对应的Go值
func DataPtrsVal(value any) (any, error) {
	// 添加参数验证
	if value == nil {
		return nil, errors.New("value pointer is nil")
	}
	// 检查是否为指针类型
	val := reflect.ValueOf(value)
	if val.Kind() != reflect.Ptr {
		return nil, errors.Errorf("value is not a pointer, got: %v", val.Kind())
	}
	if val.IsNil() {
		return nil, errors.New("value pointer is nil")
	}
	columnData := val.Elem().Interface()
	switch v := columnData.(type) {
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return v, nil
	case float32, float64:
		return v, nil
	case sql.NullInt64:
		if !v.Valid {
			return nil, nil
		} else {
			return v.Int64, nil
		}
	case sql.NullBool:
		if !v.Valid {
			return nil, nil
		} else {
			return v.Bool, nil
		}
	case sql.NullFloat64:
		if !v.Valid {
			return nil, nil
		} else {
			return v.Float64, nil
		}
	case sql.NullString:
		if !v.Valid {
			return nil, nil
		} else {
			return v.String, nil
		}
	case sql.RawBytes:
		if v == nil {
			return nil, nil
		} else {
			b := make([]byte, len(v))
			copy(b, v)
			return b, nil
		}
	case sql.NullTime:
		if !v.Valid {
			return nil, nil
		} else {
			return v.Time, nil
		}
	default:
		return nil, fmt.Errorf("failed to catch type: %v", reflect.TypeOf(columnData))
	}
}

// GetScanPtrSafe 安全地获取扫描指针
// 处理特殊类型（如RawBytes和时间类型）的扫描指针
func GetScanPtrSafe(columnValue any, columnType *sql.ColumnType) (any, error) {
	// 添加参数验证
	if columnValue == nil {
		return nil, errors.New("columnValue is nil")
	}
	if columnType == nil {
		return nil, errors.New("columnType is nil")
	}
	scanType := GetScanType(columnType)
	if scanType.String() == "sql.RawBytes" {
		// 检查是否为指针类型
		val := reflect.ValueOf(columnValue)
		if val.Kind() != reflect.Ptr {
			return nil, errors.Errorf("columnValue is not a pointer, got: %v", val.Kind())
		}
		if val.IsNil() {
			return nil, errors.New("columnValue pointer is nil")
		}
		data := val.Elem().Interface()
		dataRawBytes, ok := data.(sql.RawBytes)
		if !ok {
			return nil, errors.Errorf("[GetScanPtrSafe] failed to convert sql.RawBytes, got: %T", data)
		}
		var b sql.RawBytes
		if dataRawBytes != nil {
			b = make(sql.RawBytes, len(dataRawBytes))
			copy(b, dataRawBytes)
		} else {
			log.Warnf("dataRawBytes is nil columnValue:%v columnType:%v", columnValue, columnType)
		}
		return &b, nil
	} else if isColumnTime(columnType.DatabaseTypeName()) {
		v, flag := ParseTimeByColumnType(columnValue, columnType.DatabaseTypeName())
		if !flag {
			return columnValue, nil
		}
		return &v, nil
	} else {
		return columnValue, nil
	}
}

// DataPtrColumnValues 将数据指针转换为列名到值的映射
// 结合GetScanPtrSafe和DataPtrsVal处理数据指针
func DataPtrColumnValues(columnTypes []*sql.ColumnType, vPtrs []any) (map[string]any, error) {
	// 添加参数验证
	if len(columnTypes) == 0 || len(vPtrs) == 0 {
		return make(map[string]any), nil
	}
	if len(columnTypes) != len(vPtrs) {
		return nil, errors.Errorf("columnTypes length (%d) != vPtrs length (%d)", len(columnTypes), len(vPtrs))
	}

	ret := make(map[string]any, len(columnTypes)) // 预分配容量
	for i := range columnTypes {
		colName := columnTypes[i].Name()
		v, err := GetScanPtrSafe(vPtrs[i], columnTypes[i])
		if err != nil {
			return nil, err
		}
		value, err := DataPtrsVal(v)
		if err != nil {
			return nil, err
		}
		ret[colName] = value
	}
	return ret, nil
}

// Query 执行SQL查询并返回结果
// 提供便捷的查询接口，自动处理数据扫描和类型转换
func Query(ctx context.Context, conn *sql.DB, query string, args ...any) (data []map[string]any, err error) {
	// 添加参数验证
	if conn == nil {
		return nil, errors.New("database connection is nil")
	}
	if query == "" {
		return nil, errors.New("query string is empty")
	}

	data = make([]map[string]any, 0) // 使用make预分配切片
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		result := make(map[string]any) // 使用make预分配map
		vPtrs := DataPtrs(cts)
		if err = rows.Scan(vPtrs...); err != nil {
			return nil, err
		}
		if result, err = DataPtrColumnValues(cts, vPtrs); err != nil {
			return nil, err
		}
		data = append(data, result)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return data, nil
}

// 时间格式常量
const (
	DateFormat     = "2006-01-02"
	DateTimeFormat = "2006-01-02 15:04:05"
)

// 最小时间常量，用于时间验证
var (
	MinDate      = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
	MinDateTime  = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
	MinTimestamp = time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)
)

// ParseTimeByColumnType 根据列类型解析时间字符串
// 将时间字符串转换为sql.NullTime类型
func ParseTimeByColumnType(timeStr any, columnType string) (sql.NullTime, bool) {
	// 添加参数验证
	if timeStr == nil {
		return sql.NullTime{}, false
	}
	if columnType == "" {
		return sql.NullTime{}, false
	}

	tStr, ok := timeStr.(*sql.NullString)
	if !ok {
		return sql.NullTime{}, false
	}
	if !tStr.Valid {
		return sql.NullTime{}, false
	}

	typeName := columnTimeType(columnType)
	if typeName == "" {
		return sql.NullTime{}, false
	}

	t, err := ParseTime(typeName, tStr.String)
	if err != nil {
		return sql.NullTime{}, false
	}
	return sql.NullTime{Time: t, Valid: true}, true
}

// 时间类型常量
const (
	DATE      = "DATE"
	DATETIME  = "DATETIME"
	TIMESTAMP = "TIMESTAMP"
)

// ParseTime 解析时间字符串
// 根据时间类型解析字符串为time.Time，并进行有效性验证
func ParseTime(typeName, timeStr string) (time.Time, error) {
	// 添加参数验证
	if typeName == "" {
		return time.Time{}, errors.New("typeName is empty")
	}
	if timeStr == "" {
		return time.Time{}, errors.New("timeStr is empty")
	}

	var minTime time.Time
	var layout string
	switch typeName {
	case DATE:
		layout = DateFormat
		minTime = MinDate
	case DATETIME:
		layout = DateTimeFormat
		minTime = MinDateTime
	case TIMESTAMP:
		layout = DateTimeFormat
		minTime = MinTimestamp
	default:
		return time.Time{}, errors.Errorf("[parseTime] invalid columnType:%v", typeName)
	}

	timeS, err := time.Parse(layout, timeStr)
	if err != nil {
		return time.Time{}, errors.Errorf("failed to parse time '%s' with layout '%s': %v", timeStr, layout, err)
	}

	if timeS.Before(minTime) {
		return time.Time{}, errors.Errorf("[parseTime] %s is before %s", timeS, minTime)
	}
	return timeS, nil
}

func FormatSQLData(value any, dataType string) (any, error) {
	if isColumnFloat(dataType) {
		return transform.ToFloat64(value)
	}
	if isColumnTime(dataType) {
		return transform.ToTime(value)
	}
	if isBooleanColumn(dataType) {
		return transform.ToBool(value)
	}
	if isColumnInt(dataType) {
		return transform.ToInt(value)
	}
	if isColumnBinary(dataType) {
		return transform.ToBytes(value)
	}
	if isColumnString(dataType) {
		return transform.ToString(value)
	}
	return value, nil
}
