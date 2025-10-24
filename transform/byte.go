package transform

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	// 二进制单位常量（基于1024）
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
	PB = 1024 * TB
	EB = 1024 * PB
	// ZB和YB超出uint64范围，在代码中动态计算
)

// ParseBytes 将字符串转换为字节数（基础单位：bytes）
// 支持的格式: "1024", "1KB", "512MB", "2GB", "1TB", "1PB", "1EB"
// 单位不区分大小写，支持 B/KB/MB/GB/TB/PB/EB
// 同时支持简写形式：K/M/G/T/P/E
// 注意：ZB和YB超出uint64范围，不支持
func ParseBytes(s string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}

	// 去除空格并转为大写
	s = strings.ToUpper(strings.TrimSpace(s))

	// 优化的正则表达式，支持更多格式
	// 支持整数和小数，支持科学计数法
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s*([KMGTPE]?B?I?B?)$`)
	matches := re.FindStringSubmatch(s)

	if len(matches) != 3 {
		return 0, fmt.Errorf("invalid format: %s, expected format like '1024', '1KB', '1.5MB'", s)
	}

	// 解析数字部分
	numStr := matches[1]
	unitStr := matches[2]

	// 转换数字，支持科学计数法
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number '%s': %v", numStr, err)
	}

	if num < 0 {
		return 0, fmt.Errorf("negative value not allowed: %g", num)
	}

	// 检查数值溢出
	if num > float64(^uint64(0)) {
		return 0, fmt.Errorf("value too large: %g", num)
	}

	// 确定单位乘数（二进制单位，1024为基数）
	var multiplier uint64 = 1
	switch unitStr {
	case "", "B":
		multiplier = 1
	case "KB", "K", "KIB":
		multiplier = KB
	case "MB", "M", "MIB":
		multiplier = MB
	case "GB", "G", "GIB":
		multiplier = GB
	case "TB", "T", "TIB":
		multiplier = TB
	case "PB", "P", "PIB":
		multiplier = PB
	case "EB", "E", "EIB":
		multiplier = EB
	default:
		return 0, fmt.Errorf("unsupported unit: %s", unitStr)
	}

	// 计算最终字节数，检查溢出
	if num > float64(^uint64(0))/float64(multiplier) {
		return 0, fmt.Errorf("result overflow: %g * %d", num, multiplier)
	}

	result := uint64(num * float64(multiplier))
	return result, nil
}

// FormatBytes 将字节数格式化为可读的字符串（基础单位：bytes）
// 例如: 1024 -> "1KB", 1048576 -> "1MB", 1073741824 -> "1GB"
// 自动选择最合适的单位进行显示
func FormatBytes(bytes uint64) string {
	if bytes == 0 {
		return "0B"
	}

	// 单位数组，按从大到小排序
	units := []struct {
		name string
		size uint64
	}{
		{"EB", EB},
		{"PB", PB},
		{"TB", TB},
		{"GB", GB},
		{"MB", MB},
		{"KB", KB},
		{"B", 1},
	}

	for _, unit := range units {
		if bytes >= unit.size {
			value := float64(bytes) / float64(unit.size)
			// 如果是整数，不显示小数点
			if value == float64(uint64(value)) {
				return fmt.Sprintf("%.0f%s", value, unit.name)
			}
			// 显示最多2位小数，去除末尾的0
			formatted := fmt.Sprintf("%.2f", value)
			formatted = strings.TrimRight(formatted, "0")
			formatted = strings.TrimRight(formatted, ".")
			return fmt.Sprintf("%s%s", formatted, unit.name)
		}
	}

	return fmt.Sprintf("%dB", bytes)
}

// MustParseBytes 类似 ParseBytes，但在出错时panic
func MustParseBytes(s string) uint64 {
	bytes, err := ParseBytes(s)
	if err != nil {
		panic(fmt.Sprintf("failed to parse bytes: %v", err))
	}
	return bytes
}
