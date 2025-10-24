package compare

import "fmt"

// if left > right return 1 Greater  left < right return -1
func CompareColumn(scanColumn []string, left, right map[string]any) (int, error) {
	for _, column := range scanColumn {
		leftValue, ok := left[column]
		if !ok {
			return 0, fmt.Errorf("compareMax input a:%v not found column:%v", left, column)
		}
		rightValue, ok := right[column]
		if !ok {
			return 0, fmt.Errorf("compareMax input a:%v not found column:%v", right, column)
		}
		flag, err := Compare(leftValue, rightValue)
		if err != nil {
			return 0, err
		}
		if flag != Equal {
			return flag, nil
		}
	}
	return Equal, nil

}
