package compare

import "sort"

func CompareSlicesEquality(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	newA, newB := make([]string, len(a)), make([]string, len(b))
	copy(newA, a)
	copy(newB, b)
	sort.Strings(newA)
	sort.Strings(newB)
	for i := range newA {
		if newA[i] != newB[i] {
			return false
		}
	}
	return true
}
