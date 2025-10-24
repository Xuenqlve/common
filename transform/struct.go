package transform

func StringSliceToMap(list []string) map[string]struct{} {
	result := make(map[string]struct{}, len(list))
	for _, str := range list {
		result[str] = struct{}{}
	}
	return result
}
