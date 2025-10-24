package chunk

type Range struct {
	Lower map[string]any
	Upper map[string]any
}

func NewRange(lower, upper map[string]any) *Range {
	return &Range{
		Lower: lower,
		Upper: upper,
	}
}
