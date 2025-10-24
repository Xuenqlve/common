package chunk

type Bound struct {
	Column string
	Lower  any
	Upper  any

	HasLower bool
	HasUpper bool
}

type Chunk struct {
	Bounds       []*Bound
	columnOffset map[string]int
}

func NewChunkRange() *Chunk {
	return &Chunk{
		Bounds:       make([]*Bound, 0, 2),
		columnOffset: make(map[string]int),
	}
}

func NewChunkScanColumns(scanColumns []string) *Chunk {
	bounds := make([]*Bound, 0, len(scanColumns))
	columnOffset := map[string]int{}
	for index, column := range scanColumns {
		columnOffset[column] = index
		bounds = append(bounds, &Bound{
			Column:   column,
			HasLower: false,
			HasUpper: false,
		})
	}
	return &Chunk{
		Bounds:       bounds,
		columnOffset: columnOffset,
	}
}

func InitChunk(scanColumns []string, scanRange *Range) *Chunk {
	bounds := make([]*Bound, 0, len(scanColumns))
	columnOffset := map[string]int{}
	for index, column := range scanColumns {
		columnOffset[column] = index
		lower, hasLower := scanRange.Lower[column]
		upper, hasUpper := scanRange.Upper[column]
		bounds = append(bounds, &Bound{
			Column:   column,
			Lower:    lower,
			Upper:    upper,
			HasLower: hasLower,
			HasUpper: hasUpper,
		})
	}
	return &Chunk{
		Bounds:       bounds,
		columnOffset: columnOffset,
	}
}

func (r *Chunk) GetRange() *Range {
	lower, upper := map[string]any{}, map[string]any{}
	for _, bound := range r.Bounds {
		if bound.HasLower {
			lower[bound.Column] = bound.Lower
		}
		if bound.HasUpper {
			upper[bound.Column] = bound.Upper
		}
	}
	return NewRange(lower, upper)
}

func (r *Chunk) Copy() *Chunk {
	newChunk := NewChunkRange()
	for _, bound := range r.Bounds {
		newChunk.addBound(&Bound{
			Column:   bound.Column,
			Lower:    bound.Lower,
			Upper:    bound.Upper,
			HasLower: bound.HasLower,
			HasUpper: bound.HasUpper,
		})
	}
	return newChunk
}

func (r *Chunk) UpdateLower(column string, lower any) {
	if offset, ok := r.columnOffset[column]; ok {
		r.Bounds[offset].Lower = lower
		r.Bounds[offset].HasLower = true
		return
	}
	// add a new bound
	r.addBound(&Bound{
		Column:   column,
		Lower:    lower,
		HasLower: true,
	})
}

func (r *Chunk) UpdateUpper(column string, upper any) {
	if offset, ok := r.columnOffset[column]; ok {
		// update the bound
		r.Bounds[offset].Upper = upper
		r.Bounds[offset].HasUpper = true
		return
	}
	// add a new bound
	r.addBound(&Bound{
		Column:   column,
		Upper:    upper,
		HasUpper: true,
	})
}

func (r *Chunk) Update(column string, lower, upper any, updateLower, updateUpper bool) {
	if offset, ok := r.columnOffset[column]; ok {
		// update the bound
		if updateLower {
			r.Bounds[offset].Lower = lower
			r.Bounds[offset].HasLower = true
		}
		if updateUpper {
			r.Bounds[offset].Upper = upper
			r.Bounds[offset].HasUpper = true
		}
		return
	}
	// add a new bound
	r.addBound(&Bound{
		Column:   column,
		Lower:    lower,
		Upper:    upper,
		HasLower: updateLower,
		HasUpper: updateUpper,
	})
}

func (r *Chunk) Clone() *Chunk {
	newChunk := NewChunkRange()
	for _, bound := range r.Bounds {
		newChunk.addBound(&Bound{
			Column:   bound.Column,
			Lower:    bound.Lower,
			Upper:    bound.Upper,
			HasLower: bound.HasLower,
			HasUpper: bound.HasUpper,
		})
	}
	//newChunk.Where = r.Where
	//newChunk.Args = r.Args
	for i, v := range r.columnOffset {
		newChunk.columnOffset[i] = v
	}
	return newChunk
}

func (r *Chunk) CopyAndUpdate(column string, lower, upper any, updateLower, updateUpper bool) *Chunk {
	newChunk := r.Copy()
	newChunk.Update(column, lower, upper, updateLower, updateUpper)
	return newChunk
}

func (r *Chunk) addBound(bound *Bound) {
	r.Bounds = append(r.Bounds, bound)
	r.columnOffset[bound.Column] = len(r.Bounds) - 1
}
