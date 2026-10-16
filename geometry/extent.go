package geometry

type Extent struct {
	LowerLeft  Point
	UpperRight Point
}

func (e Extent) Min() [2]float64 {
	return e.LowerLeft.ToXY()
}
func (e Extent) Max() [2]float64 {
	return e.UpperRight.ToXY()
}
func (e *Extent) Contains(p Point) bool {
	// Check if point is in bounding box
	return p.X < e.UpperRight.X && p.X > e.LowerLeft.X &&
		p.Y < e.UpperRight.Y && p.Y > e.LowerLeft.Y
}
