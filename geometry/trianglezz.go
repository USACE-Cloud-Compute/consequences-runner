package geometry

import (
	"errors"
)

type TriangleZZ struct {
	p1     *PointZZ
	p2     *PointZZ
	p3     *PointZZ
	extent Extent
}

func CreateTriangleZZ(a *PointZZ, b *PointZZ, c *PointZZ) TriangleZZ {
	var minx, miny, maxx, maxy float64
	minx = 180
	miny = 180
	maxx = -180
	maxy = -180
	if maxx < a.X {
		maxx = a.X
	}
	if maxx < b.X {
		maxx = b.X
	}
	if maxx < c.X {
		maxx = c.X
	}
	if minx > a.X {
		minx = a.X
	}
	if minx > b.X {
		minx = b.X
	}
	if minx > c.X {
		minx = c.X
	}
	if maxy < a.Y {
		maxy = a.Y
	}
	if maxy < b.Y {
		maxy = b.Y
	}
	if maxy < c.Y {
		maxy = c.Y
	}
	if miny > a.Y {
		miny = a.Y
	}
	if miny > b.Y {
		miny = b.Y
	}
	if miny > c.Y {
		miny = c.Y
	}
	e := Extent{LowerLeft: Point{X: minx, Y: miny}, UpperRight: Point{X: maxx, Y: maxy}}
	return TriangleZZ{p1: a, p2: b, p3: c, extent: e}
}

// https://codeplea.com/triangular-interpolation
func (t TriangleZZ) GetValue(x float64, y float64, zidx int) (float64, float64, error) {
	invDenom := 1 / ((t.p2.Y-t.p3.Y)*(t.p1.X-t.p3.X) + (t.p3.X-t.p2.X)*(t.p1.Y-t.p3.Y))
	w1 := ((t.p2.Y-t.p3.Y)*(x-t.p3.X) + (t.p3.X-t.p2.X)*(y-t.p3.Y)) * invDenom
	w2 := ((t.p3.Y-t.p1.Y)*(x-t.p3.X) + (t.p1.X-t.p3.X)*(y-t.p3.Y)) * invDenom
	w3 := 1.0 - w1 - w2
	if w1 >= 0 && w2 >= 0 && w3 >= 0 {
		return (w1*t.p1.ZSwl[zidx] + w2*t.p2.ZSwl[zidx] + w3*t.p3.ZSwl[zidx]), (w1*t.p1.ZHm0[zidx] + w2*t.p2.ZHm0[zidx] + w3*t.p3.ZHm0[zidx]), nil
	}
	return -9999, -9999, errors.New("Point Outside Triangle")
}
func (t TriangleZZ) GetValues(x float64, y float64) ([]float64, []float64, error) {
	invDenom := 1 / ((t.p2.Y-t.p3.Y)*(t.p1.X-t.p3.X) + (t.p3.X-t.p2.X)*(t.p1.Y-t.p3.Y))
	w1 := ((t.p2.Y-t.p3.Y)*(x-t.p3.X) + (t.p3.X-t.p2.X)*(y-t.p3.Y)) * invDenom
	w2 := ((t.p3.Y-t.p1.Y)*(x-t.p3.X) + (t.p1.X-t.p3.X)*(y-t.p3.Y)) * invDenom
	w3 := 1.0 - w1 - w2
	lenz := len(t.p1.ZSwl)
	vals := make([]float64, lenz)
	hmos := make([]float64, lenz)
	if w1 >= 0 && w2 >= 0 && w3 >= 0 {
		ele := (w1*t.p1.ZElev + w2*t.p2.ZElev + w3*t.p3.ZElev)
		for i, z := range t.p1.ZSwl {
			if len(t.p2.ZSwl) == 0 {
				return nil, nil, errors.New("Not all points have data")
			}
			if len(t.p3.ZSwl) == 0 {
				return nil, nil, errors.New("Not all points have data")
			}
			swl := (w1*z + w2*t.p2.ZSwl[i] + w3*t.p3.ZSwl[i])
			//should i do data checks on ele?
			vals[i] = swl - ele
			hmos[i] = (w1*t.p1.ZHm0[i] + w2*t.p2.ZHm0[i] + w3*t.p3.ZHm0[i])
		}
		return vals, hmos, nil
	}
	return []float64{-9999}, []float64{-9999}, errors.New("Point Outside Triangle")
}

func (t *TriangleZZ) Extent() Extent {
	return t.extent
}
func (t TriangleZZ) HasData() bool {
	equalsThree := 0
	if len(t.p1.ZSwl) > 0 {
		equalsThree++
	}
	if len(t.p2.ZSwl) > 0 {
		equalsThree++
	}
	if len(t.p3.ZSwl) > 0 {
		equalsThree++
	}
	return equalsThree == 3
}
func (t *TriangleZZ) Points() []float64 {
	return []float64{t.p1.X, t.p1.Y, t.p2.X, t.p2.Y, t.p3.X, t.p3.Y}
}
