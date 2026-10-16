package geometry

import "errors"

type Triangle struct {
	p1     *PointZ
	p2     *PointZ
	p3     *PointZ
	extent Extent
}

func CreateTriangle(a *PointZ, b *PointZ, c *PointZ) Triangle {
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
	return Triangle{p1: a, p2: b, p3: c, extent: e}
}

//https://codeplea.com/triangular-interpolation
func (t Triangle) GetValue(x float64, y float64, zidx int) (float64, error) {
	invDenom := 1 / ((t.p2.Y-t.p3.Y)*(t.p1.X-t.p3.X) + (t.p3.X-t.p2.X)*(t.p1.Y-t.p3.Y))
	w1 := ((t.p2.Y-t.p3.Y)*(x-t.p3.X) + (t.p3.X-t.p2.X)*(y-t.p3.Y)) * invDenom
	w2 := ((t.p3.Y-t.p1.Y)*(x-t.p3.X) + (t.p1.X-t.p3.X)*(y-t.p3.Y)) * invDenom
	w3 := 1.0 - w1 - w2
	if w1 >= 0 && w2 >= 0 && w3 >= 0 {
		return (w1*t.p1.Z[zidx] + w2*t.p2.Z[zidx] + w3*t.p3.Z[zidx]), nil
	}
	return -9999, errors.New("Point Outside Triangle")
}
func (t Triangle) GetValues(x float64, y float64) ([]float64, error) {
	invDenom := 1 / ((t.p2.Y-t.p3.Y)*(t.p1.X-t.p3.X) + (t.p3.X-t.p2.X)*(t.p1.Y-t.p3.Y))
	w1 := ((t.p2.Y-t.p3.Y)*(x-t.p3.X) + (t.p3.X-t.p2.X)*(y-t.p3.Y)) * invDenom
	w2 := ((t.p3.Y-t.p1.Y)*(x-t.p3.X) + (t.p1.X-t.p3.X)*(y-t.p3.Y)) * invDenom
	w3 := 1.0 - w1 - w2
	lenz := len(t.p1.Z)
	vals := make([]float64, lenz)
	if w1 >= 0 && w2 >= 0 && w3 >= 0 {
		for i, z := range t.p1.Z {
			vals[i] = (w1*z + w2*t.p2.Z[i] + w3*t.p3.Z[i])
		}
		return vals, nil
	}
	return []float64{-9999}, errors.New("Point Outside Triangle")
}
func (t *Triangle) Extent() Extent {
	return t.extent
}
