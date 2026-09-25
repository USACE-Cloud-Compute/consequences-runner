package geometry

import (
	"errors"
	"math"

	"github.com/HydrologicEngineeringCenter/go-statistics/statistics"
)

type TriangleWithPayload struct {
	p1     *PointWithPayload
	p2     *PointWithPayload
	p3     *PointWithPayload
	extent Extent
}

func CreateTriangleWithPayload(a *PointWithPayload, b *PointWithPayload, c *PointWithPayload) TriangleWithPayload {
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
	return TriangleWithPayload{p1: a, p2: b, p3: c, extent: e}
}

// https://codeplea.com/triangular-interpolation
func (t TriangleWithPayload) GetValue(x float64, y float64, zidx int, prob float64) (map[Parameter]float64, error) {
	invDenom := 1 / ((t.p2.Y-t.p3.Y)*(t.p1.X-t.p3.X) + (t.p3.X-t.p2.X)*(t.p1.Y-t.p3.Y))
	w1 := ((t.p2.Y-t.p3.Y)*(x-t.p3.X) + (t.p3.X-t.p2.X)*(y-t.p3.Y)) * invDenom
	w2 := ((t.p3.Y-t.p1.Y)*(x-t.p3.X) + (t.p1.X-t.p3.X)*(y-t.p3.Y)) * invDenom
	w3 := 1.0 - w1 - w2
	output := make(map[Parameter]float64)
	if w1 >= 0 && w2 >= 0 && w3 >= 0 {
		for k, _ := range t.p1.Data {
			if k != Terrain {
				output[k] = (w1*t.p1.Data[k][zidx].InvCDF(prob) + w2*t.p2.Data[k][zidx].InvCDF(prob) + w3*t.p3.Data[k][zidx].InvCDF(prob))
			}
		}
		return output, nil
	}
	return output, errors.New("Point Outside Triangle")
}
func (t TriangleWithPayload) GetValues(x float64, y float64, prob float64) (map[Parameter][]float64, error) {
	invDenom := 1 / ((t.p2.Y-t.p3.Y)*(t.p1.X-t.p3.X) + (t.p3.X-t.p2.X)*(t.p1.Y-t.p3.Y))
	w1 := ((t.p2.Y-t.p3.Y)*(x-t.p3.X) + (t.p3.X-t.p2.X)*(y-t.p3.Y)) * invDenom
	w2 := ((t.p3.Y-t.p1.Y)*(x-t.p3.X) + (t.p1.X-t.p3.X)*(y-t.p3.Y)) * invDenom
	w3 := 1.0 - w1 - w2
	output := make(map[Parameter][]float64)
	if w1 >= 0 && w2 >= 0 && w3 >= 0 {
		//we shouldnt have a different terrain by frequency, but maybe we will someday? right now we have no error defined for terrain, but maybe we will some day.
		ele := (w1*t.p1.Data[Terrain][0].CentralTendency() + w2*t.p2.Data[Terrain][0].CentralTendency() + w3*t.p3.Data[Terrain][0].CentralTendency())
		output[Terrain] = make([]float64, 1)
		output[Terrain][0] = ele
		for k, v := range t.p1.Data {
			if k != Terrain {
				output[k] = make([]float64, len(v))
				for i, z := range t.p1.Data[k] {
					if k != Terrain {
						val := (w1*z.InvCDF(prob) + w2*t.p2.Data[k][i].InvCDF(prob) + w3*t.p3.Data[k][i].InvCDF(prob))
						if k == SWL {
							//should i do data checks on ele?
							val = val - ele
						}
						output[k][i] = val
					}

				}
			}

		}

		return output, nil
	}
	return output, errors.New("Point Outside Triangle")
}

func (t *TriangleWithPayload) Extent() Extent {
	return t.extent
}
func (t TriangleWithPayload) HasData() bool {
	p1swl, p1ok := t.p1.Data[SWL]
	if p1ok {
		if !dataOk(p1swl) {
			return false
		}
	} else {
		return false
	}
	p2swl, p2ok := t.p2.Data[SWL]
	if p2ok {
		if !dataOk(p2swl) {
			return false
		}
	} else {
		return false
	}
	p3swl, p3ok := t.p3.Data[SWL]
	if p3ok {
		if !dataOk(p3swl) {
			return false
		}
	} else {
		return false
	}

	return true
}
func dataOk(p []statistics.ContinuousDistribution) bool {
	for _, d := range p {
		if !math.IsNaN(d.CentralTendency()) {
			if !math.IsInf(d.CentralTendency(), -1) {
				if !math.IsInf(d.CentralTendency(), 1) {
					return true
				}
			}
		}
	}
	return false
}
func (t *TriangleWithPayload) Points() []float64 {
	return []float64{t.p1.X, t.p1.Y, t.p2.X, t.p2.Y, t.p3.X, t.p3.Y}
}
