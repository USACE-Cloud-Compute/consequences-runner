package geometry

import (
	"fmt"
	"os"
	"strings"
)

type Polygon struct {
	Points []Point
	Extent Extent
}

func CreatePolygon(points []Point) Polygon {
	//compute bounding box
	var minx, miny, maxx, maxy float64
	minx = 180
	miny = 180
	maxx = -180
	maxy = -180
	for _, p := range points {
		if maxx < p.X {
			maxx = p.X
		}
		if minx > p.X {
			minx = p.X
		}
		if maxy < p.Y {
			maxy = p.Y
		}
		if miny > p.Y {
			miny = p.Y
		}
	}
	e := Extent{UpperRight: Point{X: maxx, Y: maxy}, LowerLeft: Point{X: minx, Y: miny}}
	return Polygon{Points: points, Extent: e}
}
func (polygon *Polygon) Contains(p Point) bool {
	// Checks if point is inside polygon
	// If point not in bounding box return false immediately
	if !polygon.Extent.Contains(p) {
		return false
	}
	// If the point is in the bounding box then we need to check the polygon
	nverts := len(polygon.Points)
	intersect := false
	verts := polygon.Points
	j := 0
	for i := 1; i < nverts; i++ {

		if ((verts[i].Y > p.Y) != (verts[j].Y > p.Y)) &&
			(p.X < (verts[j].X-verts[i].X)*(p.Y-verts[i].Y)/(verts[j].Y-verts[i].Y)+verts[i].X) {
			intersect = !intersect
		}

		j = i

	}
	return intersect
}

//to geojson?
func (polygon *Polygon) ToGeoJson(fp string) {
	w, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	defer w.Close()
	if err != nil {
		panic(err)
	}
	s := "{\"type\": \"FeatureCollection\",\"features\": [{\"type\": \"Feature\",\"geometry\": {\"type\": \"LineString\",\"coordinates\": ["
	for _, p := range polygon.Points {
		s += "[" + fmt.Sprintf("%g, %g", p.X, p.Y) + "],"
	}
	s = strings.TrimRight(s, ",")
	s += "]},\"properties\": {\"prop1\": 0.0}}]}"
	fmt.Fprintf(w, s)
}
