Debugging InitAdcircCSV

process_TIN
	1. opens csv file, iterates row by row
		- rows have x, y, elevation, 2, 5, ..., 1000yr depths
	2. each x,y,[]depths stored in slice []geometry.PointZ --> points
	3. creates TIN (t) with `geometry.CreateTin(points, nodata, p)`, where p is hull created from points
	4. returns t

adcircCSVHazardProvider.Hazard(l) --> hazards.CoastalEvent{}
	1. Creates geometry.Point from l.X, l.Y
	2. Reports number of structures processed so far
	3. if the point is within the TIN hull, v := ComputeValue(x, y)
	4. return hazards.CoastalEvent{Depth: v, Salinity: true}

ComputeValue(x, y) --> depth float64
	gets depth value by searching the TIN rtree
	initialze return value v = nodata
	t.Tree.Search() <-- How does this function work??? 
		- updates v value inplace, returns bool
	return v


rtree.RTree.Search(min [2]float64, max [2]float64, iter func(min [2]float64, max [2]float64, data interface{}) bool)
	1. take value, cast to TriangleZZ tri
		- in debug, value was type TriangleZ <-- This is the problem!!! But I don't know what is causing it
		- where is value coming from??? are we iterating from min to max? How could this be if we are passing the same x,y to min and max params?
	2. v = tri.GetValue(x, y, t.zidx)

TriangleZZ.GetValue(x float64, y float64, zidx int) (float64, float64, error)
	- use math to calculate value from values on triangle vertices
	- barycentric interpolation


