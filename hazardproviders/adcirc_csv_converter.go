package hazardproviders

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/furstenheim/ConcaveHull"
	"github.com/tidwall/rtree"
	"github.com/usace-cloud-compute/consequences-runner/geometry"
)

type CoastalFrequency int

const (
	Two          CoastalFrequency = 3
	Five         CoastalFrequency = 4
	Ten          CoastalFrequency = 5
	Twenty       CoastalFrequency = 6
	Fifty        CoastalFrequency = 7
	OneHundred   CoastalFrequency = 8
	TwoHundred   CoastalFrequency = 9
	FiveHundred  CoastalFrequency = 10
	OneThousand  CoastalFrequency = 11
	FiveThousand CoastalFrequency = 12
	TenThousand  CoastalFrequency = 13
)

var CoastalFrequencies = []CoastalFrequency{
	Two,
	Five,
	Ten,
	Twenty,
	Fifty,
	OneHundred,
	TwoHundred,
	FiveHundred,
	OneThousand,
	FiveThousand,
	TenThousand,
}

// AEF_0.50_SDam
func (c CoastalFrequency) String() string {
	return [...]string{"Two", "Five", "Ten", "Twenty", "Fifty", "OneHundred", "TwoHundred", "FiveHundred", "OneThousand", "FiveThousand", "TenThousand"}[c-3]
}

/*
	This is where the hard part lives...
	//raw data is in meters - unknown projection atm.
	//There is no header, first row is real data.
	//delimiter is comma
	//lon,lat is not dependable, negative values should indicate lon...
	//lon,lat,elevation(meters),.5,.2,.1,.05,.02,.01,.005,.002,.001,.0002,.0001 (all depths in meter, still water level)
	//some xy locations have all zeros for depths.
	//xy location represents nodes on a triangular irregular mesh.
	//BE represents Best Estimate, CL90 90% exceedacnce value, SLC0 1996, SLC1 mid future condition, SLC2 high future condition
	//data organized by state.
*/

func process_TIN(fp string) (*geometry.Tin, error) {
	f, err := os.Open(fp)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	nodata := 0.0
	xidx := 0
	yidx := 1
	firstrow := true
	//we dont know how big the file will be, so we have to make a guess.
	dimSize := 0
	points := make([]geometry.PointZ, dimSize)
	count := 0
	ps := make([]float64, dimSize)
	for scanner.Scan() {
		lines := strings.Split(scanner.Text(), ",")
		//check if first value is negative to determine lat/lon
		if firstrow {
			testval, err := strconv.ParseFloat(lines[0], 64)
			if err != nil {
				panic(err)
			}
			if testval < 0 {
				//0 is negative, must be lon
			} else {
				yidx = 0
				xidx = 1
			}
			firstrow = false
		}
		xval, err := strconv.ParseFloat(lines[xidx], 64)
		if err != nil {
			panic(err)
		}
		ps = append(ps, xval)
		yval, err := strconv.ParseFloat(lines[yidx], 64)
		if err != nil {
			panic(err)
		}
		ps = append(ps, yval)
		terrain, err := strconv.ParseFloat(lines[2], 64)
		if err != nil {
			panic(err)
		}
		//loop over z values
		zvals := make([]float64, len(CoastalFrequencies))
		for i, zconst := range CoastalFrequencies {
			zval, err := strconv.ParseFloat(lines[int(zconst)], 64) //need to read all values and load into an array now.
			if err != nil {
				panic(err)
			}
			if terrain < 0 {
				zval = zval + terrain //(minus a negative to get value above sea level...)
			}
			if zval == 0 {
				zval = nodata
			} else {
				zval *= 3.28084 //convert from meters to feet
			}
			zvals[i] = zval
		}
		p := geometry.Point{X: xval, Y: yval}
		points = append(points, geometry.PointZ{Point: &p, Z: zvals})
		count++
	}
	fmt.Printf("read %v lines from %v\n", count, fp)
	fmt.Println("Creating Concave Hull...")
	flathull := ConcaveHull.Compute(ConcaveHull.FlatPoints(ps))
	ptcount := len(flathull) / 2
	hull := make([]geometry.Point, ptcount+1)
	index := 0
	for i := 0; i < len(flathull); i += 2 {
		hull[index] = geometry.Point{X: flathull[i], Y: flathull[i+1]}
		index++
	}
	hull[index] = geometry.Point{X: flathull[0], Y: flathull[1]}
	p := geometry.CreatePolygon(hull)
	t, err := geometry.CreateTin(points, nodata, p)
	return t, err
	/*
		pts := t.ConvexHull
		s := "{\"type\": \"FeatureCollection\",\"features\": [{\"type\": \"Feature\",\"geometry\": {\"type\": \"LineString\",\"coordinates\": ["
		for _, p := range pts {
			s += "[" + fmt.Sprintf("%g, %g",p.X, p.Y) + "],"
		}

		s = strings.TrimRight(s, ",")
		s += "]},\"properties\": {\"prop1\": 0.0}}]}"

		fmt.Println(s)

		s := strings.TrimRight(fp,".csv")
		t.Json(s + ".json")
	*/
}

/*
logic is:
  - 1st read nodes
  - add probability arrays for SWL/Hm0 to each node
  - which probability runs?
  - 2nd build triangles using the nodes
  - build queryable rtree index on triangles
  - 3rd create concave hull
  - why do points option?
*/
func processGrdAndCSVs(grdfp string, swlfp string, hm0fp string) (*geometry.Tin, error) {
	grdf, err := os.Open(grdfp)
	if err != nil {
		panic(err)
	}
	defer grdf.Close()

	scanner := bufio.NewScanner(grdf)
	//we dont know how big the file will be, so we have to make a guess.
	scanner.Scan() // burn the header
	scanner.Scan() //count of triangles and points
	row2 := scanner.Text()
	vals := strings.Split(row2, "  ") //not sure this will always work correctly
	dimNSize, _ := strconv.ParseInt(vals[1], 10, 64)
	dimTSize, _ := strconv.ParseInt(vals[0], 10, 64)
	nodes := make(map[int32]geometry.PointZZ)

	triangles := make(map[int64]geometry.TriangleZZ)
	var triangleCounter int64
	var pointCounter int64
	triangleCounter = 0
	pointCounter = 0
	var minx, miny, maxx, maxy float64
	minx = 180
	miny = 180
	maxx = -180
	maxy = -180
	loadData := true
	for scanner.Scan() {
		if pointCounter < dimNSize { //points come first.
			if pointCounter == 0 {
				fmt.Println("reading nodes")
			}
			pointCounter += 1
			line := strings.Split(scanner.Text(), " ") //is there a way to group spaces?
			//nodeid|X|Y|Z
			var nodeid int32
			nodeid = 0
			xval := 0.0
			yval := 0.0
			zval := 0.0 //terrain
			valcount := 0
			for _, v := range line {
				if v != "" {
					valcount += 1
					switch valcount {
					case 1:
						tmpInt, err := strconv.ParseInt(v, 10, 32)
						if err != nil {
							panic(err)
						}
						nodeid = int32(tmpInt)
					case 2:
						xval, _ = strconv.ParseFloat(v, 64)
					case 3:
						yval, _ = strconv.ParseFloat(v, 64)
					case 4:
						zval, _ = strconv.ParseFloat(v, 64) //terrain
					}
				}
			}
			nodes[nodeid] = geometry.PointZZ{Point: &geometry.Point{X: xval, Y: yval}, ZElev: zval}
		} else if triangleCounter < dimTSize { //triangles come second.
			if loadData {
				//read the other two csv files to load in data into the nodes.
				swlf, err := os.Open(swlfp)
				if err != nil {
					panic(err)
				}
				defer swlf.Close()

				swlscanner := bufio.NewScanner(swlf)
				swlscanner.Scan() // burn the header
				swlscanner.Scan() //probabilities
				hmof, err := os.Open(hm0fp)
				if err != nil {
					panic(err)
				}
				defer hmof.Close()
				hmoscanner := bufio.NewScanner(hmof)
				hmoscanner.Scan() //burn the header
				hmoscanner.Scan() //probabilities
				nantucketfrequencies := []float64{1e+01, 5e+00, 2e+00, 1e+00, 5e-01, 2e-01, 1e-01, 5e-02, 2e-02, 1e-02, 5e-03, 2e-03, 1e-03, 5e-04, 2e-04, 1e-04, 5e-05, 2e-05, 1e-05, 5e-06, 2e-06, 1e-06}
				nodeididx := 1
				nodata := 0.0
				for swlscanner.Scan() {
					hmoscanner.Scan()
					//parse each row.
					swllines := strings.Split(swlscanner.Text(), ",")
					hmolines := strings.Split(hmoscanner.Text(), ",")
					nodeidval, err := strconv.ParseInt(swllines[nodeididx], 10, 32)
					if err != nil {
						panic(err)
					}
					nodeid := int32(nodeidval)

					//loop over z values
					zswlvals := make([]float64, len(nantucketfrequencies))
					zhmovals := make([]float64, len(nantucketfrequencies))
					node := nodes[nodeid]
					for i, _ := range nantucketfrequencies {
						zswlval, err := strconv.ParseFloat(swllines[i+4], 64) //need to read all values and load into an array now.
						zhmo, err2 := strconv.ParseFloat(hmolines[i+4], 64)
						if err != nil {
							//panic(err)
							zswlvals[i] = nodata
							zhmovals[i] = nodata
						} else {
							if node.ZElev < 0 {
								zswlval = zswlval + node.ZElev //(minus a negative to get value above sea level...)
							}
							if zswlval == 0 {
								zswlval = nodata
							} else {
								zswlval *= 3.28084 //convert from meters to feet
							}
							zswlvals[i] = zswlval
							if err2 != nil {
								zhmovals[i] = nodata
							} else {
								if zhmo == 0 {
									zhmo = nodata
								} else {
									zhmo *= 3.28084 //convert from meters to feet
								}
								zhmovals[i] = zhmo
							}
						}

					}
					node.ZHm0 = zhmovals
					node.ZSwl = zswlvals
					nodes[nodeid] = node
				}
				loadData = false
			}

			if triangleCounter == 0 {
				fmt.Println("reading triangles")
			}
			triangleCounter += 1
			line := strings.Split(scanner.Text(), " ") //is there a way to group spaces?
			//triangleid|vertcount|a|b|c
			var triangleid, aidx, bidx, cidx int64
			triangleid = 0
			aidx = 0
			bidx = 0
			cidx = 0
			valcount := 0
			for _, v := range line {
				if v != "" {
					valcount += 1
					switch valcount {
					case 1:
						triangleid, _ = strconv.ParseInt(v, 10, 32)
					case 2:
						//skip count of vertices
						break
					case 3:
						aidx, _ = strconv.ParseInt(v, 10, 64)
					case 4:
						bidx, _ = strconv.ParseInt(v, 10, 64)
					case 5:
						cidx, _ = strconv.ParseInt(v, 10, 64)
					}
				}
			}
			a, aok := nodes[int32(aidx)]
			if !aok {
				panic(fmt.Sprintf("Not a ok! TriangleCounter is %v, a index is %v, and total triangles is %v", triangleCounter, aidx, dimTSize))
			}
			b, bok := nodes[int32(bidx)]
			if !bok {
				panic(fmt.Sprintf("Not b ok! TriangleCounter is %v, b index is %v, and total triangles is %v", triangleCounter, bidx, dimTSize))
			}
			c, cok := nodes[int32(cidx)]
			if !cok {
				panic(fmt.Sprintf("Not c ok! TriangleCounter is %v, c index is %v, and total triangles is %v", triangleCounter, cidx, dimTSize))
			}
			t := geometry.CreateTriangleZZ(&a, &b, &c)
			triangles[triangleid] = t

		} else {
			//must be in the hull def?
			break
		}
	}

	//should probably reduce the space to remove triangles.
	var tr rtree.RTree
	ps := make([]float64, 0)
	kept := 0
	culled := 0
	for _, triangle := range triangles {
		if triangle.HasData() {
			//add to tree?
			e := triangle.Extent()
			tr.Insert(e.LowerLeft.ToXY(), e.UpperRight.ToXY(), triangle)
			if e.Max()[0] > maxx {
				maxx = e.Max()[0]
			} else {
				if e.Min()[0] < minx {
					minx = e.Min()[0]
				}
			}
			if e.Max()[1] > maxy {
				maxy = e.Max()[1]
			} else {
				if e.Min()[1] < miny {
					miny = e.Min()[1]
				}
			}
			ps = append(ps, triangle.Points()...)
			kept += 1
		} else {
			culled += 1
		}
	}
	fmt.Println(fmt.Sprintf("kept %v, culled %v", kept, culled))
	fmt.Println("Finished reading, computing Hull")
	if len(ps) > 2 {
		flathull := ConcaveHull.Compute(ConcaveHull.FlatPoints(ps))
		ptcount := len(flathull) / 2
		hull := make([]geometry.Point, ptcount+1)
		index := 0
		for i := 0; i < len(flathull); i += 2 {
			hull[index] = geometry.Point{X: flathull[i], Y: flathull[i+1]}
			index++
		}
		hull[index] = geometry.Point{X: flathull[0], Y: flathull[1]}
		p := geometry.CreatePolygon(hull)
		return &geometry.Tin{MaxX: maxx, MinX: minx, MaxY: maxy, MinY: miny, Tree: tr, Hull: p}, err
	} else {
		fmt.Println("No Points, no Hull")
		return &geometry.Tin{MaxX: maxx, MinX: minx, MaxY: maxy, MinY: miny, Tree: tr}, err
	}

}

/*
logic is:
  - 1st read nodes
  - add probability arrays for SWL/Hm0 to each node
  - which probability runs?
  - 2nd build triangles using the nodes
  - build queryable rtree index on triangles
  - 3rd create concave hull
  - why do points option?
*/
func processGrdAndCSV(grdfp string, swlfp string) (*geometry.Tin, error) {
	grdf, err := os.Open(grdfp)
	if err != nil {
		panic(err)
	}
	defer grdf.Close()

	scanner := bufio.NewScanner(grdf)
	//we dont know how big the file will be, so we have to make a guess.
	scanner.Scan() // burn the header
	scanner.Scan() //count of triangles and points
	row2 := scanner.Text()
	vals := strings.Split(row2, "  ") //not sure this will always work correctly
	dimNSize, _ := strconv.ParseInt(vals[1], 10, 64)
	dimTSize, _ := strconv.ParseInt(vals[0], 10, 64)
	nodes := make(map[int32]geometry.PointZZ)
	nodeLookup := make(map[string]int32)
	triangles := make(map[int64]geometry.TriangleZZ)
	var triangleCounter int64
	var pointCounter int64
	triangleCounter = 0
	pointCounter = 0
	var minx, miny, maxx, maxy float64
	minx = 180
	miny = 180
	maxx = -180
	maxy = -180
	loadData := true
	terrainMismatches := 0
	for scanner.Scan() {
		if pointCounter < dimNSize { //points come first.
			if pointCounter == 0 {
				fmt.Println("reading nodes")
			}
			pointCounter += 1
			line := strings.Split(scanner.Text(), " ") //is there a way to group spaces?
			//nodeid|X|Y|Z
			var nodeid int32
			nodeid = 0
			xval := 0.0
			yval := 0.0
			zval := 0.0 //terrain
			valcount := 0
			for _, v := range line {
				if v != "" {
					valcount += 1
					switch valcount {
					case 1:
						tmpInt, err := strconv.ParseInt(v, 10, 32)
						if err != nil {
							panic(err)
						}
						nodeid = int32(tmpInt)
					case 2:
						xval, _ = strconv.ParseFloat(v, 64)
					case 3:
						yval, _ = strconv.ParseFloat(v, 64)
					case 4:
						zval, _ = strconv.ParseFloat(v, 64) //terrain
					}
				}
			}
			identifier := fmt.Sprintf("%f,%f", xval, yval)
			nodeLookup[identifier] = nodeid
			nodes[nodeid] = geometry.PointZZ{Point: &geometry.Point{X: xval, Y: yval}, ZElev: zval}
		} else if triangleCounter < dimTSize { //triangles come second.
			if loadData {
				//read the other two csv files to load in data into the nodes.
				swlf, err := os.Open(swlfp)
				if err != nil {
					panic(err)
				}
				defer swlf.Close()

				swlscanner := bufio.NewScanner(swlf)
				//swlscanner.Scan() // burn the header
				//swlscanner.Scan() //probabilities
				frequencies := CoastalFrequencies //[]float64{1e+01, 5e+00, 2e+00, 1e+00, 5e-01, 2e-01, 1e-01, 5e-02, 2e-02, 1e-02, 5e-03, 2e-03, 1e-03, 5e-04, 2e-04, 1e-04, 5e-05, 2e-05, 1e-05, 5e-06, 2e-06, 1e-06}
				nodata := 0.0

				for swlscanner.Scan() {
					//parse each row.
					swllines := strings.Split(swlscanner.Text(), ",")
					swlxval := 0.0
					swlyval := 0.0
					swlelev, _ := strconv.ParseFloat(swllines[2], 64)
					tmpx, _ := strconv.ParseFloat(swllines[0], 64)
					tmpy, _ := strconv.ParseFloat(swllines[1], 64)
					if tmpx > 0 {
						swlyval = tmpx
						swlxval = tmpy
					} else {
						swlxval = tmpx
						swlyval = tmpy
					}
					nodeidval := fmt.Sprintf("%f,%f", swlxval, swlyval)
					if err != nil {
						panic(err)
					}
					nodeid := nodeLookup[nodeidval]
					node := nodes[nodeid]
					if math.Abs(node.ZElev) != math.Abs(swlelev) {
						terrainMismatches += 1
					}
					//loop over z values
					zswlvals := make([]float64, len(frequencies))
					zhmovals := make([]float64, len(frequencies))

					for i, _ := range frequencies {
						zhmovals[i] = nodata
						zswlval, err := strconv.ParseFloat(swllines[i+3], 64) //need to read all values and load into an array now.
						if err != nil {
							//panic(err)
							zswlvals[i] = nodata
						} else {
							if node.ZElev < 0 {
								zswlval = zswlval + node.ZElev //(minus a negative to get value above sea level...)
							}
							if zswlval == 0 {
								zswlval = nodata
							} else {
								zswlval *= 3.28084 //convert from meters to feet
							}
							zswlvals[i] = zswlval
						}

					}

					node.ZHm0 = zhmovals
					node.ZSwl = zswlvals
					nodes[nodeid] = node
				}
				loadData = false
			}

			if triangleCounter == 0 {
				fmt.Println("reading triangles")
			}
			triangleCounter += 1
			line := strings.Split(scanner.Text(), " ") //is there a way to group spaces?
			//triangleid|vertcount|a|b|c
			var triangleid, aidx, bidx, cidx int64
			triangleid = 0
			aidx = 0
			bidx = 0
			cidx = 0
			valcount := 0
			for _, v := range line {
				if v != "" {
					valcount += 1
					switch valcount {
					case 1:
						triangleid, _ = strconv.ParseInt(v, 10, 32)
					case 2:
						//skip count of vertices
						break
					case 3:
						aidx, _ = strconv.ParseInt(v, 10, 64)
					case 4:
						bidx, _ = strconv.ParseInt(v, 10, 64)
					case 5:
						cidx, _ = strconv.ParseInt(v, 10, 64)
					}
				}
			}
			a, aok := nodes[int32(aidx)]
			if !aok {
				panic(fmt.Sprintf("Not a ok! TriangleCounter is %v, a index is %v, and total triangles is %v", triangleCounter, aidx, dimTSize))
			}
			b, bok := nodes[int32(bidx)]
			if !bok {
				panic(fmt.Sprintf("Not b ok! TriangleCounter is %v, b index is %v, and total triangles is %v", triangleCounter, bidx, dimTSize))
			}
			c, cok := nodes[int32(cidx)]
			if !cok {
				panic(fmt.Sprintf("Not c ok! TriangleCounter is %v, c index is %v, and total triangles is %v", triangleCounter, cidx, dimTSize))
			}
			t := geometry.CreateTriangleZZ(&a, &b, &c)
			triangles[triangleid] = t

		} else {
			//must be in the hull def?
			break
		}
	}

	//should probably reduce the space to remove triangles.
	var tr rtree.RTree
	ps := make([]float64, 0)
	kept := 0
	culled := 0
	for _, triangle := range triangles {
		if triangle.HasData() {
			//add to tree?
			e := triangle.Extent()
			tr.Insert(e.LowerLeft.ToXY(), e.UpperRight.ToXY(), triangle)
			if e.Max()[0] > maxx {
				maxx = e.Max()[0]
			} else {
				if e.Min()[0] < minx {
					minx = e.Min()[0]
				}
			}
			if e.Max()[1] > maxy {
				maxy = e.Max()[1]
			} else {
				if e.Min()[1] < miny {
					miny = e.Min()[1]
				}
			}
			ps = append(ps, triangle.Points()...)
			kept += 1
		} else {
			culled += 1
		}
	}
	fmt.Println(fmt.Sprintf("kept %v, culled %v", kept, culled))
	fmt.Println(fmt.Sprintf("found %v terrain mismatches", terrainMismatches))
	fmt.Println("Finished reading, computing Hull")
	if len(ps) > 2 {
		flathull := ConcaveHull.Compute(ConcaveHull.FlatPoints(ps))
		ptcount := len(flathull) / 2
		hull := make([]geometry.Point, ptcount+1)
		index := 0
		for i := 0; i < len(flathull); i += 2 {
			hull[index] = geometry.Point{X: flathull[i], Y: flathull[i+1]}
			index++
		}
		hull[index] = geometry.Point{X: flathull[0], Y: flathull[1]}
		p := geometry.CreatePolygon(hull)
		return &geometry.Tin{MaxX: maxx, MinX: minx, MaxY: maxy, MinY: miny, Tree: tr, Hull: p}, err
	} else {
		fmt.Println("No Points, no Hull")
		return &geometry.Tin{MaxX: maxx, MinX: minx, MaxY: maxy, MinY: miny, Tree: tr}, err
	}

}
