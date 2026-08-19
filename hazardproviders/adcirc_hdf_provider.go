package hazardproviders

import (
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/HydrologicEngineeringCenter/go-coastal/geometry"
	// "github.com/HydrologicEngineeringCenter/consequences-runner/geometry"
	"github.com/HydrologicEngineeringCenter/go-statistics/statistics"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/furstenheim/ConcaveHull"
	"github.com/tidwall/rtree"
)

/*
@Notes:
 - AcNode map is indexed by AdcircNode id
 - AcNode and AcTriangles are joined via AdcircNode id
 - probability data is joined by AcNode.Index=probability ordinal position
*/

var CHL_HDFGRID_TC string = "Elements"
var CHL_HDFGRID_NI string = "Nodes"
var CHL_HDFGRID_NIDX string = "ADCIRC Node IDs"
var CHL_HDFGRID_AEF string = "AEF Values"
var NODATA float64 = -9999999.0

// type HazardProvider interface {
// 	ProvideHazardBoundary() (geography.BBox, error)
// 	//NextElement() []float64
// 	SelectFrequency(index int)
// 	ProvideHazard(location geography.Location) (hazards.HazardEvent, error)
// 	ProvideHazards(location geography.Location) ([]hazards.HazardEvent, error)
// 	Close()
// }

type AcNode struct {
	Point AcPoint
	//Index      int32 //nodes oridinal position in hdf dataset.  Should be equyal to "model node id-1"
	AdcircNode int32
	ZHm0       []float64
	ZSwl       []float64
	ZHm0stdev  []float64
	ZSwlstdev  []float64
}

func (an AcNode) PointWithPayload() geometry.PointWithPayload {
	data := make(map[geometry.Parameter][]statistics.ContinuousDistribution)
	ele := make([]statistics.ContinuousDistribution, 1)
	dist, err := statistics.InitDeterministic(an.Point.Z)
	if err != nil {
		log.Print("bummer.")
		//panic?
		panic(err)
	}
	ele[0] = dist
	data[geometry.Terrain] = ele
	swldists := make([]statistics.ContinuousDistribution, len(an.ZHm0))
	hmodists := make([]statistics.ContinuousDistribution, len(an.ZHm0))
	for idx, _ := range an.ZHm0 {
		swldists[idx] = statistics.NormalDistribution{Mean: an.ZSwl[idx], StandardDeviation: 0} //an.ZSwlstdev[idx]}
		hmodists[idx] = statistics.NormalDistribution{Mean: an.ZHm0[idx], StandardDeviation: 0} //an.ZHm0stdev[idx]}
	}
	data[geometry.SWL] = swldists
	data[geometry.HM0] = hmodists
	return geometry.PointWithPayload{
		Point: &geometry.Point{
			X: an.Point.X,
			Y: an.Point.Y,
		},
		Data: data,
	}
}

type AcTriangle struct {
	ElementId int32
	NodeA     int32 //adcircnode id
	NodeB     int32 //adcircnode id
	NodeC     int32 //adcircnode id
	//UrbLat    float64
	//UrbLon    float64
	//LrbLat    float64
	//LrbLon    float64
}

func (at AcTriangle) TriangleWithPayload(nodes map[int32]AcNode) geometry.TriangleWithPayload {
	n1 := nodes[at.NodeA].PointWithPayload()
	n2 := nodes[at.NodeB].PointWithPayload()
	n3 := nodes[at.NodeC].PointWithPayload()
	return geometry.CreateTriangleWithPayload(&n1, &n2, &n3)
}

type AcPoint struct {
	X float64
	Y float64
	Z float64
}

// /////////////////////////////////////////////////////////
// ///////////////HDF ADCERC HAZARD PROVIDER///////////////
type HdfAdcercHazardProvider struct {
	ds                       *geometry.Tin
	queryCount               int64
	actualComputedStructures int64
	computeStart             time.Time
	frequencies              []float64
	selectedFrequency        int
}

func NewHdfAdcercHazardProvider(grdfile string, probSwlFile string, probHmoFile string, dataset string) (*HdfAdcercHazardProvider, error) {
	triangles, err := ReadTriangles(grdfile)
	if err != nil {
		return nil, err
	}

	nodes, err := ReadNodes(grdfile)
	if err != nil {
		return nil, err
	}

	aef, err := ReadAEF(probSwlFile)
	if err != nil {
		return nil, err
	}
	//for i, v := range aef {
	//	aef[i] = 1.0 / v
	//}

	probSwl, err := ReadProbabilities(probSwlFile, dataset)
	if err != nil {
		return nil, err
	}
	nodeIdxSwl, err := ReadNodeTable(probSwlFile)
	if err != nil {
		return nil, err
	}
	probHmo, err := ReadProbabilities(probHmoFile, dataset)
	if err != nil {
		return nil, err
	}
	nodeIdxHmo, err := ReadNodeTable(probHmoFile)
	if err != nil {
		return nil, err
	}
	hzp := HdfAdcercHazardBuilder{
		triangles:    triangles,
		nodes:        nodes,
		probabilites: aef,
		nodeIdxHmo:   nodeIdxHmo,
		nodeIdxSwl:   nodeIdxSwl,
		probHmo:      probHmo,
		probSwl:      probSwl,
		stdevHmo:     &HdfDataset{},
		stdevSwl:     &HdfDataset{},
	}
	err = hzp.assignProbsToNodes()
	c := time.Now()
	tin := hzp.buildTin()
	return &HdfAdcercHazardProvider{ds: tin, computeStart: c, frequencies: aef}, nil
}

func ReadNodeTable(hdfFilePath string) (map[int32]int32, error) {
	nodalOptions := HdfReadOptions{
		Dtype:           reflect.Float64,
		IncrementalRead: true,
		IncrementSize:   1000, //read 1000 rows at a time
	}
	nodeData, err := NewHdfDataset(hdfFilePath, CHL_HDFGRID_NIDX, nodalOptions)
	if err != nil {
		return nil, err
	}
	defer nodeData.Close()

	nodeIdxs := make(map[int32]int32)

	row := []float64{}
	for i := 0; i < nodeData.Rows(); i++ {
		nodeData.ReadRow(i, &row)
		nodeIdxs[int32(row[0])] = int32(i)
	}
	return nodeIdxs, nil
}
func (hazP *HdfAdcercHazardProvider) SelectFrequency(index int) {
	hazP.selectedFrequency = index
}
func (hazP *HdfAdcercHazardProvider) ProvideHazards(l geography.Location) ([]hazards.HazardEvent, error) {
	hazP.queryCount++
	//check if point is in the hull polygon.
	p := geometry.Point{X: l.X, Y: l.Y}
	if hazP.queryCount%100000 == 0 {
		n := time.Since(hazP.computeStart)
		fmt.Print("Compute Time: ")
		fmt.Println(n)
		fmt.Println(fmt.Sprintf("Processed %v structures, with %v valid depths", hazP.queryCount, hazP.actualComputedStructures))
	}
	if hazP.ds.Hull.Contains(p) {
		v, err := hazP.ds.ComputeValues(l.X, l.Y)
		if err != nil {
			return nil, err
		}
		hazP.actualComputedStructures++
		return v, nil
	}
	notIn := hazardproviders.NoHazardFoundError{Input: "Point Not In Polygon"}
	return nil, notIn
}
func (hazP *HdfAdcercHazardProvider) ProvideHazard(l geography.Location) (hazards.HazardEvent, error) {
	hazP.queryCount++
	//check if point is in the hull polygon.
	p := geometry.Point{X: l.X, Y: l.Y}
	if hazP.queryCount%100000 == 0 {
		n := time.Since(hazP.computeStart)
		fmt.Print("Compute Time: ")
		fmt.Println(n)
		fmt.Println(fmt.Sprintf("Processed %v structures, with %v valid depths", hazP.queryCount, hazP.actualComputedStructures))
	}
	if hazP.ds.Hull.Contains(p) {
		v, err := hazP.ds.ComputeValues(l.X, l.Y)
		if err != nil {
			return nil, err
		}
		hazP.actualComputedStructures++

		return v[hazP.selectedFrequency], nil
	}
	notIn := hazardproviders.NoHazardFoundError{Input: "Point Not In Polygon"}
	return nil, notIn
}
func (hazP *HdfAdcercHazardProvider) ProvideHazardBoundary() (geography.BBox, error) {
	bbox := make([]float64, 4)
	bbox[0] = hazP.ds.MinX //upper left x
	bbox[1] = hazP.ds.MaxY //upper left y
	bbox[2] = hazP.ds.MaxX //lower right x
	bbox[3] = hazP.ds.MinY //lower right y
	return geography.BBox{Bbox: bbox}, nil
}

func (hazP *HdfAdcercHazardProvider) Frequencies() []float64 {
	return hazP.frequencies
}

func (hazP *HdfAdcercHazardProvider) Close() {
	n := time.Since(hazP.computeStart)
	fmt.Print("Compute Complete")
	fmt.Print("Compute Time was: ")
	fmt.Println(n)
	fmt.Println(fmt.Sprintf("Processed %v structures, with %v valid depths", hazP.queryCount, hazP.actualComputedStructures))

}

///////////////////////////////////////////////////////////
/////////////////HDF ADCERC HAZARD Builder/////////////////

// @TODO need to make sure we close all datasets
type HdfAdcercHazardBuilder struct {
	triangles    map[int32]AcTriangle
	nodes        map[int32]AcNode
	probabilites []float64
	nodeIdxHmo   map[int32]int32
	nodeIdxSwl   map[int32]int32
	probHmo      *HdfDataset
	probSwl      *HdfDataset
	stdevHmo     *HdfDataset
	stdevSwl     *HdfDataset
}

func (hzp *HdfAdcercHazardBuilder) buildTin() *geometry.Tin {
	var minx, miny, maxx, maxy float64
	minx = 180
	miny = 180
	maxx = -180
	maxy = -180
	var tr rtree.RTree
	ps := make([]float64, 0)
	kept := 0
	culled := 0
	for _, t := range hzp.triangles {
		triangle := t.TriangleWithPayload(hzp.nodes)
		if triangle.HasData() {
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
	log.Println(fmt.Sprintf("kept %v, culled %v", kept, culled))
	log.Println("Finished reading, computing Hull")
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
		return &geometry.Tin{MaxX: maxx, MinX: minx, MaxY: maxy, MinY: miny, Tree: tr, Hull: p}
	} else {
		log.Println("No Points, no Hull")
		return &geometry.Tin{MaxX: maxx, MinX: minx, MaxY: maxy, MinY: miny, Tree: tr}
	}

}

func (hzp *HdfAdcercHazardBuilder) assignProbsToNodes() error {
	swlRow := []float64{}
	hmoRow := []float64{}
	for _, node := range hzp.nodes {
		err := hzp.probSwl.ReadRow(int(hzp.nodeIdxSwl[node.AdcircNode]), &swlRow)
		if err != nil {
			return err
		}
		swl := processProbRow(swlRow, hzp.nodes[node.AdcircNode].Point.Z)

		err = hzp.probHmo.ReadRow(int(hzp.nodeIdxHmo[node.AdcircNode]), &hmoRow)
		if err != nil {
			return err
		}
		hmo := processProbRow(hmoRow, 0.0) //do not modify hmo based on ground elevation//hzp.nodes[node.AdcircNode].Point.Z)
		node.ZSwl = swl
		node.ZHm0 = hmo
		hzp.nodes[node.AdcircNode] = node
	}
	return nil
}

func processProbRow(probs []float64, nodeElev float64) []float64 {
	for i := 0; i < len(probs); i++ {
		val := probs[i]
		if nodeElev < 0 {
			val = val + nodeElev
		}
		if val == 0 { //@QUESTION: without a tolerance, is this comparison useful?
			val = NODATA //@QUESTION: since NODATA==0, why are we doing this?
		} else {
			val = val * 3.28084 //convert to feet
		}
		probs[i] = val
	}
	return probs
}

/////////////////////////////////////////////////////////////////
/////UTILITY Reading Functions for In Memory Privider////////////

// @QUESTION: is a 32bit int large enough for big models?
// @QUESTION: is LACS a study, hence we use a study node?
// should be able to remove all functions below this point.
func ReadTriangles(hdfFilePath string) (map[int32]AcTriangle, error) {
	triangularConnsOptions := HdfReadOptions{
		Dtype:           reflect.Float64,
		IncrementalRead: true,
		IncrementSize:   1000, //read 1000 rows at a time
	}
	tc, err := NewHdfDataset(hdfFilePath, CHL_HDFGRID_TC, triangularConnsOptions)
	if err != nil {
		return nil, err
	}
	defer tc.Close()
	triangles := make(map[int32]AcTriangle)

	row := []float64{}
	for i := 0; i < tc.Rows(); i++ {
		tc.ReadRow(i, &row)

		node := AcTriangle{
			ElementId: int32(row[0]),
			NodeA:     int32(row[2]), //index 1 is number of nodes in an element.
			NodeB:     int32(row[3]),
			NodeC:     int32(row[4]),
		}
		triangles[node.ElementId] = node
	}
	return triangles, nil
}

func ReadNodes(hdfFilePath string) (map[int32]AcNode, error) {

	nodalOptions := HdfReadOptions{
		Dtype:           reflect.Float64,
		IncrementalRead: true,
		IncrementSize:   1000, //read 1000 rows at a time
	}
	nodeData, err := NewHdfDataset(hdfFilePath, CHL_HDFGRID_NI, nodalOptions)
	if err != nil {
		return nil, err
	}
	defer nodeData.Close()

	nodes := make(map[int32]AcNode)

	row := []float64{}
	for i := 0; i < nodeData.Rows(); i++ {
		nodeData.ReadRow(i, &row)

		node := AcNode{
			Point: AcPoint{
				X: row[2],
				Y: row[1],
				Z: row[3],
			},
			AdcircNode: int32(row[0]),
		}
		nodes[node.AdcircNode] = node
	}
	return nodes, nil
}

func ReadAEF(hdfFilePath string) ([]float64, error) {
	options := HdfReadOptions{
		Dtype: reflect.Float64,
	}
	dataset, err := NewHdfDataset(hdfFilePath, CHL_HDFGRID_AEF, options)
	if err != nil {
		return nil, err
	}
	defer dataset.Close()

	cols := dataset.Cols()
	data := make([]float64, cols)
	dataset.ReadInto(&data)
	return data, nil
}

func ReadProbabilities(hdfFilePath string, hdfDataPath string) (*HdfDataset, error) {
	options := HdfReadOptions{
		Dtype:        reflect.Float64,
		ReadOnCreate: true,
	}
	return NewHdfDataset(hdfFilePath, hdfDataPath, options)
}
