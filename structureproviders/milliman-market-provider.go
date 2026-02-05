package structureproviders

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/structures"
	"github.com/dewberry/gdal"
)

type MillimanDataSet struct {
	FilePath              string
	assets                []MillimanAsset
	OccTypeProvider       structures.JsonOccupancyTypeProvider
	FoundationUncertainty *structures.FoundationUncertainty
	deterministic         bool
	seed                  int64
}
type MillimanAsset struct {
	acctnum              string
	location             string
	bldg_ded             float64
	bldg_limit           float64
	cnt_ded              float64
	cnt_limit            float64
	state                string
	postcode             int
	lon                  float64
	lat                  float64
	bldg_val             float64
	cnt_val              float64
	const_code           Construction_Type //1 frame 2 masonary
	num_stories          int
	yr_built             int
	foundation_type      Foundation_Type //2=basement, 4 crawlspace, 6 pier, 7 fill or wall, 8 slab, 9 pile
	basement             Basement_Type   //0 no basement, 1 unfinished basement, 2 finished basement.
	first_floor_elev     int             //seemingly this is foundation height not ffe.
	base_flood_elevation float64         //unknown or int in the files i see... must convert unknown to -901 or something.
	elev_ft              float64
}
type Basement_Type int

const (
	NONE       Basement_Type = 0
	FINISHED   Basement_Type = 2
	UNFINISHED Basement_Type = 1
)

type Construction_Type int

func (c Construction_Type) toString() string {
	switch c {
	case FRAME:
		return "Frame"
	case MASONARY:
		return "Masonary"
	default:
		return "Frame"
	}
}

const (
	FRAME    Construction_Type = 1
	MASONARY Construction_Type = 2
)

type Foundation_Type int

func (f Foundation_Type) toString() string {
	switch f {
	case BASEMEMENT:
		return "Basement"
	case CRAWLSPACE:
		return "Crawl"
	case PIER:
		return "Pier"
	case WALL_OR_FILL:
		return "WALL_OR_FILL"
	case SLAB:
		return "Slab"
	case PILE:
		return "Pile"
	default:
		return "slab"
	}
}

const (
	BASEMEMENT   Foundation_Type = 2
	CRAWLSPACE   Foundation_Type = 4
	PIER         Foundation_Type = 6
	WALL_OR_FILL Foundation_Type = 7
	SLAB         Foundation_Type = 8
	PILE         Foundation_Type = 9
)

func (a MillimanAsset) toStructureDeterministic(m map[string]structures.OccupancyTypeDeterministic) structures.StructureDeterministic {
	basementType := "NB"
	if a.basement != NONE {
		basementType = "WB" //currenly not supporting finished vs not finished.
	}
	if a.foundation_type == CRAWLSPACE {
		basementType += "-C"
	}
	if a.foundation_type == PIER {
		basementType += "-P"
	}
	occtype := fmt.Sprintf("RES1-%vS%v", a.num_stories, basementType)
	//add construction type
	ot, ok := m[occtype]
	if !ok {
		ot = m["RES1-1SNB"]
	}
	s := structures.StructureDeterministic{
		BaseStructure: structures.BaseStructure{
			Name:            a.acctnum,
			DamCat:          "RES",
			CBFips:          "12345",
			X:               a.lon,
			Y:               a.lat,
			GroundElevation: a.elev_ft,
		},
		FoundType:        a.foundation_type.toString(),
		FirmZone:         "unknown",
		ConstructionType: a.const_code.toString(),
		StructVal:        a.bldg_val,
		ContVal:          a.cnt_val,
		FoundHt:          float64(a.first_floor_elev),
		NumStories:       int32(a.num_stories),
		OccType:          ot,
	}
	return s
}
func (a MillimanAsset) toStructureStochastic(m map[string]structures.OccupancyTypeStochastic) structures.StructureStochastic {
	basementType := "NB"
	if a.basement != NONE {
		basementType = "WB"
	}
	if a.foundation_type == CRAWLSPACE {
		basementType += "-C"
	}
	if a.foundation_type == PIER {
		basementType += "-P"
	}
	occtype := fmt.Sprintf("RES1-%vS%v", a.num_stories, basementType)
	//add construction type
	ot, ok := m[occtype]
	if !ok {
		ot = m["RES1-1SNB"]
	}
	s := structures.StructureStochastic{
		BaseStructure: structures.BaseStructure{
			Name:            a.acctnum,
			DamCat:          "RES",
			CBFips:          "12345",
			X:               a.lon,
			Y:               a.lat,
			GroundElevation: a.elev_ft,
		},
		FoundType:        a.foundation_type.toString(),
		FirmZone:         "unknown",
		ConstructionType: a.const_code.toString(),
		StructVal:        consequences.ParameterValue{Value: a.bldg_val},
		ContVal:          consequences.ParameterValue{Value: a.cnt_val},
		FoundHt:          consequences.ParameterValue{Value: float64(a.first_floor_elev)},
		NumStories:       int32(a.num_stories),
		OccType:          ot,
	}
	return s
}
func (mds *MillimanDataSet) filter(bbox geography.BBox) *MillimanDataSet {
	newds := MillimanDataSet{
		FilePath:              mds.FilePath,
		OccTypeProvider:       mds.OccTypeProvider,
		deterministic:         mds.deterministic,
		seed:                  mds.seed,
		FoundationUncertainty: mds.FoundationUncertainty,
	}
	newAssets := make([]MillimanAsset, 0)
	for _, a := range mds.assets {
		if contains(bbox, geography.Location{X: a.lon, Y: a.lat}) {
			newAssets = append(newAssets, a)
		}
	}
	newds.assets = newAssets
	return &newds
}
func contains(bbox geography.BBox, p geography.Location) bool {
	return bbox.Bbox[0] <= p.X && p.X <= bbox.Bbox[2] && bbox.Bbox[3] <= p.Y && p.Y <= bbox.Bbox[1]
}
func InitMillimanStructureProvider(filepath string) (*MillimanDataSet, error) {
	//validation?
	gpk, err := initalizestructureprovider(filepath)
	return &gpk, err
}
func InitMillimanStructureProviderwithOcctypePath(filepath string, occtypefp string) (*MillimanDataSet, error) {
	//validation?
	gpk, err := initalizestructureprovider(filepath)
	gpk.setOcctypeProvider(true, occtypefp)
	return &gpk, err
}
func (ds *MillimanDataSet) UpdateFoundationHeightUncertainty(useFile bool, foundationHeightUncertaintyJsonFilePath string) {
	if useFile {
		fh, err := structures.InitFoundationUncertaintyFromFile(foundationHeightUncertaintyJsonFilePath)
		if err != nil {
			fh, _ = structures.InitFoundationUncertainty()
		}
		ds.FoundationUncertainty = fh
	} else {
		fh, _ := structures.InitFoundationUncertainty()
		ds.FoundationUncertainty = fh
	}
}
func tofloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(err)
	}
	return f
}
func toint(s string) int {
	i, err := strconv.ParseInt(s, 10, 0)
	if err != nil {
		panic(err)
	}
	return int(i)
}
func tobfe(s string) float64 {
	if s == "unknown" {
		return -901.00
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(err)
	}
	return f
}
func toConstruction(s string) Construction_Type {
	switch s {
	case "1":
		return FRAME
	case "2":
		return MASONARY
	default:
		return FRAME
	}
}
func tofoundationtype(s string) Foundation_Type {
	switch s {
	case "2":
		return BASEMEMENT
	case "4":
		return CRAWLSPACE
	case "6":
		return PIER
	case "7":
		return WALL_OR_FILL
	case "8":
		return SLAB
	case "9":
		return PILE
	default:
		return SLAB
	}
}
func tobasement(s string) Basement_Type {
	switch s {
	case "0":
		return NONE
	case "1":
		return UNFINISHED
	case "2":
		return FINISHED
	default:
		return NONE
	}
}
func initalizestructureprovider(filepath string) (MillimanDataSet, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return MillimanDataSet{}, err
	}
	rows := strings.Split(string(data), "\r\n")
	assets := make([]MillimanAsset, len(rows)-2)
	for i, r := range rows {
		if i != 0 {
			vals := strings.Split(r, ",")
			if len(vals) == 21 {
				asset := MillimanAsset{
					acctnum:              vals[0],
					location:             vals[1],
					bldg_ded:             tofloat(vals[2]),
					bldg_limit:           tofloat(vals[3]),
					cnt_ded:              tofloat(vals[4]),
					cnt_limit:            tofloat(vals[5]),
					state:                vals[6],
					postcode:             toint(vals[7]),
					lon:                  tofloat(vals[9]),
					lat:                  tofloat(vals[10]),
					bldg_val:             tofloat(vals[11]),
					cnt_val:              tofloat(vals[12]),
					const_code:           toConstruction(vals[13]),
					num_stories:          toint(vals[14]),
					yr_built:             toint(vals[15]),
					foundation_type:      tofoundationtype(vals[16]),
					basement:             tobasement(vals[17]),
					first_floor_elev:     toint(vals[18]),
					base_flood_elevation: tobfe(vals[19]),
					elev_ft:              tofloat(vals[20]),
				}
				assets[i-1] = asset
			}
		}
	}
	otp := structures.JsonOccupancyTypeProvider{}
	otp.InitDefault()
	fh, err := structures.InitFoundationUncertainty()
	if err != nil {
		return MillimanDataSet{}, err
	}
	m := MillimanDataSet{
		FilePath:              filepath,
		assets:                assets,
		OccTypeProvider:       otp,
		deterministic:         true,
		seed:                  1234,
		FoundationUncertainty: fh,
	}

	return m, nil
}
func (mds *MillimanDataSet) setOcctypeProvider(useFilepath bool, filepath string) {
	if useFilepath {
		otp := structures.JsonOccupancyTypeProvider{}
		otp.InitLocalPath(filepath)
		mds.OccTypeProvider = otp
	} else {
		otp := structures.JsonOccupancyTypeProvider{}
		otp.InitDefault()
		mds.OccTypeProvider = otp
	}
}
func (mds *MillimanDataSet) SetDeterministic(useDeterministic bool) {
	mds.deterministic = useDeterministic
}
func (mds *MillimanDataSet) SetSeed(seed int64) {
	mds.seed = seed
}
func (mds *MillimanDataSet) SpatialReference() string {
	sr := gdal.CreateSpatialReference("")
	sr.FromEPSG(4326)
	wkt, err := sr.ToWKT()
	if err != nil {
		return ""
	}
	return wkt
}
func (mds *MillimanDataSet) UpdateSpatialReference(sr_wkt string) {
	// unimplemented
	fmt.Println("could not set spatial reference")
}

// StreamByFips a streaming service for structure stochastic based on a bounding box
func (mds *MillimanDataSet) ByFips(fipscode string, sp consequences.StreamProcessor) {
	if mds.deterministic {
		mds.processFipsStreamDeterministic(fipscode, sp)
	} else {
		mds.processFipsStream(fipscode, sp)
	}

}
func (mds *MillimanDataSet) processFipsStream(fipscode string, sp consequences.StreamProcessor) {
	m := mds.OccTypeProvider.OccupancyTypeMap()
	//define a default occtype in case of emergancy
	r := rand.New(rand.NewSource(mds.seed))
	//filter mds by fips (no fips available so process the whole inventory)
	for _, a := range mds.assets {
		s := a.toStructureStochastic(m)
		s.ApplyFoundationHeightUncertanty(mds.FoundationUncertainty)
		s.UseUncertainty = true
		sd := s.SampleStructure(r.Int63())
		sp(sd)
	}
}
func (mds *MillimanDataSet) processFipsStreamDeterministic(fipscode string, sp consequences.StreamProcessor) {
	m := mds.OccTypeProvider.OccupancyTypeMap()
	m2 := swapOcctypeMap(m)
	//filter by fips (no fips available so doing the whole inventory.)
	for _, a := range mds.assets {
		s := a.toStructureDeterministic(m2)
		sp(s)

	}
}
func (mds *MillimanDataSet) ByBbox(bbox geography.BBox, sp consequences.StreamProcessor) {
	if mds.deterministic {
		mds.processBboxStreamDeterministic(bbox, sp)
	} else {
		mds.processBboxStream(bbox, sp)
	}

}
func (mds *MillimanDataSet) processBboxStream(bbox geography.BBox, sp consequences.StreamProcessor) {
	m := mds.OccTypeProvider.OccupancyTypeMap()
	r := rand.New(rand.NewSource(mds.seed))
	ds := mds.filter(bbox)
	for _, a := range ds.assets {
		s := a.toStructureStochastic(m)
		s.ApplyFoundationHeightUncertanty(mds.FoundationUncertainty)
		s.UseUncertainty = true
		sd := s.SampleStructure(r.Int63())
		sp(sd)
	}
}

func (mds *MillimanDataSet) processBboxStreamDeterministic(bbox geography.BBox, sp consequences.StreamProcessor) {
	m := mds.OccTypeProvider.OccupancyTypeMap()
	m2 := swapOcctypeMap(m)
	ds := mds.filter(bbox)
	for _, a := range ds.assets {
		s := a.toStructureDeterministic(m2)
		sp(s)

	}
}

func swapOcctypeMap(
	m map[string]structures.OccupancyTypeStochastic,
) map[string]structures.OccupancyTypeDeterministic {
	m2 := make(map[string]structures.OccupancyTypeDeterministic)
	for name, ot := range m {
		m2[name] = ot.CentralTendency()
	}
	return m2
}
