package hazardproviders

import (
	"encoding/csv"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/USACE/go-consequences/geography"
	gc "github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/dewberry/gdal"
)

type stormsimLifecycleMultiHazardProvider struct {
	// specify drivers for each?
	arrivals  []time.Time
	depths    []float64
	durations []float64
	process   gc.HazardFunction
	bbox      geography.BBox
}

func InitStormSim(eventsFP string, eventsDriver string, responsesFP string, responsesDriver string, reachesFP string, reachesDriver string) (stormsimLifecycleMultiHazardProvider, error) {

	// parse eventsFile
	// not needed. all info in eventsFile is also in stormResponsesFile

	//TODO: check file type. For now assume csv
	eventsFile, err := os.Open(eventsFP)
	if err != nil {
		panic(err)
	}
	eventReader := csv.NewReader(eventsFile)
	rows, err := eventReader.ReadAll()
	if err != nil {
		panic(err)
	}
	storm_ids := []string{} // will use the length of this slice to pre-allocate slice sizes for parsed storms
	for _, row := range rows[1:] {
		lifecycle, err := strconv.Atoi(row[1])
		if err != nil {
			panic(err)
		}
		storm_id := row[8]
		if lifecycle == 0 { // allow user to specify lifecycle?
			// compute for multiHazard currently doesn't support multiple lifecycles
			storm_ids = append(storm_ids, storm_id)
		}
	}

	arrivals, depths, durations, err := parseResponsesFile(responsesFP, "", eventsDriver, len(storm_ids))

	// get bbox from reachesFile
	//TODO: read geometry from gpkg
	// using csv for now
	reachesFile, err := os.Open(reachesFP)
	if err != nil {
		panic(err)
	}
	reachesReader := csv.NewReader(reachesFile)
	rows2, err := reachesReader.ReadAll()
	if err != nil {
		panic(err)
	}
	coords := rows2[1]
	xmin, err := strconv.ParseFloat(coords[1], 64)
	if err != nil {
		panic(err)
	}
	ymin, err := strconv.ParseFloat(coords[2], 64)
	if err != nil {
		panic(err)
	}
	xmax, err := strconv.ParseFloat(coords[3], 64)
	if err != nil {
		panic(err)
	}
	ymax, err := strconv.ParseFloat(coords[4], 64)
	if err != nil {
		panic(err)
	}
	b := geography.BBox{
		Bbox: []float64{xmin, ymin, xmax, ymax},
	}

	return stormsimLifecycleMultiHazardProvider{
		arrivals:  arrivals,
		depths:    depths,
		durations: durations,
		process:   gc.ArrivalDepthAndDurationHazardFunction(),
		bbox:      b,
	}, nil
}

// type gdalDataSet struct {
// 	FilePath  string
// 	LayerName string
// 	schemaIDX []int
// 	ds        *gdal.DataSource
// }

func eventsSchema() []string {
	s := []string{"location_id", "lifecycle", "year_offset", "year", "month", "day", "hour", "timestamp", "storm_id"}
	return s
}

func responsesSchema() []string {
	s := []string{"location_id", "date", "storm_id", "lifecycle", "stage"}
	return s
}

func reachesSchema() []string {
	s := []string{"Name", "NameAbbreviated", "Note", "NOAAStation", "NOAAStationName", "SavePoint", "geom"}
	return s
}

func parseResponsesFile(filepath string, layername string, driver string, n int) ([]time.Time, []float64, []float64, error) {
	arrivals := make([]time.Time, n)
	depths := make([]float64, n)
	durations := make([]float64, n)
	//TODO: combine arrivals, depths, durations into an ADD struct to reduce number of return values

	d := gdal.OGRDriverByName(driver)
	// for i := 0; i < gdal.OGRDriverCount(); i++ { // can use this to ensure given driver is available
	// 	fmt.Println(gdal.OGRDriverByIndex(i).Name())
	// }

	ds, dsok := d.Open(filepath, int(gdal.ReadOnly))
	if !dsok {
		return arrivals, depths, durations, errors.New("error opening responses file of type " + driver)
	}

	layer := ds.LayerByIndex(0)
	def := layer.Definition()

	s := responsesSchema()
	sIDX := make([]int, len(s))

	for i, f := range s {
		idx := def.FieldIndex(f)
		if idx < 0 {
			return arrivals, depths, durations, errors.New("gdal dataset at path " + filepath + " Expected field named " + f + " none was found")
		}
		sIDX[i] = idx
	}

	// fmt.Println(layer.Name())
	// fmt.Println(layer.Definition().FieldCount())

	// for j := 0; j < layer.Definition().FieldCount(); j++ {
	// 	fmt.Println(layer.Definition().FieldDefinition(j).Name())
	// }

	fc, _ := layer.FeatureCount(true)
	// fmt.Printf("The layer has %v features\n", fc)
	var curStormIndex int = 0
	var curStormID string
	var curStormLifecycle int
	var curStormStart time.Time
	var curStormEnd time.Time
	var curStormPeakStage float64

	for i := range fc {
		feat := layer.NextFeature()
		if feat != nil {
			// "location_id", "date", "storm_id", "lifecycle", "stage"
			d := feat.FieldAsString(sIDX[1]) // also returns a bool. Does true mean ok?
			date_i, err := time.Parse("2006-01-02 15:04:05", d)
			if err != nil {
				panic(err)
			}
			storm_id := feat.FieldAsString(sIDX[2])
			lifecycle_i := feat.FieldAsInteger(sIDX[3])
			stage_i := feat.FieldAsFloat64(sIDX[4]) // how to handle possible NA values here?

			if (storm_id == curStormID) && (lifecycle_i == curStormLifecycle) {
				curStormEnd = date_i
				if stage_i > curStormPeakStage {
					curStormPeakStage = stage_i
					// curStormPeakTime = date_i
				}
			} else {
				// we've reached a new storm in the series
				// save results from previous storm
				if i > 0 { // can't save previous storm on row 0
					if curStormLifecycle == 0 { // not handling multiple lifecycles currently
						arrivals[curStormIndex] = curStormStart
						depths[curStormIndex] = curStormPeakStage
						duration := curStormEnd.Sub(curStormStart)
						durations[curStormIndex] = duration.Hours() / 24.0
						curStormIndex++
					}
				}
				curStormID = storm_id
				curStormLifecycle = lifecycle_i
				curStormStart = date_i
				curStormEnd = date_i
				curStormPeakStage = stage_i
				// curStormPeakTime = date_i
			}
		}
	}
	// need to add final storm details to slices
	if curStormLifecycle == 0 {
		arrivals[curStormIndex] = curStormStart
		depths[curStormIndex] = curStormPeakStage
		duration := curStormEnd.Sub(curStormStart)
		durations[curStormIndex] = duration.Hours() * 24.0

	}
	return arrivals, depths, durations, nil
}

func (c stormsimLifecycleMultiHazardProvider) Close() {
	// what goes here after switching to using gdal?
}

func (c stormsimLifecycleMultiHazardProvider) Hazard(l geography.Location) (hazards.HazardEvent, error) {
	var hm hazards.ArrivalDepthandDurationEventMulti

	for i, d := range c.depths {
		hd := hazards.HazardData{
			Depth:       d,
			Velocity:    0,
			ArrivalTime: c.arrivals[i],
			Erosion:     0,
			Duration:    c.durations[i],
			WaveHeight:  0,
			Salinity:    false,
			Qualitative: "",
		}
		var h hazards.HazardEvent
		h, err := c.process(hd, h)
		if err != nil {
			panic(err)
		}
		hm.Append(h.(hazards.ArrivalDepthandDurationEvent))
	}
	return &hm, nil
}

func (c stormsimLifecycleMultiHazardProvider) HazardBoundary() (geography.BBox, error) {
	return c.bbox, nil
}
