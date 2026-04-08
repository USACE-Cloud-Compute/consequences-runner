package hazardproviders

import (
	"errors"
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
	geom      gdal.Geometry
}

// create struct to hold the arguments and

type StormSimInfo struct {
	EventsFP           string
	EventsDriver       string
	EventsLayername    string
	ResponsesFP        string
	ResponsesDriver    string
	ResponsesLayername string
	ReachesFP          string
	ReachesDriver      string
	ReachesLayername   string
	Lifecycle          int
}

func InitStormSim(ssi StormSimInfo) (stormsimLifecycleMultiHazardProvider, error) {

	reaches, err := parseReachesFile(ssi.ReachesFP, ssi.ReachesLayername, ssi.ReachesDriver)
	if err != nil {
		panic(err)
	}
	events, err := parseEventsFile(ssi.EventsFP, ssi.EventsLayername, ssi.EventsDriver)
	if err != nil {
		panic(err)
	}

	//TODO: how to handle case where there are multiple reaches?
	reach := reaches[0]
	reachevents := events[reach.reachID][ssi.Lifecycle]

	add, err := parseResponsesFile(ssi.ResponsesFP, ssi.ResponsesLayername, ssi.ResponsesDriver, len(reachevents), ssi.Lifecycle)

	return stormsimLifecycleMultiHazardProvider{
		arrivals:  add.arrivals,
		depths:    add.depths,
		durations: add.durations,
		process:   gc.ArrivalDepthAndDurationHazardFunction(),
		bbox:      reach.Bbox,
		geom:      reach.Geom,
	}, nil
}

func eventsSchema() []string {
	s := []string{"location_id", "lifecycle", "year_offset", "year", "month", "day", "hour", "timestamp", "storm_id"}
	return s
}

func responsesSchema() []string {
	s := []string{"location_id", "date", "stormevent_id", "storm_id", "lifecycle", "stage"}
	return s
}

func reachesSchema() []string {
	s := []string{"Name", "NameAbbreviated", "Note", "NOAAStation", "NOAAStationName", "SavePoint"}
	return s
}

type Reach struct {
	reachID   string
	reachName string
	Bbox      geography.BBox
	Geom      gdal.Geometry
}

func parseReachesFile(filepath string, layername string, driver string) ([]Reach, error) {
	var ret = []Reach{}

	d := gdal.OGRDriverByName(driver)
	ds, dsok := d.Open(filepath, int(gdal.ReadOnly))
	if !dsok {
		return ret, errors.New("error opening reaches file of type " + driver)
	}

	var layer gdal.Layer
	if layername != "" {
		layer = ds.LayerByName(layername)
	} else {
		layer = ds.LayerByIndex(0)
	}
	def := layer.Definition()
	s := reachesSchema()
	sIDX := make([]int, len(s))

	for i, f := range s {
		idx := def.FieldIndex(f)

		if idx < 0 {
			return ret, errors.New("gdal dataset at path " + filepath + " Expected field named " + f + " none was found")
		}
		sIDX[i] = idx
	}

	fc, _ := layer.FeatureCount(true)
	ret = make([]Reach, fc)

	for i := range fc {
		feat := layer.NextFeature()
		if feat != nil {
			//NOTE: "Name", "NameAbbreviated", "Note", "NOAAStation", "NOAAStationName", "SavePoint", "geom"
			reach_id := feat.FieldAsString(sIDX[1])
			reach_name := feat.FieldAsString(sIDX[0])
			geom := feat.Geometry()
			env := geom.Envelope()
			xmin := env.MinX()
			xmax := env.MaxX()
			ymin := env.MinY()
			ymax := env.MaxY()
			bb := geography.BBox{Bbox: []float64{xmin, ymin, xmax, ymax}}
			ret[i] = Reach{reachID: reach_id, reachName: reach_name, Geom: geom, Bbox: bb}
		}
	}

	return ret, nil
}

func parseEventsFile(filepath string, layername string, driver string) (map[string]map[int][]string, error) {
	ret := make(map[string]map[int][]string) // map[location_id]map[lifecycle][]storm_ids

	d := gdal.OGRDriverByName(driver)
	ds, dsok := d.Open(filepath, int(gdal.ReadOnly))
	if !dsok {
		return ret, errors.New("error opening events file of type " + driver)
	}

	var layer gdal.Layer
	if layername != "" {
		layer = ds.LayerByName(layername)
	} else {
		layer = ds.LayerByIndex(0)
	}
	def := layer.Definition()
	s := eventsSchema()
	sIDX := make([]int, len(s))

	for i, f := range s {
		idx := def.FieldIndex(f)
		if idx < 0 {
			return ret, errors.New("gdal dataset at path " + filepath + " Expected field named " + f + " none was found")
		}
		sIDX[i] = idx
	}

	fc, _ := layer.FeatureCount(true)

	for range fc {
		feat := layer.NextFeature()
		if feat != nil {
			//NOTE: "location_id", "lifecycle", "year_offset", "year", "month", "day", "hour", "timestamp", "storm_id"
			loc_id := feat.FieldAsString(sIDX[0])
			lifecycle := feat.FieldAsInteger(sIDX[1])
			storm_id := feat.FieldAsString(sIDX[8])

			if ret[loc_id] == nil {
				ret[loc_id] = make(map[int][]string)
			}
			if ret[loc_id][lifecycle] == nil {
				ret[loc_id][lifecycle] = []string{storm_id}
			} else {
				ret[loc_id][lifecycle] = append(ret[loc_id][lifecycle], storm_id)
			}
		}
	}

	return ret, nil
}

type ADDInfo struct {
	arrivals  []time.Time
	depths    []float64
	durations []float64
}

func parseResponsesFile(filepath string, layername string, driver string, n int, lifecycle int) (ADDInfo, error) {
	arrivals := make([]time.Time, n)
	depths := make([]float64, n)
	durations := make([]float64, n)
	ret := ADDInfo{
		arrivals:  arrivals,
		depths:    depths,
		durations: durations,
	}

	d := gdal.OGRDriverByName(driver)
	// for i := 0; i < gdal.OGRDriverCount(); i++ { // can use this to ensure given driver is available
	// 	fmt.Println(gdal.OGRDriverByIndex(i).Name())
	// }

	ds, dsok := d.Open(filepath, int(gdal.ReadOnly))
	if !dsok {
		return ret, errors.New("error opening responses file of type " + driver)
	}

	var layer gdal.Layer
	if layername != "" {
		layer = ds.LayerByName(layername)
	} else {
		layer = ds.LayerByIndex(0)
	}
	def := layer.Definition()

	s := responsesSchema()
	sIDX := make([]int, len(s))

	for i, f := range s {
		idx := def.FieldIndex(f)
		if idx < 0 {
			return ret, errors.New("gdal dataset at path " + filepath + " Expected field named " + f + " none was found")
		}
		sIDX[i] = idx
	}

	fc, _ := layer.FeatureCount(true)
	// fmt.Printf("The layer has %v features\n", fc)
	var curStormIndex int = 0
	var curStormID string
	var curStormLifecycle int //
	var curStormStart time.Time
	var curStormEnd time.Time
	var curStormPeakStage float64

	for i := range fc {
		feat := layer.NextFeature()
		if feat != nil {
			d := feat.FieldAsString(sIDX[1])
			date_i, err := time.Parse("2006-01-02 15:04:05", d)
			if err != nil {
				panic(err)
			}
			storm_id := feat.FieldAsString(sIDX[2])
			lifecycle_i := feat.FieldAsInteger(sIDX[4])
			//TODO: handle NA values for stage
			stage_i := feat.FieldAsFloat64(sIDX[5])

			if storm_id == curStormID {
				curStormEnd = date_i
				if stage_i > curStormPeakStage {
					curStormPeakStage = stage_i
					// curStormPeakTime = date_i
				}
			} else {
				// we've reached a new storm in the series
				// save results from previous storm
				if i > 0 { // can't save previous storm if we're on row 0
					if curStormLifecycle == lifecycle { // not handling multiple lifecycles currently
						ret.arrivals[curStormIndex] = curStormStart
						ret.depths[curStormIndex] = curStormPeakStage
						duration := curStormEnd.Sub(curStormStart)
						ret.durations[curStormIndex] = duration.Hours() / 24.0
						curStormIndex++
					}
				}
				// reset all tracking variables to current row
				curStormID = storm_id
				curStormLifecycle = lifecycle_i
				curStormStart = date_i
				curStormEnd = date_i
				curStormPeakStage = stage_i
				// curStormPeakTime = date_i
			}
		}
	}
	ret.arrivals[curStormIndex] = curStormStart
	ret.depths[curStormIndex] = curStormPeakStage
	duration := curStormEnd.Sub(curStormStart)
	ret.durations[curStormIndex] = duration.Hours() / 24.0
	return ret, nil
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
	test_geom := gdal.Create(gdal.GT_Point)
	test_geom.AddPoint(l.X, l.Y, 0)

	if !c.geom.Contains(test_geom) {
		return &hm, errors.New("Provided hazard location is outside the reach boundary")
	}
	return &hm, nil
}

func (c stormsimLifecycleMultiHazardProvider) HazardBoundary() (geography.BBox, error) {
	return c.bbox, nil
}
