package hazardproviders

import (
	"encoding/csv"
	"os"
	"strconv"
	"time"

	"github.com/USACE/go-consequences/geography"
	gc "github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
)

type stormsimLifecycleMultiHazardProvider struct {
	eventsFile         *os.File
	stormResponsesFile *os.File // TODO: better name
	reachesFile        *os.File
	// specify drivers for each?
	arrivals  []time.Time
	depths    []float64
	durations []float64
	process   gc.HazardFunction
	bbox      geography.BBox
}

func InitStormSim(eventsFP string, responsesFP string, reachesFP string) (stormsimLifecycleMultiHazardProvider, error) {

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

	// parse stormResponsesFile
	stormResponsesFile, err := os.Open(responsesFP)
	if err != nil {
		panic(err)
	}
	arrivals, depths, durations := parseStormsFile(stormResponsesFile, len(storm_ids))

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
		eventsFile:         eventsFile,
		stormResponsesFile: stormResponsesFile,
		reachesFile:        reachesFile,
		arrivals:           arrivals,
		depths:             depths,
		durations:          durations,
		process:            gc.ArrivalDepthAndDurationHazardFunction(),
		bbox:               b,
	}, nil
}

func parseStormsFile(file *os.File, n int) ([]time.Time, []float64, []float64) {

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	arrivals := make([]time.Time, n)
	depths := make([]float64, n)
	durations := make([]float64, n)

	var curStormIndex int = 0
	var curStormID string
	var curStormLifecycle int
	var curStormStart time.Time
	var curStormEnd time.Time
	// var curStormPeakTime time.Time // not sure if this is worth tracking
	var curStormPeakStage float64
	for i, row := range rows[1:] {
		date_i, err := time.Parse("2006-01-02 15:04:05", row[1])
		if err != nil {
			panic(err)
		}
		storm_id := row[2]
		lifecycle_i, err := strconv.Atoi(row[3])
		if err != nil {
			panic(err)
		}
		stage_i, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			panic(err)
		}

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
					durations[curStormIndex] = duration.Hours() * 24.0
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
	// need to add final storm details to slices
	if curStormLifecycle == 0 {
		arrivals[curStormIndex] = curStormStart
		depths[curStormIndex] = curStormPeakStage
		duration := curStormEnd.Sub(curStormStart)
		durations[curStormIndex] = duration.Hours() * 24.0

	}
	return arrivals, depths, durations
}

func (c stormsimLifecycleMultiHazardProvider) Close() {
	c.eventsFile.Close()
	c.stormResponsesFile.Close()
	c.reachesFile.Close()
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
