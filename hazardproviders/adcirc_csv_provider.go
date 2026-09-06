package hazardproviders

import (
	"fmt"
	"time"

	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/usace-cloud-compute/consequences-runner/geometry"
)

type adcircCSVHazardProvider struct {
	//csv *csv.Reader
	ds                       *geometry.Tin
	queryCount               int64
	actualComputedStructures int64
	computeStart             time.Time
}

// Init creates and produces an unexported csvHazardProvider
func InitAdcircCSV(fp string) *adcircCSVHazardProvider {
	// Open the file
	t, err := process_TIN(fp)
	if err != nil {
		panic(err)
	}
	c := time.Now()
	return &adcircCSVHazardProvider{ds: t, computeStart: c}
}
func InitAdcircCSVWithGrd(fp string, grdfp string) *adcircCSVHazardProvider {
	// Open the file
	t, err := processGrdAndCSV(grdfp, fp)
	if err != nil {
		panic(err)
	}
	c := time.Now()
	return &adcircCSVHazardProvider{ds: t, computeStart: c}
}
func InitWithGrdAndWave(grdfp string, swlfp string, hmofp string) *adcircCSVHazardProvider {
	// Open the file
	t, err := processGrdAndCSVs(grdfp, swlfp, hmofp)
	if err != nil {
		panic(err)
	}
	//jsonfp := strings.Replace(grdfp, ".grd", ".json", -1)
	//t.Hull.ToGeoJson(jsonfp)
	c := time.Now()
	return &adcircCSVHazardProvider{ds: t, computeStart: c}
}
func (csv *adcircCSVHazardProvider) SelectFrequency(zidx int) {
	csv.ds.SetFrequency(zidx)
}

func (csv adcircCSVHazardProvider) Hazard(l geography.Location) (hazards.HazardEvent, error) {
	h := hazards.MultiFrequencyCoastalEvent{}

	csv.queryCount++
	//check if point is in the hull polygon.
	p := geometry.Point{X: l.X, Y: l.Y}

	if csv.ds.Hull.Contains(p) {
		v, err := csv.ds.ComputeValues(l.X, l.Y)
		if err != nil {
			return nil, err
		}
		v2 := make([]hazards.CoastalEvent, len(v))
		for i, vi := range v {
			vc := vi.(hazards.CoastalEvent) // do we need to check success here? vc, ok := ...?
			v2[i] = vc
		}
		csv.actualComputedStructures++
		freqs := []float64{0.5, 0.2, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001, 0.0002, 0.0001}
		h.Frequencies = freqs
		h.Events = v2

		return h, nil
	}
	notIn := hazardproviders.NoHazardFoundError{Input: "Point Not In Polygon"}
	return h, notIn
}

func (csv *adcircCSVHazardProvider) Hazard_old(l geography.Location) (hazards.HazardEvent, error) {
	h := hazards.CoastalEvent{}
	csv.queryCount++
	//check if point is in the hull polygon.
	p := geometry.Point{X: l.X, Y: l.Y}
	if csv.queryCount%100000 == 0 {
		n := time.Since(csv.computeStart)
		fmt.Print("Compute Time: ")
		fmt.Println(n)
		fmt.Println(fmt.Sprintf("Processed %v structures, with %v valid depths", csv.queryCount, csv.actualComputedStructures))
	}
	if csv.ds.Hull.Contains(p) {
		v, err := csv.ds.ComputeValue(l.X, l.Y)
		if err != nil {
			h.SetDepth(-9999.0)
			return h, err
		}
		h.SetDepth(v)
		h.SetSalinity(true)
		csv.actualComputedStructures++
		return h, nil
	}
	notIn := hazardproviders.NoHazardFoundError{Input: "Point Not In Polygon"}
	h.SetDepth(-9999.0)
	return h, notIn
}

// implement
func (csv *adcircCSVHazardProvider) ProvideHazards(l geography.Location) ([]hazards.HazardEvent, error) {
	csv.queryCount++
	//check if point is in the hull polygon.
	p := geometry.Point{X: l.X, Y: l.Y}
	if csv.queryCount%100000 == 0 {
		n := time.Since(csv.computeStart)
		fmt.Print("Compute Time: ")
		fmt.Println(n)
		fmt.Println(fmt.Sprintf("Processed %v structures, with %v valid depths", csv.queryCount, csv.actualComputedStructures))
	}
	if csv.ds.Hull.Contains(p) {
		v, err := csv.ds.ComputeValues(l.X, l.Y)
		if err != nil {
			return nil, err
		}
		csv.actualComputedStructures++
		return v, nil
	}
	notIn := hazardproviders.NoHazardFoundError{Input: "Point Not In Polygon"}
	return nil, notIn
}

// implement
func (csv adcircCSVHazardProvider) HazardBoundary() (geography.BBox, error) {
	bbox := make([]float64, 4)
	bbox[0] = csv.ds.MinX //upper left x
	bbox[1] = csv.ds.MaxY //upper left y
	bbox[2] = csv.ds.MaxX //lower right x
	bbox[3] = csv.ds.MinY //lower right y
	return geography.BBox{Bbox: bbox}, nil
}

// implement
func (csv *adcircCSVHazardProvider) Close() {
	//do nothing?
	n := time.Since(csv.computeStart)
	fmt.Print("Compute Complete")
	fmt.Print("Compute Time was: ")
	fmt.Println(n)
	fmt.Println(fmt.Sprintf("Processed %v structures, with %v valid depths", csv.queryCount, csv.actualComputedStructures))

}
