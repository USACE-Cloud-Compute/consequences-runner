package actions

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/USACE/go-consequences/structures"
	"github.com/usace-cloud-compute/consequences-runner/structureproviders"
)

func (ar *ComputeFrequencyAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	//vsis3prefix := a.Parameters.GetStringOrFail(vsis3prefixKey)
	depthGridPathString := a.Attributes.GetStringOrFail(DepthGridPathsKey)       // expected this is a vsis3 object
	velocityGridPathString := a.Attributes.GetStringOrFail(VelocityGridPathsKey) // expected this is a vsis3 object
	//durationGridPaths := a.Parameters.GetStringOrFail(DurationGridPathsKey)// expected this is a vsis3 object
	frequencystring := a.Attributes.GetStringOrFail(FrequenciesKey)
	inventoryPathKey := a.Attributes.GetStringOrFail(inventoryPathKey) //expected this is local - needs to agree with the payload input datasource name
	inventoryDriver := a.Attributes.GetStringOrFail(inventoryDriverKey)

	outputDriver := a.Attributes.GetStringOrFail(outputDriverKey)
	outputFileName := a.Attributes.GetStringOrFail(outputFileNameKey) //expected this is local - needs to agree with the payload output datasource name
	//useKnowledgeUncertainty, err := strconv.ParseBool(a.Parameters.GetStringOrFail(useKnowledgeUncertaintyKey))
	damageFunctionPath := a.Attributes.GetStringOrFail(damageFunctionPathKey) //expected this is local - needs to agree with the payload input datasource name
	// frequencies expected to be comma separated variables of floats.
	stringFrequencies := strings.Split(frequencystring, ", ")
	frequencies := make([]float64, 0)
	for _, s := range stringFrequencies {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		frequencies = append(frequencies, f)
	}
	// grid paths expected to be comma separated variables of string path parts
	DepthGridPaths := strings.Split(depthGridPathString, ", ")
	VelocityGridPaths := strings.Split(velocityGridPathString, ", ")
	if len(DepthGridPaths) != len(VelocityGridPaths) {
		return errors.New("depth grids and velocity grids have different numbers of paths")
	}
	if len(DepthGridPaths) != len(frequencies) {
		return errors.New("hazard grids have different numbers of paths than the frequencies list")
	}
	hps := make([]hazardproviders.HazardProvider, 0)
	for i, dp := range DepthGridPaths {
		hpi := hazardproviders.HazardProviderInfo{
			Hazards: []hazardproviders.HazardProviderParameterAndPath{{
				Hazard:   hazards.Depth,
				FilePath: dp,
			}, {
				Hazard:   hazards.Velocity,
				FilePath: VelocityGridPaths[i],
			}},
		}
		hp, err := hazardproviders.InitMulti(hpi)
		if err != nil {
			return err
		}
		hps = append(hps, hp)
	}
	// inventory path expected to be a local path
	// damage function path expected to be a local path
	var abstractSP consequences.StreamProvider
	var sr string
	if inventoryDriver == "MILLIMAN" {
		sp, err := structureproviders.InitMillimanStructureProviderwithOcctypePath(inventoryPathKey, damageFunctionPath)
		sp.SetDeterministic(true)
		if err != nil {
			return err
		}
		fmt.Sprintln(sp.FilePath)
		sr = sp.SpatialReference()
		abstractSP = sp
	} else {
		sp, err := structureprovider.InitStructureProviderwithOcctypePath(inventoryPathKey, tablename, inventoryDriver, damageFunctionPath)
		sp.SetDeterministic(true)
		if err != nil {
			return err
		}
		fmt.Sprintln(sp.FilePath)
		sr = sp.SpatialReference()
		abstractSP = sp
	}
	//results writer
	outfp := outputFileName //fmt.Sprintf("%s/%s", localData, outputFileName)
	var rw consequences.ResultsWriter

	rw, err := resultswriters.InitSpatialResultsWriter_WKT_Projected(outfp, outputLayerName, outputDriver, sr)
	if err != nil {
		return err
	}
	defer rw.Close()

	ComputeMultiFrequency(hps, frequencies, abstractSP, rw)
	return nil
}

func ComputeMultiFrequency(hps []hazardproviders.HazardProvider, freqs []float64, sp consequences.StreamProvider, w consequences.ResultsWriter) {
	fmt.Printf("Computing %v frequencies\n", len(freqs))
	//ASSUMPTION hazard providers and frequencies are in the same order
	//ASSUMPTION ordered by most frequent to least frequent event
	//ASSUMPTION! get bounding box from largest frequency.

	largestHp := hps[len(hps)-1]
	bbox, err := largestHp.HazardBoundary()
	if err != nil {
		fmt.Print(err)
		return
	}
	//set up output tables for all frequencies.
	header := []string{"ORIG_ID", "REPVAL", "STORY", "FOUND_T", "FOUND_H", "x", "y", "OccType", "DamCat", "BASEFIN", "FFH", "DEMFT", "BAAL", "CAAL", "TAAL", "PROB"}

	for _, f := range freqs {
		header = append(header, fmt.Sprintf("%2.6fS", f))
		header = append(header, fmt.Sprintf("%2.6fC", f))
		header = append(header, fmt.Sprintf("%2.6fH", f))
	}

	sp.ByBbox(bbox, func(f consequences.Receptor) {
		s, sok := f.(structures.StructureDeterministic)
		if !sok {
			return
		}
		results := []interface{}{s.Name, s.StructVal, s.NumStories, s.FoundType, s.FoundHt, s.Location().X, s.Location().Y, s.OccType.Name, s.DamCat, "unkown", s.FoundHt + s.GroundElevation, s.GroundElevation, 0.0, 0.0, 0.0, 0.0}

		sEADs := make([]float64, len(freqs))
		cEADs := make([]float64, len(freqs))
		hazarddata := make([]hazards.HazardEvent, len(freqs))
		//ProvideHazard works off of a geography.Location
		gotWet := false
		firstProb := 0.0
		for index, hp := range hps {
			d, err := hp.Hazard(geography.Location{X: f.Location().X, Y: f.Location().Y})
			hazarddata = append(hazarddata, d)
			//compute damages based on hazard being able to provide depth

			if err == nil {
				r, err3 := f.Compute(d)
				if err3 == nil {
					if !gotWet {
						firstProb = freqs[index]
					}
					gotWet = true
					sdam, err := r.Fetch("structure damage")
					if err != nil {
						//panic?
						sEADs[index] = 0.0
					} else {
						damage := sdam.(float64)
						sEADs[index] = damage
					}
					cdam, err := r.Fetch("content damage")
					if err != nil {
						//panic?
						cEADs[index] = 0.0
					} else {
						damage := cdam.(float64)
						cEADs[index] = damage
					}
				}
				results = append(results, sEADs[index])
				results = append(results, cEADs[index])
				b, err := json.Marshal(d)
				if err != nil {
					log.Fatal(err)
				}
				shaz := string(b)
				results = append(results, shaz)
			} else {
				//record zeros?
				results = append(results, 0.0)
				results = append(results, 0.0)
				results = append(results, "no hazard")
			}
		}
		results[15] = firstProb
		sEAD := ComputeEAD(sEADs, freqs) //use compute special ead to not create triangle below the most frequent event
		results[12] = sEAD
		cEAD := ComputeEAD(cEADs, freqs) //use compute special ead to not create triangle below the most frequent event
		results[13] = cEAD
		results[14] = sEAD + cEAD
		var ret = consequences.Result{Headers: header, Result: results}
		if gotWet {
			w.Write(ret)
		}

	})

}

// ComputeEAD integrates under the damage frequency curve but does calculate the first triangle between 1 and the first frequency.
func ComputeEAD(damages []float64, freq []float64) float64 {
	//this differs from computeEAD in that it specifically does calculate the first triangle between 1 and the first frequency to interpolate damages to zero.
	if len(damages) != len(freq) {
		panic("frequency curve is unbalanced")
	}
	triangle := 0.0
	square := 0.0
	x1 := freq[0]
	y1 := damages[0]
	eadT := 0.0
	if len(damages) > 1 {
		for i := 1; i < len(freq); i++ {
			xdelta := x1 - freq[i]
			square = xdelta * y1
			if square != 0.0 { //we dont know where damage really begins until we see it. we can guess it is inbetween ordinates, but who knows.
				triangle = ((xdelta) * -(y1 - damages[i])) / 2.0
			} else {
				triangle = ((xdelta) * -(y1 - damages[i])) / 2.0
			}
			eadT += square + triangle
			x1 = freq[i]
			y1 = damages[i]
		}
	}
	if x1 != 0.0 {
		xdelta := x1 - 0.0
		eadT += xdelta * y1 //no extrapolation, just continue damages out as if it were truth for all remaining probability.
	}
	return eadT
}
