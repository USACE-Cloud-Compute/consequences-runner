package actions

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/USACE/go-consequences/compute"
	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	gc "github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/USACE/go-consequences/structures"
	"github.com/usace-cloud-compute/cc-go-sdk"
	lhp "github.com/usace-cloud-compute/consequences-runner/hazardproviders"
	"github.com/usace-cloud-compute/consequences-runner/structureproviders"
)

const (
	meandepthgridDatasourceName                 string = "mean-depth-grids"     //plugin datasource name required
	stdevdepthgridDatasourceName                string = "stdev-depth-grids"    //plugin datasource name required
	meanvelocitygridDatasourceName              string = "mean-velocity-grids"  //plugin datasource name required
	stdevvelocitygridDatasourceName             string = "stdev-velocity-grids" //plugin datasource name required
	verticalSliceName                           string = "vertical-slice"
	femaMultiParameterFrequencyBasedActionName  string = "compute-fema-frequency"
	femaSingleParameterFrequencyBasedActionName string = "compute-fema-frequency-single-parameter"
)

func init() {
	cc.ActionRegistry.RegisterAction(femaMultiParameterFrequencyBasedActionName, &FemaMultiParameterFrequencyBasedAction{})
	cc.ActionRegistry.RegisterAction(femaSingleParameterFrequencyBasedActionName, &FemaSingleParameterFrequencyBasedAction{})
}

type FemaMultiParameterFrequencyBasedAction struct {
	cc.ActionRunnerBase
}
type FemaSingleParameterFrequencyBasedAction struct {
	cc.ActionRunnerBase
}

func (ar *FemaMultiParameterFrequencyBasedAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	//vsis3prefix := a.Parameters.GetStringOrFail(vsis3prefixKey)
	meandepthGridPathString := a.Attributes.GetStringOrFail(meandepthgridDatasourceName)         // expected this is a vsis3 object
	meanvelocityGridPathString := a.Attributes.GetStringOrFail(meanvelocitygridDatasourceName)   // expected this is a vsis3 object
	stdevdepthGridPathString := a.Attributes.GetStringOrFail(stdevdepthgridDatasourceName)       // expected this is a vsis3 object
	stdevvelocityGridPathString := a.Attributes.GetStringOrFail(stdevvelocitygridDatasourceName) // expected this is a vsis3 object
	//durationGridPaths := a.Parameters.GetStringOrFail(DurationGridPathsKey)// expected this is a vsis3 object
	frequencystring := a.Attributes.GetStringOrFail(FrequenciesKey)
	verticalSlicestring := a.Attributes.GetStringOrFail(verticalSliceName)
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
	// vertical slices expected to be comma separated variables of floats.
	stringverticalslice := strings.Split(verticalSlicestring, ", ")
	verticalslices := make([]float64, 0)
	for _, s := range stringverticalslice {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		verticalslices = append(verticalslices, f)
	}
	// grid paths expected to be comma separated variables of string path parts
	MeanDepthGridPaths := strings.Split(meandepthGridPathString, ", ")
	MeanVelocityGridPaths := strings.Split(meanvelocityGridPathString, ", ")
	StdevDepthGridPaths := strings.Split(stdevdepthGridPathString, ", ")
	StdevVelocityGridPaths := strings.Split(stdevvelocityGridPathString, ", ")
	if len(MeanDepthGridPaths) != len(MeanVelocityGridPaths) {
		return errors.New("depth grids and velocity grids have different numbers of paths")
	}
	if len(MeanDepthGridPaths) != len(frequencies) {
		return errors.New("hazard grids have different numbers of paths than the frequencies list")
	}
	hps := make([]lhp.Mean_and_stdev_HazardProvider, 0)
	process := func(valueIn hazards.HazardData, hazard hazards.HazardEvent) (hazards.HazardEvent, error) {
		if valueIn.Depth <= 0 {
			return hazard, gc.NoHazardFoundError{}
		}
		if valueIn.Velocity <= 0 {
			return hazard, gc.NoHazardFoundError{}
		}
		h := hazards.HazardData{
			Depth:    valueIn.Depth,
			Velocity: valueIn.Velocity,
		}
		e := hazards.HazardDataToMultiParameter(h)
		return e, nil
	}
	for i, dp := range MeanDepthGridPaths {
		hp, err := lhp.Init(dp, StdevDepthGridPaths[i], MeanVelocityGridPaths[i], StdevVelocityGridPaths[i], verticalslices)
		if err != nil {
			return err
		}
		hp.SetProcess(process)
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

	ComputeMultiFrequencyMeanStdev(hps, frequencies, abstractSP, rw)
	return nil
}
func ComputeMultiFrequencyMeanStdev(hps []lhp.Mean_and_stdev_HazardProvider, freqs []float64, sp consequences.StreamProvider, w consequences.ResultsWriter) {
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
		header = append(header, fmt.Sprintf("%1.6fMS", f))
		header = append(header, fmt.Sprintf("%1.6fSS", f))
		header = append(header, fmt.Sprintf("%1.6fMC", f))
		header = append(header, fmt.Sprintf("%1.6fSC", f))
		header = append(header, fmt.Sprintf("%1.6fMH", f))
		header = append(header, fmt.Sprintf("%1.6fSH", f))
	}

	sp.ByBbox(bbox, func(f consequences.Receptor) {
		s, sok := f.(structures.StructureDeterministic)
		if !sok {
			return
		}
		results := []interface{}{s.Name, s.StructVal, s.NumStories, s.FoundType, s.FoundHt, s.Location().X, s.Location().Y, s.OccType.Name, s.DamCat, "unkown", s.FoundHt + s.GroundElevation, s.GroundElevation, 0.0, 0.0, 0.0, 0.0}

		msEADs := make([]float64, len(freqs))
		mcEADs := make([]float64, len(freqs))
		ssEADs := make([]float64, len(freqs))
		scEADs := make([]float64, len(freqs))
		//ProvideHazard works off of a geography.Location
		gotWet := false
		firstProb := 0.0
		for index, hp := range hps {
			d, err := hp.Hazards(geography.Location{X: f.Location().X, Y: f.Location().Y})
			//compute damages based on hazard being able to provide depth
			if err != nil {
				results = append(results, 0.0)
				results = append(results, 0.0)
				results = append(results, 0.0)
				results = append(results, 0.0)
				results = append(results, "no-hazard")
				results = append(results, "no-hazard")
				continue
			}
			meanSliceDamage := 0.0
			meanSliceContent := 0.0
			meanSliceDepth := 0.0
			meanSliceVelocity := 0.0
			stdevSliceDamage := 0.0
			stdevSliceContent := 0.0
			stdevSliceDepth := 0.0
			stdevSliceVelocity := 0.0
			sampleSize := 0
			for _, hazard := range d {
				sampleSize++
				r, err3 := f.Compute(hazard)
				sliceDepth := hazard.Depth()
				sliceVelocity := hazard.Velocity()
				sliceContent := 0.0
				sliceStructure := 0.0
				if err3 == nil {
					if !gotWet {
						firstProb = freqs[index] //how does this make sense?
					}
					gotWet = true
					sliceStructureI, err := r.Fetch("structure damage")
					if err != nil {
						log.Fatal("could not fetch structure damage")
					}
					sliceStructure = sliceStructureI.(float64)
					sliceContentI, err := r.Fetch("content damage")
					if err != nil {
						log.Fatal("could not fetch content damage")
					}
					sliceContent = sliceContentI.(float64)
				} else {
					sliceStructure = 0.0
					sliceContent = 0.0
					sliceDepth = 0.0
					sliceVelocity = 0.0
				}
				meanSliceDamage, stdevSliceDamage = meanAndStdev(meanSliceDamage, stdevSliceDamage, sampleSize, sliceStructure)
				meanSliceContent, stdevSliceContent = meanAndStdev(meanSliceContent, stdevSliceContent, sampleSize, sliceContent)
				meanSliceDepth, stdevSliceDepth = meanAndStdev(meanSliceDepth, stdevSliceDepth, sampleSize, sliceDepth)
				meanSliceVelocity, stdevSliceVelocity = meanAndStdev(meanSliceVelocity, stdevSliceVelocity, sampleSize, sliceVelocity)

			}

			msEADs[index] = meanSliceDamage
			mcEADs[index] = meanSliceContent
			ssEADs[index] = math.Sqrt(stdevSliceDamage)
			scEADs[index] = math.Sqrt(stdevSliceContent)
			meanHazarddata := hazards.HazardData{
				Depth:    meanSliceDepth,
				Velocity: math.Sqrt(meanSliceVelocity),
			}
			stdevHazarddata := hazards.HazardData{
				Depth:    stdevSliceDepth,
				Velocity: math.Sqrt(stdevSliceVelocity),
			}
			meanHazard := hazards.HazardDataToMultiParameter(meanHazarddata)
			stdevHazard := hazards.HazardDataToMultiParameter(stdevHazarddata)
			results = append(results, msEADs[index])
			results = append(results, ssEADs[index])
			results = append(results, mcEADs[index])
			results = append(results, scEADs[index])
			b, err := json.Marshal(meanHazard)
			if err != nil {
				log.Fatal(err)
			}
			stdevb, err := json.Marshal(stdevHazard)
			if err != nil {
				log.Fatal(err)
			}
			shaz := string(b)
			stdevsshaz := string(stdevb)
			results = append(results, shaz)
			results = append(results, stdevsshaz)
		}
		results[15] = firstProb
		sEAD := compute.ComputeSpecialEAD(msEADs, freqs) //use compute special ead to not create triangle below the most frequent event
		results[12] = sEAD
		cEAD := compute.ComputeSpecialEAD(mcEADs, freqs) //use compute special ead to not create triangle below the most frequent event
		results[13] = cEAD
		results[14] = sEAD + cEAD
		var ret = consequences.Result{Headers: header, Result: results}
		if gotWet {
			w.Write(ret)
		}

	})

}
func meanAndStdev(mean float64, stdev float64, sampleSize int, value float64) (float64, float64) {
	if sampleSize == 1 {
		stdev = 0
		mean = value
	} else {
		stdev = (((float64(sampleSize-2) / float64(sampleSize-1)) * stdev) + (math.Pow((value-mean), 2))/float64(sampleSize))
		mean = mean + ((value - mean) / float64(sampleSize))
	}

	return mean, stdev
}

func (ar *FemaSingleParameterFrequencyBasedAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	//vsis3prefix := a.Parameters.GetStringOrFail(vsis3prefixKey)
	meandepthGridPathString := a.Attributes.GetStringOrFail(meandepthgridDatasourceName) // expected this is a vsis3 object
	//meanvelocityGridPathString := a.Parameters.GetStringOrFail(meanvelocitygridDatasourceName)   // expected this is a vsis3 object
	stdevdepthGridPathString := a.Attributes.GetStringOrFail(stdevdepthgridDatasourceName) // expected this is a vsis3 object
	//stdevvelocityGridPathString := a.Parameters.GetStringOrFail(stdevvelocitygridDatasourceName) // expected this is a vsis3 object
	//durationGridPaths := a.Parameters.GetStringOrFail(DurationGridPathsKey)// expected this is a vsis3 object
	frequencystring := a.Attributes.GetStringOrFail(FrequenciesKey)
	verticalSlicestring := a.Attributes.GetStringOrFail(verticalSliceName)
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
	// vertical slices expected to be comma separated variables of floats.
	stringverticalslice := strings.Split(verticalSlicestring, ", ")
	verticalslices := make([]float64, 0)
	for _, s := range stringverticalslice {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		verticalslices = append(verticalslices, f)
	}
	// grid paths expected to be comma separated variables of string path parts
	MeanDepthGridPaths := strings.Split(meandepthGridPathString, ", ")
	//MeanVelocityGridPaths := strings.Split(meanvelocityGridPathString, ", ")
	StdevDepthGridPaths := strings.Split(stdevdepthGridPathString, ", ")
	//StdevVelocityGridPaths := strings.Split(stdevvelocityGridPathString, ", ")
	if len(MeanDepthGridPaths) != len(StdevDepthGridPaths) {
		return errors.New("mean depth grids and stdev depth grids have different numbers of paths")
	}
	if len(MeanDepthGridPaths) != len(frequencies) {
		return errors.New("hazard grids have different numbers of paths than the frequencies list")
	}
	hps := make([]lhp.SingleParameter_Mean_and_stdev_HazardProvider, 0)
	process := func(valueIn hazards.HazardData, hazard hazards.HazardEvent) (hazards.HazardEvent, error) {
		if valueIn.Depth <= 0 {
			return hazard, gc.NoHazardFoundError{}
		}
		h := hazards.HazardData{
			Depth: valueIn.Depth,
		}
		e := hazards.HazardDataToMultiParameter(h)
		return e, nil
	}
	for i, dp := range MeanDepthGridPaths {
		hp, err := lhp.InitSingleParameter(dp, StdevDepthGridPaths[i], verticalslices)
		if err != nil {
			return err
		}
		hp.SetProcess(process)
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

	ComputeMultiFrequencyMeanStdev_SingleParameter(hps, frequencies, abstractSP, rw)
	return nil
}
func ComputeMultiFrequencyMeanStdev_SingleParameter(hps []lhp.SingleParameter_Mean_and_stdev_HazardProvider, freqs []float64, sp consequences.StreamProvider, w consequences.ResultsWriter) {
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
		header = append(header, fmt.Sprintf("%1.6fMS", f))
		header = append(header, fmt.Sprintf("%1.6fSS", f))
		header = append(header, fmt.Sprintf("%1.6fMC", f))
		header = append(header, fmt.Sprintf("%1.6fSC", f))
		header = append(header, fmt.Sprintf("%1.6fMH", f))
		header = append(header, fmt.Sprintf("%1.6fSH", f))
	}

	sp.ByBbox(bbox, func(f consequences.Receptor) {
		s, sok := f.(structures.StructureDeterministic)
		if !sok {
			return
		}
		results := []interface{}{s.Name, s.StructVal, s.NumStories, s.FoundType, s.FoundHt, s.Location().X, s.Location().Y, s.OccType.Name, s.DamCat, "unkown", s.FoundHt + s.GroundElevation, s.GroundElevation, 0.0, 0.0, 0.0, 0.0}

		msEADs := make([]float64, len(freqs))
		mcEADs := make([]float64, len(freqs))
		ssEADs := make([]float64, len(freqs))
		scEADs := make([]float64, len(freqs))
		//ProvideHazard works off of a geography.Location
		gotWet := false
		firstProb := 0.0
		for index, hp := range hps {
			d, err := hp.Hazards(geography.Location{X: f.Location().X, Y: f.Location().Y})
			//compute damages based on hazard being able to provide depth
			if err != nil {
				results = append(results, 0.0)
				results = append(results, 0.0)
				results = append(results, 0.0)
				results = append(results, 0.0)
				results = append(results, "no-hazard")
				results = append(results, "no-hazard")
				continue
			}
			meanSliceDamage := 0.0
			meanSliceContent := 0.0
			meanSliceDepth := 0.0
			stdevSliceDamage := 0.0
			stdevSliceContent := 0.0
			stdevSliceDepth := 0.0
			sampleSize := 0
			for _, hazard := range d {
				sampleSize++
				r, err3 := f.Compute(hazard)
				sliceDepth := hazard.Depth()
				sliceContent := 0.0
				sliceStructure := 0.0
				if err3 == nil {
					if !gotWet {
						firstProb = freqs[index] //how does this make sense?
					}
					gotWet = true
					sliceStructureI, err := r.Fetch("structure damage")
					if err != nil {
						log.Fatal("could not fetch structure damage")
					}
					sliceStructure = sliceStructureI.(float64)
					sliceContentI, err := r.Fetch("content damage")
					if err != nil {
						log.Fatal("could not fetch content damage")
					}
					sliceContent = sliceContentI.(float64)
				} else {
					sliceStructure = 0.0
					sliceContent = 0.0
					sliceDepth = 0.0
				}
				meanSliceDamage, stdevSliceDamage = meanAndStdev(meanSliceDamage, stdevSliceDamage, sampleSize, sliceStructure)
				meanSliceContent, stdevSliceContent = meanAndStdev(meanSliceContent, stdevSliceContent, sampleSize, sliceContent)
				meanSliceDepth, stdevSliceDepth = meanAndStdev(meanSliceDepth, stdevSliceDepth, sampleSize, sliceDepth)

			}

			msEADs[index] = meanSliceDamage
			mcEADs[index] = meanSliceContent
			ssEADs[index] = math.Sqrt(stdevSliceDamage)
			scEADs[index] = math.Sqrt(stdevSliceContent)
			meanHazarddata := hazards.HazardData{
				Depth: meanSliceDepth,
			}
			stdevHazarddata := hazards.HazardData{
				Depth: stdevSliceDepth,
			}
			meanHazard := hazards.HazardDataToMultiParameter(meanHazarddata)
			stdevHazard := hazards.HazardDataToMultiParameter(stdevHazarddata)
			results = append(results, msEADs[index])
			results = append(results, ssEADs[index])
			results = append(results, mcEADs[index])
			results = append(results, scEADs[index])
			b, err := json.Marshal(meanHazard)
			if err != nil {
				log.Fatal(err)
			}
			stdevb, err := json.Marshal(stdevHazard)
			if err != nil {
				log.Fatal(err)
			}
			shaz := string(b)
			stdevsshaz := string(stdevb)
			results = append(results, shaz)
			results = append(results, stdevsshaz)
		}
		results[15] = firstProb
		sEAD := compute.ComputeSpecialEAD(msEADs, freqs) //use compute special ead to not create triangle below the most frequent event
		results[12] = sEAD
		cEAD := compute.ComputeSpecialEAD(mcEADs, freqs) //use compute special ead to not create triangle below the most frequent event
		results[13] = cEAD
		results[14] = sEAD + cEAD
		var ret = consequences.Result{Headers: header, Result: results}
		if gotWet {
			w.Write(ret)
		}

	})

}
