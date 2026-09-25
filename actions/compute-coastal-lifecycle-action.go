package actions

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/usace-cloud-compute/consequences-runner/crresultswriters"
	lhp "github.com/usace-cloud-compute/consequences-runner/hazardproviders"
	"github.com/usace-cloud-compute/consequences-runner/structureproviders"
)

func (ar *ComputeCoastalLifecycleAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	stormSimEventsPathString := a.Attributes.GetStringOrFail(stormSimEventsPath)
	stormSimResponsesPathString := a.Attributes.GetStringOrFail(stormSimResponsesPath)
	stormSimReachesPathString := a.Attributes.GetStringOrFail(stormSimReachesPath)

	ssEventsDriverString := a.Attributes.GetStringOrFail(stormSimEventsDriver)
	ssResponsesDriverString := a.Attributes.GetStringOrFail(stormSimResponsesDriver)
	ssReachesDriverString := a.Attributes.GetStringOrFail(stormSimReachesDriver)

	ssEventsLayerString := a.Attributes.GetStringOrDefault(stormSimEventsLayer, "")
	ssResponsesLayerString := a.Attributes.GetStringOrDefault(stormSimResponsesLayer, "")
	ssReachesLayerString := a.Attributes.GetStringOrDefault(stormSimReachesLayer, "")

	ssLC := a.Attributes.GetStringOrFail(stormSimLifecycle)
	ssLifecycle, err := strconv.Atoi(ssLC)
	if err != nil {
		panic(err)
	}

	inventoryPath := a.Attributes.GetStringOrFail(inventoryPathKey) //expected this is local - needs to agree with the payload input datasource name
	inventoryDriver := a.Attributes.GetStringOrFail(inventoryDriverKey)

	outputDriver := a.Attributes.GetStringOrFail(outputDriverKey)
	outputFileName := a.Attributes.GetStringOrFail(outputFileNameKey) //expected this is local - needs to agree with the payload output datasource name
	//useKnowledgeUncertainty, err := strconv.ParseBool(a.Parameters.GetStringOrFail(useKnowledgeUncertaintyKey))
	damageFunctionPath := a.Attributes.GetStringOrFail(damageFunctionPathKey) //expected this is local - needs to agree with the payload input datasource name

	ssi := lhp.StormSimInfo{
		EventsFP:           stormSimEventsPathString,
		EventsDriver:       ssEventsDriverString,
		EventsLayername:    ssEventsLayerString,
		ResponsesFP:        stormSimResponsesPathString,
		ResponsesDriver:    ssResponsesDriverString,
		ResponsesLayername: ssResponsesLayerString,
		ReachesFP:          stormSimReachesPathString,
		ReachesDriver:      ssReachesDriverString,
		ReachesLayername:   ssReachesLayerString,
		Lifecycle:          ssLifecycle,
	}

	hp, err := lhp.InitStormSim(ssi)
	if err != nil {
		panic(err)
	}
	defer hp.Close()

	var abstractSP consequences.StreamProvider
	// var sr string
	if inventoryDriver == "MILLIMAN" {
		sp, err := structureproviders.InitMillimanStructureProviderwithOcctypePath(inventoryPath, damageFunctionPath)
		sp.SetDeterministic(true)
		if err != nil {
			return err
		}
		fmt.Sprintln(sp.FilePath)
		// sr = sp.SpatialReference()
		abstractSP = sp
	} else {
		sp, err := structureprovider.InitStructureProviderwithOcctypePath(inventoryPath, tablename, inventoryDriver, damageFunctionPath)
		sp.SetDeterministic(true)
		if err != nil {
			return err
		}
		fmt.Sprintln(sp.FilePath)
		// sr = sp.SpatialReference()
		abstractSP = sp
	}

	//initalize a results writer
	//TODO: add more key-value pairs to payload for summary and events results writer details
	summaryResultsFile := fmt.Sprintf("%s/summary_%s", localData, outputFileName)
	eventsResultsFile := fmt.Sprintf("%s/events_%s", localData, outputFileName)
	rw, err := crresultswriters.InitLifecycleResultsWriter(summaryResultsFile, "summary_results", outputDriver, eventsResultsFile, "event_results", outputDriver)
	if err != nil {
		panic(err)
	}
	defer rw.Close()

	//compute results
	//get boundingbox
	fmt.Println("Getting bbox")
	bbox, err := hp.HazardBoundary()
	if err != nil {
		log.Panicf("Unable to get the raster bounding box: %s", err)
	}
	fmt.Println(bbox.ToString())
	abstractSP.ByBbox(bbox, func(f consequences.Receptor) {

		//ProvideHazard works off of a geography.Location
		d, err2 := hp.Hazard(f.Location())

		//compute damages based on hazard being able to provide depth
		if err2 == nil {
			r, err3 := f.Compute(d)
			r.Headers = append(r.Headers, "multihazard")
			bytes, err := json.Marshal(d)
			s := ""
			if err == nil {
				s = string(bytes)
			}
			r.Result = append(r.Result, s)
			if err3 == nil {
				rw.Write(r)
			}
		}
	})
	return nil

}
