package actions

import (
	"fmt"
	"log"
	"strings"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	lhp "github.com/usace-cloud-compute/consequences-runner/hazardproviders"
	"github.com/usace-cloud-compute/consequences-runner/structureproviders"
)

func (ar *ComputeCoastalFrequencyAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	tablename := a.Attributes.GetStringOrFail(tablenameKey)

	grdFile := a.Attributes.GetStringOrFail(adcircGrdPath)
	swlFile := a.Attributes.GetStringOrFail(adcircSwlPath)
	hm0File := a.Attributes.GetStringOrFail(adcircHm0Path)

	inventoryPath := a.Attributes.GetStringOrFail(inventoryPathKey) //expected this is local - needs to agree with the payload input datasource name
	inventoryDriver := a.Attributes.GetStringOrFail(inventoryDriverKey)

	outputDriver := a.Attributes.GetStringOrFail(outputDriverKey)
	outputFileName := a.Attributes.GetStringOrFail(outputFileNameKey) //expected this is local - needs to agree with the payload output datasource name
	//useKnowledgeUncertainty, err := strconv.ParseBool(a.Parameters.GetStringOrFail(useKnowledgeUncertaintyKey))
	damageFunctionPath := a.Attributes.GetStringOrFail(damageFunctionPathKey) //expected this is local - needs to agree with the payload input datasource name

	grdfpParts := strings.Split(grdFile, ".")
	grdExt := grdfpParts[len(grdfpParts)-1]

	var abstractHP hazardproviders.HazardProvider
	switch grdExt {
	case "csv":
		hp, err := lhp.InitAdcircCSVWithGrd(swlFile, grdFile)
		if err != nil {
			panic(err)
		}
		// do we need to defer hp.Close() or is that covered by the above abstractHp.Close()?
		abstractHP = hp
		defer hp.Close()
	case "h5":
		hp, err := lhp.InitAdcircHDF(grdFile, swlFile, hm0File, "Best Estimate AEF")
		if err != nil {
			panic(err)
		}
		abstractHP = hp
		defer hp.Close()
	}

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
	outputFilePath := fmt.Sprintf("%s/%s", localData, outputFileName)
	rw, err := resultswriters.InitSpatialResultsWriter(outputFilePath, "results", outputDriver)
	if err != nil {
		panic(err)
	}
	defer rw.Close()

	//compute results
	//get boundingbox
	fmt.Println("Getting bbox")
	bbox, err := abstractHP.HazardBoundary()
	if err != nil {
		log.Panicf("Unable to get the bounding box: %s", err)
	}
	fmt.Println(bbox.ToString())
	abstractSP.ByBbox(bbox, func(f consequences.Receptor) {
		//ProvideHazard works off of a geography.Location
		d, err2 := abstractHP.Hazard(geography.Location{X: f.Location().X, Y: f.Location().Y})

		//compute damages based on hazard being able to provide depth
		if err2 == nil {
			r, err3 := f.Compute(d)
			if err3 == nil {
				rw.Write(r)
			}
		}
	})
	return nil

}
