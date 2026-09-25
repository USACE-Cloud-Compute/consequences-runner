package actions

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/usace-cloud-compute/consequences-runner/crresultswriters"
)

func (ar *ComputeCoastalEventAction) Run() error {
	a := ar.Action
	// get all relevant parameters for the Chart team
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	depthGridPathString := a.Attributes.GetStringOrFail(depthgridDatasourceName)       // expected this is a vsis3 object
	velocityGridPathString := a.Attributes.GetStringOrFail(velocitygridDatasourceName) // expected this is a vsis3 object

	inventoryPath := a.Attributes.GetStringOrFail(inventoryPathKey) //expected this is local - needs to agree with the payload input datasource name
	inventoryDriver := a.Attributes.GetStringOrFail(inventoryDriverKey)

	outputDriver := a.Attributes.GetStringOrFail(outputDriverKey)
	damageFunctionPath := a.Attributes.GetStringOrFail(damageFunctionPathKey) //expected this is local - needs to agree with the payload input datasource name
	projectId := a.Attributes.GetStringOrFail(projectIdKey)
	runId := a.Attributes.GetStringOrFail(runIdKey)

	hpi := hazardproviders.HazardProviderInfo{
		Hazards: []hazardproviders.HazardProviderParameterAndPath{{
			Hazard:   hazards.Depth,
			FilePath: depthGridPathString,
		}, {
			Hazard:   hazards.Velocity,
			FilePath: velocityGridPathString,
		}},
	}
	hp, err := hazardproviders.InitMulti(hpi)
	if err != nil {
		return err
	}

	sp, err := structureprovider.InitStructureProviderwithOcctypePath(inventoryPath, tablename, inventoryDriver, damageFunctionPath)
	sp.SetDeterministic(true)
	if err != nil {
		return err
	}
	fmt.Sprintln(sp.FilePath)

	//initalize a psql results writer
	var rw consequences.ResultsWriter
	pgUser := os.Getenv(pgUserKey)
	pgPass := os.Getenv(pgPasswordKey)
	pgDB := os.Getenv(pgDbnameKey)
	pgHost := os.Getenv(pgHostKey)
	pgPort := os.Getenv(pgPortKey)
	pgSchema := os.Getenv(pgSchemaKey)

	outConnStr := fmt.Sprintf(
		"PG:dbname=%s user=%s password=%s host=%s port=%s schemas=%s",
		pgDB, pgUser, pgPass, pgHost, pgPort, pgSchema,
	)

	rw, err = crresultswriters.InitSpatialResultsWriter_PSQL(outConnStr, outputLayerName, outputDriver, pgDB)
	if err != nil {
		log.Fatalf("Failed to initialize spatial psql result writer: %s\n", err)
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
	sp.ByBbox(bbox, func(f consequences.Receptor) {
		//ProvideHazard works off of a geography.Location
		d, err2 := hp.Hazard(geography.Location{X: f.Location().X, Y: f.Location().Y})
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

			r.Headers = append(r.Headers, "project_id")
			r.Result = append(r.Result, projectId)
			r.Headers = append(r.Headers, "run_id")
			r.Result = append(r.Result, runId)

			if err3 == nil {
				rw.Write(r)
			}
		}
	})
	return nil
}
