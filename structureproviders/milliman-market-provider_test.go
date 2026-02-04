package structureproviders

import (
	"fmt"
	"testing"

	"github.com/USACE/go-consequences/compute"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/resultswriters"
)

func Test_Load(t *testing.T) {
	sp, err := InitMillimanStructureProvider("/workspaces/consequences-runner/data/TX_ucmb.csv")
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	hp, err := hazardproviders.Init("/workspaces/consequences-runner/data/aep_depth_100yr_realz_1_projected.tif")

	rw, err := resultswriters.InitSpatialResultsWriter("/workspaces/consequences-runner/data/TX_ucmb_results.gpkg", "results", "GPKG")
	defer rw.Close()
	compute.StreamAbstract(hp, sp, rw)

}
