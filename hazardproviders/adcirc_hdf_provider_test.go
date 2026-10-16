package hazardproviders

import (
	"fmt"
	"testing"

	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazards"
)

func Test_InitAdcircHDF(t *testing.T) {
	// grd_file := "/mnt/drunner/data/CHS_LACS_Grid_Information.h5"
	// swl_file := "/mnt/drunner/data/LACS/probq/CHS-LA_TS_SimBrfc_Post1RT_Nodes_SWL_AEF.h5"
	// hm0_file := "/mnt/drunner/data/LACS/probq/CHS-LA_TS_SimBrfc_Post0_Nodes_Hm0_AEF.h5"
	grd_file := "/mnt/drunner/data/test_grid.h5"
	swl_file := "/mnt/drunner/data/test_nodes_swl.h5"
	hm0_file := "/mnt/drunner/data/test_nodes_hm0.h5"
	hp, err := InitAdcircHDF(grd_file, swl_file, hm0_file, "Best Estimate AEF")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(hp)

	loc := geography.Location{
		X: -86.0,
		Y: 32.0,
	}

	haz, err := hp.Hazard(loc)
	if err != nil {
		panic(err)
	}
	mfHazard := haz.(hazards.MultiFrequencyCoastalEvent)

	var depths []float64
	for {
		depths = append(depths, mfHazard.Depth())

		if mfHazard.HasNext() {
			mfHazard.Increment()
		} else {
			break
		}
	}

	for i, v := range depths {
		fmt.Printf("Index: %v, Depth: %v\n", i, v)
	}
	hp.Close()
}
