package hazardproviders

import (
	"fmt"
	"testing"

	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazards"
)

func Test_InitAdcircHDF(t *testing.T) {
	grd_file := "/mnt/drunner/data/CHS_LACS_Grid_Information.h5"
	swl_file := "/mnt/drunner/data/LACS/probq/CHS-LA_TS_SimBrfc_Post1RT_Nodes_SWL_AEF.h5"
	hm0_file := "/mnt/drunner/data/LACS/probq/CHS-LA_TS_SimBrfc_Post0_Nodes_Hm0_AEF.h5"

	hp, err := InitAdcircHDF(grd_file, swl_file, hm0_file, "BE (standard)")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(hp)

	haz, err := hp.Hazard(geography.Location{X: -90.099577, Y: 29.949915})
	if err != nil {
		t.Fatal(err)
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
