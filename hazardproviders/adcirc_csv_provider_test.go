package hazardproviders

import (
	"fmt"
	"strings"
	"testing"

	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazards"
)

func Test_InitAdcircCSVWithGrd(t *testing.T) {
	csvFile := "/mnt/drunner/data/test-data.csv"
	grdFile := "/mnt/drunner/data/test_grid.grd"

	hp := InitAdcircCSVWithGrd(csvFile, grdFile)
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

//	func TestOpenCSV_WithCSVProvider(t *testing.T) {
//		// hp := Init("/workspaces/go-coastal/data/CHS_SACS_FL_Blending_PCHA_depth_SLC0_BE_v2020315.csv")
//		hp := InitAdcircCSV("/mnt/drunner/data/test-data.csv")
//		loc := geography.Location{
//			X: -87.51,
//			Y: 30.51,
//		}
//		haz0, err := hp.Hazard(loc)
//		if err != nil {
//			panic(err)
//		}
//		hp.SelectFrequency(5)
//		haz1, err := hp.Hazard(loc)
//		if err != nil {
//			panic(err)
//		}
//		fmt.Println(haz0)
//		fmt.Println(haz1)
//		hp.Close()
//	}
func Test_triangulation(t *testing.T) {
	process_TIN("/mnt/drunner/data/SACS/AL/CHS-SACS_AL_PCHA_Nodal_Inundation_Depth_SLC0_BE_vOct2023.csv")
	// process_TIN("/workspaces/go-coastal/data/CHS_SACS_FL_Blending_PCHA_depth_SLC0_BE_v2020315_a.csv")
}

func Test_ConcaveHull(t *testing.T) {
	// f := OneHundred
	fp := "/workspaces/go-coastal/data/CHS_SACS_FL_Blending_PCHA_depth_SLC0_BE_v2020315.csv"
	hp := InitAdcircCSV(fp)
	// hp.SelectFrequency(int(f) - int(Two)) //offset to zero based index
	s := strings.TrimRight(fp, ".csv")
	hp.ds.Hull.ToGeoJson(s + "_concavehull.json")
}
func Test_ConcaveHull_GRD(t *testing.T) {
	fp := "/workspaces/go-coastal/data/SACS/sacs_sa_base_g001.grd"
	hp := InitWithGrdAndWave(fp, "/workspaces/go-coastal/data/NACS_Nantucket_PCHA_SLC0_SWL_BE_v20210722.csv", "/workspaces/go-coastal/data/NACS_Nantucket_PCHA_SLC0_Hm0_BE_v20210722.csv")
	hp.ds.Hull.ToGeoJson("/workspaces/go-coastal/data/SACS/mesh.json")
	fmt.Println(hp)
}
