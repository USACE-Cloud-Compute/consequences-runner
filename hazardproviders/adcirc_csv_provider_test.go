package hazardproviders

import (
	"fmt"
	"strings"
	"testing"
)

func TestOpenCSV_WithCSVProvider(t *testing.T) {
	// hp := Init("/workspaces/go-coastal/data/CHS_SACS_FL_Blending_PCHA_depth_SLC0_BE_v2020315.csv")
	hp := InitAdcercCSV("/mnt/drunner/data/SACS/FL/CHS-SACS_FL_PCHA_Nodal_Inundation_Depth_Blended_SLC0_BE_vOct2023.csv")
	hp.Close()
}
func Test_triangulation(t *testing.T) {
	process_TIN("/workspaces/go-coastal/data/CHS_SACS_FL_Blending_PCHA_depth_SLC0_BE_v2020315_a.csv")
}

func Test_ConcaveHull(t *testing.T) {
	f := OneHundred
	fp := "/workspaces/go-coastal/data/CHS_SACS_FL_Blending_PCHA_depth_SLC0_BE_v2020315.csv"
	hp := InitAdcercCSV(fp)
	hp.SelectFrequency(int(f) - int(Two)) //offset to zero based index
	s := strings.TrimRight(fp, ".csv")
	hp.ds.Hull.ToGeoJson(s + "_concavehull.json")
}
func Test_ConcaveHull_GRD(t *testing.T) {
	fp := "/workspaces/go-coastal/data/SACS/sacs_sa_base_g001.grd"
	hp := InitWithGrdAndWave(fp, "/workspaces/go-coastal/data/NACS_Nantucket_PCHA_SLC0_SWL_BE_v20210722.csv", "/workspaces/go-coastal/data/NACS_Nantucket_PCHA_SLC0_Hm0_BE_v20210722.csv")
	hp.ds.Hull.ToGeoJson("/workspaces/go-coastal/data/SACS/mesh.json")
	fmt.Println(hp)
}
