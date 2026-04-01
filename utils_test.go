package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/usace-cloud-compute/consequences-runner/hazardproviders"
)

/*
	func Test_ConvertParquet(t *testing.T) {
		//ConvertGpkgToParquet("Bluestone Local")
		ConvertGpkgToParquet("Bluestone Upper")
		ConvertGpkgToParquet("Coal")
		ConvertGpkgToParquet("Elk Middle")
		ConvertGpkgToParquet("Elk at Sutton")
		ConvertGpkgToParquet("Gauley Lower")
		ConvertGpkgToParquet("Gauley at Summersville")
		ConvertGpkgToParquet("Greenbrier")
		ConvertGpkgToParquet("Lower Kanawha-Elk Lower")
		ConvertGpkgToParquet("Lower New")
		ConvertGpkgToParquet("New Middle")
		ConvertGpkgToParquet("New-Little River")
		ConvertGpkgToParquet("Upper Kanawha")
		ConvertGpkgToParquet("Upper New at Claytor")
	}
*/
func Test_Main(t *testing.T) {
	main()
}
func Test_Logic(t *testing.T) {
	inventoryDriver := "SHP"
	result := strings.Compare(inventoryDriver, "GPKG")
	fmt.Println(result)
	result2 := strings.Compare(inventoryDriver, "JSON")
	fmt.Println(result2)
	if strings.Compare(inventoryDriver, "GPKG") == 0 || strings.Compare(inventoryDriver, "JSON") == 0 {
		log.Fatal("Terminating the plugin.  Only GPKG, SHP or PARQUET drivers support at this time\n")
	}
}

func Test_Download(t *testing.T) {

	remote_root := "/model-library/ffrd-trinity/production/simulations/"
	local_root := "/workspaces/consequences-runner/data/trinity/production/simulations/"
	objects := []string{
		/*"summary-data/aep-grids/%v/mean_aep_depth_10yr.tif",
		"summary-data/aep-grids/%v/mean_aep_depth_20yr.tif",
		"summary-data/aep-grids/%v/mean_aep_depth_50yr.tif",
		"summary-data/aep-grids/%v/mean_aep_depth_100yr.tif",
		"summary-data/aep-grids/%v/mean_aep_depth_200yr.tif",
		"summary-data/aep-grids/%v/mean_aep_depth_500yr.tif",
		"summary-data/aep-grids/%v/mean_aep_depth_1000yr.tif",
		"summary-data/aep-grids/%v/mean_aep_depth_2000yr.tif",
		"summary-data/aep-grids/%v/stdev_aep_depth_10yr.tif",
		"summary-data/aep-grids/%v/stdev_aep_depth_20yr.tif",
		"summary-data/aep-grids/%v/stdev_aep_depth_50yr.tif",
		"summary-data/aep-grids/%v/stdev_aep_depth_100yr.tif",
		"summary-data/aep-grids/%v/stdev_aep_depth_200yr.tif",
		"summary-data/aep-grids/%v/stdev_aep_depth_500yr.tif",*/
		"summary-data/aep-grids/%v/stdev_aep_depth_1000yr.tif",
		/*"summary-data/aep-grids/%v/stdev_aep_depth_2000yr.tif",*/
	}
	/*eventFile := "/workspaces/consequences-runner/data/trinity/production/samples.csv"
	data, err := os.ReadFile(eventFile)
	if err != nil {
		t.Fail()
	}
	strdata := string(data)
	events := strings.Split(strdata, ",")
	*/
	//bridgeport, east-fork
	events := []string{"livingston"}
	remote_objects := []string{}
	local_objects := []string{}
	for _, r := range events {
		for _, f := range objects {
			o := fmt.Sprintf(f, r)
			remote := fmt.Sprintf("%v%v", remote_root, o)
			local := fmt.Sprintf("%v%v", local_root, o)
			remote_objects = append(remote_objects, remote)
			local_objects = append(local_objects, local)
		}
	}

	Download(remote_objects, local_objects)
}
func Test_Reproject(t *testing.T) {

	local_root := "/workspaces/consequences-runner/data/trinity/production/simulations/"
	objects := []string{
		"summary-data/aep-grids/%v/stdev_aep_depth_1000yr%v.tif",
	}
	events := []string{"livingston"}

	for _, r := range events {
		for _, f := range objects {
			src_file := fmt.Sprintf(f, r, "")
			dest_file := fmt.Sprintf(f, r, "_4326")
			src := fmt.Sprintf("%v%v", local_root, src_file)
			dest := fmt.Sprintf("%v%v", local_root, dest_file)
			cmd := exec.Command("gdalwarp", "-t_srs", "EPSG:4326", "-of", "COG", "-co", "COMPRESS=DEFLATE", "-co", "NUM_THREADS=ALL_CPUS", src, dest)
			output, err := cmd.Output()
			if err != nil {
				log.Fatalf("Error executing command: %s", err)
			}

			fmt.Println("Command Successfully Executed")
			fmt.Println(string(output))
		}
	}

}
func Test_Compute(t *testing.T) {
	//

	events := []string{"bardwell-creek", "bedias-creek", "blw-bear", "blw-clear-fork", "blw-east-fork", "blw-elkhart", "blw-richland", "blw-west-fork", "bridgeport", "cedar-creek", "chambers-creek", "clear-creek", "clear-fork", "denton", "eagle-mountain", "east-fork", "kickapoo", "lavon", "lewisville", "livingston", "mill-creek", "mountain", "ray-hubbard", "ray-roberts", "rchlnd-chmbers", "white-rock"}

	for _, r := range events {
		os.Setenv("MODEL_PREFIX", r)
		fmt.Println("running " + os.Getenv("MODEL_PREFIX"))
		main()
	}

}
func Test_Format(t *testing.T) {
	//

	freqs := []float64{0.1, 0.05, 0.02, 0.01, 0.002, 0.005, 0.001, 0.0005}

	for _, f := range freqs {
		val := 1 / f
		fmt.Println(fmt.Sprintf("%1.0fYr_MS", val))
		fmt.Println(fmt.Sprintf("%1.6fMS", f))
	}

}

func Test_ParseStormsFile(t *testing.T) {
	responsesFP := "/workspaces/consequences-runner/data/coastal/stage_EventDate_LC_responses.csv"

	arrivals_expected := []time.Time{
		time.Date(2033, time.Month(8), 22, 14, 7, 0, 0, time.UTC),
		time.Date(2034, time.Month(8), 19, 19, 31, 39, 0, time.UTC),
		time.Date(2034, time.Month(8), 28, 8, 07, 19, 0, time.UTC),
		time.Date(2034, time.Month(9), 16, 8, 3, 47, 0, time.UTC),
		time.Date(2034, time.Month(10), 12, 7, 19, 13, 0, time.UTC),
		time.Date(2035, time.Month(7), 17, 4, 16, 10, 0, time.UTC),
		time.Date(2035, time.Month(10), 20, 9, 19, 48, 0, time.UTC),
		time.Date(2036, time.Month(9), 17, 6, 47, 54, 0, time.UTC),
		time.Date(2037, time.Month(10), 01, 18, 45, 0, 0, time.UTC),
		time.Date(2037, time.Month(10), 24, 6, 15, 12, 0, time.UTC),
		time.Date(2040, time.Month(6), 13, 14, 13, 24, 0, time.UTC),
		time.Date(2040, time.Month(8), 19, 23, 56, 21, 0, time.UTC),
		time.Date(2040, time.Month(8), 30, 6, 53, 14, 0, time.UTC),
		time.Date(2040, time.Month(9), 26, 7, 11, 16, 0, time.UTC),
		time.Date(2041, time.Month(10), 5, 0, 7, 32, 0, time.UTC),
		time.Date(2041, time.Month(11), 15, 6, 8, 48, 0, time.UTC),
		time.Date(2042, time.Month(9), 6, 23, 29, 45, 0, time.UTC),
	}
	depths_expected := []float64{
		13.0, 1.19023548877452, 0.00189844601579208, 1.0171892210224,
		1.1684668225599, 2.67564711780564, 1.11368483591891, 5.95024604558782e-06,
		0.00610899031783363, 0.0051434691700571, 1.29154912885772, 1.10496133027909,
		0.00478200367925463, 1.09444238480238, 1.01478090469665, 13.0, 0.012841852964191,
	}
	durations_expected := []float64{
		2.0, 2.0, 2.0, 2.0, 4.0, 2.0,
		4.0, 2.0, 2.0, 4.0, 2.0, 2.0,
		2.0, 2.0, 2.0, 1.0, 2.0,
	}
	arrivals, depths, durations, err := hazardproviders.ParseResponsesFile(responsesFP, "using-layer_id=0", "CSV", len(arrivals_expected))
	if err != nil {
		panic(err)
	}

	for i, arrival := range arrivals {
		if arrival != arrivals_expected[i] {
			t.Errorf("Fail: Expected %v, got %v", arrivals_expected[i], arrival)
		} else {
			fmt.Println("Arrival Correct")
		}
		if depths[i] != depths_expected[i] {
			t.Errorf("Fail: Expected %v, got %v", depths_expected[i], depths[i])
		} else {
			fmt.Println("Depth correct")
		}
		if durations[i] != durations_expected[i] {
			t.Errorf("Fail: Expected %v, got %v", durations_expected[i], durations[i])
		} else {
			fmt.Println("Duration Correct")
		}
	}

}

func Test_ComputeCoastalEventAction(t *testing.T) {

	main()
}
