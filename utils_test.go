package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
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
