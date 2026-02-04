package actions

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

/*
import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/usace/cc-go-sdk"
)

func Test_SummarizeOutputs(t *testing.T) {
	a := cc.Action{
		Name:        "summarize-output",
		Type:        "summarize-output",
		Description: "summarize-output",
		IOManager: cc.IOManager{
			Attributes: map[string]any{
				"tableName":                 "damages",
				"blockFilePath":             "/workspaces/consequences-runner/data/checkpoint-validation/simulations/validation/blockfile.json",
				"realizationNumber":         1,
				"resultPathPattern":         "/workspaces/consequences-runner/data/scenario-complex-levees/simulations/%v/consequences/duwamish/Duwamish_NSIv2022_Calibrated_consequences.gpkg",
				"outputDriver":              "GPKG",
				"realizationResultFilePath": "/workspaces/consequences-runner/data/scenario-complex-levees/simulations/summary-outputs/realization_1_consequences.csv",
			}},
	}
	err := SummarizeOutputs(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}
func Test_SummarizeOutputsToBlocks(t *testing.T) {
	a := cc.Action{
		Name:        "summarize-output",
		Type:        "summarize-output",
		Description: "summarize-output",
		IOManager: cc.IOManager{
			Attributes: map[string]any{
				"tableName":                 "damages",
				"blockFilePath":             "/workspaces/consequences-runner/data/checkpoint-validation/simulations/validation/blockfile.json",
				"realizationNumber":         1,
				"resultPathPattern":         "/workspaces/consequences-runner/data/checkpoint-validation/simulations/validation/%v/consequences/duwamish/Duwamish_NSIv2022_Calibrated_consequences.gpkg",
				"outputDriver":              "GPKG",
				"realizationResultFilePath": "/workspaces/consequences-runner/data/checkpoint-validation/simulations/validation/summary-outputs/realization_1_block_consequences.csv",
			}},
	}
	err := SummarizeOutputsToBlocks(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}
func Test_SummarizeOutputsToFrequency(t *testing.T) {
	a := cc.Action{
		Name:        "summarize-output",
		Type:        "summarize-output",
		Description: "summarize-output",
		IOManager: cc.IOManager{
			Attributes: map[string]any{
				"tableName":                        "damages",
				"eadOrdinateCap":                   50, //ordinate for the 10 year
				"blockFilePath":                    "/workspaces/consequences-runner/data/checkpoint-validation/simulations/validation/blockfile.json",
				"realizationNumber":                1,
				"resultPathPattern":                "/workspaces/consequences-runner/data/scenario-simple-levees/simulations/%v/consequences/duwamish/Duwamish_NSIv2022_Calibrated_consequences.gpkg",
				"outputDriver":                     "GPKG",
				"spatialOutputDriver":              "GPKG",
				"outputTableName":                  "summary",
				"realizationResultFilePath":        "/workspaces/consequences-runner/data/scenario-simple-levees/simulations/summary-outputs/realization_1_frequency_consequences.csv",
				"realizationSpatialResultFilePath": "/workspaces/consequences-runner/data/scenario-simple-levees/simulations/summary-outputs/checkpoint-validation_realization_1_event_based_frequency_consequences_values_10year.gpkg",
			}},
	}
	err := SummarizeOutputsToFrequency(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}
func Test_SummarizeOutputsToWatershedFrequency(t *testing.T) {
	a := cc.Action{
		Name:        "summarize-output",
		Type:        "summarize-output",
		Description: "summarize-output",
		IOManager: cc.IOManager{
			Attributes: map[string]any{
				"tableName":                 "damages",
				"blockFilePath":             "/workspaces/consequences-runner/data/checkpoint-validation/simulations/validation/blockfile.json",
				"realizationNumber":         1,
				"resultPathPattern":         "/workspaces/consequences-runner/data/scenario-simple-levees/simulations/%v/consequences/duwamish/Duwamish_NSIv2022_Calibrated_consequences.gpkg",
				"outputDriver":              "GPKG",
				"spatialOutputDriver":       "GPKG",
				"outputTableName":           "summary",
				"realizationResultFilePath": "/workspaces/consequences-runner/data/scenario-simple-levees/simulations/summary-outputs/realization_1_SummedAALs_consequences.csv",
			}},
	}
	err := SummarizeOutputsToWatershedFrequency(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}*/

func Test_PostProcessCsv(t *testing.T) {
	path := "/workspaces/consequences-runner/data/scenario-simple-levees/simulations/summary-outputs/realization_1_block_consequences.csv"
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fail()
	}
	f := string(bytes)
	lines := strings.Split(f, "\n")
	rows := make([]BlockRow, 0)
	depthequalsvelocity := 0
	damageequalsdepth := 0
	velocityequalsdamage := 0
	noneequal := 0
	for i, l := range lines {

		if i != 0 {
			vals := strings.Split(l, ",")
			if len(vals) > 2 {
				b, _ := strconv.Atoi(vals[1])
				dam, _ := strconv.Atoi(vals[5])
				d, _ := strconv.Atoi(vals[11])
				v, _ := strconv.Atoi(vals[13])
				row := BlockRow{
					Block:           b,
					Name:            vals[2],
					DamageEventID:   dam,
					DepthEventID:    d,
					VelocityEventId: v,
				}
				if d == v {
					depthequalsvelocity++
				}
				if d == dam {
					damageequalsdepth++
				} else {
					if v != dam {
						noneequal++
					}
				}
				if v == dam {
					velocityequalsdamage++
				}
				rows = append(rows, row)
			}

		}
	}
	count := len(rows)
	fmt.Printf("Depth Event Number Equals Velocity Event Number : %.4f\n", float32(depthequalsvelocity)/float32(count))
	fmt.Printf("Damage Event Number Equals Depth Event Number : %.4f\n", float32(damageequalsdepth)/float32(count))
	fmt.Printf("Damage Event Number Equals Velocity Event Number : %.4f\n", float32(velocityequalsdamage)/float32(count))
	fmt.Printf("No Event Numbers Equal : %.4f\n", float32(noneequal)/float32(count))
}

type BlockRow struct {
	Block           int
	Name            string
	X               float64
	Y               float64
	DamageEventID   int
	DepthEventID    int
	VelocityEventId int
}

