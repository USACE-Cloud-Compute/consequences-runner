package hazardproviders

import (
	"fmt"
	"testing"
	"time"
)

func Test_ParseReachesFile(t *testing.T) {}

func Test_ParseEventsFile(t *testing.T) {
	eventsFP := "/workspaces/consequences-runner/data/coastal/events.csv"

	events, err := parseEventsFile(eventsFP, "", "CSV")
	if err != nil {
		panic(err)
	}
	//the events in the first lifecycle for DE001 reach
	expected := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18"}
	got := events["DE001"][0]

	for i, val := range got {
		e := expected[i]
		if val != e {
			t.Errorf("Fail: Expected %v, got %v", e, val)
		}
	}
}

func Test_ParseResponsesFile(t *testing.T) {
	responsesFP := "/workspaces/consequences-runner/data/coastal/q_aggregate_loc_DE001_lc_1.parquet"

	arrivals_expected := []time.Time{
		time.Date(2033, time.Month(10), 15, 15, 38, 16, 0, time.UTC),
		time.Date(2034, time.Month(8), 24, 10, 52, 13, 0, time.UTC),
		time.Date(2035, time.Month(7), 11, 5, 48, 15, 0, time.UTC),
		time.Date(2035, time.Month(8), 25, 12, 38, 1, 0, time.UTC),
		time.Date(2035, time.Month(9), 20, 9, 25, 54, 0, time.UTC),
		time.Date(2035, time.Month(10), 17, 6, 24, 53, 0, time.UTC),
		time.Date(2036, time.Month(9), 19, 5, 44, 43, 0, time.UTC),
		time.Date(2037, time.Month(8), 30, 12, 55, 19, 0, time.UTC),
		time.Date(2037, time.Month(9), 22, 12, 18, 32, 0, time.UTC),
		time.Date(2038, time.Month(8), 15, 20, 22, 26, 0, time.UTC),
		time.Date(2038, time.Month(9), 16, 10, 1, 50, 0, time.UTC),
		time.Date(2039, time.Month(8), 26, 17, 47, 49, 0, time.UTC),
		time.Date(2040, time.Month(9), 13, 1, 36, 58, 0, time.UTC),
		time.Date(2040, time.Month(9), 25, 4, 17, 23, 0, time.UTC),
		time.Date(2040, time.Month(10), 4, 13, 16, 13, 0, time.UTC),
		time.Date(2041, time.Month(8), 20, 4, 9, 17, 0, time.UTC),
		time.Date(2042, time.Month(9), 3, 6, 47, 6, 0, time.UTC),
		time.Date(2042, time.Month(9), 11, 15, 47, 36, 0, time.UTC),
		time.Date(2042, time.Month(9), 18, 20, 43, 54, 0, time.UTC),
	}
	watersurface_expected := []float64{
		5.065454720317759e-06, 0.07954816684712816, 0.06067342218875873,
		7.633939500733733, 0.0003424453866254134, 0.005582318033540606, 0.10717274871617052,
		0.0007989156854487938, 2.216023976299491, 0.0014245964507591457,
		1.077396874304933, 0.00034754523979087676, 0.025909502986701766, 13,
		1.0045047113893832, 2.428102985323753e-05, 0.030396290414466436, 0.00023983922662932866, 4.337107109226778,
	}
	durations_expected := []float64{
		2.0, 2.0, 4.0, 2.0,
		1.0, 1.0, 2.0, 2.0,
		2.0, 4.0, 2.0, 1.0,
		2.0, 1.0, 2.0, 1.0, 4.0, 2.0, 2.0,
	}
	add, err := parseResponsesFile(responsesFP, "", "PARQUET", len(arrivals_expected), 1)
	if err != nil {
		panic(err)
	}

	for i, arrival := range add.arrivals {
		if arrival != arrivals_expected[i] {
			t.Errorf("Fail: Expected %v, got %v", arrivals_expected[i], arrival)
		} else {
			fmt.Println("Arrival Correct")
		}
		if add.watersurface[i] != watersurface_expected[i] {
			t.Errorf("Fail: Expected %v, got %v", watersurface_expected[i], add.watersurface[i])
		} else {
			fmt.Println("Depth correct")
		}
		if add.durations[i] != durations_expected[i] {
			t.Errorf("Fail: Expected %v, got %v", durations_expected[i], add.durations[i])
		} else {
			fmt.Println("Duration Correct")
		}
	}

}
