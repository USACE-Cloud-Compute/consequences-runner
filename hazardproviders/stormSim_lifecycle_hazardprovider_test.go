package hazardproviders

import (
	"fmt"
	"testing"
	"time"
)

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
	arrivals, depths, durations, err := parseResponsesFile(responsesFP, "using-layer_id=0", "CSV", len(arrivals_expected))
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
