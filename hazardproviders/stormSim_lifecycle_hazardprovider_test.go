package hazardproviders

import (
	"fmt"
	"testing"
	"time"
)

func Test_ParseReachesFile(t *testing.T) {}

func Test_ParseEventsFile(t *testing.T) {
	eventsFP := "/workspaces/consequences-runner/data/coastal/EventDate_LC.csv"

	events, err := parseEventsFile(eventsFP, "", "CSV")
	if err != nil {
		panic(err)
	}

	expected := []string{"190", "485", "833", "990", "575", "664", "176", "609", "984", "294", "610", "562", "112", "268", "161", "859", "335"}
	got := events["DE001"][0]

	for i, val := range got {
		e := expected[i]
		if val != e {
			t.Errorf("Fail: Expected %v, got %v", e, val)
		}
	}
}

func Test_ParseResponsesFile(t *testing.T) {
	responsesFP := "/workspaces/consequences-runner/data/coastal/stage_responses_loc_DE001_lc_0.csv"

	arrivals_expected := []time.Time{
		time.Date(2033, time.Month(8), 26, 5, 50, 29, 0, time.UTC),
		time.Date(2033, time.Month(9), 10, 1, 13, 39, 0, time.UTC),
		time.Date(2033, time.Month(10), 4, 7, 47, 32, 0, time.UTC),
		time.Date(2033, time.Month(10), 14, 22, 47, 59, 0, time.UTC),
		time.Date(2033, time.Month(11), 8, 11, 10, 48, 0, time.UTC),
		time.Date(2034, time.Month(9), 19, 14, 33, 50, 0, time.UTC),
		time.Date(2035, time.Month(8), 20, 22, 31, 42, 0, time.UTC),
		time.Date(2036, time.Month(6), 19, 20, 38, 29, 0, time.UTC),
		time.Date(2036, time.Month(9), 29, 4, 32, 9, 0, time.UTC),
		time.Date(2037, time.Month(11), 4, 5, 20, 28, 0, time.UTC),
		time.Date(2038, time.Month(7), 9, 2, 11, 13, 0, time.UTC),
		time.Date(2039, time.Month(9), 18, 8, 33, 4, 0, time.UTC),
		time.Date(2040, time.Month(9), 8, 14, 17, 10, 0, time.UTC),
		time.Date(2041, time.Month(9), 3, 15, 12, 6, 0, time.UTC),
		time.Date(2041, time.Month(10), 12, 5, 26, 57, 0, time.UTC),
		time.Date(2042, time.Month(9), 20, 21, 50, 47, 0, time.UTC),
		time.Date(2042, time.Month(11), 19, 2, 48, 24, 0, time.UTC),
	}
	depths_expected := []float64{
		0.979270524147806, 1.54121902357747, 3.34393311836401e-07,
		2.54157606740306e-06, 13, 3.10613618627195, 0.0118805618230561,
		0.000230443433672538, 0.000439569934077712, 0.0123576597179041,
		7.99622779970534e-05, 13, 6.78775218723548e-05, 1.02305022821015,
		2.68301460998456e-05, 0.00243471711551489, 1.90291529387485e-06,
	}
	durations_expected := []float64{
		2.0, 2.0, 2.0, 1.0,
		2.0, 2.0, 2.0, 1.0,
		2.0, 1.0, 2.0, 1.0,
		2.0, 4.0, 4.0, 1.0, 4.0,
	}
	add, err := parseResponsesFile(responsesFP, "", "CSV", len(arrivals_expected), 0)
	if err != nil {
		panic(err)
	}

	for i, arrival := range add.arrivals {
		if arrival != arrivals_expected[i] {
			t.Errorf("Fail: Expected %v, got %v", arrivals_expected[i], arrival)
		} else {
			fmt.Println("Arrival Correct")
		}
		if add.depths[i] != depths_expected[i] {
			t.Errorf("Fail: Expected %v, got %v", depths_expected[i], add.depths[i])
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
