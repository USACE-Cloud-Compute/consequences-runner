package hazardproviders

import "testing"

// StormSim stage is a water-surface elevation; go-consequences samples damage
// on depth above ground. Before this conversion existed, a structure standing
// on 4.53 ft of ground was scored as if the ground were at the datum.
func Test_depthsAboveGround(t *testing.T) {
	stages := []float64{13.0, 9.0, 4.53, 1.0}

	t.Run("ground is removed from every storm", func(t *testing.T) {
		got := depthsAboveGround(stages, 4.53)
		want := []float64{8.47, 4.47, 0.0, -3.53}
		for i := range want {
			if diff := got[i] - want[i]; diff > 1e-9 || diff < -1e-9 {
				t.Errorf("stage %v: got %v, want %v", stages[i], got[i], want[i])
			}
		}
	})

	t.Run("water below ground stays negative rather than clamping", func(t *testing.T) {
		// the damage function decides what a negative depth means, as it does
		// for the depth-grid providers; clamping here would silently wet a
		// structure the water never reached
		got := depthsAboveGround([]float64{1.0}, 11.0)
		if got[0] != -10.0 {
			t.Errorf("got %v, want -10", got[0])
		}
	})

	t.Run("zero ground leaves the series untouched", func(t *testing.T) {
		got := depthsAboveGround(stages, 0)
		for i := range stages {
			if got[i] != stages[i] {
				t.Errorf("index %d: got %v, want %v", i, got[i], stages[i])
			}
		}
	})

	t.Run("the input series is not mutated", func(t *testing.T) {
		original := []float64{13.0, 9.0}
		depthsAboveGround(original, 4.53)
		if original[0] != 13.0 || original[1] != 9.0 {
			t.Errorf("input was modified: %v", original)
		}
	})
}
