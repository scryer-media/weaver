package benchmark

import (
	"math"
	"reflect"
	"testing"
)

func TestSummarizePairedUsesCompletePairsDeterministically(t *testing.T) {
	samples := []PairedSample{
		{Baseline: 100, Candidate: 80},
		{Baseline: 110, Candidate: 88},
		{Baseline: 90, Candidate: 72},
		{Baseline: 105, Candidate: 84},
	}
	before := append([]PairedSample(nil), samples...)
	first, err := SummarizePaired(samples, 17, DefaultBootstrapResamples)
	if err != nil {
		t.Fatal(err)
	}
	second, err := SummarizePaired(samples, 17, DefaultBootstrapResamples)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("fixed-seed summary is not deterministic: %#v != %#v", first, second)
	}
	if !reflect.DeepEqual(samples, before) {
		t.Fatal("summary mutated its raw paired observations")
	}
	if first.Count != len(samples) || math.Abs(first.GeometricMeanRatio-0.8) > 1e-12 {
		t.Fatalf("unexpected paired summary: %#v", first)
	}
	if math.Abs(first.RatioConfidence95Low-0.8) > 1e-12 || math.Abs(first.RatioConfidence95High-0.8) > 1e-12 {
		t.Fatalf("constant paired ratios should have a point confidence interval: %#v", first)
	}
}

func TestSummarizePairedRejectsInvalidInputs(t *testing.T) {
	for name, samples := range map[string][]PairedSample{
		"too few":  {{Baseline: 1, Candidate: 1}},
		"zero":     {{Baseline: 1, Candidate: 1}, {Baseline: 0, Candidate: 1}},
		"negative": {{Baseline: 1, Candidate: 1}, {Baseline: 1, Candidate: -1}},
		"nan":      {{Baseline: 1, Candidate: 1}, {Baseline: 1, Candidate: math.NaN()}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := SummarizePaired(samples, 1, 100); err == nil {
				t.Fatal("invalid paired observations were accepted")
			}
		})
	}
	if _, err := SummarizePaired([]PairedSample{{1, 1}, {1, 1}}, 1, 0); err == nil {
		t.Fatal("zero bootstrap resamples were accepted")
	}
}
