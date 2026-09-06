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

func TestSummarizeAcrossFixturesWeightsFixturesEqually(t *testing.T) {
	// Fixture "many" ran four blocks at ratio 0.5; fixture "few" ran two at
	// 2.0. Equal weight per fixture makes the pooled ratio exactly 1.0; block
	// weighting would have pulled it to 0.5^(4/6) * 2^(2/6) = 0.79.
	samples := map[string][]PairedSample{
		"many": {{Baseline: 100, Candidate: 50}, {Baseline: 200, Candidate: 100}, {Baseline: 80, Candidate: 40}, {Baseline: 120, Candidate: 60}},
		"few":  {{Baseline: 100, Candidate: 200}, {Baseline: 50, Candidate: 100}},
	}
	first, err := SummarizeAcrossFixtures(samples, 17, DefaultBootstrapResamples)
	if err != nil {
		t.Fatal(err)
	}
	second, err := SummarizeAcrossFixtures(samples, 17, DefaultBootstrapResamples)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("fixed-seed cross-fixture summary is not deterministic: %#v != %#v", first, second)
	}
	if first.FixtureCount != 2 || first.PairedBlocks != 6 || !reflect.DeepEqual(first.Fixtures, []string{"few", "many"}) {
		t.Fatalf("unexpected fixture accounting: %#v", first)
	}
	if math.Abs(first.GeometricMeanRatio-1.0) > 1e-12 {
		t.Fatalf("fixtures were not weighted equally: %#v", first)
	}
	if math.Abs(first.RatioConfidence95Low-1.0) > 1e-12 || math.Abs(first.RatioConfidence95High-1.0) > 1e-12 {
		t.Fatalf("constant within-fixture ratios should have a point interval: %#v", first)
	}
	if first.Weighting != CrossFixtureWeighting {
		t.Fatalf("summary must state its weighting rule: %#v", first)
	}
}

func TestSummarizeAcrossFixturesRejectsInvalidInputs(t *testing.T) {
	valid := []PairedSample{{Baseline: 100, Candidate: 80}, {Baseline: 110, Candidate: 88}}
	for name, samples := range map[string]map[string][]PairedSample{
		"no fixtures":  {},
		"single block": {"a": valid, "b": {{Baseline: 100, Candidate: 80}}},
		"non-positive": {"a": valid, "b": {{Baseline: 100, Candidate: 0}, {Baseline: 100, Candidate: 80}}},
	} {
		if _, err := SummarizeAcrossFixtures(samples, 17, 100); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
	if _, err := SummarizeAcrossFixtures(map[string][]PairedSample{"a": valid}, 17, 0); err == nil {
		t.Fatal("zero resamples: expected an error")
	}
}
