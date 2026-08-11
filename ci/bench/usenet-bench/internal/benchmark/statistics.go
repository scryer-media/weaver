package benchmark

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
)

const DefaultBootstrapResamples = 10_000

// PairedSample is one complete randomized-block observation. Candidate and
// Baseline must be like-for-like positive measurements from the same block.
type PairedSample struct {
	Baseline  float64
	Candidate float64
}

// PairedSummary reports the raw-scale centers and candidate/baseline ratio.
// Confidence bounds are a deterministic percentile bootstrap over complete
// pairs; no observation is removed as an outlier.
type PairedSummary struct {
	Count                   int     `json:"count"`
	BaselineMedian          float64 `json:"baseline_median"`
	CandidateMedian         float64 `json:"candidate_median"`
	BaselineCoefficientVar  float64 `json:"baseline_coefficient_of_variation"`
	CandidateCoefficientVar float64 `json:"candidate_coefficient_of_variation"`
	GeometricMeanRatio      float64 `json:"geometric_mean_candidate_over_baseline"`
	RatioConfidence95Low    float64 `json:"ratio_confidence_95_low"`
	RatioConfidence95High   float64 `json:"ratio_confidence_95_high"`
	BootstrapSeed           int64   `json:"bootstrap_seed"`
	BootstrapResampleCount  int     `json:"bootstrap_resample_count"`
}

func SummarizePaired(samples []PairedSample, bootstrapSeed int64, resamples int) (PairedSummary, error) {
	if len(samples) < 2 {
		return PairedSummary{}, fmt.Errorf("paired summary requires at least two complete blocks")
	}
	if resamples < 1 {
		return PairedSummary{}, fmt.Errorf("bootstrap resample count must be positive")
	}
	baseline := make([]float64, len(samples))
	candidate := make([]float64, len(samples))
	logRatios := make([]float64, len(samples))
	for index, sample := range samples {
		if !finitePositive(sample.Baseline) || !finitePositive(sample.Candidate) {
			return PairedSummary{}, fmt.Errorf("paired sample %d contains a non-finite or non-positive measurement", index+1)
		}
		baseline[index] = sample.Baseline
		candidate[index] = sample.Candidate
		logRatios[index] = math.Log(sample.Candidate / sample.Baseline)
	}

	rng := rand.New(rand.NewSource(bootstrapSeed)) // #nosec G404 -- deterministic statistical resampling.
	bootstrapped := make([]float64, resamples)
	for iteration := range resamples {
		var sum float64
		for range samples {
			sum += logRatios[rng.Intn(len(logRatios))]
		}
		bootstrapped[iteration] = math.Exp(sum / float64(len(samples)))
	}
	slices.Sort(bootstrapped)

	return PairedSummary{
		Count:                   len(samples),
		BaselineMedian:          median(baseline),
		CandidateMedian:         median(candidate),
		BaselineCoefficientVar:  coefficientOfVariation(baseline),
		CandidateCoefficientVar: coefficientOfVariation(candidate),
		GeometricMeanRatio:      math.Exp(mean(logRatios)),
		RatioConfidence95Low:    percentile(bootstrapped, 0.025),
		RatioConfidence95High:   percentile(bootstrapped, 0.975),
		BootstrapSeed:           bootstrapSeed,
		BootstrapResampleCount:  resamples,
	}, nil
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}

func mean(values []float64) float64 {
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func median(values []float64) float64 {
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	middle := len(ordered) / 2
	if len(ordered)%2 == 1 {
		return ordered[middle]
	}
	return (ordered[middle-1] + ordered[middle]) / 2
}

func coefficientOfVariation(values []float64) float64 {
	average := mean(values)
	var sumSquares float64
	for _, value := range values {
		delta := value - average
		sumSquares += delta * delta
	}
	return math.Sqrt(sumSquares/float64(len(values)-1)) / average
}

func percentile(ordered []float64, probability float64) float64 {
	if len(ordered) == 1 {
		return ordered[0]
	}
	position := probability * float64(len(ordered)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return ordered[lower]
	}
	weight := position - float64(lower)
	return ordered[lower]*(1-weight) + ordered[upper]*weight
}
