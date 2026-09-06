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

// CrossFixtureSummary is one figure over several fixtures of the same class.
// Every fixture carries equal weight: its paired log ratios are averaged
// first, and the fixture means are averaged second, so a fixture that ran
// more blocks does not count for more than one that ran fewer. The interval
// resamples blocks within each fixture, holding the fixture set fixed — the
// fixtures are the declared corpus, not a sample from a population, and the
// figure makes no claim beyond them.
type CrossFixtureSummary struct {
	FixtureCount           int      `json:"fixture_count"`
	PairedBlocks           int      `json:"paired_blocks"`
	Fixtures               []string `json:"fixtures"`
	GeometricMeanRatio     float64  `json:"geometric_mean_candidate_over_baseline"`
	RatioConfidence95Low   float64  `json:"ratio_confidence_95_low"`
	RatioConfidence95High  float64  `json:"ratio_confidence_95_high"`
	Weighting              string   `json:"weighting"`
	BootstrapSeed          int64    `json:"bootstrap_seed"`
	BootstrapResampleCount int      `json:"bootstrap_resample_count"`
}

// CrossFixtureWeighting states the rule the figure is computed under.
const CrossFixtureWeighting = "equal weight per fixture; paired blocks resampled within each fixture"

// SummarizeAcrossFixtures pools the paired samples of several fixtures into
// one CrossFixtureSummary. samples maps fixture id to that fixture's complete
// paired blocks; every fixture needs at least two.
func SummarizeAcrossFixtures(samples map[string][]PairedSample, bootstrapSeed int64, resamples int) (CrossFixtureSummary, error) {
	if len(samples) == 0 {
		return CrossFixtureSummary{}, fmt.Errorf("cross-fixture summary requires at least one fixture")
	}
	if resamples < 1 {
		return CrossFixtureSummary{}, fmt.Errorf("bootstrap resample count must be positive")
	}
	fixtures := make([]string, 0, len(samples))
	for fixture := range samples {
		fixtures = append(fixtures, fixture)
	}
	slices.Sort(fixtures)

	logRatios := make([][]float64, len(fixtures))
	fixtureMeans := make([]float64, len(fixtures))
	blocks := 0
	for index, fixture := range fixtures {
		pairs := samples[fixture]
		if len(pairs) < 2 {
			return CrossFixtureSummary{}, fmt.Errorf("fixture %s has %d complete blocks, want at least two", fixture, len(pairs))
		}
		ratios := make([]float64, len(pairs))
		for position, sample := range pairs {
			if !finitePositive(sample.Baseline) || !finitePositive(sample.Candidate) {
				return CrossFixtureSummary{}, fmt.Errorf("fixture %s paired sample %d contains a non-finite or non-positive measurement", fixture, position+1)
			}
			ratios[position] = math.Log(sample.Candidate / sample.Baseline)
		}
		logRatios[index] = ratios
		fixtureMeans[index] = mean(ratios)
		blocks += len(pairs)
	}

	rng := rand.New(rand.NewSource(bootstrapSeed)) // #nosec G404 -- deterministic statistical resampling.
	bootstrapped := make([]float64, resamples)
	for iteration := range resamples {
		var acrossFixtures float64
		for _, ratios := range logRatios {
			var sum float64
			for range ratios {
				sum += ratios[rng.Intn(len(ratios))]
			}
			acrossFixtures += sum / float64(len(ratios))
		}
		bootstrapped[iteration] = math.Exp(acrossFixtures / float64(len(logRatios)))
	}
	slices.Sort(bootstrapped)

	return CrossFixtureSummary{
		FixtureCount:           len(fixtures),
		PairedBlocks:           blocks,
		Fixtures:               fixtures,
		GeometricMeanRatio:     math.Exp(mean(fixtureMeans)),
		RatioConfidence95Low:   percentile(bootstrapped, 0.025),
		RatioConfidence95High:  percentile(bootstrapped, 0.975),
		Weighting:              CrossFixtureWeighting,
		BootstrapSeed:          bootstrapSeed,
		BootstrapResampleCount: resamples,
	}, nil
}
