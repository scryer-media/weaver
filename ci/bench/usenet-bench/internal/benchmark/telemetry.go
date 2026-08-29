package benchmark

import (
	"fmt"
	"strings"
)

type CounterStatus string

const (
	CounterMeasured    CounterStatus = "measured"
	CounterUnavailable CounterStatus = "unavailable"
)

// CounterValue makes unavailable hardware counters explicit. A missing value
// is never interpreted as zero.
type CounterValue struct {
	Status CounterStatus `json:"status"`
	Value  *uint64       `json:"value,omitempty"`
	Reason string        `json:"reason,omitempty"`
}

func MeasuredCounter(value uint64) CounterValue {
	return CounterValue{Status: CounterMeasured, Value: &value}
}

func UnavailableCounter(reason string) CounterValue {
	return CounterValue{Status: CounterUnavailable, Reason: reason}
}

func (c CounterValue) validate(name string) error {
	switch c.Status {
	case CounterMeasured:
		if c.Value == nil || strings.TrimSpace(c.Reason) != "" {
			return fmt.Errorf("%s measured counter must have a value and no unavailable reason", name)
		}
	case CounterUnavailable:
		if c.Value != nil || strings.TrimSpace(c.Reason) == "" {
			return fmt.Errorf("%s unavailable counter must have a reason and no value", name)
		}
	default:
		return fmt.Errorf("%s has unsupported counter status %q", name, c.Status)
	}
	return nil
}

// CounterMeasurement describes one resource counter. CPU time and retired
// instructions may come from different collectors and scopes, so their
// provenance is recorded independently rather than implied by the run.
type CounterMeasurement struct {
	Scope            string `json:"scope"`
	Collector        string `json:"collector"`
	CollectorVersion string `json:"collector_version"`
	CounterValue
}

func MeasuredMeasurement(scope, collector, collectorVersion string, value uint64) CounterMeasurement {
	return CounterMeasurement{
		Scope:            scope,
		Collector:        collector,
		CollectorVersion: collectorVersion,
		CounterValue:     MeasuredCounter(value),
	}
}

func UnavailableMeasurement(scope, collector, collectorVersion, reason string) CounterMeasurement {
	return CounterMeasurement{
		Scope:            scope,
		Collector:        collector,
		CollectorVersion: collectorVersion,
		CounterValue:     UnavailableCounter(reason),
	}
}

func (m CounterMeasurement) validate(name string) error {
	if m.Scope != "client_container" && m.Scope != "client_process" {
		return fmt.Errorf("%s must use client_container or client_process scope", name)
	}
	if strings.TrimSpace(m.Collector) == "" || strings.TrimSpace(m.CollectorVersion) == "" {
		return fmt.Errorf("%s requires collector and collector version", name)
	}
	return m.CounterValue.validate(name)
}

// ResourceMetrics record CPU time and retired instructions for the actual
// client workload, never the benchmark controller alone. Each counter carries
// its own provenance so reviewers can interpret availability correctly.
type ResourceMetrics struct {
	CPUTimeNanoseconds  CounterMeasurement `json:"cpu_time_nanoseconds"`
	InstructionsRetired CounterMeasurement `json:"instructions_retired"`
}

func (m ResourceMetrics) Validate() error {
	if err := m.CPUTimeNanoseconds.validate("cpu_time_nanoseconds"); err != nil {
		return err
	}
	return m.InstructionsRetired.validate("instructions_retired")
}
