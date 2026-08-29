package clientadapter

import (
	"testing"
	"time"
)

func TestValidateWeaverCLIReport(t *testing.T) {
	queued := time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	valid := weaverCLITerminalReport{
		SchemaVersion: 1,
		QueuedAt:      queued,
		CompletionAt:  queued.Add(time.Second),
		Status:        "complete",
	}
	if err := validateWeaverCLIReport(valid); err != nil {
		t.Fatalf("valid report rejected: %v", err)
	}

	for name, report := range map[string]weaverCLITerminalReport{
		"unsupported schema": {
			SchemaVersion: 2,
			QueuedAt:      queued,
			CompletionAt:  queued.Add(time.Second),
			Status:        "complete",
		},
		"failed status": {
			SchemaVersion: 1,
			QueuedAt:      queued,
			CompletionAt:  queued.Add(time.Second),
			Status:        "failed",
		},
		"reverse timestamps": {
			SchemaVersion: 1,
			QueuedAt:      queued.Add(time.Second),
			CompletionAt:  queued,
			Status:        "complete",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateWeaverCLIReport(report); err == nil {
				t.Fatal("invalid report was accepted")
			}
		})
	}
}
