package clientadapter

import "testing"

func TestNZBGetHistoryCompleteAcceptsSuccessfulDetailSuffix(t *testing.T) {
	for _, status := range []string{"SUCCESS", "SUCCESS/UNPACK", "COMPLETED", "COMPLETE"} {
		if !nzbgetHistoryComplete(status) {
			t.Fatalf("successful NZBGet status %q was not terminal", status)
		}
	}
	for _, status := range []string{"QUEUED", "DOWNLOADING", "FAILURE"} {
		if nzbgetHistoryComplete(status) {
			t.Fatalf("non-terminal NZBGet status %q was accepted", status)
		}
	}
}
