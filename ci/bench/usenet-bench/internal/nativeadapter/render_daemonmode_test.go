package nativeadapter

import (
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// NZBGet pauses every activity when it rejects one configuration line, and
// current releases reject DaemonMode. The Docker lane found this first; the
// argv already puts the process in foreground server mode, so the setting was
// only ever a way to lose a run to a silent pause.
func TestNativeNZBGetDoesNotStateDaemonMode(t *testing.T) {
	spec, err := renderProduct(testConfig(benchmark.NZBGet))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(spec.Content), "DaemonMode") {
		t.Fatalf("native NZBGet config still states DaemonMode:\n%s", spec.Content)
	}
}
