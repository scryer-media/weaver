package weaver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// startingWeaver serves the way a weaver that has not finished starting
// looks to the readiness wait — every request refused with 503 — until the
// returned switch is flipped, and as a ready weaver afterwards. The test holds
// the listener throughout, so nothing else on the machine can take its port
// between the refused poll and the answered one.
func startingWeaver(t *testing.T) (string, *atomic.Bool) {
	t.Helper()
	var up atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "<html></html>")
	})
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"data":{"version":"0.0.0"}}`)
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "still starting", http.StatusServiceUnavailable)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL + "/graphql", &up
}

func TestAwaitGraphQLReturnsTheExitOfAChildThatNeverServed(t *testing.T) {
	url, _ := startingWeaver(t)
	logPath := filepath.Join(t.TempDir(), "weaver.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	cmd := exec.Command("sh", "-c", "echo 'config rejected at startup' >&2; exit 3")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	watch := watchChildExit(cmd, logPath)

	// Each pause waits for the child's exit rather than for time, so the wait
	// is decided by the exit and by nothing else.
	pauses := 0
	err = awaitGraphQL(url, watch.Probe, func() {
		pauses++
		<-watch.done
	})
	if err == nil {
		t.Fatal("a child that exited without serving must end the wait with an error")
	}
	for _, want := range []string{"exit status 3", "config rejected at startup", "still starting"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error must carry %q; got %v", want, err)
		}
	}
	if pauses > 1 {
		t.Errorf("the wait must return at the first poll after the exit, not keep polling; paused %d times", pauses)
	}
}

func TestAwaitGraphQLSucceedsWhenTheServerAnswersLate(t *testing.T) {
	url, up := startingWeaver(t)

	pauses := 0
	probes := 0
	err := awaitGraphQL(url, func() (bool, string) {
		probes++
		return false, ""
	}, func() {
		pauses++
		if pauses == 1 {
			// The weaver finishes starting only now, after the first poll was
			// refused.
			up.Store(true)
		}
	})
	if err != nil {
		t.Fatalf("a server that answers late must end the wait in success; got %v", err)
	}
	if pauses != 1 || probes != 1 {
		t.Errorf("non-vacuity: the first poll must have been refused and the second answered; paused %d, probed %d", pauses, probes)
	}
}
