package weaver

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// unusedLocalURL returns a localhost URL on a port that nothing listens on,
// and the address, so a test can bring a server up there later.
func unusedLocalURL(t *testing.T) (string, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}
	return "http://" + addr + "/graphql", addr
}

func TestAwaitGraphQLReturnsTheExitOfAChildThatNeverServed(t *testing.T) {
	url, _ := unusedLocalURL(t)
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
	for _, want := range []string{"exit status 3", "config rejected at startup", "connection refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error must carry %q; got %v", want, err)
		}
	}
	if pauses > 1 {
		t.Errorf("the wait must return at the first poll after the exit, not keep polling; paused %d times", pauses)
	}
}

func TestAwaitGraphQLSucceedsWhenThePortComesUpLate(t *testing.T) {
	url, addr := unusedLocalURL(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "<html></html>")
	})
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"data":{"version":"0.0.0"}}`)
	})
	server := &http.Server{Handler: mux}
	defer server.Close()

	pauses := 0
	probes := 0
	err := awaitGraphQL(url, func() (bool, string) {
		probes++
		return false, ""
	}, func() {
		pauses++
		if pauses == 1 {
			// The weaver binds its port only now, after the first poll was
			// refused.
			listener, listenErr := net.Listen("tcp", addr)
			if listenErr != nil {
				t.Fatalf("bring the port up: %v", listenErr)
			}
			go func() { _ = server.Serve(listener) }()
		}
	})
	if err != nil {
		t.Fatalf("a port that comes up late must end the wait in success; got %v", err)
	}
	if pauses != 1 || probes != 1 {
		t.Errorf("non-vacuity: the first poll must have been refused and the second answered; paused %d, probed %d", pauses, probes)
	}
}
