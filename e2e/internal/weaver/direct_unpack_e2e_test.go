package weaver

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zeebo/blake3"
)

// This opt-in suite owns a fresh native Weaver process, SQLite database and
// loopback NNTP fixture. It never discovers or operates on an existing stack.
// Run with WEAVER_DIRECT_UNPACK_E2E_BIN pointing at the binary under review.
func TestDirectUnpackE2E(t *testing.T) {
	bin := os.Getenv("WEAVER_DIRECT_UNPACK_E2E_BIN")
	if bin == "" {
		t.Skip("set WEAVER_DIRECT_UNPACK_E2E_BIN to run the real-process archive matrix")
	}
	if !filepath.IsAbs(bin) {
		t.Fatal("WEAVER_DIRECT_UNPACK_E2E_BIN must be absolute")
	}
	for _, tool := range []string{"par2", "xz", "zip"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.MkdirTemp("", "weaver-direct-unpack-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	url, logPath := startUnpackWeaver(t, bin, root, nntp.listener.Addr().(*net.TCPAddr).Port)
	api := provisionUnpackAPI(t, root, url)
	for _, format := range unpackFormats {
		t.Run(format, func(t *testing.T) {
			files, payload := unpackFixture(t, filepath.Join(root, "sources", format), format)
			modes := []string{"clean", "missing", "corrupt"}
			if format == "split" {
				modes = append(modes, "joined-missing", "missing-part", "missing-first", "missing-last", "short-first", "long-first")
			}
			for _, mode := range modes {
				t.Run(mode, func(t *testing.T) {
					slug := strings.ReplaceAll(format, ".", "-") + "-" + mode
					fixtureDir := filepath.Join(root, "fixtures", slug)
					if err := os.MkdirAll(fixtureDir, 0755); err != nil {
						t.Fatal(err)
					}
					posted := map[string][]byte{}
					for name, data := range files {
						posted[name] = data
					}
					if mode == "joined-missing" {
						joined := map[string][]byte{"payload.bin": payload}
						unpackParity(t, fixtureDir, joined)
						for name, data := range joined {
							if strings.HasSuffix(name, ".par2") {
								posted[name] = data
							}
						}
					} else if mode != "clean" {
						unpackParity(t, fixtureDir, posted)
					}
					if mode == "missing-part" {
						delete(posted, "payload.bin.002")
					}
					if mode == "missing-first" {
						delete(posted, "payload.bin.001")
					}
					if mode == "missing-last" {
						delete(posted, "payload.bin.004")
					}
					// PAR2 describes the original geometry. Both article and file
					// CRCs describe the posted geometry, so CRC rejection cannot
					// accidentally provide the chase invalidation being tested.
					minimumStaged := int64(1)
					if mode == "short-first" || mode == "long-first" {
						first := posted["payload.bin.001"]
						if mode == "short-first" {
							first = first[:len(first)-64*1024]
						} else {
							first = append(append([]byte(nil), first...), make([]byte, 64*1024)...)
						}
						posted["payload.bin.001"] = first
						minimumStaged = int64(len(first) + 64*1024)
					}
					gate := &unpackGate{released: make(chan struct{})}
					var once sync.Once
					release := func() { once.Do(func() { close(gate.released) }) }
					defer release()
					nzb := nntp.publishUnpack(slug, mode, posted, gate)
					if err := os.WriteFile(filepath.Join(fixtureDir, slug+".nzb"), nzb, 0644); err != nil {
						t.Fatal(err)
					}
					job, err := api.submit(nzb, slug)
					if err != nil {
						t.Fatal(err)
					}
					outputName := "payload.bin"
					if format == "zip64-stream" {
						outputName = "-"
					}
					stage := filepath.Join(root, "complete", ".weaver-direct-unpack", strconv.Itoa(job))
					var stagedPath string
					var stagedBytes int64
					deadline := time.Now().Add(20 * time.Second)
					for time.Now().Before(deadline) {
						stagedPath, stagedBytes = unpackOutput(stage, outputName)
						if stagedBytes >= minimumStaged && gate.held.Load() > 0 {
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
					if stagedBytes < minimumStaged || gate.held.Load() == 0 {
						t.Fatalf("no extraction while archive BODY responses are held: job=%d status=%s stage=%s bytes=%d held=%d log=%s", job, api.status(job), stagedPath, stagedBytes, gate.held.Load(), logPath)
					}
					statusBefore := api.status(job)
					if statusBefore == "COMPLETED" || statusBefore == "FAILED" {
						t.Fatalf("terminal before held article release: %s", statusBefore)
					}
					evidence := map[string]any{"jobId": job, "format": format, "mode": mode, "stagedPath": stagedPath, "stagedBytesBeforeRelease": stagedBytes, "heldBodies": gate.held.Load(), "statusBeforeRelease": statusBefore}
					evidence["minimumStagedBytes"] = minimumStaged
					release()
					deadline = time.Now().Add(90 * time.Second)
					status := ""
					for time.Now().Before(deadline) {
						status = api.status(job)
						if status == "COMPLETED" || status == "FAILED" {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					if status != "COMPLETED" {
						t.Fatalf("job %d ended %s; log=%s", job, status, logPath)
					}
					output := filepath.Join(root, "complete", slug, outputName)
					actual, err := os.ReadFile(output)
					if err != nil {
						t.Fatal(err)
					}
					want, got := blake3.Sum256(payload), blake3.Sum256(actual)
					if len(actual) != len(payload) || got != want {
						t.Fatalf("output mismatch: bytes=%d/%d BLAKE3=%x/%x", len(actual), len(payload), got, want)
					}
					raw, err := os.ReadFile(logPath)
					if err != nil {
						t.Fatal(err)
					}
					jobLog := unpackJobLog(string(raw), job)
					if !strings.Contains(jobLog, directUnpackArmedMessage) {
						t.Fatal("no direct-unpack admission evidence")
					}
					if mode == "clean" && !strings.Contains(jobLog, directUnpackConsumedMessage) {
						t.Fatal("clean job re-extracted instead of consuming its chase")
					}
					if mode != "clean" && !strings.Contains(jobLog, "PAR2 repair wrote its outputs") {
						t.Fatal("damaged job has no evidence that PAR2 wrote repaired outputs")
					}
					evidence["terminalStatus"] = status
					evidence["outputBLAKE3"] = fmt.Sprintf("%x", got)
					data, err := json.MarshalIndent(evidence, "", "  ")
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(fixtureDir, "evidence.json"), data, 0644); err != nil {
						t.Fatal(err)
					}
					t.Logf("job=%d direct output=%d bytes before release; %s; BLAKE3=%x", job, stagedBytes, status, got)
				})
			}
		})
	}
}

func unpackOutput(root, name string) (string, int64) {
	var found string
	var size int64
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && entry.Type().IsRegular() && entry.Name() == name {
			if info, err := entry.Info(); err == nil && info.Size() > size {
				found, size = path, info.Size()
			}
		}
		return nil
	})
	return found, size
}

func unpackJobLog(raw string, job int) string {
	var lines []string
	for _, line := range strings.Split(raw, "\n") {
		line = ansiEscape.ReplaceAllString(line, "")
		if directLogJobID(line) == strconv.Itoa(job) {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func startUnpackWeaver(t *testing.T, bin, root string, nntpPort int, extraEnv ...string) (string, string) {
	t.Helper()
	url, logPath, _ := startManagedUnpackWeaver(t, bin, root, "weaver.log", nntpPort, extraEnv...)
	return url, logPath
}

func startManagedUnpackWeaver(t *testing.T, bin, root, logName string, nntpPort int, extraEnv ...string) (string, string, func()) {
	t.Helper()
	databaseURL := nativeUnpackPostgresURL(t, root)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	config := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true
max_retries = 1
[[servers]]
id = 1
host = "127.0.0.1"
port = %d
tls = false
connections = 8
active = true
priority = 0
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), nntpPort)
	path := filepath.Join(root, "weaver.toml")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, logName)
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "--config", path, "serve", "--port", strconv.Itoa(port))
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "WEAVER_") && !strings.HasPrefix(entry, "RUST_LOG=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "WEAVER_FORCE_KEY_FILE=1", "WEAVER_DIRECT_UNPACK=true", "RUST_LOG=info,weaver_server_core::pipeline::completion=debug", "NO_COLOR=1")
	cmd.Env = append(cmd.Env, extraEnv...)
	if databaseURL != "" {
		cmd.Env = append(cmd.Env, "WEAVER_DATABASE_URL="+databaseURL)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stop := sync.OnceFunc(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		logFile.Close()
	})
	t.Cleanup(stop)
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	return url, logPath, stop
}
