package weaver

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Repaired embedded protection is still an ordinary source after restart.
// Changing its protected bytes must invalidate the prior successful repair.
func TestPar3InsideRestartE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set native Weaver and official PAR3 reference binaries")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("binaries must use absolute paths")
	}
	for _, format := range []string{"zip", "zip64", "7z"} {
		t.Run(format, func(t *testing.T) {
			root, err := os.MkdirTemp("", "weaver-par3-inside-restart-")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("preserved artifacts: %s", root)
			dir := filepath.Join(root, "sources")
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			name, original, inserted, payload := par3InsideFixture(t, reference, dir, format)
			posted := map[string][]byte{name: bytes.Clone(inserted)}
			posted[name][12] ^= 0x80
			other := make([]byte, 262144)
			rng := rand.New(rand.NewPCG(37, 97))
			for i := range other {
				other[i] = byte(rng.Uint32())
			}
			carriers := map[string][]byte{"other.bin": bytes.Clone(other)}
			second := filepath.Join(dir, "second")
			if err := os.MkdirAll(second, 0755); err != nil {
				t.Fatal(err)
			}
			par3ReferenceParity(t, reference, second, carriers, []string{"-e8", "-s32768", "-c8"})
			for file, body := range carriers {
				posted[file] = body
			}
			posted["other.bin"][100000] ^= 0x80
			nntp := startUnpackNNTP(t)
			port := nntp.listener.Addr().(*net.TCPAddr).Port
			url, firstLog, stop := startManagedUnpackWeaver(t, bin, root, "before.log", port,
				"RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
			api := provisionUnpackAPI(t, root, url)
			slug := "par3-inside-restart-" + format
			nzb := nntp.publishUnpack(slug, "clean", posted, nil)
			gate := &unpackGate{released: make(chan struct{})}
			release := sync.OnceFunc(func() { close(gate.released) })
			t.Cleanup(release)
			nntp.mu.Lock()
			for index, file := range unpackSortedNames(posted) {
				if !strings.Contains(file, ".vol") {
					continue
				}
				prefix := fmt.Sprintf("%s-%d-", slug, index)
				for id, article := range nntp.articles {
					if strings.HasPrefix(id, prefix) {
						article.gate = gate
						nntp.articles[id] = article
					}
				}
			}
			nntp.mu.Unlock()
			job, err := api.submit(nzb, slug)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "intermediate", slug, name)
			ready := false
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				actual, readErr := os.ReadFile(path)
				log, logErr := os.ReadFile(firstLog)
				repaired := false
				if logErr == nil {
					for _, line := range strings.Split(string(log), "\n") {
						if strings.Contains(line, fmt.Sprintf("job_id=%d ", job)) && strings.Contains(line, "PAR3 repair installed verified outputs") {
							repaired = true
						}
					}
				}
				if readErr == nil && len(actual) > len(original) && actual[12] == original[12] && gate.held.Load() > 0 && repaired {
					ready = true
					break
				}
				if api.status(job) == "FAILED" {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !ready {
				t.Fatalf("never reached embedded repaired wait: status=%s log=%s", api.status(job), firstLog)
			}
			stop()
			damaged, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			damaged[len(original)/2] ^= 0x80
			if err := os.WriteFile(path, damaged, 0644); err != nil {
				t.Fatal(err)
			}
			release()
			url, secondLog, _ := startManagedUnpackWeaver(t, bin, root, "after.log", port,
				"RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
			api.url = url
			status := ""
			deadline = time.Now().Add(90 * time.Second)
			for time.Now().Before(deadline) {
				status = api.status(job)
				if status == "COMPLETED" || status == "FAILED" {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			var history struct {
				HistoryItem *struct {
					Error       *string
					FailedBytes uint64
					Health      uint32
				}
			}
			if err := api.query(`query($id:Int!) {historyItem(id:$id) {error failedBytes health}}`, map[string]any{"id": job}, &history); err != nil {
				t.Fatal(err)
			}
			if status != "COMPLETED" || history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
				t.Fatalf("embedded restart status=%s history=%+v log=%s", status, history.HistoryItem, secondLog)
			}
			for file, want := range map[string][]byte{"payload.bin": payload, "other.bin": other} {
				actual, err := os.ReadFile(filepath.Join(root, "complete", slug, file))
				if err != nil || !bytes.Equal(actual, want) {
					t.Fatalf("wrong restart output %s: %v", file, err)
				}
			}
			api.assertEmbeddedRepairWarnings(t, job, 2)
		})
	}
}
