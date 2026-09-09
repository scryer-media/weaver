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

// A recovery wait in one job cannot monopolize the native worker or prevent
// independent FFT and Data-only repairs from reaching terminal delivery.
func TestPar3ConcurrentE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set native Weaver and official PAR3 reference binaries")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("binaries must use absolute paths")
	}
	root, err := os.MkdirTemp("", "weaver-par3-concurrent-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	port := nntp.listener.Addr().(*net.TCPAddr).Port
	url, logPath, _ := startManagedUnpackWeaver(t, bin, root, "weaver.log", port,
		"RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
	api := provisionUnpackAPI(t, root, url)
	gate := &unpackGate{released: make(chan struct{})}
	release := sync.OnceFunc(func() { close(gate.released) })
	t.Cleanup(release)
	ids := []int{}
	payloads := [][]byte{}
	slugs := []string{"blocked-cauchy", "ready-fft", "ready-data"}
	for index, slug := range slugs {
		data := make([]byte, 262144)
		rng := rand.New(rand.NewPCG(uint64(41+index), 101))
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		payloads = append(payloads, data)
		posted := map[string][]byte{"payload.bin": bytes.Clone(data)}
		options := []string{"-e1", "-s32768", "-c8"}
		if index == 1 {
			options[0] = "-e8"
		}
		if index == 2 {
			options = append(options, "-D")
		}
		dir := filepath.Join(root, "sources", slug)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		par3ReferenceParity(t, reference, dir, posted, options)
		if index == 2 {
			delete(posted, "payload.bin")
			for name := range posted {
				if strings.Contains(name, ".vol") {
					delete(posted, name)
				}
			}
		} else {
			posted["payload.bin"][100000] ^= 0x80
		}
		nzb := nntp.publishUnpack(slug, "clean", posted, nil)
		if index == 0 {
			nntp.mu.Lock()
			for fileIndex, name := range unpackSortedNames(posted) {
				if !strings.Contains(name, ".vol") {
					continue
				}
				prefix := fmt.Sprintf("%s-%d-", slug, fileIndex)
				for id, article := range nntp.articles {
					if strings.HasPrefix(id, prefix) {
						article.gate = gate
						nntp.articles[id] = article
					}
				}
			}
			nntp.mu.Unlock()
		}
		job, err := api.submit(nzb, slug)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, job)
		if index == 0 {
			deadline := time.Now().Add(30 * time.Second)
			for gate.held.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
			}
			if gate.held.Load() == 0 {
				t.Fatalf("first job did not reach recovery wait: %s", logPath)
			}
		}
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if api.status(ids[1]) == "COMPLETED" && api.status(ids[2]) == "COMPLETED" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, index := range []int{1, 2} {
		if status := api.status(ids[index]); status != "COMPLETED" {
			t.Fatalf("independent job starved: %s status=%s log=%s", slugs[index], status, logPath)
		}
	}
	if status := api.status(ids[0]); status == "COMPLETED" || status == "FAILED" {
		t.Fatalf("blocked job bypassed its missing recovery: %s", status)
	}
	release()
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if api.status(ids[0]) == "COMPLETED" || api.status(ids[0]) == "FAILED" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for index, job := range ids {
		if status := api.status(job); status != "COMPLETED" {
			t.Fatalf("job %s did not complete: %s log=%s", slugs[index], status, logPath)
		}
		actual, err := os.ReadFile(filepath.Join(root, "complete", slugs[index], "payload.bin"))
		if err != nil || !bytes.Equal(actual, payloads[index]) {
			t.Fatalf("wrong output for %s: %v", slugs[index], err)
		}
		var history struct {
			HistoryItem *struct {
				FailedBytes uint64
				Health      uint32
			}
		}
		if err := api.query(`query($id:Int!) {historyItem(id:$id) {failedBytes health}}`, map[string]any{"id": job}, &history); err != nil {
			t.Fatal(err)
		}
		if history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
			t.Fatalf("unhealthy history for %s: %+v", slugs[index], history.HistoryItem)
		}
	}
}
