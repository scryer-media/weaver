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

func TestPar3RestartE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set the native Weaver and official PAR3 reference binaries")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("both binaries must be absolute")
	}
	root, err := os.MkdirTemp("", "weaver-par3-restart-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	dir := filepath.Join(root, "sources")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{}
	rng := rand.New(rand.NewPCG(23, 71))
	for _, name := range []string{"first.bin", "second.bin"} {
		data := make([]byte, 262144)
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		expected[name] = data
	}
	posted := map[string][]byte{}
	for name, data := range expected {
		posted[name] = bytes.Clone(data)
	}
	par3ReferenceParity(t, reference, dir, posted, []string{"-e1", "-s32768", "-c8"})
	posted["first.bin"][100000] ^= 0x80
	nntp := startUnpackNNTP(t)
	nntpPort := nntp.listener.Addr().(*net.TCPAddr).Port
	url, firstLog, stop := startManagedUnpackWeaver(t, bin, root, "weaver-before.log", nntpPort,
		"RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
	api := provisionUnpackAPI(t, root, url)
	slug := "par3-restart-changed-source"
	nzb := nntp.publishUnpack(slug, "clean", posted, nil)
	gate := &unpackGate{released: make(chan struct{})}
	release := sync.OnceFunc(func() { close(gate.released) })
	t.Cleanup(release)
	nntp.mu.Lock()
	for index, name := range unpackSortedNames(posted) {
		if !strings.Contains(name, ".vol") {
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
	if err := os.WriteFile(filepath.Join(root, slug+".nzb"), nzb, 0644); err != nil {
		t.Fatal(err)
	}
	job, err := api.submit(nzb, slug)
	if err != nil {
		t.Fatal(err)
	}
	ready := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		allLanded := true
		for name, data := range expected {
			info, err := os.Stat(filepath.Join(root, "intermediate", slug, name))
			allLanded = allLanded && err == nil && info.Size() == int64(len(data))
		}
		log, _ := os.ReadFile(firstLog)
		if allLanded && gate.held.Load() > 0 && strings.Contains(string(log), "status=NeedRecovery") {
			ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("job never reached the restart point: %s log=%s", api.status(job), firstLog)
	}
	before := map[string]int{}
	nntp.mu.Lock()
	for id, count := range nntp.requests {
		before[id] = count
	}
	nntp.mu.Unlock()
	stop()
	path := filepath.Join(root, "intermediate", slug, "second.bin")
	changed, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = changed.WriteAt([]byte{expected["second.bin"][170000] ^ 0x80}, 170000)
	if err == nil {
		err = changed.Sync()
	}
	closeErr := changed.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("change stopped-process source: write=%v close=%v", err, closeErr)
	}
	release()
	url, secondLog, _ := startManagedUnpackWeaver(t, bin, root, "weaver-after.log", nntpPort,
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
	after := map[string]int{}
	nntp.mu.Lock()
	for id, count := range nntp.requests {
		after[id] = count
	}
	nntp.mu.Unlock()
	par3WriteJSON(t, filepath.Join(root, "restart-evidence.json"), map[string]any{
		"jobId": job, "status": status, "history": history.HistoryItem, "beforeRequests": before, "afterRequests": after,
	})
	if status != "COMPLETED" || history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
		failure := ""
		if history.HistoryItem != nil && history.HistoryItem.Error != nil {
			failure = *history.HistoryItem.Error
		}
		t.Fatalf("restart status=%s error=%s log=%s", status, failure, secondLog)
	}
	for name, data := range expected {
		actual, err := os.ReadFile(filepath.Join(root, "complete", slug, name))
		if err != nil || !bytes.Equal(actual, data) {
			t.Fatalf("restart did not repair %s: %v", name, err)
		}
	}
	for index, name := range unpackSortedNames(posted) {
		if _, protected := expected[name]; !protected {
			continue
		}
		prefix := fmt.Sprintf("%s-%d-", slug, index)
		for id, count := range after {
			if strings.HasPrefix(id, prefix) && count != before[id] {
				t.Fatalf("restart refetched completed source %s: %s (%d -> %d)", name, id, before[id], count)
			}
		}
	}
}
