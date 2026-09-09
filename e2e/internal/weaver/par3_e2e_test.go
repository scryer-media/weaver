package weaver

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/blake3"
)

// Each run owns its process, loopback listener and SQLite database. The parity
// packets are unmodified output from the pinned official reference; only the
// regenerated protected input or article availability changes between cases.
func TestPar3E2E(t *testing.T) {
	bin := os.Getenv("WEAVER_PAR3_E2E_BIN")
	if bin == "" {
		t.Skip("set WEAVER_PAR3_E2E_BIN to run the native PAR3 pipeline")
	}
	if !filepath.IsAbs(bin) {
		t.Fatal("WEAVER_PAR3_E2E_BIN must be absolute")
	}
	root, err := os.MkdirTemp("", "weaver-par3-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	url, logPath := startUnpackWeaver(t, bin, root, nntp.listener.Addr().(*net.TCPAddr).Port)
	api := provisionUnpackAPI(t, root, url)
	payload := make([]byte, 262144)
	for i := range payload {
		payload[i] = byte(i*17 + (i>>8)*13 + 7)
	}
	fixtureDir := filepath.Join("testdata", "par3-native")
	manifestBytes, err := os.ReadFile(filepath.Join(fixtureDir, "sha256.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]string
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"clean", "corrupt", "missing", "missing-index", "indexless", "late-index", "unrecoverable"} {
		t.Run(mode, func(t *testing.T) {
			files := map[string][]byte{"payload.bin": bytes.Clone(payload)}
			paths, err := filepath.Glob(filepath.Join(fixtureDir, "*.par3"))
			if err != nil || len(paths) != 4 {
				t.Fatalf("official PAR3 fixtures: %v (%d files)", err, len(paths))
			}
			for _, path := range paths {
				if mode == "indexless" && filepath.Base(path) == "set.par3" {
					continue
				}
				if mode == "unrecoverable" && strings.Contains(filepath.Base(path), ".vol") {
					continue
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if fmt.Sprintf("%x", sha256.Sum256(data)) != manifest[filepath.Base(path)] {
					t.Fatalf("official carrier digest mismatch: %s", path)
				}
				files[filepath.Base(path)] = data
			}
			articleMode := "clean"
			if mode == "corrupt" || mode == "unrecoverable" || mode == "missing-index" || mode == "indexless" || mode == "late-index" {
				// Both yEnc checksums match these damaged bytes. Only PAR3's
				// authenticated fingerprints can detect and correct the change.
				files["payload.bin"][100000] ^= 0x80
			}
			if mode == "missing" {
				articleMode = "missing"
			}
			slug := "par3-" + mode
			nzb := nntp.publishUnpack(slug, articleMode, files, nil)
			var indexGate *unpackGate
			if mode == "missing-index" || mode == "late-index" {
				nntp.mu.Lock()
				id := fmt.Sprintf("%s-1-0@direct-unpack.test", slug)
				article := nntp.articles[id]
				if mode == "missing-index" {
					article.missing = true
				} else {
					indexGate = &unpackGate{released: make(chan struct{})}
					article.gate = indexGate
				}
				nntp.articles[id] = article
				nntp.mu.Unlock()
			}
			if err := os.WriteFile(filepath.Join(root, slug+".nzb"), nzb, 0644); err != nil {
				t.Fatal(err)
			}
			job, err := api.submit(nzb, slug)
			if err != nil {
				t.Fatal(err)
			}
			if indexGate != nil {
				deadline := time.Now().Add(10 * time.Second)
				landed := false
				for time.Now().Before(deadline) {
					info, err := os.Stat(filepath.Join(root, "intermediate", slug, "payload.bin"))
					if err == nil && info.Size() == int64(len(payload)) && indexGate.held.Load() > 0 {
						landed = true
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				before := api.status(job)
				close(indexGate.released)
				if !landed || before == "COMPLETED" || before == "FAILED" {
					t.Fatalf("late metadata barrier: landed=%v status=%s", landed, before)
				}
			}
			deadline := time.Now().Add(90 * time.Second)
			status := ""
			for time.Now().Before(deadline) {
				status = api.status(job)
				if status == "COMPLETED" || status == "FAILED" {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			wantStatus := "COMPLETED"
			if mode == "unrecoverable" {
				wantStatus = "FAILED"
			}
			if status != wantStatus {
				t.Fatalf("job=%d status=%s want=%s log=%s", job, status, wantStatus, logPath)
			}
			if status == "COMPLETED" {
				actual, err := os.ReadFile(filepath.Join(root, "complete", slug, "payload.bin"))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(actual, payload) {
					t.Fatalf("wrong output: got=%x want=%x", blake3.Sum256(actual), blake3.Sum256(payload))
				}
				carriers, err := filepath.Glob(filepath.Join(root, "complete", slug, "*.par3"))
				if err != nil || len(carriers) != 0 {
					t.Fatalf("spent standalone protection was not cleaned: %v (%v)", carriers, err)
				}
			}
			nntp.mu.Lock()
			requests := map[string]int{}
			for id, count := range nntp.requests {
				if strings.HasPrefix(id, slug+"-") {
					requests[id] = count
				}
			}
			nntp.mu.Unlock()
			for index, name := range unpackSortedNames(files) {
				if !strings.Contains(name, ".vol") {
					continue
				}
				count := 0
				for id, hits := range requests {
					if strings.HasPrefix(id, fmt.Sprintf("%s-%d-", slug, index)) {
						count += hits
					}
				}
				if (mode == "clean" || name == "set.vol3+1.par3") && count != 0 {
					t.Fatalf("unneeded recovery carrier downloaded: %s (%d requests)", name, count)
				}
				if mode == "missing" && name == "set.vol1+2.par3" && count != 1 {
					t.Fatalf("repair needs only the first article from this carrier: got %d requests", count)
				}
			}
			evidence, err := json.MarshalIndent(map[string]any{"jobId": job, "status": status, "requests": requests, "expectedBlake3": blake3.Sum256(payload)}, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, slug+"-evidence.json"), evidence, 0644); err != nil {
				t.Fatal(err)
			}
		})
	}
}
