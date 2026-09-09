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
	for _, mode := range []string{"clean", "renamed", "renamed-collision", "corrupt", "missing", "missing-index", "indexless", "late-index", "unrecoverable"} {
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
			if mode == "unrecoverable" {
				// Remove every repeated donor from the regenerated protected
				// input. Carrier bytes remain the official fixture unchanged.
				clear(files["payload.bin"])
			}
			if mode == "missing" {
				articleMode = "missing"
			}
			if strings.HasPrefix(mode, "renamed") {
				files["obfuscated.dat"] = files["payload.bin"]
				delete(files, "payload.bin")
			}
			slug := "par3-" + mode
			nzb := nntp.publishUnpack(slug, articleMode, files, nil)
			var indexGate *unpackGate
			if mode == "missing-index" || mode == "late-index" || mode == "renamed-collision" {
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
					name := "payload.bin"
					if mode == "renamed-collision" {
						name = "obfuscated.dat"
					}
					info, err := os.Stat(filepath.Join(root, "intermediate", slug, name))
					if err == nil && info.Size() == int64(len(payload)) && indexGate.held.Load() > 0 {
						landed = true
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				before := api.status(job)
				if mode == "renamed-collision" {
					if err := os.WriteFile(filepath.Join(root, "intermediate", slug, "payload.bin"), []byte("existing unrelated output"), 0644); err != nil {
						close(indexGate.released)
						t.Fatal(err)
					}
				}
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
			if mode == "unrecoverable" || mode == "renamed-collision" {
				wantStatus = "FAILED"
			}
			if status != wantStatus {
				t.Fatalf("job=%d status=%s want=%s log=%s", job, status, wantStatus, logPath)
			}
			if mode == "renamed-collision" {
				for name, expected := range map[string][]byte{"obfuscated.dat": payload, "payload.bin": []byte("existing unrelated output")} {
					actual, err := os.ReadFile(filepath.Join(root, "intermediate", slug, name))
					if err != nil || !bytes.Equal(actual, expected) {
						t.Fatalf("collision changed %s: %v", name, err)
					}
				}
				var history struct{ HistoryItem *struct{ Error *string } }
				if err := api.query(`query($id:Int!) {historyItem(id:$id) {error}}`, map[string]any{"id": job}, &history); err != nil {
					t.Fatal(err)
				}
				if history.HistoryItem == nil || history.HistoryItem.Error == nil || !strings.Contains(*history.HistoryItem.Error, "PAR3 content placement failed: cannot place") {
					t.Fatalf("expected persisted placement collision: %+v", history.HistoryItem)
				}
				return
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
				if (mode == "clean" || mode == "renamed" || name == "set.vol3+1.par3") && count != 0 {
					t.Fatalf("unneeded recovery carrier downloaded: %s (%d requests)", name, count)
				}
				if mode == "missing" && count != 0 {
					t.Fatalf("repeated donor bytes need no recovery carrier: %s (%d requests)", name, count)
				}
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
			verificationMode := mode
			if mode == "renamed" {
				verificationMode = "clean"
			}
			verificationEvents := api.assertPar3VerificationHistory(t, job, verificationMode)
			evidence, err := json.MarshalIndent(map[string]any{"jobId": job, "status": status, "requests": requests, "expectedBlake3": blake3.Sum256(payload), "history": history.HistoryItem, "verificationEvents": verificationEvents}, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, slug+"-evidence.json"), evidence, 0644); err != nil {
				t.Fatal(err)
			}
			if status == "COMPLETED" && (history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000) {
				t.Fatalf("verified PAR3 delivery retained failed articles in history: %+v", history.HistoryItem)
			}
		})
	}
}

func (a unpackAPI) assertPar3VerificationHistory(t *testing.T, job int, mode string) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var result struct {
			JobEvents []struct{ Kind, Message string }
		}
		if err := a.query(`query($id:Int!) {jobEvents(jobId:$id) {kind message}}`, map[string]any{"id": job}, &result); err != nil {
			t.Fatal(err)
		}
		var messages []string
		passed, incomplete, terminal := 0, 0, false
		for _, event := range result.JobEvents {
			if event.Kind == "JOB_VERIFICATION_COMPLETE" && strings.HasPrefix(event.Message, "PAR3 verification ") {
				messages = append(messages, event.Message)
				if event.Message == "PAR3 verification passed" {
					passed++
				} else {
					incomplete++
				}
			}
			terminal = terminal || event.Kind == "JOB_COMPLETED" || event.Kind == "JOB_FAILED"
		}
		if terminal {
			if mode == "clean" && (passed != 1 || incomplete != 0) {
				t.Fatalf("clean native verification was duplicated or missing: %v", messages)
			}
			if mode != "clean" && incomplete == 0 {
				t.Fatalf("native damage verdict missing: %v", messages)
			}
			if mode == "unrecoverable" && passed != 0 {
				t.Fatalf("unrecoverable input claimed verification success: %v", messages)
			}
			if mode != "unrecoverable" && passed == 0 {
				t.Fatalf("verified delivery has no native pass: %v", messages)
			}
			return messages
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job=%d: native verification history was not persisted", job)
	return nil
}
