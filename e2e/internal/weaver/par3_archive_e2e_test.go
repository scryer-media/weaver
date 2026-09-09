package weaver

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This extends the native harness using official reference output generated in
// the preserved run directory. It never constructs or edits PAR3 packet bytes.
func TestPar3ArchiveE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set WEAVER_PAR3_E2E_BIN and WEAVER_PAR3_REFERENCE_BIN for archive scenarios")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("both binaries must use absolute paths")
	}
	root, err := os.MkdirTemp("", "weaver-par3-archives-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	url, logPath := startUnpackWeaver(t, bin, root, nntp.listener.Addr().(*net.TCPAddr).Port,
		"WEAVER_RAR_DIRECT_STORE=true", "RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace,weaver_server_core::pipeline::direct_store::router=debug")
	api := provisionUnpackAPI(t, root, url)
	for _, format := range []string{"zip", "zip64", "split", "rar-store", "rar-encrypted"} {
		t.Run(format, func(t *testing.T) {
			dir := filepath.Join(root, "sources", format)
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			files, payload, member, password := par3ArchiveFixture(t, dir, format)
			par3ArchiveParity(t, reference, dir, files)
			modes := []string{"clean", "renamed", "omitted", "omitted-nested", "corrupt", "missing", "mixed-prefer-par2", "mixed-fallback"}
			if strings.HasPrefix(format, "rar-") {
				modes = append(modes, "disguised-carrier")
			}
			if format == "rar-encrypted" {
				modes = append(modes, "missing-two")
			}
			for _, mode := range modes {
				t.Run(mode, func(t *testing.T) {
					posted := make(map[string][]byte, len(files))
					for name, data := range files {
						posted[name] = bytes.Clone(data)
					}
					mixed := strings.HasPrefix(mode, "mixed-")
					if mixed {
						par2Dir := filepath.Join(dir, mode+"-par2")
						if err := os.MkdirAll(par2Dir, 0755); err != nil {
							t.Fatal(err)
						}
						protected := make(map[string][]byte)
						for name, data := range files {
							if !strings.HasSuffix(name, ".par3") {
								protected[name] = data
							}
						}
						args := []string{"create", "-q", "-s65536", "-c20", filepath.Join(par2Dir, "repair.par2")}
						for _, name := range unpackSortedNames(protected) {
							args = append(args, filepath.Join(par2Dir, name))
						}
						unpackParity(t, par2Dir, protected)
						par2, err := exec.LookPath("par2")
						if err != nil {
							t.Fatal(err)
						}
						binary, err := os.ReadFile(par2)
						if err != nil {
							t.Fatal(err)
						}
						hashes := make(map[string]string)
						for name, data := range protected {
							hashes[name] = fmt.Sprintf("%x", sha256.Sum256(data))
							if strings.HasSuffix(name, ".par2") && (mode != "mixed-fallback" || !strings.Contains(name, ".vol")) {
								posted[name] = data
							}
						}
						par3WriteJSON(t, filepath.Join(par2Dir, "provenance.json"), map[string]any{
							"binary": par2, "binarySHA256": fmt.Sprintf("%x", sha256.Sum256(binary)),
							"arguments": args, "fileSHA256": hashes,
						})
					}
					if mode == "corrupt" || mode == "disguised-carrier" || mixed {
						for _, name := range unpackSortedNames(posted) {
							if !strings.HasSuffix(name, ".par3") && !strings.HasSuffix(name, ".par2") {
								// Corrupt the protected archive before yEnc encoding so
								// both transport CRCs describe the damaged input.
								posted[name][len(posted[name])/2] ^= 0x80
								break
							}
						}
					}
					if mode == "disguised-carrier" {
						// The first official recovery carrier includes metadata.
						// Omit the other carriers and change only its posted name.
						carrier, ok := posted["repair.vol0+1.par3"]
						if !ok {
							t.Fatal("missing first official recovery carrier")
						}
						for name := range posted {
							if strings.HasSuffix(name, ".par3") {
								delete(posted, name)
							}
						}
						posted["000.metadata.bin"] = carrier
					}
					if mode == "renamed" {
						for _, name := range unpackSortedNames(posted) {
							if !strings.HasSuffix(name, ".par3") {
								posted["obfuscated.dat"] = posted[name]
								delete(posted, name)
								break
							}
						}
					}
					if strings.HasPrefix(mode, "omitted") {
						for name := range posted {
							if strings.HasSuffix(name, ".par3") {
								delete(posted, name)
							}
						}
						if mode == "omitted-nested" {
							nested := make(map[string][]byte)
							for name, data := range posted {
								nested["nested/"+name] = data
							}
							posted = nested
						}
						omittedDir := filepath.Join(dir, mode+"-data")
						if err := os.MkdirAll(omittedDir, 0755); err != nil {
							t.Fatal(err)
						}
						par3ReferenceParity(t, reference, omittedDir, posted, []string{"-e1", "-D", "-s65536", "-c1"})
						for name := range posted {
							if !strings.HasSuffix(name, ".par3") || strings.Contains(name, ".vol") {
								delete(posted, name)
							}
						}
					}
					articleMode := "clean"
					if strings.HasPrefix(mode, "missing") {
						articleMode = mode
					}
					slug := "par3-archive-" + format + "-" + mode
					nzb := nntp.publishUnpack(slug, articleMode, posted, nil)
					var release func()
					if mode == "disguised-carrier" {
						gate := &unpackGate{released: make(chan struct{})}
						var released bool
						release = func() {
							if !released {
								close(gate.released)
								released = true
							}
						}
						t.Cleanup(release)
						// Hold the corrupted volume until the unnamed carrier has
						// authenticated. Other volumes remain available to download.
						for index, name := range unpackSortedNames(posted) {
							if !strings.HasSuffix(name, ".rar") {
								continue
							}
							prefix := fmt.Sprintf("%s-%d-", slug, index)
							nntp.mu.Lock()
							for id, article := range nntp.articles {
								if strings.HasPrefix(id, prefix) {
									article.gate = gate
									nntp.articles[id] = article
								}
							}
							nntp.mu.Unlock()
							break
						}
					}
					if err := os.WriteFile(filepath.Join(root, slug+".nzb"), nzb, 0644); err != nil {
						t.Fatal(err)
					}
					job, err := api.submitWithPassword(nzb, slug, password)
					if err != nil {
						t.Fatal(err)
					}
					api.cancelOnFailure(t, job)
					if release != nil {
						authenticated := false
						deadline := time.Now().Add(15 * time.Second)
						for time.Now().Before(deadline) && !authenticated {
							log, err := os.ReadFile(logPath)
							if err != nil {
								t.Fatal(err)
							}
							for _, line := range strings.Split(string(log), "\n") {
								if strings.Contains(line, fmt.Sprintf("job_id=%d ", job)) && strings.Contains(line, "PAR3 carrier worker settled") && strings.Contains(line, "sets=1") {
									authenticated = true
								}
							}
							if !authenticated {
								time.Sleep(20 * time.Millisecond)
							}
						}
						if !authenticated {
							t.Fatalf("unnamed carrier never authenticated: job=%d log=%s", job, logPath)
						}
						release()
					}
					status := ""
					deadline := time.Now().Add(90 * time.Second)
					for time.Now().Before(deadline) {
						status = api.status(job)
						if status == "COMPLETED" || status == "FAILED" {
							break
						}
						time.Sleep(50 * time.Millisecond)
					}
					nntp.mu.Lock()
					requests := map[string]int{}
					for id, count := range nntp.requests {
						if strings.HasPrefix(id, slug+"-") {
							requests[id] = count
						}
					}
					nntp.mu.Unlock()
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
					failure := ""
					if history.HistoryItem != nil && history.HistoryItem.Error != nil {
						failure = *history.HistoryItem.Error
					}
					par3WriteJSON(t, filepath.Join(root, slug+"-evidence.json"), map[string]any{
						"jobId": job, "status": status, "error": failure, "history": history.HistoryItem, "requests": requests, "expectedSHA256": fmt.Sprintf("%x", sha256.Sum256(payload)),
					})
					if status != "COMPLETED" {
						t.Fatalf("job=%d status=%s error=%s log=%s", job, status, failure, logPath)
					}
					if history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
						t.Fatalf("verified PAR3 archive retained failed articles in history: %+v", history.HistoryItem)
					}
					actual, err := os.ReadFile(filepath.Join(root, "complete", slug, member))
					if err != nil || !bytes.Equal(actual, payload) {
						t.Fatalf("extracted member mismatch: %v got=%x want=%x", err, sha256.Sum256(actual), sha256.Sum256(payload))
					}
					if strings.HasPrefix(format, "rar-") && mode != "renamed" && !strings.HasPrefix(mode, "omitted") {
						log, err := os.ReadFile(logPath)
						if err != nil {
							t.Fatal(err)
						}
						finalized, selectiveRepair := false, false
						for _, line := range strings.Split(string(log), "\n") {
							if strings.Contains(line, fmt.Sprintf("job_id=%d ", job)) {
								if strings.Contains(line, "direct-store set demoted") {
									t.Fatal("RAR expected to stay direct unexpectedly demoted")
								}
								finalized = finalized || strings.Contains(line, "direct-store set finalized without materializing a volume")
								selectiveRepair = selectiveRepair || (strings.Contains(line, "PAR3 repair installed verified outputs") && strings.Contains(line, " files=2 "))
							}
						}
						if !finalized {
							t.Fatal("RAR did not finish through direct-store verification")
						}
						if mode == "missing-two" && !selectiveRepair {
							t.Fatal("two-volume repair did not limit installation to the damaged files")
						}
					}
					if mode == "clean" || mode == "renamed" || mode == "corrupt" || mixed || (mode == "missing" && format == "rar-store") {
						for index, name := range unpackSortedNames(posted) {
							if (mode == "clean" || mode == "renamed" || mode == "mixed-prefer-par2" || name != "repair.vol0+1.par3") && strings.Contains(name, ".vol") && strings.HasSuffix(name, ".par3") {
								for id, count := range requests {
									if strings.HasPrefix(id, fmt.Sprintf("%s-%d-", slug, index)) && count != 0 {
										t.Fatalf("archive requested unneeded recovery: %s", name)
									}
								}
							}
						}
					}
				})
			}
		})
	}
}

func par3ArchiveFixture(t *testing.T, dir, format string) (map[string][]byte, []byte, string, string) {
	t.Helper()
	if !strings.HasPrefix(format, "rar-") {
		files, payload := unpackFixture(t, dir, format)
		return files, payload, "payload.bin", ""
	}
	fixtures := filepath.Join("..", "..", "..", "server", "crates", "weaver-server-core", "tests", "fixtures")
	pattern, member, password := "rar5_store.rar", "small.txt", ""
	if format == "rar-encrypted" {
		pattern, member, password = "rar5_enc_mv_video.part*.rar", "test_clip.mkv", "testpass123"
	}
	paths, err := filepath.Glob(filepath.Join(fixtures, "rar5", pattern))
	if err != nil || len(paths) == 0 {
		t.Fatalf("RAR fixtures: %v", err)
	}
	files := map[string][]byte{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.Base(path)] = data
	}
	// Read the expected member from the existing archive using an independent
	// installed extractor. Archive and expected-member digests are recorded.
	args := []string{"x", "-so", "-y"}
	if password != "" {
		args = append(args, "-p"+password)
	}
	args = append(args, paths[0], member)
	payload, err := exec.Command("7zz", args...).Output()
	if err != nil {
		t.Fatal(err)
	}
	return files, payload, member, password
}

func par3ArchiveParity(t *testing.T, reference, dir string, files map[string][]byte) {
	t.Helper()
	par3ReferenceParity(t, reference, dir, files, []string{"-s32768", "-c8", "-e1"})
}

func par3ReferenceParity(t *testing.T, reference, dir string, files map[string][]byte, options []string) string {
	t.Helper()
	binary, err := os.ReadFile(reference)
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{"create"}, options...)
	args = append(args, "repair.par3")
	manifest := map[string]string{}
	for _, name := range unpackSortedNames(files) {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), files[name], 0644); err != nil {
			t.Fatal(err)
		}
		manifest[name] = fmt.Sprintf("%x", sha256.Sum256(files[name]))
		args = append(args, name)
	}
	cmd := exec.Command(reference, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if writeErr := os.WriteFile(filepath.Join(dir, "creation.txt"), out, 0644); writeErr != nil {
		t.Fatal(writeErr)
	}
	if err != nil {
		t.Fatalf("official PAR3 creation: %v: %s", err, out)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.par3"))
	if err != nil || len(paths) < 2 {
		t.Fatalf("missing official carriers: %v", err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.Base(path)] = data
		manifest[filepath.Base(path)] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	par3WriteJSON(t, filepath.Join(dir, "provenance.json"), map[string]any{
		"reference": reference, "referenceSHA256": fmt.Sprintf("%x", sha256.Sum256(binary)), "arguments": args, "sha256": manifest,
	})
	listing := exec.Command(reference, "list", "-v", "-v", "repair.par3")
	listing.Dir = dir
	listed, err := listing.CombinedOutput()
	if err != nil {
		t.Fatalf("official PAR3 listing: %v: %s", err, listed)
	}
	if err := os.WriteFile(filepath.Join(dir, "listing.txt"), listed, 0644); err != nil {
		t.Fatal(err)
	}
	return string(out) + string(listed)
}

func par3WriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}
