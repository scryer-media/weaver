package weaver

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Every protection packet is emitted by the pinned official insertion command.
// Damage changes only regenerated protected archive bytes before yEnc encoding.
func TestPar3InsideE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set WEAVER_PAR3_E2E_BIN and WEAVER_PAR3_REFERENCE_BIN for embedded protection scenarios")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("both binaries must use absolute paths")
	}
	root, err := os.MkdirTemp("", "weaver-par3-inside-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	url, logPath := startUnpackWeaver(t, bin, root, nntp.listener.Addr().(*net.TCPAddr).Port,
		"RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
	api := provisionUnpackAPI(t, root, url)
	for _, format := range []string{"zip", "zip64", "7z", "zip-large", "zip64-large", "7z-large"} {
		t.Run(format, func(t *testing.T) {
			dir := filepath.Join(root, "sources", format)
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			name, original, inserted, payload := par3InsideFixture(t, reference, dir, format)
			modes := []string{"clean", "corrupt-body", "corrupt-header", "missing", "protection-only", "insufficient"}
			if strings.HasSuffix(format, "-large") {
				modes = []string{"clean", "corrupt-header", "missing", "protection-only"}
				if strings.HasPrefix(format, "zip") {
					modes = append(modes, "corrupt-footer")
				}
			}
			modes = append(modes, "renamed", "obfuscated")
			for _, mode := range modes {
				t.Run(mode, func(t *testing.T) {
					posted := bytes.Clone(inserted)
					switch mode {
					case "corrupt-body":
						posted[len(original)/2] ^= 0x80
					case "corrupt-header":
						posted[12] ^= 0x80
					case "corrupt-footer":
						// Official insertion duplicates the original EOCD after
						// the PAR3 packets. Damage only that container footer.
						originalFooter := bytes.LastIndex(original, []byte("PK\x05\x06"))
						footer := bytes.LastIndex(posted, []byte("PK\x05\x06"))
						if originalFooter < 0 || footer < len(original) || !bytes.Equal(original[originalFooter:], posted[footer:]) {
							t.Fatal("expected official duplicated ZIP footer")
						}
						posted[footer] ^= 0x80
					case "insufficient":
						for at := 65536; at < len(original)-65536; at += 65536 {
							posted[at] ^= 0x80
						}
					}
					slug := "par3-inside-" + format + "-" + mode
					postedName := name
					if mode == "renamed" {
						postedName = "renamed" + filepath.Ext(name)
					}
					if mode == "obfuscated" {
						postedName = "opaque.dat"
					}
					nzb := nntp.publishInside(slug, mode, postedName, posted, len(original))
					if err := os.WriteFile(filepath.Join(root, slug+".nzb"), nzb, 0644); err != nil {
						t.Fatal(err)
					}
					job, err := api.submit(nzb, slug)
					if err != nil {
						t.Fatal(err)
					}
					api.cancelOnFailure(t, job)
					status := ""
					deadline := time.Now().Add(45 * time.Second)
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
					failure := ""
					if history.HistoryItem != nil && history.HistoryItem.Error != nil {
						failure = *history.HistoryItem.Error
					}
					par3WriteJSON(t, filepath.Join(root, slug+"-evidence.json"), map[string]any{
						"jobId": job, "status": status, "error": failure, "history": history.HistoryItem,
						"expectedSHA256": fmt.Sprintf("%x", sha256.Sum256(payload)),
					})
					api.assertEmbeddedRepairWarning(t, job, mode != "clean" && mode != "insufficient" && mode != "renamed" && mode != "obfuscated")
					if mode == "insufficient" {
						if status != "FAILED" || !strings.Contains(failure, "PAR3") {
							t.Fatalf("expected native insufficient-recovery failure, got %s: %s; log=%s", status, failure, logPath)
						}
						if _, err := os.Stat(filepath.Join(root, "complete", slug, "payload.bin")); !os.IsNotExist(err) {
							t.Fatalf("unrecoverable archive published output: %v", err)
						}
						return
					}
					if status != "COMPLETED" {
						t.Fatalf("job=%d status=%s error=%s log=%s", job, status, failure, logPath)
					}
					if history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
						t.Fatalf("embedded repair retained failed delivery: %+v", history.HistoryItem)
					}
					actual, err := os.ReadFile(filepath.Join(root, "complete", slug, "payload.bin"))
					if err != nil || !bytes.Equal(actual, payload) {
						t.Fatalf("extracted member mismatch: %v", err)
					}
					log, err := os.ReadFile(logPath)
					if err != nil {
						t.Fatal(err)
					}
					authenticated := false
					for _, line := range strings.Split(string(log), "\n") {
						if strings.Contains(line, fmt.Sprintf("job_id=%d ", job)) && strings.Contains(line, "PAR3 carrier worker settled") && strings.Contains(line, "sets=1") {
							authenticated = true
						}
					}
					if !authenticated {
						t.Fatalf("embedded protection was not authenticated: log=%s", logPath)
					}
				})
			}
		})
	}
}

// Query persisted history, waiting for its terminal event to avoid mistaking
// an unflushed warning for an absent warning on clean or failed jobs.
func (a unpackAPI) assertEmbeddedRepairWarning(t *testing.T, job int, expected bool) {
	t.Helper()
	want := 0
	if expected {
		want = 1
	}
	a.assertEmbeddedRepairWarnings(t, job, want)
}

func (a unpackAPI) assertEmbeddedRepairWarnings(t *testing.T, job, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var result struct {
			JobEvents []struct{ Kind, Message string }
		}
		if err := a.query(`query($id:Int!) {jobEvents(jobId:$id) {kind message}}`, map[string]any{"id": job}, &result); err != nil {
			t.Fatal(err)
		}
		warnings, terminal := 0, false
		for _, event := range result.JobEvents {
			if event.Kind == "REPAIR_COMPLETE" && strings.Contains(event.Message, "Embedded PAR3 protection replaced") {
				warnings++
				if !strings.Contains(event.Message, "original carrier could not be restored byte for byte") {
					t.Fatalf("unexpected repair warning: %s", event.Message)
				}
			}
			terminal = terminal || event.Kind == "JOB_COMPLETED" || event.Kind == "JOB_FAILED"
		}
		if terminal {
			if warnings != want {
				t.Fatalf("job=%d: got %d persisted repair warnings, want %d", job, warnings, want)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job=%d: terminal event was not persisted", job)
}

func par3InsideFixture(t *testing.T, reference, dir, format string) (string, []byte, []byte, []byte) {
	t.Helper()
	large := strings.HasSuffix(format, "-large")
	format = strings.TrimSuffix(format, "-large")
	fixtureFormat := format
	if format == "7z" {
		fixtureFormat = "zip"
	}
	files, payload := unpackFixture(t, dir, fixtureFormat)
	name := unpackSortedNames(files)[0]
	if large {
		payload = bytes.Repeat(payload, 4)
		if err := os.WriteFile(filepath.Join(dir, "payload.bin"), payload, 0644); err != nil {
			t.Fatal(err)
		}
	}
	if format == "7z" || large {
		var cmd *exec.Cmd
		if format == "7z" {
			name = "archive.7z"
			cmd = exec.Command("7zz", "a", "-t7z", "-mx=0", name, "payload.bin")
		} else {
			// Rebuild the enlarged stored ZIP instead of updating an archive
			// whose member timestamp may compare equal within this test.
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
			args := []string{"-0"}
			if format == "zip64" {
				args = append(args, "-fz")
			}
			args = append(args, name, "payload.bin")
			cmd = exec.Command("zip", args...)
		}
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s fixture: %v: %s", format, err, out)
		}
		if err := os.WriteFile(filepath.Join(dir, "archive-creation.txt"), out, 0644); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, name)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"insert", name}
	cmd := exec.Command(reference, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if writeErr := os.WriteFile(filepath.Join(dir, "insertion.txt"), out, 0644); writeErr != nil {
		t.Fatal(writeErr)
	}
	if err != nil {
		t.Fatalf("official insertion: %v: %s", err, out)
	}
	inserted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(inserted) <= len(original) || !bytes.Equal(inserted[:len(original)], original) {
		t.Fatal("official insertion changed protected archive bytes")
	}
	binary, err := os.ReadFile(reference)
	if err != nil {
		t.Fatal(err)
	}
	par3WriteJSON(t, filepath.Join(dir, "provenance.json"), map[string]any{
		"reference": reference, "referenceSHA256": fmt.Sprintf("%x", sha256.Sum256(binary)), "arguments": args,
		"originalSHA256": fmt.Sprintf("%x", sha256.Sum256(original)), "insertedSHA256": fmt.Sprintf("%x", sha256.Sum256(inserted)),
		"payloadSHA256": fmt.Sprintf("%x", sha256.Sum256(payload)), "protectedLength": len(original),
	})
	return name, original, inserted, payload
}

// Small positioned articles expose a recoverable interior hole without
// changing the reference's default insertion geometry or packet bytes.
func (s *unpackNNTP) publishInside(slug, mode, name string, data []byte, originalLength int) []byte {
	const segment = 16 * 1024
	var nzb bytes.Buffer
	nzb.WriteString(`<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">`)
	count := (len(data) + segment - 1) / segment
	fmt.Fprintf(&nzb, `<file poster="fixture" date="1" subject="%s"><groups><group>alt.test</group></groups><segments>`, xmlUnpackText(fmt.Sprintf(`"%s" yEnc (%d/%d)`, name, 1, count)))
	wholeCRC := crc32.ChecksumIEEE(data)
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := 0; index < count; index++ {
		start, end := index*segment, min((index+1)*segment, len(data))
		id := fmt.Sprintf("%s-%d@par3-inside.test", slug, index)
		article := unpackArticle{missing: (mode == "missing" && index == count/2) || (mode == "protection-only" && index == (originalLength+segment-1)/segment)}
		article.body = unpackYenc(name, data[start:end], index+1, count, start+1, len(data), wholeCRC)
		s.articles[id] = article
		fmt.Fprintf(&nzb, `<segment bytes="%d" number="%d">%s</segment>`, len(article.body), index+1, id)
	}
	nzb.WriteString(`</segments></file></nzb>`)
	return nzb.Bytes()
}
