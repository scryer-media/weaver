package weaver

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Both native formats protect regenerated inputs. Damage changes source bytes
// before yEnc encoding; unavailable parity omits complete official carriers.
func TestPar3MixedE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set WEAVER_PAR3_E2E_BIN and WEAVER_PAR3_REFERENCE_BIN for mixed-format scenarios")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("both binaries must use absolute paths")
	}
	par2, err := exec.LookPath("par2")
	if err != nil {
		t.Fatal(err)
	}
	par2Binary, err := os.ReadFile(par2)
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("", "weaver-par3-mixed-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	url, logPath := startUnpackWeaver(t, bin, root, nntp.listener.Addr().(*net.TCPAddr).Port,
		"RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
	api := provisionUnpackAPI(t, root, url)
	for _, scenario := range []struct {
		name                                            string
		overlap, damage, omitPar2Recovery, insufficient bool
		conflict, postPar2Original, secondPar2          bool
		missingPar2Metadata                             bool
	}{
		{name: "independent-clean"},
		{name: "independent-repair", damage: true},
		{name: "independent-par2-failure", damage: true, omitPar2Recovery: true},
		{name: "independent-missing-par2-metadata-clean", omitPar2Recovery: true, missingPar2Metadata: true},
		{name: "independent-missing-par2-metadata-repair", damage: true, omitPar2Recovery: true, missingPar2Metadata: true},
		{name: "overlap-clean", overlap: true},
		{name: "overlap-prefer-par2", overlap: true, damage: true},
		{name: "overlap-fallback", overlap: true, damage: true, omitPar2Recovery: true},
		{name: "overlap-missing-par2-metadata-clean", overlap: true, omitPar2Recovery: true, missingPar2Metadata: true},
		{name: "overlap-missing-par2-metadata-repair", overlap: true, damage: true, omitPar2Recovery: true, missingPar2Metadata: true},
		{name: "overlap-two-par2-clean", overlap: true, secondPar2: true},
		{name: "overlap-two-par2-prefer", overlap: true, damage: true, secondPar2: true},
		{name: "overlap-two-par2-fallback", overlap: true, damage: true, omitPar2Recovery: true, secondPar2: true},
		{name: "overlap-two-par2-conflict-par2-complete", overlap: true, conflict: true, postPar2Original: true, secondPar2: true},
		{name: "overlap-two-par2-conflict-par3-complete", overlap: true, conflict: true, secondPar2: true},
		{name: "overlap-two-par2-conflict-after-fallback", overlap: true, conflict: true, damage: true, omitPar2Recovery: true, secondPar2: true},
		{name: "overlap-insufficient", overlap: true, damage: true, omitPar2Recovery: true, insufficient: true},
		{name: "overlap-conflict-par2-complete", overlap: true, conflict: true, postPar2Original: true},
		{name: "overlap-conflict-par3-complete", overlap: true, conflict: true},
		{name: "overlap-conflict-after-fallback", overlap: true, conflict: true, damage: true, omitPar2Recovery: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(79, 83))
			payload := make([]byte, 1048576+123)
			for i := range payload {
				payload[i] = byte(rng.Uint32())
			}
			first := map[string][]byte{"payload.bin": payload}
			secondName := "second.bin"
			secondPayload := bytes.Clone(payload)
			if scenario.overlap {
				secondName = "payload.bin"
			} else {
				for i := range secondPayload {
					secondPayload[i] ^= 0x5a
				}
			}
			if scenario.conflict {
				// Each native reference protects its own internally consistent
				// source. Their authenticated descriptions disagree with each other.
				secondPayload[19] ^= 0x40
			}
			second := map[string][]byte{secondName: secondPayload}
			dir := filepath.Join(root, "sources", scenario.name)
			par2Dir, par3Dir := filepath.Join(dir, "par2"), filepath.Join(dir, "par3")
			for _, path := range []string{par2Dir, par3Dir} {
				if err := os.MkdirAll(path, 0755); err != nil {
					t.Fatal(err)
				}
			}
			unpackParity(t, par2Dir, first)
			par3ReferenceParity(t, reference, par3Dir, second, []string{"-e1", "-s32768", "-c8"})
			par2Hashes := map[string]string{}
			posted := map[string][]byte{}
			for name, data := range first {
				par2Hashes[name] = fmt.Sprintf("%x", sha256.Sum256(data))
				if scenario.omitPar2Recovery && strings.Contains(name, ".vol") {
					continue
				}
				posted[name] = bytes.Clone(data)
			}
			par3WriteJSON(t, filepath.Join(dir, "par2-provenance.json"), map[string]any{
				"binary": par2, "binarySHA256": fmt.Sprintf("%x", sha256.Sum256(par2Binary)),
				"arguments":  []string{"create", "-q", "-s65536", "-c20", filepath.Join(par2Dir, "repair.par2"), filepath.Join(par2Dir, "payload.bin")},
				"fileSHA256": par2Hashes,
			})
			for name, data := range second {
				posted[name] = bytes.Clone(data)
			}
			if scenario.postPar2Original {
				posted["payload.bin"] = bytes.Clone(payload)
			}
			expected := map[string][]byte{"payload.bin": payload, secondName: secondPayload}
			if scenario.damage {
				for name := range expected {
					posted[name][len(posted[name])/2] ^= 0x80
					if scenario.insufficient {
						for block := 0; block < 9; block++ {
							posted[name][block*32768+64] ^= 0x80
						}
					}
				}
			}
			if scenario.secondPar2 {
				// A clean witness makes this a distinct PAR2 set protecting the
				// same damaged payload. Every set must independently settle.
				witness := []byte("a separately protected clean witness")
				extra := map[string][]byte{"payload.bin": bytes.Clone(payload), "witness.bin": witness}
				extraDir := filepath.Join(dir, "second-par2")
				if err := os.MkdirAll(extraDir, 0755); err != nil {
					t.Fatal(err)
				}
				unpackParity(t, extraDir, extra)
				hashes := map[string]string{}
				for name, data := range extra {
					hashes[name] = fmt.Sprintf("%x", sha256.Sum256(data))
					if strings.HasSuffix(name, ".par2") && (!scenario.omitPar2Recovery || !strings.Contains(name, ".vol")) {
						posted["secondary"+strings.TrimPrefix(name, "repair")] = data
					}
				}
				posted["witness.bin"], expected["witness.bin"] = witness, witness
				par3WriteJSON(t, filepath.Join(extraDir, "provenance.json"), map[string]any{
					"binary": par2, "binarySHA256": fmt.Sprintf("%x", sha256.Sum256(par2Binary)),
					"arguments":  []string{"create", "-q", "-s65536", "-c20", filepath.Join(extraDir, "repair.par2"), filepath.Join(extraDir, "payload.bin"), filepath.Join(extraDir, "witness.bin")},
					"fileSHA256": hashes, "postedCarrierPrefix": "secondary",
				})
			}
			slug := "par3-mixed-" + scenario.name
			nzb := nntp.publishUnpack(slug, "clean", posted, nil)
			if scenario.missingPar2Metadata {
				// Keep the declared official index in the NZB, but make every
				// article unavailable. Its filename cannot confer a native verdict.
				nntp.mu.Lock()
				missing := 0
				for index, name := range unpackSortedNames(posted) {
					if !strings.HasSuffix(name, ".par2") {
						continue
					}
					for id, article := range nntp.articles {
						if strings.HasPrefix(id, fmt.Sprintf("%s-%d-", slug, index)) {
							article.missing = true
							nntp.articles[id] = article
							missing++
						}
					}
				}
				nntp.mu.Unlock()
				if missing == 0 {
					t.Fatal("fixture did not declare an unavailable PAR2 index")
				}
			}
			if err := os.WriteFile(filepath.Join(root, slug+".nzb"), nzb, 0644); err != nil {
				t.Fatal(err)
			}
			job, err := api.submit(nzb, slug)
			if err != nil {
				t.Fatal(err)
			}
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
			requests := map[string]int{}
			nntp.mu.Lock()
			for id, count := range nntp.requests {
				if strings.HasPrefix(id, slug+"-") {
					requests[id] = count
				}
			}
			nntp.mu.Unlock()
			recoveryRequests := map[string]int{"par2": 0, "par3": 0}
			for index, name := range unpackSortedNames(posted) {
				if !strings.Contains(name, ".vol") {
					continue
				}
				format := strings.TrimPrefix(filepath.Ext(name), ".")
				for id, count := range requests {
					if strings.HasPrefix(id, fmt.Sprintf("%s-%d-", slug, index)) {
						recoveryRequests[format] += count
					}
				}
			}
			par3WriteJSON(t, filepath.Join(root, slug+"-evidence.json"), map[string]any{
				"jobId": job, "status": status, "history": history.HistoryItem, "requests": requests, "recoveryRequests": recoveryRequests,
			})
			failure := ""
			if history.HistoryItem != nil && history.HistoryItem.Error != nil {
				failure = *history.HistoryItem.Error
			}
			logBytes, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			var jobLines []string
			for _, line := range strings.Split(string(logBytes), "\n") {
				for _, field := range strings.Fields(line) {
					if field == fmt.Sprintf("job_id=%d", job) {
						jobLines = append(jobLines, line)
						break
					}
				}
			}
			jobLog := strings.Join(jobLines, "\n")
			for _, line := range jobLines {
				if strings.Contains(line, "unprotected file(s)") && strings.Contains(line, ".par3") {
					t.Fatalf("recovery carrier incorrectly reported as delivered payload: %s", line)
				}
			}
			if scenario.missingPar2Metadata && !scenario.overlap {
				if status != "FAILED" || !strings.Contains(failure, "PAR2 metadata discovery exhausted") {
					t.Fatalf("unrelated PAR3 evidence excused unverified payload: status=%s error=%s log=%s", status, failure, logPath)
				}
				if scenario.damage && strings.Count(jobLog, "PAR3 repair installed verified outputs") != 1 {
					t.Fatalf("independent PAR3 repair did not settle before metadata failure: log=%s", logPath)
				}
				for name := range expected {
					if _, err := os.Stat(filepath.Join(root, "complete", slug, name)); !os.IsNotExist(err) {
						t.Fatalf("unverified mixed job delivered %s: %v", name, err)
					}
				}
				return
			}
			if scenario.damage && scenario.omitPar2Recovery && !scenario.insufficient {
				installed := strings.Index(jobLog, "PAR3 repair installed verified outputs")
				if installed < 0 || strings.Count(jobLog, "PAR3 repair installed verified outputs") != 1 {
					t.Fatalf("fallback must install exactly one native repair: log=%s", logPath)
				}
				if scenario.overlap && !scenario.conflict {
					verifiedSets := map[string]bool{}
					for _, line := range strings.Split(jobLog[installed:], "\n") {
						if !strings.Contains(line, "PAR2 clean set verification source") || !strings.Contains(line, `verification_mode="authoritative"`) {
							continue
						}
						for _, field := range strings.Fields(line) {
							if id, ok := strings.CutPrefix(field, "recovery_set_id="); ok && id != "" {
								verifiedSets[id] = true
							}
						}
					}
					wantSets := 1
					if scenario.secondPar2 {
						wantSets = 2
					}
					if scenario.missingPar2Metadata {
						wantSets = 0
					}
					if len(verifiedSets) != wantSets {
						t.Fatalf("fallback verified %d native PAR2 sets, want %d: log=%s", len(verifiedSets), wantSets, logPath)
					}
				}
				if !scenario.overlap {
					if status != "FAILED" || !strings.Contains(failure, "PAR2 recovery failed") {
						t.Fatalf("independent PAR3 success erased the PAR2 failure: status=%s error=%s", status, failure)
					}
					for name := range expected {
						if _, err := os.Stat(filepath.Join(root, "complete", slug, name)); !os.IsNotExist(err) {
							t.Fatalf("partially repairable job delivered %s: %v", name, err)
						}
					}
					return
				}
			}
			if scenario.conflict {
				if status != "FAILED" || !(strings.Contains(failure, "conflicting PAR2 and PAR3") || (scenario.omitPar2Recovery && strings.Contains(failure, "PAR2 recovery failed"))) {
					t.Fatalf("conflicting native descriptions did not terminate safely: status=%s error=%s log=%s", status, failure, logPath)
				}
				if scenario.omitPar2Recovery && recoveryRequests["par3"] == 0 {
					t.Fatal("conflict case never exercised fallback repair")
				}
				for name := range expected {
					if _, err := os.Stat(filepath.Join(root, "complete", slug, name)); !os.IsNotExist(err) {
						t.Fatalf("conflicting sets delivered %s: %v", name, err)
					}
				}
				return
			}
			if scenario.insufficient {
				if status != "FAILED" || !strings.Contains(strings.ToLower(failure), "recovery") {
					t.Fatalf("unrepairable mixed set lacks terminal recovery failure: status=%s error=%s log=%s", status, failure, logPath)
				}
				if recoveryRequests["par3"] == 0 {
					t.Fatalf("PAR2 exhaustion never reached the PAR3 recovery attempt: %v", recoveryRequests)
				}
				for name := range expected {
					if _, err := os.Stat(filepath.Join(root, "complete", slug, name)); !os.IsNotExist(err) {
						t.Fatalf("unrepairable mixed set delivered %s: %v", name, err)
					}
				}
				return
			}
			if status != "COMPLETED" {
				t.Fatalf("mixed job status=%s error=%s requests=%v log=%s", status, failure, recoveryRequests, logPath)
			}
			if history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
				t.Fatalf("verified mixed job has unhealthy delivery: %+v", history.HistoryItem)
			}
			for name, data := range expected {
				actual, err := os.ReadFile(filepath.Join(root, "complete", slug, name))
				if err != nil || !bytes.Equal(actual, data) {
					t.Fatalf("mixed output %s differs from protected input: %v", name, err)
				}
			}
			if !scenario.damage || (scenario.overlap && !scenario.omitPar2Recovery) {
				if recoveryRequests["par3"] != 0 {
					t.Fatalf("PAR3 recovery downloaded before PAR2's successful verdict: %v", recoveryRequests)
				}
			} else if recoveryRequests["par3"] == 0 {
				t.Fatal("damaged PAR3 source never requested recovery")
			}
			if !scenario.damage && recoveryRequests["par2"] != 0 {
				t.Fatalf("clean mixed job downloaded PAR2 recovery: %v", recoveryRequests)
			}
			if scenario.damage && !scenario.omitPar2Recovery && recoveryRequests["par2"] == 0 {
				t.Fatal("damaged PAR2 source never used its preferred engine")
			}
		})
	}
}
