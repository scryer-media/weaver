package weaver

import (
	"bytes"
	"crypto/sha256"
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

// Official creation and verbose listing establish the exercised geometry. The
// application only verifies and repairs; all parity is created by the reference.
func TestPar3GeometryE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set WEAVER_PAR3_E2E_BIN and WEAVER_PAR3_REFERENCE_BIN for geometry scenarios")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("both binaries must use absolute paths")
	}
	root, err := os.MkdirTemp("", "weaver-par3-geometry-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	url, logPath := startUnpackWeaver(t, bin, root, nntp.listener.Addr().(*net.TCPAddr).Port,
		"RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
	api := provisionUnpackAPI(t, root, url)
	type geometry struct {
		name    string
		size    int
		options []string
		facts   []string
	}
	cases := []geometry{
		{"aliases", 131072 + 123, []string{"-e1", "-s32768", "-c1"}, nil},
		{"full-nested-cauchy", 262144 + 123, []string{"-e1", "-s32768", "-c9"}, nil},
		{"full-nested-fft", 262144 + 123, []string{"-e8", "-s32768", "-c9"}, []string{"FFT based Reed-Solomon Codes"}},
		{"full-cauchy", 262144 + 123, []string{"-e1", "-s32768", "-c9"}, nil},
		{"full-fft", 262144 + 123, []string{"-e8", "-s32768", "-c9"}, []string{"FFT based Reed-Solomon Codes"}},
		{"gf16", 1048576 + 123, []string{"-e1", "-s1024", "-c8"}, []string{"Galois field size = 2", "Galois field generator = 0x1100B"}},
		{"fft", 1048576 + 123, []string{"-e8", "-s32768", "-c8"}, []string{"FFT based Reed-Solomon Codes"}},
		{"uneven-cohorts", 1048576 + 32768 + 123, []string{"-e8", "-i2", "-s32768", "-c9"}, []string{"Actual block count = 34", "Number of cohort = 3"}},
		{"over-65536", 65538*64 + 17, []string{"-e8", "-i2", "-s64", "-c9"}, []string{"Actual block count = 65538", "Number of cohort = 3"}},
		{"aligned-dedup", 32768, []string{"-e1", "-d1", "-s32768", "-c4"}, []string{"Actual block count = 1", "Deduplication = 15"}},
		{"sliding-dedup", 32768, []string{"-e1", "-d2", "-s32768", "-c4"}, []string{"Actual block count = 1", "Deduplication = 15"}},
		{"data-only", 131072 + 123, []string{"-e1", "-D", "-s32768", "-c1"}, []string{"Wrote archive file"}},
		{"packed-tails", 8192 + 80, []string{"-e1", "-s8192", "-c4"}, []string{"Tail packing = 2"}},
		{"multiple-sets", 262144 + 123, []string{"-e1", "-s32768", "-c8"}, nil},
		{"shared-compatible", 262144 + 123, []string{"-e1", "-s32768", "-c8"}, nil},
		{"shared-conflict", 262144 + 123, []string{"-e1", "-s32768", "-c8"}, nil},
	}
	for _, spec := range cases {
		t.Run(spec.name, func(t *testing.T) {
			dir := filepath.Join(root, "sources", spec.name)
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewPCG(19, 41))
			payload := make([]byte, spec.size)
			for i := range payload {
				payload[i] = byte(rng.Uint32())
			}
			switch spec.name {
			case "aligned-dedup":
				payload = bytes.Repeat(payload, 16)
			case "sliding-dedup":
				payload = bytes.Repeat(append([]byte("prefix13bytes"), payload...), 16)
			}
			expected := map[string][]byte{"payload.bin": payload}
			if spec.name == "aliases" {
				expected["alias.bin"] = bytes.Clone(payload)
			}
			if strings.HasPrefix(spec.name, "full-") {
				expected["empty.bin"] = []byte{}
				expected["inline.bin"] = bytes.Clone(payload[:17])
			}
			if spec.name == "packed-tails" {
				expected["second.bin"] = append(bytes.Clone(payload[:8192]), bytes.Repeat([]byte{23}, 96)...)
				expected["third.bin"] = append(bytes.Clone(payload[:8192]), bytes.Repeat([]byte{71}, 128)...)
			}
			if strings.HasPrefix(spec.name, "full-nested-") {
				nested := make(map[string][]byte)
				for name, data := range expected {
					nested["nested/deeper/"+name] = data
				}
				expected = nested
			}
			carriers := make(map[string][]byte)
			for name, data := range expected {
				carriers[name] = data
			}
			facts := par3ReferenceParity(t, reference, dir, carriers, spec.options)
			for _, fact := range spec.facts {
				if !strings.Contains(facts, fact) {
					t.Fatalf("reference did not establish %q: %s", fact, facts)
				}
			}
			if spec.name == "multiple-sets" || strings.HasPrefix(spec.name, "shared-") {
				// Distinct official input sets use different codecs and unique
				// carrier names. Carrier bytes remain exactly as the reference wrote them.
				renamed := make(map[string][]byte, len(carriers))
				for name, data := range carriers {
					if strings.HasSuffix(name, ".par3") {
						name = "cauchy." + name
					}
					renamed[name] = data
				}
				carriers = renamed
				second := bytes.Clone(payload)
				otherName := "payload.bin"
				if spec.name == "multiple-sets" {
					for i := range second {
						second[i] ^= 0x5a
					}
					otherName = "second.bin"
					expected[otherName] = second
				} else if spec.name == "shared-conflict" {
					second[0] ^= 0x80
				}
				secondDir := filepath.Join(dir, "second-set")
				if err := os.MkdirAll(secondDir, 0755); err != nil {
					t.Fatal(err)
				}
				other := map[string][]byte{otherName: second}
				blockSize := "-s32768"
				if strings.HasPrefix(spec.name, "shared-") {
					blockSize = "-s16384"
				}
				otherFacts := par3ReferenceParity(t, reference, secondDir, other, []string{"-e8", blockSize, "-c8"})
				if !strings.Contains(otherFacts, "FFT based Reed-Solomon Codes") {
					t.Fatalf("second set did not establish FFT geometry: %s", otherFacts)
				}
				for name, data := range other {
					if name == "payload.bin" {
						continue
					}
					if strings.HasSuffix(name, ".par3") {
						name = "fft." + name
					}
					carriers[name] = data
				}
			}
			if spec.name == "data-only" {
				// Omit complete official recovery carriers. Data packets must
				// supply the repair; no packet bytes are constructed or edited.
				for name := range carriers {
					if strings.Contains(name, ".vol") {
						delete(carriers, name)
					}
				}
			}
			modes := []string{"clean", "corrupt"}
			switch spec.name {
			case "aliases":
				modes = []string{"donor-alias"}
			case "gf16", "fft", "packed-tails":
				modes = append(modes, "donor-corrupt", "donor-shift")
			case "full-cauchy":
				modes = []string{"omitted", "omitted-collision"}
			case "full-fft":
				modes = []string{"omitted"}
			case "full-nested-cauchy", "full-nested-fft":
				modes = []string{"omitted", "donor-clean", "donor-shift"}
			case "aligned-dedup", "sliding-dedup", "data-only":
				modes = append(modes, "omitted")
			}
			if spec.name == "packed-tails" {
				modes = append(modes, "omitted")
			}
			if spec.name == "uneven-cohorts" {
				modes = append(modes, "cohort-deficit")
			}
			if spec.name == "multiple-sets" {
				modes = append(modes, "only-first-corrupt")
			}
			for _, mode := range modes {
				t.Run(mode, func(t *testing.T) {
					posted := make(map[string][]byte)
					for name, data := range carriers {
						posted[name] = bytes.Clone(data)
					}
					if strings.HasPrefix(mode, "donor-") {
						donor := bytes.Clone(payload)
						if mode == "donor-corrupt" {
							donor[len(donor)/2] ^= 0x80
						}
						if mode == "donor-shift" {
							donor = append([]byte("prefix13bytes"), donor...)
						}
						posted["opaque.dat"] = donor
						delete(posted, "payload.bin")
						if strings.HasPrefix(spec.name, "full-nested-") {
							for name := range expected {
								delete(posted, name)
							}
							for name := range posted {
								if strings.Contains(name, ".vol") {
									delete(posted, name)
								}
							}
						}
						if mode == "donor-alias" {
							delete(posted, "alias.bin")
							for name := range posted {
								if strings.Contains(name, ".vol") {
									delete(posted, name)
								}
							}
						}
					}
					if strings.HasPrefix(mode, "omitted") {
						for name := range expected {
							delete(posted, name)
						}
					}
					if mode == "corrupt" {
						for name, data := range expected {
							offset := len(data) / 2
							if spec.name == "packed-tails" {
								offset = len(data) - 1
							}
							posted[name][offset] ^= 0x80
						}
					}
					if mode == "only-first-corrupt" {
						posted["payload.bin"][len(payload)/2] ^= 0x80
					}
					if mode == "cohort-deficit" {
						// Four bad input blocks in one cohort cannot use the six
						// surplus recovery blocks belonging to the other cohorts.
						for block := 0; block < 4; block++ {
							posted["payload.bin"][block*3*32768+64] ^= 0x80
						}
					}
					slug := "par3-geometry-" + spec.name + "-" + mode
					nzb := nntp.publishUnpack(slug, "clean", posted, nil)
					var release func()
					if mode == "omitted-collision" {
						gate := &unpackGate{released: make(chan struct{})}
						release = sync.OnceFunc(func() { close(gate.released) })
						t.Cleanup(release)
						nntp.mu.Lock()
						for id, article := range nntp.articles {
							if strings.HasPrefix(id, slug+"-") {
								article.gate = gate
								nntp.articles[id] = article
							}
						}
						nntp.mu.Unlock()
					}
					if err := os.WriteFile(filepath.Join(root, slug+".nzb"), nzb, 0644); err != nil {
						t.Fatal(err)
					}
					job, err := api.submit(nzb, slug)
					if err != nil {
						t.Fatal(err)
					}
					api.cancelOnFailure(t, job)
					if release != nil {
						if err := os.WriteFile(filepath.Join(root, "intermediate", slug, "payload.bin"), []byte("unclaimed existing output"), 0644); err != nil {
							t.Fatal(err)
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
					nntp.mu.Lock()
					requests := map[string]int{}
					for id, count := range nntp.requests {
						if strings.HasPrefix(id, slug+"-") {
							requests[id] = count
						}
					}
					nntp.mu.Unlock()
					hashes := map[string]string{}
					for name, data := range expected {
						hashes[name] = fmt.Sprintf("%x", sha256.Sum256(data))
					}
					par3WriteJSON(t, filepath.Join(root, slug+"-evidence.json"), map[string]any{
						"jobId": job, "status": status, "history": history.HistoryItem, "requests": requests, "expectedSHA256": hashes,
					})
					if mode == "cohort-deficit" {
						if status != "FAILED" || history.HistoryItem == nil || history.HistoryItem.Error == nil || !strings.Contains(*history.HistoryItem.Error, "recovery") {
							t.Fatalf("cohort deficit did not fail with a recovery verdict: status=%s history=%+v log=%s", status, history.HistoryItem, logPath)
						}
						if _, err := os.Stat(filepath.Join(root, "complete", slug, "payload.bin")); !os.IsNotExist(err) {
							t.Fatalf("unrepairable cohort delivered an output: %v", err)
						}
						return
					}
					if spec.name == "shared-conflict" {
						if status != "FAILED" || history.HistoryItem == nil || history.HistoryItem.Error == nil || !strings.Contains(*history.HistoryItem.Error, "contradictory authenticated PAR3") {
							t.Fatalf("conflicting descriptions were not refused: status=%s history=%+v log=%s", status, history.HistoryItem, logPath)
						}
						if _, err := os.Stat(filepath.Join(root, "complete", slug, "payload.bin")); !os.IsNotExist(err) {
							t.Fatalf("contradictory sets published an output: %v", err)
						}
						return
					}
					if mode == "omitted-collision" {
						if status != "FAILED" || history.HistoryItem == nil || history.HistoryItem.Error == nil || !strings.Contains(*history.HistoryItem.Error, "unclaimed output already exists") {
							t.Fatalf("unclaimed output was not refused: status=%s history=%+v log=%s", status, history.HistoryItem, logPath)
						}
						actual, err := os.ReadFile(filepath.Join(root, "intermediate", slug, "payload.bin"))
						if err != nil || string(actual) != "unclaimed existing output" {
							t.Fatalf("collision overwrote existing output: %v", err)
						}
						return
					}
					if status != "COMPLETED" {
						failure := ""
						if history.HistoryItem != nil && history.HistoryItem.Error != nil {
							failure = *history.HistoryItem.Error
						}
						t.Fatalf("job=%d status=%s error=%s log=%s", job, status, failure, logPath)
					}
					if history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
						t.Fatalf("verified geometry has unhealthy delivery: %+v", history.HistoryItem)
					}
					for name, data := range expected {
						actual, err := os.ReadFile(filepath.Join(root, "complete", slug, name))
						if err != nil || !bytes.Equal(actual, data) {
							t.Fatalf("output %s differs from reference-protected input: %v", name, err)
						}
					}
					if mode == "clean" || mode == "only-first-corrupt" {
						for index, name := range unpackSortedNames(posted) {
							if mode == "only-first-corrupt" && !strings.HasPrefix(name, "fft.") {
								continue
							}
							if !strings.Contains(name, ".vol") {
								continue
							}
							for id, count := range requests {
								if count != 0 && strings.HasPrefix(id, fmt.Sprintf("%s-%d-", slug, index)) {
									t.Fatalf("clean geometry downloaded recovery carrier %s", name)
								}
							}
						}
					}
				})
			}
		})
	}
}
