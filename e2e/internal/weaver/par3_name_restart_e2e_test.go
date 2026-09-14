package weaver

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPar3NameRestartE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set native Weaver and official PAR3 reference binaries")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("binaries must use absolute paths")
	}
	for _, format := range []string{"payload", "payload-case", "zip", "zip64", "7z"} {
		for _, phase := range []string{"intent", "moved", "persisted"} {
			t.Run(format+"-"+phase, func(t *testing.T) {
				root, err := os.MkdirTemp("", "weaver-par3-name-restart-")
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("preserved artifacts: %s", root)
				dir := filepath.Join(root, "sources")
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
				var name string
				var payload []byte
				posted := map[string][]byte{}
				oldName := "opaque.dat"
				if format == "payload-case" {
					oldName = "PAYLOAD.BIN"
				}
				if strings.HasPrefix(format, "payload") {
					name = "payload.bin"
					payload = make([]byte, 262144)
					rng := rand.New(rand.NewPCG(31, 89))
					for i := range payload {
						payload[i] = byte(rng.Uint32())
					}
					posted[name] = bytes.Clone(payload)
					par3ReferenceParity(t, reference, dir, posted, []string{"-e1", "-s32768", "-c8"})
					posted[oldName] = posted[name]
					delete(posted, name)
				} else {
					var inserted []byte
					name, _, inserted, payload = par3InsideFixture(t, reference, dir, format)
					posted[oldName] = inserted
				}
				nntp := startUnpackNNTP(t)
				port := nntp.listener.Addr().(*net.TCPAddr).Port
				failpoint := "par3.content_name." + phase
				url, firstLog, stop := startManagedUnpackWeaver(t, bin, root, "before.log", port,
					"WEAVER_E2E_FAILPOINT="+failpoint, "RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
				api := provisionUnpackAPI(t, root, url)
				slug := "par3-name-restart-" + format + "-" + phase
				nzb := nntp.publishUnpack(slug, "clean", posted, nil)
				job, err := api.submit(nzb, slug)
				if err != nil {
					t.Fatal(err)
				}
				tripped := false
				deadline := time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) {
					log, _ := os.ReadFile(firstLog)
					if bytes.Contains(log, []byte("tripping e2e failpoint")) && bytes.Contains(log, []byte(failpoint)) {
						tripped = true
						break
					}
					time.Sleep(25 * time.Millisecond)
				}
				if !tripped {
					t.Fatalf("name placement failpoint was not reached: log=%s", firstLog)
				}
				stop()
				hasName := func(base, wanted string) bool {
					entries, err := os.ReadDir(filepath.Join(root, base, slug))
					if os.IsNotExist(err) {
						return false
					}
					if err != nil {
						t.Fatal(err)
					}
					for _, entry := range entries {
						if entry.Name() == wanted {
							return true
						}
					}
					return false
				}
				oldExists, targetExists := hasName("intermediate", oldName), hasName("intermediate", name)
				if phase == "intent" {
					if !oldExists || targetExists {
						t.Fatalf("intent changed source names before move: old=%v target=%v", oldExists, targetExists)
					}
				} else if oldExists || !targetExists {
					t.Fatalf("atomic move did not preserve its output: old=%v target=%v", oldExists, targetExists)
				}
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
				if status != "COMPLETED" {
					t.Fatalf("name restart failed: job=%d status=%s log=%s", job, status, secondLog)
				}
				actual, err := os.ReadFile(filepath.Join(root, "complete", slug, "payload.bin"))
				if err != nil || !bytes.Equal(actual, payload) {
					t.Fatalf("wrong renamed output: %v", err)
				}
				for _, base := range []string{"intermediate", "complete"} {
					if hasName(base, oldName) {
						t.Fatalf("retired source name survived restart: %s", base)
					}
				}
				nntp.mu.Lock()
				defer nntp.mu.Unlock()
				for index, file := range unpackSortedNames(posted) {
					if !strings.Contains(file, ".vol") {
						continue
					}
					prefix := fmt.Sprintf("%s-%d-", slug, index)
					for id, hits := range nntp.requests {
						if strings.HasPrefix(id, prefix) && hits != 0 {
							t.Fatalf("clean name replay requested parity: %s", file)
						}
					}
				}
			})
		}
	}
}
