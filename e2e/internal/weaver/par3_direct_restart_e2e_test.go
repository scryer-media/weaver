package weaver

import (
	"bytes"
	"database/sql"
	"encoding/xml"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Restart after durable direct coverage exists, before any PAR3 repair starts.
// Both processes and their database/NNTP fixture belong exclusively to this test.
func TestPar3DirectRestartE2E(t *testing.T) {
	bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
	if bin == "" || reference == "" {
		t.Skip("set the native Weaver and official PAR3 reference binaries")
	}
	if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) {
		t.Fatal("both binaries must be absolute")
	}
	for _, scenario := range []struct {
		name, format, interrupt                           string
		nzbPassword, changedPartial, insufficient, cancel bool
	}{
		{name: "rar-store", format: "rar-store"},
		{name: "rar-store-cancel", format: "rar-store", cancel: true},
		{name: "rar-store-repair-sync-crash", format: "rar-store", interrupt: "sync"},
		{name: "rar-store-repair-publish-crash", format: "rar-store", interrupt: "publish"},
		{name: "rar-encrypted-repair-sync-crash", format: "rar-encrypted", nzbPassword: true, interrupt: "sync"},
		{name: "rar-encrypted-repair-publish-crash", format: "rar-encrypted", nzbPassword: true, interrupt: "publish"},
		{name: "rar-encrypted-cancel", format: "rar-encrypted", nzbPassword: true, cancel: true},
		{name: "rar-encrypted", format: "rar-encrypted"},
		{name: "rar-encrypted-nzb-password", format: "rar-encrypted", nzbPassword: true},
		{name: "rar-store-changed-partial", format: "rar-store", changedPartial: true},
		{name: "rar-encrypted-changed-partial", format: "rar-encrypted", nzbPassword: true, changedPartial: true},
		{name: "rar-encrypted-changed-partial-insufficient", format: "rar-encrypted", nzbPassword: true, changedPartial: true, insufficient: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			root, err := os.MkdirTemp("", "weaver-par3-direct-restart-")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("preserved artifacts: %s", root)
			dir := filepath.Join(root, "sources")
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			posted, payload, member, password := par3ArchiveFixture(t, dir, scenario.format)
			protected := unpackSortedNames(posted)
			if scenario.changedPartial && scenario.format == "rar-encrypted" && !scenario.insufficient {
				// Re-encrypting damaged plaintext changes the following CBC
				// chain, so this recovery case needs more than eight blocks.
				par3ReferenceParity(t, reference, dir, posted, []string{"-s32768", "-c24", "-e1"})
			} else {
				par3ArchiveParity(t, reference, dir, posted)
			}
			// Damage only the original archive, before yEnc CRCs are generated.
			posted[protected[0]][len(posted[protected[0]])/2] ^= 0x80
			nntp := startUnpackNNTP(t)
			port := nntp.listener.Addr().(*net.TCPAddr).Port
			env := []string{"WEAVER_RAR_DIRECT_STORE=true", "RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace,weaver_server_core::pipeline::direct_store=debug"}
			url, firstLog, stop := startManagedUnpackWeaver(t, bin, root, "before.log", port, env...)
			api := provisionUnpackAPI(t, root, url)
			slug := "par3-direct-restart-" + scenario.name
			nzb := nntp.publishUnpack(slug, "clean", posted, nil)
			if scenario.nzbPassword {
				var escaped bytes.Buffer
				if err := xml.EscapeText(&escaped, []byte(password)); err != nil {
					t.Fatal(err)
				}
				const root = `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">`
				if bytes.Count(nzb, []byte(root)) != 1 || password == "" {
					t.Fatal("expected one NZB root and a nonempty fixture password")
				}
				head := root + `<head><meta type="password">` + escaped.String() + `</meta></head>`
				nzb = bytes.Replace(nzb, []byte(root), []byte(head), 1)
				password = ""
			}
			gate := &unpackGate{released: make(chan struct{})}
			release := sync.OnceFunc(func() { close(gate.released) })
			t.Cleanup(release)
			nntp.mu.Lock()
			for index, name := range unpackSortedNames(posted) {
				if !strings.Contains(name, ".vol") || !strings.HasSuffix(name, ".par3") {
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
			job, err := api.submitWithPassword(nzb, slug, password)
			if err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(root, "weaver.db"))+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			ready := false
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				var checkpoints int
				err := db.QueryRow("SELECT COUNT(*) FROM active_direct_coverage WHERE job_id = ?", job).Scan(&checkpoints)
				log, _ := os.ReadFile(firstLog)
				if err == nil && checkpoints > 0 && gate.held.Load() > 0 && strings.Contains(string(log), "status=NeedRecovery") {
					ready = true
					break
				}
				if api.status(job) == "FAILED" {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if !ready {
				t.Fatalf("job never reached durable direct recovery wait: %s log=%s", api.status(job), firstLog)
			}
			if password != "" {
				var stored string
				if err := db.QueryRow("SELECT password FROM active_jobs WHERE job_id = ?", job).Scan(&stored); err != nil {
					t.Fatal(err)
				}
				if !strings.HasPrefix(stored, "enc:v1:") || stored == password {
					t.Fatal("submitted archive password was not encrypted at rest")
				}
			}
			before := map[string]int{}
			nntp.mu.Lock()
			for id, count := range nntp.requests {
				before[id] = count
			}
			nntp.mu.Unlock()
			if scenario.cancel {
				var cancelled struct{ CancelJob bool }
				if err := api.query(`mutation($id:Int!) {cancelJob(id:$id)}`, map[string]any{"id": job}, &cancelled); err != nil || !cancelled.CancelJob {
					t.Fatalf("cancel recovery-wait job: accepted=%v err=%v", cancelled.CancelJob, err)
				}
			}
			stop()
			if scenario.changedPartial {
				// Mutate only this stopped fixture's extracted backing. Native
				// evidence must be rebuilt, not trusted from the coverage row.
				staging := filepath.Join(root, "complete", ".weaver-staging", fmt.Sprint(job))
				var partials []string
				err := filepath.WalkDir(staging, func(path string, entry os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ".direct.partial") {
						partials = append(partials, path)
					}
					return nil
				})
				if err != nil || len(partials) != 1 {
					t.Fatalf("expected one private member backing: files=%v err=%v", partials, err)
				}
				partial, err := os.OpenFile(partials[0], os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				offset := int64(len(payload) * 2 / 3)
				var value [1]byte
				_, err = partial.ReadAt(value[:], offset)
				if err == nil {
					value[0] ^= 0x80
					_, err = partial.WriteAt(value[:], offset)
				}
				if err == nil {
					err = partial.Sync()
				}
				closeErr := partial.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("change stopped direct backing: write=%v close=%v", err, closeErr)
				}
			}
			secondEnv := append([]string(nil), env...)
			if scenario.interrupt != "" {
				secondEnv = append(secondEnv, "WEAVER_E2E_FAILPOINT=direct_store.barrier."+scenario.interrupt)
			}
			url, secondLog, stopSecond := startManagedUnpackWeaver(t, bin, root, "after.log", port, secondEnv...)
			logs := []string{firstLog, secondLog}
			api.url = url
			release()
			if scenario.interrupt != "" {
				tripped := false
				deadline = time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) {
					log, _ := os.ReadFile(secondLog)
					if bytes.Contains(log, []byte("tripping e2e failpoint")) && bytes.Contains(log, []byte("direct_store.barrier."+scenario.interrupt)) {
						tripped = true
						break
					}
					time.Sleep(25 * time.Millisecond)
				}
				if !tripped {
					t.Fatalf("owned process never reached the repair checkpoint failpoint: log=%s", secondLog)
				}
				stopSecond()
				original, err := os.ReadFile(filepath.Join(dir, protected[0]))
				if err != nil {
					t.Fatal(err)
				}
				rebuilt, err := os.ReadFile(filepath.Join(root, "intermediate", slug, protected[0]))
				if err != nil || !bytes.Equal(rebuilt, original) {
					t.Fatalf("failpoint preceded native repair installation: read=%v equal=%v", err, bytes.Equal(rebuilt, original))
				}
				var checkpoints int
				if err := db.QueryRow("SELECT COUNT(*) FROM active_direct_coverage WHERE job_id = ?", job).Scan(&checkpoints); err != nil {
					t.Fatal(err)
				}
				wantCheckpoints := 0
				if scenario.interrupt == "publish" {
					wantCheckpoints = 1
				}
				if checkpoints != wantCheckpoints {
					t.Fatalf("crash published the wrong checkpoint state: hook=%s rows=%d want=%d", scenario.interrupt, checkpoints, wantCheckpoints)
				}
				if _, err := os.Stat(filepath.Join(root, "complete", slug, member)); !os.IsNotExist(err) {
					t.Fatalf("interrupted repair published an output: %v", err)
				}
				url, thirdLog, _ := startManagedUnpackWeaver(t, bin, root, "after-interruption.log", port, env...)
				logs = append(logs, thirdLog)
				api.url = url
			}
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
					Attention   *struct{ Code string }
				}
			}
			if err := api.query(`query($id:Int!) {historyItem(id:$id) {error failedBytes health attention {code}}}`, map[string]any{"id": job}, &history); err != nil {
				t.Fatal(err)
			}
			after := map[string]int{}
			nntp.mu.Lock()
			for id, count := range nntp.requests {
				after[id] = count
			}
			nntp.mu.Unlock()
			par3WriteJSON(t, filepath.Join(root, "evidence.json"), map[string]any{"jobId": job, "status": status, "history": history.HistoryItem, "beforeRequests": before, "afterRequests": after})
			if scenario.cancel {
				// Cancellation uses the existing FAILED queue state and its
				// distinct history attention code; it is not a repair failure.
				if status != "FAILED" || history.HistoryItem == nil || history.HistoryItem.Attention == nil || history.HistoryItem.Attention.Code != "CANCELLED" {
					t.Fatalf("cancelled job lost its terminal verdict after restart: status=%s history=%+v log=%s", status, history.HistoryItem, secondLog)
				}
				for _, table := range []string{"active_jobs", "active_direct_coverage"} {
					var rows int
					if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE job_id = ?", job).Scan(&rows); err != nil || rows != 0 {
						t.Fatalf("cancelled job retained restorable %s rows: rows=%d err=%v", table, rows, err)
					}
				}
				if _, err := os.Stat(filepath.Join(root, "complete", slug, member)); !os.IsNotExist(err) {
					t.Fatalf("cancelled job published an output: %v", err)
				}
				for id, count := range after {
					if count != before[id] {
						t.Fatalf("cancelled job requested another article: %s (%d -> %d)", id, before[id], count)
					}
				}
				return
			}
			if scenario.insufficient {
				if status != "FAILED" || history.HistoryItem == nil || history.HistoryItem.Error == nil ||
					!strings.Contains(*history.HistoryItem.Error, "compatible recovery remains insufficient") {
					t.Fatalf("damaged CBC chain with eight recovery blocks must fail explicitly: status=%s history=%+v", status, history.HistoryItem)
				}
				if _, err := os.Stat(filepath.Join(root, "complete", slug, member)); !os.IsNotExist(err) {
					t.Fatalf("insufficient recovery published an output: %v", err)
				}
				log, err := os.ReadFile(secondLog)
				if err != nil || !bytes.Contains(log, []byte("direct-store set resumed from its coverage checkpoint")) ||
					bytes.Contains(log, []byte("PAR3 repair installed verified outputs")) || bytes.Contains(log, []byte("direct-store set demoted")) {
					t.Fatalf("insufficient recovery must retain direct coverage without installing or demoting: %v log=%s", err, secondLog)
				}
				return
			}
			if status != "COMPLETED" || history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
				failure := ""
				if history.HistoryItem != nil && history.HistoryItem.Error != nil {
					failure = *history.HistoryItem.Error
				}
				t.Fatalf("direct restart status=%s error=%s log=%s", status, failure, secondLog)
			}
			actual, err := os.ReadFile(filepath.Join(root, "complete", slug, member))
			if err != nil || !bytes.Equal(actual, payload) {
				t.Fatalf("restarted extraction differs: %v", err)
			}
			resumed, finalized := false, false
			for _, path := range logs {
				log, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, line := range strings.Split(string(log), "\n") {
					if !strings.Contains(line, fmt.Sprintf("job_id=%d ", job)) {
						continue
					}
					if scenario.interrupt != "sync" && (strings.Contains(line, "direct-store set demoted") || strings.Contains(line, "direct-store coverage refused at restore")) {
						t.Fatalf("direct restart lost usable coverage: %s", line)
					}
					resumed = resumed || strings.Contains(line, "direct-store set resumed from its coverage checkpoint")
					finalized = finalized || strings.Contains(line, "direct-store set finalized without materializing a volume")
				}
			}
			if !resumed || (scenario.interrupt != "sync" && !finalized) {
				t.Fatalf("restart must resume and finalize direct coverage: resumed=%v finalized=%v log=%s", resumed, finalized, secondLog)
			}
			// A crash before the new checkpoint commits deliberately forfeits
			// its coverage. Refetch is allowed, but byte-exact delivery above
			// remains mandatory. A published checkpoint must avoid refetch.
			if scenario.interrupt == "sync" {
				return
			}
			for index, name := range unpackSortedNames(posted) {
				if strings.HasSuffix(name, ".par3") {
					continue
				}
				prefix := fmt.Sprintf("%s-%d-", slug, index)
				for id, count := range after {
					if strings.HasPrefix(id, prefix) && count != before[id] {
						t.Fatalf("restart refetched completed archive source %s: %s (%d -> %d)", name, id, before[id], count)
					}
				}
			}
		})
	}
}
