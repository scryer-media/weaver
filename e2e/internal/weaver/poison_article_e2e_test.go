package weaver

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/blake3"
)

// One article must not be able to stop a setup that has a single server.
//
// This opt-in suite owns a fresh native Weaver process and the loopback NNTP
// fixture, configured with exactly one server. Run with
// WEAVER_POISON_ARTICLE_E2E_BIN pointing at the binary under review. The
// connection-breaking case waits out the retry holds and takes several minutes.
func TestPoisonArticleE2E(t *testing.T) {
	bin := os.Getenv("WEAVER_POISON_ARTICLE_E2E_BIN")
	if bin == "" {
		t.Skip("set WEAVER_POISON_ARTICLE_E2E_BIN to run the real-process poison-article cases")
	}
	if !filepath.IsAbs(bin) {
		t.Fatal("WEAVER_POISON_ARTICLE_E2E_BIN must be absolute")
	}
	if _, err := exec.LookPath("par2"); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("", "weaver-poison-article-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	url, logPath := startUnpackWeaver(t, bin, root, nntp.listener.Addr().(*net.TCPAddr).Port)
	api := provisionUnpackAPI(t, root, url)
	files, payload := unpackFixture(t, filepath.Join(root, "sources"), "tar")

	publish := func(t *testing.T, slug string, parity bool, mutate func(id string, article *unpackArticle)) int {
		t.Helper()
		posted := map[string][]byte{}
		for name, data := range files {
			posted[name] = data
		}
		if parity {
			dir := filepath.Join(root, "fixtures", slug)
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			unpackParity(t, dir, posted)
		}
		nzb := nntp.publishUnpack(slug, "clean", posted, nil)
		nntp.mu.Lock()
		for id, article := range nntp.articles {
			if strings.HasPrefix(id, slug+"-") {
				mutate(id, &article)
				nntp.articles[id] = article
			}
		}
		nntp.mu.Unlock()
		job, err := api.submit(nzb, slug)
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	await := func(t *testing.T, job int, within time.Duration) {
		t.Helper()
		deadline := time.Now().Add(within)
		status := ""
		for time.Now().Before(deadline) {
			status = api.status(job)
			if status == "COMPLETED" || status == "FAILED" {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		if status != "COMPLETED" {
			t.Fatalf("job %d is %s after %s; log=%s", job, status, within, logPath)
		}
	}
	verify := func(t *testing.T, slug string) {
		t.Helper()
		actual, err := os.ReadFile(filepath.Join(root, "complete", slug, "payload.bin"))
		if err != nil {
			t.Fatal(err)
		}
		if want, got := blake3.Sum256(payload), blake3.Sum256(actual); len(actual) != len(payload) || got != want {
			t.Fatalf("output mismatch: bytes=%d/%d BLAKE3=%x/%x", len(actual), len(payload), got, want)
		}
	}
	jobLog := func(t *testing.T, job int) string {
		t.Helper()
		raw, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		return unpackJobLog(string(raw), job)
	}

	// Lines between the yEnc trailer and the NNTP terminator are not part of
	// the article. Every article of the job carries one, so a client that
	// refuses them cannot finish any of it.
	t.Run("trailer-junk", func(t *testing.T) {
		slug := "poison-trailer-junk"
		job := publish(t, slug, false, func(_ string, article *unpackArticle) {
			article.body = append(article.body, "X-Served-By: a line after the trailer\n"...)
		})
		await(t, job, 90*time.Second)
		verify(t, slug)
		if strings.Contains(jobLog(t, job), "malformed multiline terminator") {
			t.Fatal("a line after the trailer was treated as a broken terminator")
		}
	})

	// One article ends the connection on every fetch. That is a transport
	// fault, which costs the article nothing and counts against the server,
	// and the article is the highest-priority work the server's recovery probe
	// can pick. The job behind it has to download regardless, and the article
	// has to end up failed and repaired rather than retried forever.
	t.Run("connection-breaker", func(t *testing.T) {
		poisonSlug, cleanSlug := "poison-connection-breaker", "poison-bystander"
		var poisoned string
		poison := publish(t, poisonSlug, true, func(id string, article *unpackArticle) {
			// The middle article of the archive, which publishUnpack lists
			// first: file 0.
			if poisoned == "" && strings.HasPrefix(id, poisonSlug+"-0-") && strings.HasSuffix(id, "-32@direct-unpack.test") {
				article.breaksConnection = true
				poisoned = id
			}
		})
		if poisoned == "" {
			t.Fatal("no article was poisoned")
		}
		bystander := publish(t, cleanSlug, false, func(string, *unpackArticle) {})

		await(t, bystander, 3*time.Minute)
		verify(t, cleanSlug)

		await(t, poison, 15*time.Minute)
		verify(t, poisonSlug)
		log := jobLog(t, poison)
		if !strings.Contains(log, "PAR2 repair wrote its outputs") {
			t.Fatal("the poisoned job completed without evidence of a PAR2 repair")
		}
		nntp.mu.Lock()
		requests := nntp.requests[poisoned]
		nntp.mu.Unlock()
		// Two free transport retries, then the article's own budget.
		if requests < 3 || requests > 12 {
			t.Fatalf("the poisoned article was requested %d times", requests)
		}
		t.Logf("poisoned article %s requested %d times", poisoned, requests)
	})
}
