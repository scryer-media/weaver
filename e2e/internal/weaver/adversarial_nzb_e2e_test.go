// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/blake3"
)

// NZBs whose own text lies or attacks: control characters in a group or a
// message id, a dead post declared far larger than it is, thousands of spare
// ids for one part, a flood of password metas, a job name past the rename
// bound and a megabyte group on a repairable post.
//
// The corpus lane cannot write these, because nyuu writes its NZB. This
// opt-in suite owns a fresh native Weaver process and the loopback NNTP
// fixture, writes each NZB itself, and files each article in the fixture by
// hand. Run with WEAVER_ADVERSARIAL_NZB_E2E_BIN pointing at the binary under
// review. Every case is followed by a clean bystander job that must complete
// byte-equal, so a case that wedges the pipeline fails there.
func TestAdversarialNZBE2E(t *testing.T) {
	bin := os.Getenv("WEAVER_ADVERSARIAL_NZB_E2E_BIN")
	if bin == "" {
		t.Skip("set WEAVER_ADVERSARIAL_NZB_E2E_BIN to run the real-process adversarial NZB cases")
	}
	if !filepath.IsAbs(bin) {
		t.Fatal("WEAVER_ADVERSARIAL_NZB_E2E_BIN must be absolute")
	}
	if _, err := exec.LookPath("par2"); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("", "weaver-adversarial-nzb-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("preserved artifacts: %s", root)
	nntp := startUnpackNNTP(t)
	url, logPath := startUnpackWeaver(t, bin, root, nntp.listener.Addr().(*net.TCPAddr).Port)
	api := provisionUnpackAPI(t, root, url)
	h := &advNZBHarness{nntp: nntp, api: api, root: root, logPath: logPath}

	// A group and a message id may not carry a line break: written to the
	// server, one would end the command and start another. Each part's first
	// id and the file's first group smuggle a BODY for a canary id behind a
	// CR LF; the honest alternate and group after them serve the file. The
	// CR LF and tab ids and the CR LF group are dropped at parse, so each
	// part is held under its honest id and the file under its honest group.
	// The job completes and the fixture never sees a request for a canary.
	t.Run("control-characters", func(t *testing.T) {
		slug := "adv-nzb-control-characters"
		name := "control.s01e01.mkv"
		data := advNZBPayload(slug, 4*advNZBArticleBytes)
		parts := h.post(slug, name, data, advNZBArticleBytes)
		var canaries []string
		for index := range parts {
			canary := fmt.Sprintf("%s-canary-%d@adversarial-nzb.test", slug, index+1)
			canaries = append(canaries, canary)
			hostile := []string{
				xmlUnpackText(fmt.Sprintf("%s-crlf-%d@adversarial-nzb.test>\r\nBODY <%s", slug, index+1, canary)),
				xmlUnpackText(fmt.Sprintf("%s-tab-%d\t@adversarial-nzb.test", slug, index+1)),
			}
			parts[index].ids = append(hostile, parts[index].ids...)
		}
		groupCanary := slug + "-canary-group@adversarial-nzb.test"
		canaries = append(canaries, groupCanary)
		file := advNZBFile{
			name:     name,
			groups:   []string{xmlUnpackText("alt.test\r\nBODY <" + groupCanary + ">"), "alt.test"},
			segments: parts,
		}
		job := h.submit(t, slug, nil, file)
		h.awaitStatus(t, job, "COMPLETED")
		h.verify(t, slug, name, data)
		for _, canary := range canaries {
			if requests := h.requests(canary); requests != 0 {
				t.Errorf("the canary %s was requested %d times: a hostile value reached the wire", canary, requests)
			}
		}
		h.bystander(t, slug)
	})

	// A post whose first article is declared 32 MiB and whose every other
	// article is a missing byte. Nothing in it can be fetched, so the job
	// fails; abandoning the dead post weighs it once, not once per article.
	// After the verdict none of its ids is asked for again.
	t.Run("dead-post", func(t *testing.T) {
		slug := "adv-nzb-dead-post"
		const tail = 20000
		parts := []advNZBSegment{{number: 1, bytes: 32 << 20, ids: []string{slug + "-1@adversarial-nzb.test"}}}
		for number := 2; number <= tail+1; number++ {
			parts = append(parts, advNZBSegment{number: number, bytes: 1, ids: []string{fmt.Sprintf("%s-%d@adversarial-nzb.test", slug, number)}})
		}
		job := h.submit(t, slug, nil, advNZBFile{name: "dead.s01e02.mkv", groups: []string{"alt.test"}, segments: parts})
		h.awaitStatus(t, job, "FAILED")
		before := h.requestsWithPrefix(slug + "-")
		h.bystander(t, slug)
		if after := h.requestsWithPrefix(slug + "-"); after != before {
			t.Fatalf("the dead post's articles were asked for %d more times after it failed", after-before)
		}
	})

	// One part carries three thousand spare ids the server does not hold,
	// ahead of the one it does. Spending the spares is a loop, so the part is
	// served by its last id and the job completes.
	t.Run("spare-ids", func(t *testing.T) {
		slug := "adv-nzb-spare-ids"
		name := "spares.s01e03.mkv"
		data := advNZBPayload(slug, 2*advNZBArticleBytes)
		parts := h.post(slug, name, data, advNZBArticleBytes)
		var spares []string
		for index := 0; index < 3000; index++ {
			spares = append(spares, fmt.Sprintf("%s-spare-%d@adversarial-nzb.test", slug, index))
		}
		parts[0].ids = append(spares, parts[0].ids...)
		job := h.submit(t, slug, nil, advNZBFile{name: name, groups: []string{"alt.test"}, segments: parts})
		h.awaitStatus(t, job, "COMPLETED")
		h.verify(t, slug, name, data)
		h.bystander(t, slug)
	})

	// The same three thousand spares on a post older than the only server's
	// retention. No server may serve any id, so every part spends all of its
	// ids without a request and is booked missing, and the job fails. The
	// spending is one loop over the ids, not one nested call per id. The
	// retention is restored before the bystander, whose post is as old.
	t.Run("spare-ids-beyond-retention", func(t *testing.T) {
		slug := "adv-nzb-spare-ids-retention"
		name := "spares.s01e06.mkv"
		data := advNZBPayload(slug, 2*advNZBArticleBytes)
		parts := h.post(slug, name, data, advNZBArticleBytes)
		for index := range parts {
			var spares []string
			for spare := 0; spare < 3000; spare++ {
				spares = append(spares, fmt.Sprintf("%s-spare-%d-%d@adversarial-nzb.test", slug, index+1, spare))
			}
			parts[index].ids = append(spares, parts[index].ids...)
		}
		h.setRetentionDays(t, 1)
		restored := false
		defer func() {
			if !restored {
				h.setRetentionDays(t, 0)
			}
		}()
		job := h.submit(t, slug, nil, advNZBFile{name: name, groups: []string{"alt.test"}, segments: parts})
		h.awaitStatus(t, job, "FAILED")
		if requests := h.requestsWithPrefix(slug + "-"); requests != 0 {
			t.Errorf("ids no server may serve were requested %d times", requests)
		}
		h.setRetentionDays(t, 0)
		restored = true
		h.bystander(t, slug)
	})

	// A hundred thousand distinct password metas on a post that needs none.
	// The candidates are kept as a set, so the job completes.
	t.Run("password-metas", func(t *testing.T) {
		slug := "adv-nzb-password-metas"
		name := "metas.s01e04.mkv"
		data := advNZBPayload(slug, 2*advNZBArticleBytes)
		parts := h.post(slug, name, data, advNZBArticleBytes)
		metas := make([]string, 0, 100000)
		for index := 0; index < cap(metas); index++ {
			metas = append(metas, fmt.Sprintf(`<meta type="password">adv-nzb-pw-%06d</meta>`, index))
		}
		job := h.submit(t, slug, metas, advNZBFile{name: name, groups: []string{"alt.test"}, segments: parts})
		h.awaitStatus(t, job, "COMPLETED")
		h.verify(t, slug, name, data)
		h.bystander(t, slug)
	})

	// A readable job name of about 70 KiB, past the 64 KiB bound on the
	// record of full file names, over three obfuscated siblings that share
	// one stem. Submit bounds only the NZB's size, so the name is accepted.
	// The payload is past the 10 MiB rename floor and three times its
	// siblings, so the job's name is written over the obfuscated stem with
	// no full name recorded. The job completes and every file arrives
	// byte-equal under whatever name it was given.
	t.Run("long-name", func(t *testing.T) {
		title := strings.Repeat("Silver Horizon ", (70<<10)/15) + "S01E07"
		stem := "0123456789abcdef0123456789abcdef"
		var files []advNZBFile
		var payloads [][]byte
		for index, extension := range []string{".mkv", ".srt", ".nfo"} {
			size := advNZBArticleBytes
			if extension == ".mkv" {
				size = 10<<20 + 1
			}
			name := stem + extension
			data := advNZBPayload(fmt.Sprintf("long-name-%d", index), size)
			payloads = append(payloads, data)
			files = append(files, advNZBFile{name: name, groups: []string{"alt.test"}, segments: h.post(fmt.Sprintf("adv-nzb-long-name-%d", index), name, data, advNZBArticleBytes)})
		}
		job := h.submit(t, title, nil, files...)
		h.awaitStatus(t, job, "COMPLETED")
		for index, data := range payloads {
			t.Logf("%s was delivered as a name of %d bytes", files[index].name, len(h.delivered(t, data)))
		}
		h.bystander(t, "adv-nzb-long-name")
	})

	// A post of four hundred small articles whose every file names one group
	// a megabyte long, with one article missing and a recovery set beside it.
	// Retried and probed articles share the file's one group list, so the job
	// repairs and completes byte-equal.
	t.Run("megabyte-group", func(t *testing.T) {
		slug := "adv-nzb-megabyte-group"
		name := "group.s01e05.mkv"
		const segment = 16 << 10
		data := advNZBPayload(slug, 400*segment)
		posted := map[string][]byte{name: data}
		dir := filepath.Join(root, "fixtures", slug)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		unpackParity(t, dir, posted)
		group := "alt." + strings.Repeat("g", 1<<20)
		var files []advNZBFile
		for _, member := range unpackSortedNames(posted) {
			parts := h.post(slug+"-"+member, member, posted[member], segment)
			files = append(files, advNZBFile{name: member, groups: []string{group}, segments: parts})
		}
		missing := files[advNZBIndexOf(files, name)].segments[200].ids[0]
		h.withArticle(missing, func(article *unpackArticle) { article.missing = true })
		job := h.submit(t, slug, nil, files...)
		h.awaitStatus(t, job, "COMPLETED")
		h.verify(t, slug, name, data)
		h.bystander(t, slug)
	})
}

// advNZBArticleBytes is the article size the cases post at unless they name one.
const advNZBArticleBytes = 64 << 10

type advNZBHarness struct {
	nntp       *unpackNNTP
	api        unpackAPI
	root       string
	logPath    string
	bystanders int
}

// advNZBSegment is one <segment> element per id, all under one number: the
// first id is the part, the rest its alternates. Ids are XML text already.
type advNZBSegment struct {
	number int
	bytes  int
	ids    []string
}

// advNZBFile is one <file>. The groups are XML text already.
type advNZBFile struct {
	name     string
	groups   []string
	segments []advNZBSegment
}

// advNZBPayload is size bytes unique to the seed.
func advNZBPayload(seed string, size int) []byte {
	data := make([]byte, size)
	_, _ = rand.NewChaCha8(sha256.Sum256([]byte(seed))).Read(data)
	return data
}

// post files data in the fixture as yEnc articles of at most segment bytes
// and returns their segments.
func (h *advNZBHarness) post(slug, name string, data []byte, segment int) []advNZBSegment {
	count := (len(data) + segment - 1) / segment
	whole := crc32.ChecksumIEEE(data)
	parts := make([]advNZBSegment, 0, count)
	h.nntp.mu.Lock()
	defer h.nntp.mu.Unlock()
	for index := 0; index < count; index++ {
		start, end := index*segment, min((index+1)*segment, len(data))
		id := fmt.Sprintf("%s-%d@adversarial-nzb.test", slug, index+1)
		body := unpackYenc(name, data[start:end], index+1, count, start+1, len(data), whole)
		h.nntp.articles[id] = unpackArticle{body: body}
		parts = append(parts, advNZBSegment{number: index + 1, bytes: len(body), ids: []string{id}})
	}
	return parts
}

func (h *advNZBHarness) withArticle(id string, change func(*unpackArticle)) {
	h.nntp.mu.Lock()
	defer h.nntp.mu.Unlock()
	article := h.nntp.articles[id]
	change(&article)
	h.nntp.articles[id] = article
}

func (h *advNZBHarness) requests(id string) int {
	h.nntp.mu.Lock()
	defer h.nntp.mu.Unlock()
	return h.nntp.requests[id]
}

func (h *advNZBHarness) requestsWithPrefix(prefix string) int {
	h.nntp.mu.Lock()
	defer h.nntp.mu.Unlock()
	total := 0
	for id, count := range h.nntp.requests {
		if strings.HasPrefix(id, prefix) {
			total += count
		}
	}
	return total
}

// renderAdversarialNZB writes the NZB as given: metas, groups and ids are
// not escaped again.
func renderAdversarialNZB(metas []string, files []advNZBFile) []byte {
	var nzb bytes.Buffer
	nzb.WriteString(`<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">`)
	if len(metas) > 0 {
		nzb.WriteString("<head>")
		for _, meta := range metas {
			nzb.WriteString(meta)
		}
		nzb.WriteString("</head>")
	}
	for _, file := range files {
		total := 0
		for _, segment := range file.segments {
			total = max(total, segment.number)
		}
		fmt.Fprintf(&nzb, `<file poster="fixture" date="1" subject="%s"><groups>`, xmlUnpackText(fmt.Sprintf(`"%s" yEnc (1/%d)`, file.name, total)))
		for _, group := range file.groups {
			fmt.Fprintf(&nzb, "<group>%s</group>", group)
		}
		nzb.WriteString("</groups><segments>")
		for _, segment := range file.segments {
			for _, id := range segment.ids {
				fmt.Fprintf(&nzb, `<segment bytes="%d" number="%d">%s</segment>`, segment.bytes, segment.number, id)
			}
		}
		nzb.WriteString("</segments></file>")
	}
	nzb.WriteString("</nzb>")
	return nzb.Bytes()
}

func (h *advNZBHarness) submit(t *testing.T, slug string, metas []string, files ...advNZBFile) int {
	t.Helper()
	job, err := h.api.submit(renderAdversarialNZB(metas, files), slug)
	if err != nil {
		t.Fatal(err)
	}
	h.api.cancelOnFailure(t, job)
	return job
}

// awaitTerminal polls the job until it is COMPLETED or FAILED. It has no
// deadline of its own: a job that never settles is the hang these cases
// exist to catch, and the test runner's bound ends it.
func (h *advNZBHarness) awaitTerminal(t *testing.T, job int) string {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := h.api.status(job)
		if status == "COMPLETED" || status == "FAILED" {
			return status
		}
		<-ticker.C
	}
}

func (h *advNZBHarness) awaitStatus(t *testing.T, job int, want string) {
	t.Helper()
	if status := h.awaitTerminal(t, job); status != want {
		t.Fatalf("job %d settled %s, want %s; log=%s", job, status, want, h.logPath)
	}
}

func (h *advNZBHarness) verify(t *testing.T, slug, name string, data []byte) {
	t.Helper()
	actual, err := os.ReadFile(filepath.Join(h.root, "complete", slug, name))
	if err != nil {
		t.Fatal(err)
	}
	if want, got := blake3.Sum256(data), blake3.Sum256(actual); len(actual) != len(data) || got != want {
		t.Fatalf("%s: bytes=%d/%d BLAKE3=%x/%x", name, len(actual), len(data), got, want)
	}
}

// bystander posts a clean job after a case and holds it to byte equality.
func (h *advNZBHarness) bystander(t *testing.T, after string) {
	t.Helper()
	h.bystanders++
	slug := fmt.Sprintf("adv-nzb-bystander-%d", h.bystanders)
	name := "bystander.s01e09.mkv"
	data := advNZBPayload(slug, 3*advNZBArticleBytes)
	job := h.submit(t, slug, nil, advNZBFile{name: name, groups: []string{"alt.test"}, segments: h.post(slug, name, data, advNZBArticleBytes)})
	if status := h.awaitTerminal(t, job); status != "COMPLETED" {
		t.Fatalf("the bystander after %s settled %s; log=%s", after, status, h.logPath)
	}
	h.verify(t, slug, name, data)
}

// setRetentionDays rewrites the one fixture server with the given retention.
func (h *advNZBHarness) setRetentionDays(t *testing.T, days int) {
	t.Helper()
	input := map[string]any{
		"host": "127.0.0.1", "port": h.nntp.listener.Addr().(*net.TCPAddr).Port, "tls": false,
		"connections": 8, "active": true, "priority": 0, "backfill": false, "retentionDays": days,
	}
	if err := h.api.query("mutation($input:ServerInput!) {updateServer(id:1,input:$input) {id}}", map[string]any{"input": input}, nil); err != nil {
		t.Fatal(err)
	}
}

// delivered finds the file under the completed directory whose bytes are
// data and returns its name.
func (h *advNZBHarness) delivered(t *testing.T, data []byte) string {
	t.Helper()
	want := blake3.Sum256(data)
	found := ""
	err := filepath.WalkDir(filepath.Join(h.root, "complete"), func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || found != "" {
			return err
		}
		info, err := entry.Info()
		if err != nil || info.Size() != int64(len(data)) {
			return err
		}
		actual, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if blake3.Sum256(actual) == want {
			found = entry.Name()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found == "" {
		t.Fatalf("no delivered file holds the %d posted bytes", len(data))
	}
	return found
}

func advNZBIndexOf(files []advNZBFile, name string) int {
	for index, file := range files {
		if file.name == name {
			return index
		}
	}
	return -1
}
