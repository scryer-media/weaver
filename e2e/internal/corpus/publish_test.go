package corpus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// publishFixture builds a publishable corpus: a ledger with no blocked entry,
// the matching tree bytes, a build directory, and a bucket that serves back
// whatever the fake runner accepted.
type publishFixture struct {
	root     string
	buildDir string
	server   *httptest.Server
	bucket   map[string][]byte
	mutex    sync.Mutex
	puts     []string
	status   map[string]int
}

func newPublishFixture(t *testing.T, blocked bool) *publishFixture {
	t.Helper()
	fixture := &publishFixture{bucket: map[string][]byte{}, status: map[string]int{}}
	source := generatedSource()
	if blocked {
		source = blockedSource("generator pending: par2 set")
	}
	contents := map[string]string{
		"testdata/one/a.rar":  "the first fixture",
		"testdata/two/b.par2": "the second fixture",
	}
	fixture.root = writeTree(t, contents)
	ledger := newLedger(
		entry("testdata/one/a.rar", contents["testdata/one/a.rar"], generatedSource()),
		entry("testdata/two/b.par2", contents["testdata/two/b.par2"], source),
	)
	if err := ledger.Save(fixture.root); err != nil {
		t.Fatal(err)
	}
	profiles := newProfiles(map[string][]string{"all": {"testdata/**"}})
	writeJSON(t, filepath.Join(fixture.root, ProfilesFile), profiles)

	manifest, err := BuildManifest(ledger, profiles, loadToolchains(t, fixture.root))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := manifest.Encode()
	if err != nil {
		t.Fatal(err)
	}
	digest := DigestBytes(encoded)
	fixture.buildDir = t.TempDir()
	provenance := Provenance{
		SchemaVersion:  SchemaVersion,
		ManifestBLAKE3: digest,
		SourceCommit:   testCommit,
		WorkflowRun:    testRun,
	}
	provenanceBytes, err := json.MarshalIndent(provenance, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, body []byte) {
		if err := os.WriteFile(filepath.Join(fixture.buildDir, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(BuildManifestFile, encoded)
	write(BuildManifestDigestFile, []byte(digest+"\n"))
	write(BuildProvenanceFile, provenanceBytes)
	write(BuildManifestBundleFile, []byte(`{"bundle":"manifest"}`))
	write(BuildProvenanceBundleFile, []byte(`{"bundle":"provenance"}`))

	// One server plays both roles: the S3 endpoint (signed, conditional PUTs
	// under /<bucket>/) and the public read side (plain GETs under /).
	fixture.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPut {
			fixture.handlePut(writer, request)
			return
		}
		key := strings.TrimPrefix(request.URL.Path, "/")
		fixture.mutex.Lock()
		body, ok := fixture.bucket[key]
		fixture.mutex.Unlock()
		if !ok {
			http.Error(writer, "no such key", http.StatusNotFound)
			return
		}
		_, _ = writer.Write(body)
	}))
	t.Cleanup(fixture.server.Close)
	return fixture
}

// handlePut is the fake S3 endpoint: it insists on a SigV4-signed, create-only,
// unsigned-payload PUT under the bucket prefix, records the key, honours a
// scripted status, and otherwise stores the body in the fake bucket.
func (fixture *publishFixture) handlePut(writer http.ResponseWriter, request *http.Request) {
	authorization := request.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, "AWS4-HMAC-SHA256 Credential=AKID/") || !strings.Contains(authorization, "/auto/s3/aws4_request") || !strings.Contains(authorization, "Signature=") {
		http.Error(writer, "expected a SigV4 signature for AKID in region auto, service s3", http.StatusForbidden)
		return
	}
	if strings.Contains(authorization, "SECRET") || strings.Contains(request.URL.String(), "SECRET") {
		http.Error(writer, "the secret must never leave the signer", http.StatusForbidden)
		return
	}
	if request.Header.Get("If-None-Match") != "*" {
		http.Error(writer, "publication must be create-only", http.StatusBadRequest)
		return
	}
	if request.Header.Get("X-Amz-Content-Sha256") != unsignedPayload || request.Header.Get("X-Amz-Date") == "" {
		http.Error(writer, "missing SigV4 payload/date headers", http.StatusBadRequest)
		return
	}
	if request.Header.Get("Content-Type") != octetStream {
		http.Error(writer, "objects are stored as octet-stream", http.StatusBadRequest)
		return
	}
	key, ok := strings.CutPrefix(request.URL.Path, "/corpus/")
	if !ok {
		http.Error(writer, "PUT outside the bucket", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	if int64(len(body)) != request.ContentLength {
		http.Error(writer, "Content-Length must be declared", http.StatusBadRequest)
		return
	}
	fixture.mutex.Lock()
	defer fixture.mutex.Unlock()
	fixture.puts = append(fixture.puts, key)
	if status, ok := fixture.status[key]; ok {
		http.Error(writer, "scripted", status)
		return
	}
	fixture.bucket[key] = body
	writer.WriteHeader(http.StatusOK)
}

func (fixture *publishFixture) publisher() *Publisher {
	return &Publisher{
		Endpoint:        fixture.server.URL,
		Bucket:          "corpus",
		BaseURL:         fixture.server.URL,
		AccessKeyID:     "AKID",
		SecretAccessKey: "SECRET",
		Client:          fixture.server.Client(),
		Concurrency:     2,
	}
}

func TestPublishUploadsObjectsManifestAndProvenanceThenReadsBack(t *testing.T) {
	fixture := newPublishFixture(t, false)
	lock, err := fixture.publisher().Publish(context.Background(), fixture.root, fixture.buildDir, PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Validate(); err != nil {
		t.Fatalf("the printed lock entry must be valid: %v", err)
	}
	if lock.PublishedFrom.Commit != testCommit || lock.PublishedFrom.Run != testRun {
		t.Fatalf("the lock entry must carry the provenance's commit and run: %+v", lock.PublishedFrom)
	}
	fixture.mutex.Lock()
	keys := append([]string(nil), fixture.puts...)
	fixture.mutex.Unlock()
	manifestDigestValue := lock.Manifest.BLAKE3
	for _, want := range []string{
		ObjectKey(DigestBytes([]byte("the first fixture"))),
		ObjectKey(DigestBytes([]byte("the second fixture"))),
		ManifestKey(manifestDigestValue),
		ManifestBundleKey(manifestDigestValue),
		ProvenanceKey(manifestDigestValue),
		ProvenanceBundleKey(manifestDigestValue),
	} {
		if !contains(keys, want) {
			t.Errorf("%s was never uploaded (uploaded: %v)", want, keys)
		}
	}
}

// A key that already exists is read back from the public side and must match;
// content-addressed keys are never rewritten.
func TestPublishTreatsA412AsAlreadyPublishedAfterAReadBack(t *testing.T) {
	fixture := newPublishFixture(t, false)
	key := ObjectKey(DigestBytes([]byte("the first fixture")))
	fixture.status[key] = http.StatusPreconditionFailed
	fixture.bucket[key] = []byte("the first fixture")
	if _, err := fixture.publisher().Publish(context.Background(), fixture.root, fixture.buildDir, PublishOptions{}); err != nil {
		t.Fatal(err)
	}

	// The same 412 with different bytes on the far end aborts the publication
	// rather than overwriting an immutable key.
	other := newPublishFixture(t, false)
	other.status[key] = http.StatusPreconditionFailed
	other.bucket[key] = []byte("something else entirely")
	_, err := other.publisher().Publish(context.Background(), other.root, other.buildDir, PublishOptions{})
	if err == nil || !strings.Contains(err.Error(), "already exists with blake3") {
		t.Fatalf("a mismatched existing key must abort the publication, got %v", err)
	}
}

func TestPublishRefusesBlockedEntries(t *testing.T) {
	fixture := newPublishFixture(t, true)
	_, err := fixture.publisher().Publish(context.Background(), fixture.root, fixture.buildDir, PublishOptions{})
	if !errors.Is(err, ErrBlockedEntries) {
		t.Fatalf("a ledger with blocked entries must refuse publication, got %v", err)
	}
	fixture.mutex.Lock()
	defer fixture.mutex.Unlock()
	if len(fixture.puts) != 0 {
		t.Fatalf("nothing may be uploaded when publication is refused, got %v", fixture.puts)
	}
}

func TestPublishRefusesABuildDirectoryThatDoesNotHashToItsOwnDigest(t *testing.T) {
	fixture := newPublishFixture(t, false)
	path := filepath.Join(fixture.buildDir, BuildManifestDigestFile)
	if err := os.WriteFile(path, []byte(zeroDigest+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := fixture.publisher().Publish(context.Background(), fixture.root, fixture.buildDir, PublishOptions{})
	if err == nil || !strings.Contains(err.Error(), "hashes to") {
		t.Fatalf("a build directory whose manifest and digest disagree must be refused, got %v", err)
	}
}

func TestPublishRefusesWhenTheTreeDoesNotMatchTheManifest(t *testing.T) {
	fixture := newPublishFixture(t, false)
	writeFixture(t, fixture.root, "testdata/one/a.rar", "edited after the build")
	_, err := fixture.publisher().Publish(context.Background(), fixture.root, fixture.buildDir, PublishOptions{})
	if err == nil || !strings.Contains(err.Error(), "does not match the manifest") {
		t.Fatalf("a tree that drifted from the manifest must be refused, got %v", err)
	}
}

func TestPublishRefusesWithoutSignatureBundles(t *testing.T) {
	fixture := newPublishFixture(t, false)
	if err := os.Remove(filepath.Join(fixture.buildDir, BuildManifestBundleFile)); err != nil {
		t.Fatal(err)
	}
	_, err := fixture.publisher().Publish(context.Background(), fixture.root, fixture.buildDir, PublishOptions{})
	if err == nil || !strings.Contains(err.Error(), "corpus sign") {
		t.Fatalf("an unsigned build directory must be refused, got %v", err)
	}
}

// A dry run reports and uploads nothing, and does not need the bundles.
func TestPublishDryRunUploadsNothing(t *testing.T) {
	fixture := newPublishFixture(t, false)
	if err := os.Remove(filepath.Join(fixture.buildDir, BuildProvenanceBundleFile)); err != nil {
		t.Fatal(err)
	}
	var lines []string
	lock, err := fixture.publisher().Publish(context.Background(), fixture.root, fixture.buildDir, PublishOptions{
		DryRun:   true,
		Progress: func(line string) { lines = append(lines, line) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !lock.Pinned() {
		t.Fatal("a dry run still reports the lock entry it would produce")
	}
	fixture.mutex.Lock()
	defer fixture.mutex.Unlock()
	if len(fixture.puts) != 0 {
		t.Fatalf("a dry run must upload nothing, got %v", fixture.puts)
	}
	if !contains(lines, "would upload "+ManifestKey(lock.Manifest.BLAKE3)) {
		t.Fatalf("a dry run should say what it would upload: %v", lines)
	}
}

func TestPublisherValidateRejectsMisconfiguration(t *testing.T) {
	base := Publisher{
		Endpoint:        "https://account.r2.cloudflarestorage.com",
		Bucket:          "corpus",
		BaseURL:         "https://corpus.example.net",
		AccessKeyID:     "AKID",
		SecretAccessKey: "SECRET",
	}
	if err := base.validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Publisher){
		"plain http endpoint": func(publisher *Publisher) { publisher.Endpoint = "http://account.example.net" },
		"no bucket":           func(publisher *Publisher) { publisher.Bucket = "" },
		"base with a path":    func(publisher *Publisher) { publisher.BaseURL = "https://corpus.example.net/" },
		"no credentials":      func(publisher *Publisher) { publisher.SecretAccessKey = "" },
	} {
		broken := base
		mutate(&broken)
		if err := broken.validate(); err == nil {
			t.Errorf("%s should be rejected", name)
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
