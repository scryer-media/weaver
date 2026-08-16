package corpus

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// corpusServer is the published side: a manifest at its own digest and one
// object per fixture digest, with every request counted.
type corpusServer struct {
	*httptest.Server
	mutex    sync.Mutex
	objects  map[string][]byte
	manifest []byte
	digest   string
	requests map[string]int
}

func newCorpusServer(t *testing.T, manifest []byte, objects map[string][]byte) *corpusServer {
	t.Helper()
	server := &corpusServer{
		objects:  objects,
		manifest: manifest,
		digest:   DigestBytes(manifest),
		requests: map[string]int{},
	}
	server.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		path := strings.TrimPrefix(request.URL.Path, "/")
		server.mutex.Lock()
		server.requests[path]++
		server.mutex.Unlock()
		switch {
		case path == ManifestsPrefix+server.digest+".json":
			_, _ = writer.Write(server.manifest)
		case strings.HasPrefix(path, ObjectsPrefix):
			contents, ok := server.objects[strings.TrimPrefix(path, ObjectsPrefix)]
			if !ok {
				http.Error(writer, "no such object", http.StatusNotFound)
				return
			}
			_, _ = writer.Write(contents)
		default:
			http.Error(writer, "no such key", http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func (server *corpusServer) count(path string) int {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return server.requests[path]
}

// publishedCorpus builds a two-file corpus and the server that serves it.
func publishedCorpus(t *testing.T) (*corpusServer, Lock, map[string]string) {
	t.Helper()
	contents := map[string]string{
		"testdata/one/a.rar":       "the first fixture",
		"testdata/shared/clip.mkv": "the shared clip",
		"testdata/two/b.par2":      "not in the profile",
	}
	root := writeTree(t, nil)
	ledger := newLedger(
		entry("testdata/one/a.rar", contents["testdata/one/a.rar"], generatedSource()),
		entry("testdata/shared/clip.mkv", contents["testdata/shared/clip.mkv"], blockedSource("generator pending")),
		entry("testdata/two/b.par2", contents["testdata/two/b.par2"], blockedSource("generator pending")),
	)
	profiles := newProfiles(map[string][]string{
		"one": {"testdata/one/**", "testdata/shared/**"},
		"all": {"testdata/**"},
	})
	manifest, err := BuildManifest(ledger, profiles, loadToolchains(t, root))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := manifest.Encode()
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string][]byte{}
	for path, body := range contents {
		objects[DigestBytes([]byte(body))] = []byte(body)
		_ = path
	}
	server := newCorpusServer(t, encoded, objects)
	lock := LockEntry(DigestBytes(encoded), provenanceDigest, server.URL, testCommit, testRun)
	if err := lock.Validate(); err != nil {
		t.Fatal(err)
	}
	return server, lock, contents
}

func TestFetchWritesOnlyVerifiedBytesAndSkipsWhatIsPresent(t *testing.T) {
	server, lock, contents := publishedCorpus(t)
	root := t.TempDir()
	if err := Fetch(context.Background(), root, &lock, []string{"one"}, FetchOptions{Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"testdata/one/a.rar", "testdata/shared/clip.mkv"} {
		got, err := os.ReadFile(HostPath(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != contents[path] {
			t.Fatalf("%s = %q, want %q", path, got, contents[path])
		}
	}
	// The profile is honoured: a file outside it is never fetched.
	if _, err := os.Stat(HostPath(root, "testdata/two/b.par2")); !os.IsNotExist(err) {
		t.Fatal("fetch pulled a file the profile does not name")
	}
	key := ObjectsPrefix + DigestBytes([]byte(contents["testdata/one/a.rar"]))
	if got := server.count(key); got != 1 {
		t.Fatalf("object requested %d times, want 1", got)
	}
	// A second hydration re-verifies from disk and downloads nothing.
	if err := Fetch(context.Background(), root, &lock, []string{"one"}, FetchOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := server.count(key); got != 1 {
		t.Fatalf("object requested %d times after a repeat hydration, want 1", got)
	}
}

func TestFetchRefusesATamperedObject(t *testing.T) {
	server, lock, contents := publishedCorpus(t)
	digest := DigestBytes([]byte(contents["testdata/one/a.rar"]))
	server.objects[digest] = []byte("bytes that are not what the manifest describes")
	root := t.TempDir()
	err := Fetch(context.Background(), root, &lock, []string{"one"}, FetchOptions{Concurrency: 1})
	if err == nil {
		t.Fatal("a digest mismatch must fail the hydration")
	}
	if !strings.Contains(err.Error(), "blake3") {
		t.Fatalf("error should name the digest mismatch: %v", err)
	}
	if _, err := os.Stat(HostPath(root, "testdata/one/a.rar")); !os.IsNotExist(err) {
		t.Fatal("an unverified byte must never reach a fixture path")
	}
	leftovers, _ := filepath.Glob(filepath.Join(root, "testdata", "one", ".*"))
	if len(leftovers) != 0 {
		t.Fatalf("partial download left behind: %v", leftovers)
	}
}

func TestFetchRefusesATamperedManifest(t *testing.T) {
	server, lock, _ := publishedCorpus(t)
	server.manifest = append(server.manifest, ' ')
	err := Fetch(context.Background(), t.TempDir(), &lock, []string{"one"}, FetchOptions{})
	if err == nil || !strings.Contains(err.Error(), "pins") {
		t.Fatalf("a manifest that is not the pinned one must be refused, got %v", err)
	}
}

func TestFetchTreatsAMissingObjectAsAHardError(t *testing.T) {
	server, lock, contents := publishedCorpus(t)
	digest := DigestBytes([]byte(contents["testdata/one/a.rar"]))
	delete(server.objects, digest)
	err := Fetch(context.Background(), t.TempDir(), &lock, []string{"one"}, FetchOptions{Concurrency: 1})
	if err == nil || !strings.Contains(err.Error(), "testdata/one/a.rar") {
		t.Fatalf("a 404 must fail and name the fixture, got %v", err)
	}
	if got := server.count(ObjectsPrefix + digest); got != 1 {
		t.Fatalf("a 404 was retried %d times; it must not be retried", got)
	}
}

func TestFetchRefusesAnUnpinnedLockAndAnUnknownProfile(t *testing.T) {
	_, lock, _ := publishedCorpus(t)
	unpinned := Lock{
		SchemaVersion: SchemaVersion,
		Signature: LockSignature{
			CertificateIdentity:   PublishWorkflowIdentity,
			CertificateOIDCIssuer: GitHubOIDCIssuer,
		},
	}
	if err := Fetch(context.Background(), t.TempDir(), &unpinned, []string{"one"}, FetchOptions{}); !errors.Is(err, ErrNotPinned) {
		t.Fatalf("an unpinned lock must return ErrNotPinned, got %v", err)
	}
	if err := Fetch(context.Background(), t.TempDir(), &lock, nil, FetchOptions{}); err == nil {
		t.Fatal("fetch with no profile must be an error")
	}
	err := Fetch(context.Background(), t.TempDir(), &lock, []string{"nope"}, FetchOptions{})
	if err == nil || !strings.Contains(err.Error(), "not in the published manifest") {
		t.Fatalf("an unknown profile must be an error, got %v", err)
	}
}

func TestFetchRetriesA500(t *testing.T) {
	contents := "retried fixture"
	digest := DigestBytes([]byte(contents))
	var attempts int
	var mutex sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.Contains(request.URL.Path, ObjectsPrefix) {
			http.Error(writer, "unexpected", http.StatusNotFound)
			return
		}
		mutex.Lock()
		attempts++
		attempt := attempts
		mutex.Unlock()
		if attempt == 1 {
			http.Error(writer, "try again", http.StatusServiceUnavailable)
			return
		}
		_, _ = writer.Write([]byte(contents))
	}))
	defer server.Close()
	lock := LockEntry(manifestDigest, provenanceDigest, server.URL, testCommit, testRun)
	root := t.TempDir()
	file := ManifestFile{Path: "testdata/one/a.rar", Size: int64(len(contents)), BLAKE3: digest}
	if _, err := fetchOne(context.Background(), server.Client(), &lock, root, file); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("%d attempts, want 2 (one 503, one success)", attempts)
	}
}
