package seedimage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCorpusOnDisk(t *testing.T, fixtures map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for id, manifest := range fixtures {
		path := ManifestPath(root, id)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(manifest), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(LocalNZBPath(root, id), []byte("<nzb>"+id+"</nzb>"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func testCorpus(root string, ids ...string) Corpus {
	return Corpus{
		FixturesRoot:      root,
		FixtureIDs:        ids,
		RunID:             "seed-run-1",
		SegmentBytes:      750 << 10,
		Group:             "alt.binaries.test",
		MessageIDTemplate: "bench-seed-run-1-{fixture}-{0filenum}-{0part}@nntp-bench",
		BaseImage:         "e2e-nntp:local",
		BaseImageID:       "sha256:base",
	}
}

func TestFingerprintIsStableAndOrderIndependent(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{\"a\":1}", "fixture-b": "{\"b\":2}"})
	first, err := Compute(testCorpus(root, "fixture-a", "fixture-b"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Compute(testCorpus(root, "fixture-b", "fixture-a", "fixture-a"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Value != second.Value {
		t.Fatalf("fingerprint depends on argument order: %s vs %s", first.Value, second.Value)
	}
	if first.Tag != Tag(first.Value[:12]) || !strings.HasPrefix(first.Tag, Repository+":corpus-") {
		t.Fatalf("unexpected image tag %q", first.Tag)
	}
	if first.Format != FingerprintFormat {
		t.Fatalf("fingerprint format = %q", first.Format)
	}
}

func TestFingerprintCoversEverySeedInput(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{\"a\":1}"})
	base, err := Compute(testCorpus(root, "fixture-a"))
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*Corpus){
		"run id":          func(c *Corpus) { c.RunID = "seed-run-2" },
		"segment bytes":   func(c *Corpus) { c.SegmentBytes = 500 << 10 },
		"group":           func(c *Corpus) { c.Group = "alt.binaries.other" },
		"message id":      func(c *Corpus) { c.MessageIDTemplate = "other-{0part}@nntp-bench" },
		"server image":    func(c *Corpus) { c.BaseImage = "e2e-nntp:other" },
		"server image id": func(c *Corpus) { c.BaseImageID = "sha256:other" },
	}
	for label, mutate := range mutations {
		corpus := testCorpus(root, "fixture-a")
		mutate(&corpus)
		changed, err := Compute(corpus)
		if err != nil {
			t.Fatal(err)
		}
		if changed.Value == base.Value {
			t.Fatalf("changing the %s did not change the fingerprint", label)
		}
	}

	// A changed fixture manifest must also change it.
	if err := os.WriteFile(ManifestPath(root, "fixture-a"), []byte("{\"a\":2}"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := Compute(testCorpus(root, "fixture-a"))
	if err != nil {
		t.Fatal(err)
	}
	if changed.Value == base.Value {
		t.Fatal("changing a fixture manifest did not change the fingerprint")
	}
	if changed.CorpusManifestSHA256 == base.CorpusManifestSHA256 {
		t.Fatal("changing a fixture manifest did not change the corpus manifest digest")
	}
}

func TestComputeRequiresTheServerImageIdentity(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{}"})
	corpus := testCorpus(root, "fixture-a")
	corpus.BaseImageID = ""
	if _, err := Compute(corpus); err == nil {
		t.Fatal("a fingerprint without the server image id was accepted")
	}
	corpus = testCorpus(root, "fixture-a")
	corpus.RunID = ""
	if _, err := Compute(corpus); err == nil {
		t.Fatal("a fingerprint without the seed run id was accepted")
	}
}

func TestPathMappingIsSymmetric(t *testing.T) {
	if got := ImageNZBPath("fixture-a"); got != FixtureRoot+"/fixture-a/fixture-a.nzb" {
		t.Fatalf("image NZB path = %q", got)
	}
	if got := LocalNZBPath("/corpus", "fixture-a"); got != filepath.Join("/corpus", "fixture-a", "fixture-a.nzb") {
		t.Fatalf("local NZB path = %q", got)
	}
	if got := ManifestPath("/corpus", "fixture-a"); got != filepath.Join("/corpus", "fixture-a", "fixture-manifest.json") {
		t.Fatalf("manifest path = %q", got)
	}
}

// fakeDocker records what a caller asked for and serves canned answers, so
// capture, restore and status decisions are testable without a daemon.
type fakeDocker struct {
	images     map[string]string
	labels     map[string]map[string]string
	bakedNZBs  map[string]string
	container  string
	copiedFrom []string
	built      []string
	removed    []string
	buildErr   error
}

func (f *fakeDocker) ImageID(_ context.Context, ref string) (string, error) {
	return f.images[ref], nil
}

func (f *fakeDocker) ImageLabels(_ context.Context, ref string) (map[string]string, error) {
	if labels, ok := f.labels[ref]; ok {
		return labels, nil
	}
	return map[string]string{}, nil
}

func (f *fakeDocker) ContainerID(_ context.Context, _, _ string) (string, error) {
	return f.container, nil
}

func (f *fakeDocker) CopyFromContainer(_ context.Context, container, containerPath, destination string) error {
	f.copiedFrom = append(f.copiedFrom, container+":"+containerPath)
	return os.WriteFile(filepath.Join(destination, "article-1"), []byte("article"), 0o644)
}

func (f *fakeDocker) CopyFromImage(_ context.Context, _, _, destination string) error {
	for id, contents := range f.bakedNZBs {
		path := filepath.Join(destination, id, id+".nzb")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeDocker) Build(_ context.Context, contextDir, _, tag string, labels map[string]string) error {
	if f.buildErr != nil {
		return f.buildErr
	}
	f.built = append(f.built, tag)
	if f.images == nil {
		f.images = map[string]string{}
	}
	f.images[tag] = "sha256:" + tag
	if f.labels == nil {
		f.labels = map[string]map[string]string{}
	}
	copied := map[string]string{}
	for key, value := range labels {
		copied[key] = value
	}
	f.labels[tag] = copied
	if _, err := os.Stat(filepath.Join(contextDir, "Dockerfile")); err != nil {
		return err
	}
	return nil
}

func (f *fakeDocker) RemoveImage(_ context.Context, ref string) error {
	f.removed = append(f.removed, ref)
	delete(f.images, ref)
	delete(f.labels, ref)
	return nil
}

func TestCaptureThenStatusReportsAHit(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{\"a\":1}"})
	docker := &fakeDocker{images: map[string]string{"e2e-nntp:local": "sha256:base"}, container: "nntp-container"}
	options := Options{Corpus: testCorpus(root, "fixture-a"), Docker: docker, Container: "nntp-container", StageRoot: t.TempDir()}

	before, err := Inspect(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if before.Hit || before.Present || before.Reason == "" {
		t.Fatalf("an uncaptured corpus reported %#v", before)
	}

	record, err := Capture(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Preseeded || record.Image != before.Fingerprint.Tag {
		t.Fatalf("capture provenance = %#v", record)
	}
	if len(docker.built) != 1 {
		t.Fatalf("capture built %v", docker.built)
	}
	if len(docker.copiedFrom) != 1 || !strings.Contains(docker.copiedFrom[0], ArticleDir) {
		t.Fatalf("capture did not stage the article store: %v", docker.copiedFrom)
	}

	after, err := Inspect(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Hit {
		t.Fatalf("a captured corpus reported a miss: %s", after.Reason)
	}
}

func TestStatusExplainsAStaleImage(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{\"a\":1}"})
	corpus := testCorpus(root, "fixture-a")
	fingerprint, err := Compute(corpus)
	if err != nil {
		t.Fatal(err)
	}
	docker := &fakeDocker{
		images: map[string]string{"e2e-nntp:local": "sha256:base", fingerprint.Tag: "sha256:stale"},
		labels: map[string]map[string]string{fingerprint.Tag: {
			FormatLabel:      FingerprintFormat,
			FingerprintLabel: "0000",
			ManifestLabel:    "0000",
		}},
	}
	status, err := Inspect(context.Background(), Options{Corpus: corpus, Docker: docker})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Present || status.Hit {
		t.Fatalf("stale image reported %#v", status)
	}
	if !strings.Contains(status.Reason, "corpus fingerprint") {
		t.Fatalf("unhelpful miss reason: %s", status.Reason)
	}
}

func TestRestoreRefusesAFingerprintMismatch(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{\"a\":1}"})
	corpus := testCorpus(root, "fixture-a")
	fingerprint, err := Compute(corpus)
	if err != nil {
		t.Fatal(err)
	}
	docker := &fakeDocker{
		images: map[string]string{"e2e-nntp:local": "sha256:base", fingerprint.Tag: "sha256:stale"},
		labels: map[string]map[string]string{fingerprint.Tag: {
			FormatLabel:      FingerprintFormat,
			FingerprintLabel: "0000",
			ManifestLabel:    "0000",
		}},
	}
	if _, err := Restore(context.Background(), Options{Corpus: corpus, Docker: docker, StageRoot: t.TempDir()}); err == nil {
		t.Fatal("restore accepted an image whose fingerprint does not match the fixtures on disk")
	}

	missing := &fakeDocker{images: map[string]string{"e2e-nntp:local": "sha256:base"}}
	_, err = Restore(context.Background(), Options{Corpus: corpus, Docker: missing, StageRoot: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "refusing to restore") {
		t.Fatalf("restore without an image returned %v", err)
	}
}

func TestRestoreMaterializesBakedNZBs(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{\"a\":1}", "fixture-b": "{\"b\":2}"})
	corpus := testCorpus(root, "fixture-a", "fixture-b")
	fingerprint, err := Compute(corpus)
	if err != nil {
		t.Fatal(err)
	}
	// Remove one local NZB so the restore has to write it back, and leave the
	// other in place so the identical-content path is exercised too.
	if err := os.Remove(LocalNZBPath(root, "fixture-b")); err != nil {
		t.Fatal(err)
	}
	docker := &fakeDocker{
		images: map[string]string{"e2e-nntp:local": "sha256:base", fingerprint.Tag: "sha256:cached"},
		labels: map[string]map[string]string{fingerprint.Tag: {
			FormatLabel:      FingerprintFormat,
			FingerprintLabel: fingerprint.Value,
			ManifestLabel:    fingerprint.CorpusManifestSHA256,
		}},
		bakedNZBs: map[string]string{
			"fixture-a": "<nzb>fixture-a</nzb>",
			"fixture-b": "<nzb>fixture-b</nzb>",
		},
	}
	record, err := Restore(context.Background(), Options{Corpus: corpus, Docker: docker, StageRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if len(record.RestoredNZBs) != 2 || !record.Preseeded {
		t.Fatalf("restore provenance = %#v", record)
	}
	contents, err := os.ReadFile(LocalNZBPath(root, "fixture-b"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "<nzb>fixture-b</nzb>" {
		t.Fatalf("restored NZB = %q", contents)
	}
}

func TestRestoreRefusesToOverwriteADifferentLocalNZB(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{\"a\":1}"})
	corpus := testCorpus(root, "fixture-a")
	fingerprint, err := Compute(corpus)
	if err != nil {
		t.Fatal(err)
	}
	docker := &fakeDocker{
		images: map[string]string{"e2e-nntp:local": "sha256:base", fingerprint.Tag: "sha256:cached"},
		labels: map[string]map[string]string{fingerprint.Tag: {
			FormatLabel:      FingerprintFormat,
			FingerprintLabel: fingerprint.Value,
			ManifestLabel:    fingerprint.CorpusManifestSHA256,
		}},
		bakedNZBs: map[string]string{"fixture-a": "<nzb>from a different seed run</nzb>"},
	}
	_, err = Restore(context.Background(), Options{Corpus: corpus, Docker: docker, StageRoot: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("restore over a diverging local NZB returned %v", err)
	}
}

func TestLiveProvenanceRecordsAnUncachedSeed(t *testing.T) {
	root := writeCorpusOnDisk(t, map[string]string{"fixture-a": "{\"a\":1}"})
	record, err := LiveProvenance(testCorpus(root, "fixture-a"))
	if err != nil {
		t.Fatal(err)
	}
	if record.Preseeded || record.Image != "" || record.Action != "seed" {
		t.Fatalf("live provenance = %#v", record)
	}
	if record.CorpusFingerprint == "" || record.SeedRunID != "seed-run-1" {
		t.Fatalf("live provenance lost its inputs: %#v", record)
	}
}
