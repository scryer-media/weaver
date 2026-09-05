// Package seedimage bakes an already-seeded NNTP article store into a local
// Docker image so repeated benchmark runs stop reposting the same corpus.
//
// A cache hit must mean the server holds exactly the articles the fixtures on
// disk describe. The fingerprint is therefore computed from the fixture
// manifests and every seed parameter that changes an article's identity or
// size, plus the identity of the NNTP server image itself. Anything the
// fingerprint does not cover must not be allowed to change what a restored
// image serves.
package seedimage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FingerprintFormat versions the fingerprint input encoding. Change it
// whenever the set of inputs or their framing changes, so images built by an
// older harness can never be mistaken for a hit.
const FingerprintFormat = "nntp-bench-seed-image-v1"

const (
	// Repository is the local-only image repository. Nothing here is pushed.
	Repository = "weaver-nntp-bench"
	// FixtureRoot is where a captured image keeps the generated NZBs, so a
	// restore does not need the machine that produced them.
	FixtureRoot = "/bench-seed-fixtures"
	// ArticleDir mirrors NNTP_DATA_DIR in the server compose file.
	ArticleDir = "/data/articles"
)

// Image labels. They make an image self-describing, so status and restore can
// tell a stale cache from a matching one without a side-car file.
const (
	FormatLabel      = "org.scryer-media.weaver.bench.fingerprint-format"
	FingerprintLabel = "org.scryer-media.weaver.bench.corpus-fingerprint"
	ManifestLabel    = "org.scryer-media.weaver.bench.corpus-manifest-sha256"
	RunIDLabel       = "org.scryer-media.weaver.bench.seed-run-id"
	CreatedLabel     = "org.scryer-media.weaver.bench.generated-at"
)

// Corpus is the complete set of inputs a cache hit depends on.
type Corpus struct {
	// FixturesRoot holds one directory per fixture id.
	FixturesRoot string
	// FixtureIDs are the fixtures that were, or will be, seeded.
	FixtureIDs []string
	// RunID is the seed run identifier. It is an explicit input because it
	// appears in every message id, so two runs with different ids produce
	// different articles from identical fixtures.
	RunID string
	// SegmentBytes is the raw article size handed to the poster.
	SegmentBytes int
	// Group is the newsgroup the corpus was posted to.
	Group string
	// MessageIDTemplate is the scheme the poster expanded per article.
	MessageIDTemplate string
	// BaseImage is the NNTP server image tag the articles will be baked into.
	BaseImage string
	// BaseImageID is that image's local content identifier. Two servers with
	// the same tag but different contents must not share a cache entry.
	BaseImageID string
}

// Fingerprint is the resolved identity of one seeded corpus.
type Fingerprint struct {
	Format string `json:"format"`
	// Value is the full hex fingerprint.
	Value string `json:"value"`
	// Short is the 12-character prefix used in the image tag.
	Short string `json:"short"`
	// Tag is the local image tag this corpus belongs in.
	Tag string `json:"tag"`
	// CorpusManifestSHA256 digests just the fixture manifests, so a mismatch
	// can be attributed to the corpus rather than to a seed parameter.
	CorpusManifestSHA256 string `json:"corpus_manifest_sha256"`
	// FixtureIDs is the sorted, de-duplicated fixture list that was hashed.
	FixtureIDs []string `json:"fixture_ids"`
}

func (c Corpus) validate() error {
	if strings.TrimSpace(c.FixturesRoot) == "" {
		return fmt.Errorf("fixtures root is required")
	}
	if len(c.FixtureIDs) == 0 {
		return fmt.Errorf("at least one fixture id is required")
	}
	if strings.TrimSpace(c.RunID) == "" {
		return fmt.Errorf("seed run id is required: it appears in every message id")
	}
	if c.SegmentBytes < 1024 {
		return fmt.Errorf("segment bytes must be at least 1024")
	}
	if strings.TrimSpace(c.Group) == "" {
		return fmt.Errorf("newsgroup is required")
	}
	if strings.TrimSpace(c.MessageIDTemplate) == "" {
		return fmt.Errorf("message id template is required")
	}
	if strings.TrimSpace(c.BaseImage) == "" {
		return fmt.Errorf("NNTP server base image is required")
	}
	if strings.TrimSpace(c.BaseImageID) == "" {
		return fmt.Errorf("NNTP server base image id is required: an untracked server image would make a cache hit meaningless")
	}
	return nil
}

func normalizedFixtureIDs(ids []string) []string {
	unique := make(map[string]bool, len(ids))
	ordered := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || unique[id] {
			continue
		}
		unique[id] = true
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	return ordered
}

// ManifestPath is the fixture manifest a fingerprint reads for one fixture.
func ManifestPath(fixturesRoot, fixtureID string) string {
	return filepath.Join(fixturesRoot, fixtureID, "fixture-manifest.json")
}

// ImageNZBPath is where a captured image keeps one fixture's NZB.
func ImageNZBPath(fixtureID string) string {
	return FixtureRoot + "/" + fixtureID + "/" + fixtureID + ".nzb"
}

// LocalNZBPath is where the harness expects that same NZB on disk. Capture
// copies from here and restore copies back to here, so the two directions
// share one definition.
func LocalNZBPath(fixturesRoot, fixtureID string) string {
	return filepath.Join(fixturesRoot, fixtureID, fixtureID+".nzb")
}

// Tag names the local image for a fingerprint.
func Tag(short string) string {
	return Repository + ":corpus-" + short
}

// Compute reads the fixture manifests and derives the corpus fingerprint. It
// deliberately hashes the manifests rather than the payload bytes: the
// manifest already carries a BLAKE3 digest of every archive file, so this stays
// fast on a multi-gigabyte corpus while remaining sensitive to any change.
func Compute(corpus Corpus) (Fingerprint, error) {
	if err := corpus.validate(); err != nil {
		return Fingerprint{}, err
	}
	ids := normalizedFixtureIDs(corpus.FixtureIDs)
	if len(ids) == 0 {
		return Fingerprint{}, fmt.Errorf("at least one fixture id is required")
	}
	corpusHash := sha256.New()
	hash := sha256.New()
	writeField(hash, "format", []byte(FingerprintFormat))
	writeField(hash, "run-id", []byte(corpus.RunID))
	writeField(hash, "segment-bytes", []byte(fmt.Sprintf("%d", corpus.SegmentBytes)))
	writeField(hash, "group", []byte(corpus.Group))
	writeField(hash, "message-id-template", []byte(corpus.MessageIDTemplate))
	writeField(hash, "nntp-image", []byte(corpus.BaseImage))
	writeField(hash, "nntp-image-id", []byte(corpus.BaseImageID))
	for _, id := range ids {
		path := ManifestPath(corpus.FixturesRoot, id)
		contents, err := os.ReadFile(path)
		if err != nil {
			return Fingerprint{}, fmt.Errorf("read fixture manifest for %s: %w", id, err)
		}
		writeField(corpusHash, id, contents)
		writeField(hash, "fixture:"+id, contents)
	}
	value := hex.EncodeToString(hash.Sum(nil))
	return Fingerprint{
		Format:               FingerprintFormat,
		Value:                value,
		Short:                value[:12],
		Tag:                  Tag(value[:12]),
		CorpusManifestSHA256: hex.EncodeToString(corpusHash.Sum(nil)),
		FixtureIDs:           ids,
	}, nil
}

// writeField frames every input with its label and length, so no two distinct
// input sets can produce the same byte stream.
func writeField(writer io.Writer, label string, contents []byte) {
	_, _ = fmt.Fprintf(writer, "%s\x00%d\x00", label, len(contents))
	_, _ = writer.Write(contents)
	_, _ = writer.Write([]byte{0})
}
