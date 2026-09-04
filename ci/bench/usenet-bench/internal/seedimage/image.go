package seedimage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ProvenanceSchemaVersion versions the JSON this package writes into a run's
// artifact directory. It records how the server got its articles, which is a
// property of the corpus and the NNTP server, not of any client under test.
const ProvenanceSchemaVersion = 1

// Docker is the small surface this package needs. It is an interface so the
// capture, restore and status decisions are testable without a daemon.
type Docker interface {
	// ImageID returns the local content identifier of a tag, or "" when the
	// image is not present.
	ImageID(ctx context.Context, ref string) (string, error)
	// ImageLabels returns the labels of a local image.
	ImageLabels(ctx context.Context, ref string) (map[string]string, error)
	// ContainerID resolves a running Compose service to its container.
	ContainerID(ctx context.Context, project, service string) (string, error)
	// CopyFromContainer copies containerPath (a directory's contents) to a
	// local directory.
	CopyFromContainer(ctx context.Context, container, containerPath, destination string) error
	// CopyFromImage copies imagePath out of an image without running it.
	CopyFromImage(ctx context.Context, image, imagePath, destination string) error
	// Build builds contextDir into tag with the given labels.
	Build(ctx context.Context, contextDir, baseImage, tag string, labels map[string]string) error
	// RemoveImage deletes a local image.
	RemoveImage(ctx context.Context, ref string) error
}

// Options are shared by every seed-image action.
type Options struct {
	Corpus Corpus
	Docker Docker
	// Container is the already-seeded NNTP container to capture from. When it
	// is empty, ComposeProject and ComposeService resolve one.
	Container      string
	ComposeProject string
	ComposeService string
	// StageRoot is where the temporary Docker build context is assembled.
	StageRoot string
}

// Status answers whether a run can start from a pre-seeded image.
type Status struct {
	SchemaVersion int         `json:"schema_version"`
	Fingerprint   Fingerprint `json:"fingerprint"`
	// Present reports whether an image with this corpus's tag exists locally.
	Present bool `json:"image_present"`
	// Hit reports whether that image may be used as-is.
	Hit bool `json:"cache_hit"`
	// Reason always explains the verdict, hit or miss.
	Reason string            `json:"reason"`
	Labels map[string]string `json:"image_labels,omitempty"`
}

// Provenance records, for a run artifact, how the NNTP server was populated.
type Provenance struct {
	SchemaVersion int    `json:"schema_version"`
	Action        string `json:"action"`
	// Preseeded is false when the corpus was posted live for this run.
	Preseeded            bool      `json:"preseeded"`
	Image                string    `json:"image,omitempty"`
	FingerprintFormat    string    `json:"fingerprint_format"`
	CorpusFingerprint    string    `json:"corpus_fingerprint"`
	CorpusManifestSHA256 string    `json:"corpus_manifest_sha256"`
	SeedRunID            string    `json:"seed_run_id"`
	SegmentBytes         int       `json:"segment_bytes"`
	Group                string    `json:"group"`
	MessageIDTemplate    string    `json:"message_id_template"`
	BaseImage            string    `json:"nntp_base_image"`
	BaseImageID          string    `json:"nntp_base_image_id"`
	FixtureIDs           []string  `json:"fixture_ids"`
	GeneratedAt          time.Time `json:"generated_at"`
	// RestoredNZBs lists the NZB paths a restore materialized on disk.
	RestoredNZBs []string `json:"restored_nzbs,omitempty"`
}

func (options Options) resolveContainer(ctx context.Context) (string, error) {
	if strings.TrimSpace(options.Container) != "" {
		return options.Container, nil
	}
	service := options.ComposeService
	if strings.TrimSpace(service) == "" {
		service = "nntp"
	}
	if strings.TrimSpace(options.ComposeProject) == "" {
		return "", fmt.Errorf("either a container or a Compose project is required to locate the seeded NNTP server")
	}
	id, err := options.Docker.ContainerID(ctx, options.ComposeProject, service)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(id) == "" {
		return "", fmt.Errorf("no running container for Compose service %q in project %q", service, options.ComposeProject)
	}
	return id, nil
}

// resolveBaseImageID fills in the base image identifier when the caller did
// not supply one, so the fingerprint always covers the server image.
func resolveBaseImageID(ctx context.Context, options Options) (Corpus, error) {
	corpus := options.Corpus
	if strings.TrimSpace(corpus.BaseImageID) != "" {
		return corpus, nil
	}
	if strings.TrimSpace(corpus.BaseImage) == "" {
		return Corpus{}, fmt.Errorf("NNTP server base image is required")
	}
	id, err := options.Docker.ImageID(ctx, corpus.BaseImage)
	if err != nil {
		return Corpus{}, err
	}
	if strings.TrimSpace(id) == "" {
		return Corpus{}, fmt.Errorf("NNTP server image %s is not present locally; build it with `nntpbench image build`", corpus.BaseImage)
	}
	corpus.BaseImageID = id
	return corpus, nil
}

// Inspect reports whether the corpus on disk already has a usable image.
func Inspect(ctx context.Context, options Options) (Status, error) {
	corpus, err := resolveBaseImageID(ctx, options)
	if err != nil {
		return Status{}, err
	}
	fingerprint, err := Compute(corpus)
	if err != nil {
		return Status{}, err
	}
	status := Status{SchemaVersion: ProvenanceSchemaVersion, Fingerprint: fingerprint}
	id, err := options.Docker.ImageID(ctx, fingerprint.Tag)
	if err != nil {
		return Status{}, err
	}
	if strings.TrimSpace(id) == "" {
		status.Reason = fmt.Sprintf("no local image %s: this corpus has not been captured on this machine", fingerprint.Tag)
		return status, nil
	}
	status.Present = true
	labels, err := options.Docker.ImageLabels(ctx, fingerprint.Tag)
	if err != nil {
		return Status{}, err
	}
	status.Labels = labels
	if reason := labelMismatch(labels, fingerprint); reason != "" {
		status.Reason = reason
		return status, nil
	}
	status.Hit = true
	status.Reason = fmt.Sprintf("image %s matches the fixtures on disk", fingerprint.Tag)
	return status, nil
}

// labelMismatch explains why an existing image cannot be trusted, or returns
// "" when it can. A tag collision is possible in principle, so the labels are
// checked rather than the tag alone.
func labelMismatch(labels map[string]string, fingerprint Fingerprint) string {
	if labels[FormatLabel] != FingerprintFormat {
		return fmt.Sprintf("image was built with fingerprint format %q, this harness uses %q", labels[FormatLabel], FingerprintFormat)
	}
	if labels[FingerprintLabel] != fingerprint.Value {
		return fmt.Sprintf("image carries corpus fingerprint %q, the fixtures on disk hash to %q", labels[FingerprintLabel], fingerprint.Value)
	}
	if labels[ManifestLabel] != fingerprint.CorpusManifestSHA256 {
		return fmt.Sprintf("image carries corpus manifest digest %q, the fixtures on disk hash to %q", labels[ManifestLabel], fingerprint.CorpusManifestSHA256)
	}
	return ""
}

// Capture bakes the seeded article store and the generated NZBs into a local
// image. Docker commit cannot see a named volume's contents, so the article
// store is staged out of the running container and rebuilt as a build context.
func Capture(ctx context.Context, options Options) (Provenance, error) {
	corpus, err := resolveBaseImageID(ctx, options)
	if err != nil {
		return Provenance{}, err
	}
	options.Corpus = corpus
	fingerprint, err := Compute(corpus)
	if err != nil {
		return Provenance{}, err
	}
	status, err := Inspect(ctx, options)
	if err != nil {
		return Provenance{}, err
	}
	if status.Hit {
		return provenance("capture", true, fingerprint, corpus, nil), nil
	}
	if status.Present {
		// A tag whose labels do not match is unusable and must not linger:
		// the next status call would keep reporting the same stale miss.
		if err := options.Docker.RemoveImage(ctx, fingerprint.Tag); err != nil {
			return Provenance{}, fmt.Errorf("remove stale seed image %s: %w", fingerprint.Tag, err)
		}
	}
	container, err := options.resolveContainer(ctx)
	if err != nil {
		return Provenance{}, err
	}
	stageRoot := options.StageRoot
	if strings.TrimSpace(stageRoot) == "" {
		stageRoot = os.TempDir()
	}
	if err := os.MkdirAll(stageRoot, 0o755); err != nil {
		return Provenance{}, fmt.Errorf("create seed image staging root: %w", err)
	}
	stage, err := os.MkdirTemp(stageRoot, "weaver-nntp-bench-seed-"+fingerprint.Short+"-")
	if err != nil {
		return Provenance{}, fmt.Errorf("create seed image staging directory: %w", err)
	}
	defer os.RemoveAll(stage)

	articles := filepath.Join(stage, "data", "articles")
	if err := os.MkdirAll(articles, 0o755); err != nil {
		return Provenance{}, fmt.Errorf("create staged article directory: %w", err)
	}
	if err := options.Docker.CopyFromContainer(ctx, container, ArticleDir+"/.", articles); err != nil {
		return Provenance{}, fmt.Errorf("stage seeded article store: %w", err)
	}
	for _, id := range fingerprint.FixtureIDs {
		source := LocalNZBPath(corpus.FixturesRoot, id)
		destination := filepath.Join(stage, "fixtures", id, id+".nzb")
		if err := copyFile(source, destination); err != nil {
			return Provenance{}, fmt.Errorf("stage generated NZB for %s: %w", id, err)
		}
	}
	labels := map[string]string{
		FormatLabel:      FingerprintFormat,
		FingerprintLabel: fingerprint.Value,
		ManifestLabel:    fingerprint.CorpusManifestSHA256,
		RunIDLabel:       corpus.RunID,
		CreatedLabel:     time.Now().UTC().Format(time.RFC3339),
	}
	if err := writeDockerfile(stage, labels); err != nil {
		return Provenance{}, err
	}
	if err := options.Docker.Build(ctx, stage, corpus.BaseImage, fingerprint.Tag, labels); err != nil {
		removeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = options.Docker.RemoveImage(removeCtx, fingerprint.Tag)
		return Provenance{}, fmt.Errorf("build seed image %s: %w", fingerprint.Tag, err)
	}
	return provenance("capture", true, fingerprint, corpus, nil), nil
}

// Restore materializes the generated NZBs from a matching image. It never
// starts a container: the caller brings the stack up with the seeded Compose
// override, which is what actually puts the baked article store in service.
func Restore(ctx context.Context, options Options) (Provenance, error) {
	corpus, err := resolveBaseImageID(ctx, options)
	if err != nil {
		return Provenance{}, err
	}
	options.Corpus = corpus
	status, err := Inspect(ctx, options)
	if err != nil {
		return Provenance{}, err
	}
	if !status.Present {
		return Provenance{}, fmt.Errorf("refusing to restore: %s", status.Reason)
	}
	if !status.Hit {
		return Provenance{}, fmt.Errorf("refusing to restore %s: %s", status.Fingerprint.Tag, status.Reason)
	}
	stageRoot := options.StageRoot
	if strings.TrimSpace(stageRoot) == "" {
		stageRoot = os.TempDir()
	}
	if err := os.MkdirAll(stageRoot, 0o755); err != nil {
		return Provenance{}, fmt.Errorf("create seed image staging root: %w", err)
	}
	stage, err := os.MkdirTemp(stageRoot, "weaver-nntp-bench-restore-"+status.Fingerprint.Short+"-")
	if err != nil {
		return Provenance{}, fmt.Errorf("create restore staging directory: %w", err)
	}
	defer os.RemoveAll(stage)
	if err := options.Docker.CopyFromImage(ctx, status.Fingerprint.Tag, FixtureRoot+"/.", stage); err != nil {
		return Provenance{}, fmt.Errorf("read baked NZBs from %s: %w", status.Fingerprint.Tag, err)
	}
	restored := make([]string, 0, len(status.Fingerprint.FixtureIDs))
	for _, id := range status.Fingerprint.FixtureIDs {
		source := filepath.Join(stage, id, id+".nzb")
		baked, err := os.ReadFile(source)
		if err != nil {
			return Provenance{}, fmt.Errorf("image %s has no NZB for fixture %s: %w", status.Fingerprint.Tag, id, err)
		}
		destination := LocalNZBPath(corpus.FixturesRoot, id)
		existing, err := os.ReadFile(destination)
		switch {
		case err == nil && bytes.Equal(existing, baked):
			restored = append(restored, destination)
			continue
		case err == nil:
			return Provenance{}, fmt.Errorf("refusing to overwrite %s: it differs from the NZB baked into %s", destination, status.Fingerprint.Tag)
		case !os.IsNotExist(err):
			return Provenance{}, fmt.Errorf("inspect %s: %w", destination, err)
		}
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return Provenance{}, fmt.Errorf("create fixture directory for %s: %w", id, err)
		}
		if err := os.WriteFile(destination, baked, 0o644); err != nil {
			return Provenance{}, fmt.Errorf("restore NZB for %s: %w", id, err)
		}
		restored = append(restored, destination)
	}
	return provenance("restore", true, status.Fingerprint, corpus, restored), nil
}

func provenance(action string, preseeded bool, fingerprint Fingerprint, corpus Corpus, restored []string) Provenance {
	return Provenance{
		SchemaVersion:        ProvenanceSchemaVersion,
		Action:               action,
		Preseeded:            preseeded,
		Image:                fingerprint.Tag,
		FingerprintFormat:    fingerprint.Format,
		CorpusFingerprint:    fingerprint.Value,
		CorpusManifestSHA256: fingerprint.CorpusManifestSHA256,
		SeedRunID:            corpus.RunID,
		SegmentBytes:         corpus.SegmentBytes,
		Group:                corpus.Group,
		MessageIDTemplate:    corpus.MessageIDTemplate,
		BaseImage:            corpus.BaseImage,
		BaseImageID:          corpus.BaseImageID,
		FixtureIDs:           fingerprint.FixtureIDs,
		GeneratedAt:          time.Now().UTC(),
		RestoredNZBs:         restored,
	}
}

func writeDockerfile(stage string, labels map[string]string) error {
	lines := []string{
		"# Generated by `nntpbench seed-image capture`. Not checked in: it",
		"# exists only for the duration of one capture.",
		"ARG BASE_IMAGE",
		"FROM ${BASE_IMAGE}",
		"COPY data/articles/ " + ArticleDir + "/",
		"COPY fixtures/ " + FixtureRoot + "/",
	}
	for _, key := range []string{FormatLabel, FingerprintLabel, ManifestLabel, RunIDLabel, CreatedLabel} {
		lines = append(lines, fmt.Sprintf("LABEL %s=%q", key, labels[key]))
	}
	lines = append(lines, "")
	return os.WriteFile(filepath.Join(stage, "Dockerfile"), []byte(strings.Join(lines, "\n")), 0o644)
}

func copyFile(source, destination string) error {
	contents, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	return os.WriteFile(destination, contents, 0o644)
}

// LiveProvenance records that a run posted its corpus rather than restoring
// it, so every run artifact can state how the server was populated.
func LiveProvenance(corpus Corpus) (Provenance, error) {
	fingerprint, err := Compute(corpus)
	if err != nil {
		return Provenance{}, err
	}
	record := provenance("seed", false, fingerprint, corpus, nil)
	record.Image = ""
	return record, nil
}
