package corpus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// Build-directory layout. `build` writes these; `sign` adds the two bundles;
// `publish` reads all of them.
const (
	BuildManifestFile         = "manifest.json"
	BuildManifestDigestFile   = "manifest.blake3"
	BuildProvenanceFile       = "provenance.json"
	BuildManifestBundleFile   = BuildManifestFile + BundleSuffix
	BuildProvenanceBundleFile = BuildProvenanceFile + BundleSuffix
	BuildLockFile             = "lock.json"
)

const (
	putAttempts   = 3
	putRetryDelay = 2 * time.Second
	// octetStream is what every object, manifest and bundle is stored as: the
	// bucket serves bytes, and nothing downstream trusts a content type.
	octetStream = "application/octet-stream"
	// R2 speaks the S3 API; SigV4 wants a service and a region, and R2's
	// region is the literal "auto".
	s3Service = "s3"
	s3Region  = "auto"
	// unsignedPayload lets a multi-gigabyte object be streamed once: the
	// request is signed over its headers, and the transport is TLS.
	unsignedPayload = "UNSIGNED-PAYLOAD"
	// errorBodyLimit bounds how much of an S3 error response is kept for the
	// error message.
	errorBodyLimit = 64 << 10
)

// Publisher uploads one corpus revision to the R2 bucket over the S3 API with
// net/http and the AWS SigV4 signer; nothing is shelled out.
type Publisher struct {
	// Endpoint is the bucket's S3 endpoint, https://<account>.r2.cloudflarestorage.com.
	Endpoint string
	// Bucket is the corpus bucket name.
	Bucket string
	// BaseURL is the public read base recorded in lock.json.
	BaseURL string
	// AccessKeyID and SecretAccessKey are the R2 S3 credential pair. The
	// secret is never logged and never appears in a URL.
	AccessKeyID     string
	SecretAccessKey string
	// Client performs the uploads and the public read-backs. nil means a
	// default with a generous timeout.
	Client *http.Client
	// Concurrency bounds in-flight uploads. Zero means DefaultConcurrency.
	Concurrency int
}

// PublishOptions tunes one publication.
type PublishOptions struct {
	// DryRun reconstructs and verifies everything and reports what it would
	// upload, without uploading, signing or requiring bundles.
	DryRun bool
	// Progress, when set, receives one line per decision. Safe for concurrent
	// use is the caller's responsibility.
	Progress func(string)
}

// ObjectKey and manifest keys: every published address in one place.
func ObjectKey(digest string) string { return ObjectsPrefix + digest }

// ManifestKey is where a manifest lives, addressed by its own digest.
func ManifestKey(digest string) string { return ManifestsPrefix + digest + ".json" }

// ManifestBundleKey is the Sigstore bundle for that manifest.
func ManifestBundleKey(digest string) string { return ManifestKey(digest) + BundleSuffix }

// ProvenanceKey is the build metadata for that manifest.
func ProvenanceKey(digest string) string { return ManifestsPrefix + digest + ".provenance.json" }

// ProvenanceBundleKey is the Sigstore bundle for that provenance.
func ProvenanceBundleKey(digest string) string { return ProvenanceKey(digest) + BundleSuffix }

// Publish verifies the build directory against the working tree and uploads a
// corpus revision, returning the lock entry an operator pastes into
// test-corpus/lock.json through a reviewed PR.
//
// A ledger with any blocked entry refuses publication outright: nothing whose
// provenance is incomplete is seeded into the bucket, however long it has sat
// in the tree.
func (publisher *Publisher) Publish(ctx context.Context, root, buildDir string, opts PublishOptions) (Lock, error) {
	progress := opts.Progress
	if progress == nil {
		progress = func(string) {}
	}
	ledger, _, err := LoadLedger(root)
	if err != nil {
		return Lock{}, err
	}
	if blocked := ledger.Blocked(); len(blocked) > 0 {
		paths := make([]string, 0, len(blocked))
		for _, entry := range blocked {
			paths = append(paths, entry.Path)
		}
		sort.Strings(paths)
		shown := paths
		suffix := ""
		if len(shown) > 10 {
			shown, suffix = shown[:10], fmt.Sprintf("\n  … and %d more", len(paths)-10)
		}
		return Lock{}, fmt.Errorf(
			"%w: %d of %d ledger entries have no generator yet, and nothing with incomplete provenance is published. Port their generators first.\n  %s%s",
			ErrBlockedEntries, len(paths), len(ledger.Files), strings.Join(shown, "\n  "), suffix)
	}

	manifest, manifestBytes, manifestDigest, err := readBuiltManifest(buildDir)
	if err != nil {
		return Lock{}, err
	}
	provenanceBytes, err := os.ReadFile(HostPath(buildDir, BuildProvenanceFile))
	if err != nil {
		return Lock{}, fmt.Errorf("read %s: %w", BuildProvenanceFile, err)
	}
	provenance, err := decodeProvenance(provenanceBytes, manifestDigest)
	if err != nil {
		return Lock{}, err
	}
	provenanceDigest := DigestBytes(provenanceBytes)

	lock := LockEntry(manifestDigest, provenanceDigest, publisher.BaseURL, provenance.SourceCommit, provenance.WorkflowRun)
	if !opts.DryRun {
		if err := publisher.validate(); err != nil {
			return Lock{}, err
		}
		// Fail before the first byte moves rather than after: an entry that
		// cannot be pinned is not worth publishing.
		if err := lock.Validate(); err != nil {
			return Lock{}, fmt.Errorf("the lock entry this publication would produce is not valid: %w", err)
		}
		for _, name := range []string{BuildManifestBundleFile, BuildProvenanceBundleFile} {
			if info, err := os.Stat(HostPath(buildDir, name)); err != nil || info.Size() == 0 {
				return Lock{}, fmt.Errorf("%s is missing from %s: run `corpus sign --dir %s` first", name, buildDir, buildDir)
			}
		}
	}

	// Every object's bytes come from the working tree and must be exactly what
	// the manifest describes.
	objects, err := planObjects(root, manifest)
	if err != nil {
		return Lock{}, err
	}
	progress(fmt.Sprintf("%d files, %d distinct objects, manifest %s", len(manifest.Files), len(objects), manifestDigest))
	if opts.DryRun {
		for _, object := range objects {
			progress(fmt.Sprintf("would upload %s <- %s (%d bytes)", ObjectKey(object.digest), object.path, object.size))
		}
		for _, key := range []string{
			ManifestKey(manifestDigest), ManifestBundleKey(manifestDigest),
			ProvenanceKey(manifestDigest), ProvenanceBundleKey(manifestDigest),
		} {
			progress("would upload " + key)
		}
		return lock, nil
	}

	if err := publisher.uploadObjects(ctx, root, objects, progress); err != nil {
		return Lock{}, err
	}
	documents := []struct {
		key  string
		file string
	}{
		{ManifestKey(manifestDigest), HostPath(buildDir, BuildManifestFile)},
		{ManifestBundleKey(manifestDigest), HostPath(buildDir, BuildManifestBundleFile)},
		{ProvenanceKey(manifestDigest), HostPath(buildDir, BuildProvenanceFile)},
		{ProvenanceBundleKey(manifestDigest), HostPath(buildDir, BuildProvenanceBundleFile)},
	}
	for _, document := range documents {
		digest, err := DigestFile(document.file)
		if err != nil {
			return Lock{}, err
		}
		if err := publisher.upload(ctx, document.file, document.key, digest.BLAKE3, progress); err != nil {
			return Lock{}, err
		}
	}

	// A fresh consumer's first move, done here so a broken publication is
	// caught by the publisher rather than by the next CI run.
	published, err := getBytes(ctx, publisher.client(), publisher.publicURL(ManifestKey(manifestDigest)))
	if err != nil {
		return Lock{}, fmt.Errorf("read the published manifest back: %w", err)
	}
	if digest := DigestBytes(published); digest != manifestDigest {
		return Lock{}, fmt.Errorf("the manifest read back from %s has blake3 %s, expected %s",
			publisher.publicURL(ManifestKey(manifestDigest)), digest, manifestDigest)
	}
	if len(published) != len(manifestBytes) {
		return Lock{}, fmt.Errorf("the manifest read back from the bucket is %d bytes, published %d", len(published), len(manifestBytes))
	}
	progress("manifest read back and verified")
	return lock, nil
}

type plannedObject struct {
	path   string
	digest string
	size   int64
}

// planObjects maps the manifest onto tree bytes, one object per distinct
// digest (identical fixtures share an object, so a revision that changes one
// file uploads one object).
func planObjects(root string, manifest *Manifest) ([]plannedObject, error) {
	seen := map[string]struct{}{}
	var objects []plannedObject
	var problems []string
	for _, file := range manifest.Files {
		digest, err := DigestFile(HostPath(root, file.Path))
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", file.Path, err))
			continue
		}
		if digest.BLAKE3 != file.BLAKE3 || digest.Size != file.Size {
			problems = append(problems, fmt.Sprintf("%s: tree has %s/%d bytes, manifest says %s/%d",
				file.Path, digest.BLAKE3, digest.Size, file.BLAKE3, file.Size))
			continue
		}
		if _, duplicate := seen[digest.BLAKE3]; duplicate {
			continue
		}
		seen[digest.BLAKE3] = struct{}{}
		objects = append(objects, plannedObject{path: file.Path, digest: digest.BLAKE3, size: digest.Size})
	}
	if err := joinProblems("the working tree does not match the manifest", problems); err != nil {
		return nil, err
	}
	sort.Slice(objects, func(left, right int) bool { return objects[left].digest < objects[right].digest })
	return objects, nil
}

func (publisher *Publisher) uploadObjects(ctx context.Context, root string, objects []plannedObject, progress func(string)) error {
	concurrency := publisher.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	work := make(chan plannedObject)
	var waitGroup sync.WaitGroup
	var once sync.Once
	var firstError error
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for worker := 0; worker < concurrency; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for object := range work {
				if workCtx.Err() != nil {
					return
				}
				err := publisher.upload(workCtx, HostPath(root, object.path), ObjectKey(object.digest), object.digest, progress)
				if err != nil {
					once.Do(func() { firstError = err; cancel() })
					return
				}
			}
		}()
	}
	for _, object := range objects {
		select {
		case work <- object:
		case <-workCtx.Done():
		}
	}
	close(work)
	waitGroup.Wait()
	return firstError
}

// upload PUTs one file at one key, create-only. A 412 means the key exists
// already: the object is read back from the public side and its digest must
// match, otherwise the whole publication aborts — a key whose bytes are not
// what this revision describes is never accepted or overwritten.
func (publisher *Publisher) upload(ctx context.Context, file, key, digest string, progress func(string)) error {
	url := publisher.uploadURL(key)
	status, body, err := publisher.putWithRetries(ctx, file, url)
	if err != nil {
		return err
	}
	switch {
	case status >= 200 && status < 300:
		progress("uploaded " + key)
		return nil
	case status == http.StatusPreconditionFailed:
		published, err := getBytes(ctx, publisher.client(), publisher.publicURL(key))
		if err != nil {
			return fmt.Errorf("%s already exists but could not be read back: %w", key, err)
		}
		if found := DigestBytes(published); found != digest {
			return fmt.Errorf("%s already exists with blake3 %s, this revision has %s: aborting the publication rather than rewriting an immutable key",
				key, found, digest)
		}
		progress("already published " + key)
		return nil
	default:
		return fmt.Errorf("PUT %s: HTTP %d: %s", key, status, strings.TrimSpace(string(body)))
	}
}

func (publisher *Publisher) putWithRetries(ctx context.Context, file, url string) (int, []byte, error) {
	var lastErr error
	for attempt := 1; attempt <= putAttempts; attempt++ {
		status, body, err := publisher.put(ctx, file, url)
		switch {
		case err == nil && status < 500:
			return status, body, nil
		case err == nil:
			lastErr = fmt.Errorf("PUT %s: HTTP %d (attempt %d)", url, status, attempt)
		default:
			lastErr = fmt.Errorf("PUT %s (attempt %d): %w", url, attempt, err)
		}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-time.After(time.Duration(attempt) * putRetryDelay):
		}
	}
	return 0, nil, lastErr
}

// put streams one file to one key as a conditional (create-only) SigV4 PUT.
// The file is reopened per call so a retry never resends a half-consumed
// body. Credentials never appear in the URL, in a log line, or in an error.
func (publisher *Publisher) put(ctx context.Context, file, url string) (int, []byte, error) {
	source, err := os.Open(file)
	if err != nil {
		return 0, nil, err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, url, source)
	if err != nil {
		return 0, nil, err
	}
	request.ContentLength = info.Size()
	request.Header.Set("Content-Type", octetStream)
	// Create-only: a content-addressed key is written at most once and never
	// rewritten, so a published object is immutable by construction. A 412
	// means the key already exists; the caller reads it back and compares.
	request.Header.Set("If-None-Match", "*")
	request.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
	if err := publisher.sign(ctx, request); err != nil {
		return 0, nil, err
	}
	response, err := publisher.client().Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, errorBodyLimit))
	return response.StatusCode, body, nil
}

// sign applies AWS Signature Version 4 with the publisher's credential pair.
func (publisher *Publisher) sign(ctx context.Context, request *http.Request) error {
	credentials := aws.Credentials{AccessKeyID: publisher.AccessKeyID, SecretAccessKey: publisher.SecretAccessKey}
	return v4.NewSigner().SignHTTP(ctx, credentials, request, unsignedPayload, s3Service, s3Region, time.Now().UTC())
}

func (publisher *Publisher) uploadURL(key string) string {
	return strings.TrimRight(publisher.Endpoint, "/") + "/" + publisher.Bucket + "/" + key
}

func (publisher *Publisher) publicURL(key string) string {
	return strings.TrimRight(publisher.BaseURL, "/") + "/" + key
}

func (publisher *Publisher) client() *http.Client {
	if publisher.Client != nil {
		return publisher.Client
	}
	return &http.Client{Timeout: 10 * time.Minute}
}

func (publisher *Publisher) validate() error {
	var problems []string
	// The signed uploads carry an unsigned payload, so the transport must be
	// TLS; only a loopback endpoint (tests, a local fake) may be plain http.
	if !validBaseURL(publisher.Endpoint) {
		problems = append(problems, "--s3-endpoint / R2_CORPUS_S3_ENDPOINT must be the https S3 endpoint (https://<account>.r2.cloudflarestorage.com)")
	}
	if strings.TrimSpace(publisher.Bucket) == "" || strings.ContainsAny(publisher.Bucket, "/ ") {
		problems = append(problems, "--bucket / R2_CORPUS_BUCKET must be a bucket name")
	}
	if !validBaseURL(publisher.BaseURL) {
		problems = append(problems, "--base-url / R2_CORPUS_PUBLIC_URL must be an https URL without a trailing slash")
	}
	if strings.TrimSpace(publisher.AccessKeyID) == "" || strings.TrimSpace(publisher.SecretAccessKey) == "" {
		problems = append(problems, "R2_CORPUS_ACCESS_KEY_ID and R2_CORPUS_SECRET_ACCESS_KEY must both be set")
	}
	return joinProblems("publisher is not configured", problems)
}

// readBuiltManifest reads the manifest a build produced and requires it to
// match its own recorded digest, so a truncated or edited build directory
// never becomes a publication.
func readBuiltManifest(buildDir string) (*Manifest, []byte, string, error) {
	contents, err := os.ReadFile(HostPath(buildDir, BuildManifestFile))
	if err != nil {
		return nil, nil, "", fmt.Errorf("read %s: %w", BuildManifestFile, err)
	}
	recorded, err := os.ReadFile(HostPath(buildDir, BuildManifestDigestFile))
	if err != nil {
		return nil, nil, "", fmt.Errorf("read %s: %w", BuildManifestDigestFile, err)
	}
	digest := DigestBytes(contents)
	if want := strings.TrimSpace(string(recorded)); want != digest {
		return nil, nil, "", fmt.Errorf("%s says %s but %s hashes to %s", BuildManifestDigestFile, want, BuildManifestFile, digest)
	}
	manifest, err := DecodeManifest(contents)
	if err != nil {
		return nil, nil, "", err
	}
	return manifest, contents, digest, nil
}

func decodeProvenance(contents []byte, manifestDigest string) (Provenance, error) {
	var provenance Provenance
	if err := json.Unmarshal(contents, &provenance); err != nil {
		return Provenance{}, fmt.Errorf("decode %s: %w", BuildProvenanceFile, err)
	}
	if provenance.ManifestBLAKE3 != manifestDigest {
		return Provenance{}, fmt.Errorf("%s describes manifest %s, the build directory holds %s",
			BuildProvenanceFile, provenance.ManifestBLAKE3, manifestDigest)
	}
	return provenance, nil
}

// ErrBlockedEntries is what a caller matches on to explain the refusal without
// re-reading the ledger.
var ErrBlockedEntries = errors.New("the ledger has blocked entries")
