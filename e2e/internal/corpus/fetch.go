package corpus

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/zeebo/blake3"
)

// FetchOptions tunes one hydration.
type FetchOptions struct {
	// Client is the HTTP client; nil means a default with a generous timeout,
	// because objects here run to hundreds of megabytes.
	Client *http.Client
	// Concurrency bounds in-flight downloads. Zero means DefaultConcurrency.
	Concurrency int
	// Progress, when set, is called with one line per decision (skipped,
	// fetched). It is called from several goroutines; it must be safe for
	// concurrent use.
	Progress func(string)
	// Only, when non-empty, restricts the hydration to these ledger paths.
	// A path the published manifest does not carry is simply not fetched;
	// the caller sees it still missing and decides what that means. With
	// Only set, the profile list may be empty, which means "any profile".
	Only []string
}

// DefaultConcurrency is the in-flight download and upload bound.
const DefaultConcurrency = 8

const (
	fetchAttempts   = 3
	fetchRetryDelay = time.Second
)

// Fetch hydrates the named profiles from the pinned published corpus.
//
// Nothing is written that has not been verified: the manifest must match the
// digest lock.json pins, every object must match the digest the manifest
// records, and each file lands through a temporary file and a rename. A file
// already present with the right size and digest is left alone.
func Fetch(ctx context.Context, root string, lock *Lock, profileNames []string, opts FetchOptions) error {
	if !lock.Pinned() {
		return ErrNotPinned
	}
	if len(profileNames) == 0 && len(opts.Only) == 0 {
		return errors.New("fetch needs at least one profile")
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Minute}
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	progress := opts.Progress
	if progress == nil {
		progress = func(string) {}
	}

	manifest, err := fetchManifest(ctx, client, lock)
	if err != nil {
		return err
	}
	var wanted []ManifestFile
	if len(profileNames) == 0 {
		wanted = append(wanted, manifest.Files...)
	} else {
		wanted, err = resolveFrozenProfiles(manifest, profileNames)
		if err != nil {
			return err
		}
	}
	if len(opts.Only) > 0 {
		only := make(map[string]struct{}, len(opts.Only))
		for _, path := range opts.Only {
			only[path] = struct{}{}
		}
		filtered := wanted[:0]
		for _, file := range wanted {
			if _, ok := only[file.Path]; ok {
				filtered = append(filtered, file)
			}
		}
		wanted = filtered
	}
	if len(profileNames) == 0 {
		progress(fmt.Sprintf("manifest %s: %d of the requested files are published", lock.Manifest.BLAKE3, len(wanted)))
	} else {
		progress(fmt.Sprintf("manifest %s: %d files across %v", lock.Manifest.BLAKE3, len(wanted), profileNames))
	}

	work := make(chan ManifestFile)
	var waitGroup sync.WaitGroup
	var once sync.Once
	var firstError error
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	failed := func(err error) {
		once.Do(func() {
			firstError = err
			cancel()
		})
	}
	for worker := 0; worker < concurrency; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for file := range work {
				if workCtx.Err() != nil {
					return
				}
				fetched, err := fetchOne(workCtx, client, lock, root, file)
				if err != nil {
					failed(err)
					return
				}
				if fetched {
					progress(fmt.Sprintf("fetched %s (%d bytes)", file.Path, file.Size))
				} else {
					progress(fmt.Sprintf("present %s", file.Path))
				}
			}
		}()
	}
	for _, file := range wanted {
		select {
		case work <- file:
		case <-workCtx.Done():
		}
	}
	close(work)
	waitGroup.Wait()
	return firstError
}

// fetchManifest downloads the pinned manifest and refuses anything whose
// digest is not the one the lock names.
func fetchManifest(ctx context.Context, client *http.Client, lock *Lock) (*Manifest, error) {
	contents, err := getBytes(ctx, client, lock.Manifest.URL)
	if err != nil {
		return nil, fmt.Errorf("download manifest: %w", err)
	}
	if digest := DigestBytes(contents); digest != lock.Manifest.BLAKE3 {
		return nil, fmt.Errorf("manifest at %s has blake3 %s, %s pins %s",
			lock.Manifest.URL, digest, LockFile, lock.Manifest.BLAKE3)
	}
	return DecodeManifest(contents)
}

// resolveFrozenProfiles reads membership from the manifest, never from the
// working tree, so a hydration is exactly what was published.
func resolveFrozenProfiles(manifest *Manifest, profileNames []string) ([]ManifestFile, error) {
	byPath := make(map[string]ManifestFile, len(manifest.Files))
	for _, file := range manifest.Files {
		byPath[file.Path] = file
	}
	union := map[string]struct{}{}
	for _, name := range profileNames {
		members, ok := manifest.Profiles[name]
		if !ok {
			known := make([]string, 0, len(manifest.Profiles))
			for available := range manifest.Profiles {
				known = append(known, available)
			}
			sort.Strings(known)
			return nil, fmt.Errorf("profile %q is not in the published manifest (it has: %v)", name, known)
		}
		for _, member := range members {
			union[member] = struct{}{}
		}
	}
	paths := make([]string, 0, len(union))
	for path := range union {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	files := make([]ManifestFile, 0, len(paths))
	for _, path := range paths {
		file, ok := byPath[path]
		if !ok {
			return nil, fmt.Errorf("manifest profile names %s, which the manifest does not describe", path)
		}
		files = append(files, file)
	}
	return files, nil
}

// fetchOne returns whether it had to download. A file already on disk with the
// recorded size and digest is left alone: hydration is cheap to repeat.
func fetchOne(ctx context.Context, client *http.Client, lock *Lock, root string, file ManifestFile) (bool, error) {
	destination := HostPath(root, file.Path)
	if info, err := os.Stat(destination); err == nil && info.Mode().IsRegular() && info.Size() == file.Size {
		if digest, err := DigestFile(destination); err == nil && digest.BLAKE3 == file.BLAKE3 {
			return false, nil
		}
	}
	url := lock.ObjectURL(file.BLAKE3)
	var lastErr error
	for attempt := 1; attempt <= fetchAttempts; attempt++ {
		err := downloadVerified(ctx, client, url, destination, file)
		if err == nil {
			return true, nil
		}
		if !retryable(err) {
			return false, err
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Duration(attempt) * fetchRetryDelay):
		}
	}
	return false, lastErr
}

// downloadVerified streams the object into a temporary sibling, hashing as it
// goes, and only renames it into place once the digest and size match. A byte
// that has not been verified never appears at a fixture path.
func downloadVerified(ctx context.Context, client *http.Client, url, destination string, file ManifestFile) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return &transientError{cause: fmt.Errorf("GET %s: %w", url, err)}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
		_ = response.Body.Close()
	}()
	switch {
	case response.StatusCode == http.StatusNotFound:
		// A published manifest naming an object the bucket does not hold is a
		// broken publication, not a transient failure. Say which fixture.
		return fmt.Errorf("object for %s is missing from the corpus bucket (HTTP 404 for %s)", file.Path, url)
	case response.StatusCode >= 500:
		return &transientError{cause: fmt.Errorf("GET %s: HTTP %d", url, response.StatusCode)}
	case response.StatusCode != http.StatusOK:
		return fmt.Errorf("GET %s: HTTP %d", url, response.StatusCode)
	}
	hasher := blake3.New()
	var written int64
	err = WriteFileAtomic(destination, func(writer io.Writer) error {
		// Read at most one byte past the recorded size: an object larger than
		// the manifest says is wrong either way, and it must not fill the disk
		// before the size check notices.
		copied, err := io.Copy(io.MultiWriter(writer, hasher), io.LimitReader(response.Body, file.Size+1))
		written = copied
		if err != nil {
			return &transientError{cause: fmt.Errorf("read %s: %w", url, err)}
		}
		if written != file.Size {
			return fmt.Errorf("%s: %d bytes from %s, manifest says %d", file.Path, written, url, file.Size)
		}
		if digest := hex.EncodeToString(hasher.Sum(nil)); digest != file.BLAKE3 {
			return fmt.Errorf("%s: blake3 %s from %s, manifest says %s", file.Path, digest, url, file.BLAKE3)
		}
		return nil
	}, 0o644)
	return err
}

// getBytes downloads a small document (manifest, bundle, provenance) with the
// same bounded retry policy.
func getBytes(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	var lastErr error
	for attempt := 1; attempt <= fetchAttempts; attempt++ {
		contents, err := getBytesOnce(ctx, client, url)
		if err == nil {
			return contents, nil
		}
		if !retryable(err) {
			return nil, err
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt) * fetchRetryDelay):
		}
	}
	return nil, lastErr
}

func getBytesOnce(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, &transientError{cause: fmt.Errorf("GET %s: %w", url, err)}
	}
	defer response.Body.Close()
	contents, readErr := io.ReadAll(response.Body)
	switch {
	case response.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("GET %s: HTTP 404", url)
	case response.StatusCode >= 500:
		return nil, &transientError{cause: fmt.Errorf("GET %s: HTTP %d", url, response.StatusCode)}
	case response.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("GET %s: HTTP %d", url, response.StatusCode)
	}
	if readErr != nil {
		return nil, &transientError{cause: fmt.Errorf("read %s: %w", url, readErr)}
	}
	return contents, nil
}

// transientError marks the failures worth retrying: transport errors and 5xx.
// A digest mismatch or a 404 never is.
type transientError struct{ cause error }

func (err *transientError) Error() string { return err.cause.Error() }
func (err *transientError) Unwrap() error { return err.cause }

func retryable(err error) bool {
	var transient *transientError
	return errors.As(err, &transient)
}
