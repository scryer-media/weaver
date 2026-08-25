package weaver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestCaptureSeedImageCacheResolvesEachContainerAtSnapshotTime(t *testing.T) {
	set := nntpSeedImageSet{
		Profile:     "functional",
		Fingerprint: "0123456789abcdef",
		Primary:     "primary-image",
		Backup:      "backup-image",
	}
	backupID := "backup-before-restart"
	var snapshots []string
	ops := nntpSeedCacheCaptureOps{
		imageExists: func(string) bool { return false },
		resolveContainer: func(_ context.Context, _ string, service string) (string, error) {
			if service == "nntp" {
				return "primary-container", nil
			}
			return backupID, nil
		},
		snapshot: func(_ context.Context, containerID, contextDir string) error {
			snapshots = append(snapshots, containerID)
			if containerID == "primary-container" {
				backupID = "backup-after-restart"
			}
			return os.MkdirAll(contextDir, 0o755)
		},
		build:       func(context.Context, string, string, string, nntpSeedImageSet) error { return nil },
		removeImage: func(context.Context, string) error { return nil },
		processAlive: func(int) bool {
			return false
		},
	}

	err := captureSeedImageCacheWithOps(context.Background(), set, nil, nntpSeedCacheCaptureConfig{
		Project:   "test-project",
		StageRoot: t.TempDir(),
		LockRoot:  t.TempDir(),
		OwnerPID:  123,
	}, ops)
	if err != nil {
		t.Fatalf("capture cache: %v", err)
	}
	if !slices.Equal(snapshots, []string{"primary-container", "backup-after-restart"}) {
		t.Fatalf("snapshot containers = %v", snapshots)
	}
}

func TestCaptureSeedImageCacheCommitsEmbeddedArticleStores(t *testing.T) {
	set := nntpSeedImageSet{
		Profile:     "functional",
		Fingerprint: "0123456789abcdef",
		Primary:     "primary-image",
		Backup:      "backup-image",
	}
	var staged, committed []string
	var progress []int
	ops := nntpSeedCacheCaptureOps{
		imageExists: func(string) bool { return false },
		resolveContainer: func(_ context.Context, _ string, service string) (string, error) {
			return service + "-container", nil
		},
		usesDataMount: func(context.Context, string) (bool, error) { return false, nil },
		stageFixtures: func(_ context.Context, containerID, _ string) error {
			staged = append(staged, containerID)
			return nil
		},
		commit: func(_ context.Context, containerID, tag string, _ nntpSeedImageSet) error {
			committed = append(committed, containerID+"->"+tag)
			return nil
		},
		snapshot: func(context.Context, string, string) error {
			t.Fatal("direct commit path copied the article store to the host")
			return nil
		},
		build: func(context.Context, string, string, string, nntpSeedImageSet) error {
			t.Fatal("direct commit path rebuilt the article store")
			return nil
		},
		removeImage:  func(context.Context, string) error { return nil },
		processAlive: func(int) bool { return false },
	}

	err := captureSeedImageCacheWithOps(context.Background(), set, nil, nntpSeedCacheCaptureConfig{
		Project:          "test-project",
		StageRoot:        t.TempDir(),
		LockRoot:         t.TempDir(),
		OwnerPID:         123,
		CommitContainers: true,
		Progress: func(current, _ int, _ string) {
			progress = append(progress, current)
		},
	}, ops)
	if err != nil {
		t.Fatalf("capture cache: %v", err)
	}
	if !slices.Equal(staged, []string{"nntp-container", "nntp2-container"}) {
		t.Fatalf("staged containers = %v", staged)
	}
	if !slices.Equal(committed, []string{
		"nntp-container->primary-image",
		"nntp2-container->backup-image",
	}) {
		t.Fatalf("committed images = %v", committed)
	}
	if !slices.Equal(progress, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("cache progress = %v", progress)
	}
}

func TestCaptureSeedImageCacheRefusesMountedArticleStoreCommit(t *testing.T) {
	set := nntpSeedImageSet{
		Profile:     "functional",
		Fingerprint: "0123456789abcdef",
		Primary:     "primary-image",
		Backup:      "backup-image",
	}
	ops := nntpSeedCacheCaptureOps{
		imageExists: func(string) bool { return false },
		resolveContainer: func(context.Context, string, string) (string, error) {
			return "mounted-container", nil
		},
		usesDataMount: func(context.Context, string) (bool, error) { return true, nil },
		stageFixtures: func(context.Context, string, string) error {
			t.Fatal("mounted article store reached fixture staging")
			return nil
		},
		removeImage:  func(context.Context, string) error { return nil },
		processAlive: func(int) bool { return false },
	}
	err := captureSeedImageCacheWithOps(context.Background(), set, nil, nntpSeedCacheCaptureConfig{
		Project:          "test-project",
		StageRoot:        t.TempDir(),
		LockRoot:         t.TempDir(),
		OwnerPID:         123,
		CommitContainers: true,
	}, ops)
	if err == nil || !strings.Contains(err.Error(), "refusing an article-free commit") {
		t.Fatalf("mounted article store error = %v", err)
	}
}

func TestCaptureSeedImageCacheCancellationCleansPartialState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stageRoot := t.TempDir()
	lockRoot := t.TempDir()
	set := nntpSeedImageSet{
		Profile:     "functional",
		Fingerprint: "0123456789abcdef",
		Primary:     "primary-image",
		Backup:      "backup-image",
	}

	var imageMu sync.Mutex
	images := map[string]bool{}
	backupBuildStarted := make(chan struct{})
	var progressMu sync.Mutex
	var progress []int
	ops := nntpSeedCacheCaptureOps{
		imageExists: func(tag string) bool {
			imageMu.Lock()
			defer imageMu.Unlock()
			return images[tag]
		},
		resolveContainer: func(_ context.Context, _ string, service string) (string, error) {
			return service + "-container", nil
		},
		snapshot: func(_ context.Context, _ string, contextDir string) error {
			return os.MkdirAll(contextDir, 0o755)
		},
		build: func(ctx context.Context, _ string, tag, _ string, _ nntpSeedImageSet) error {
			imageMu.Lock()
			images[tag] = true
			imageMu.Unlock()
			if tag == set.Backup {
				close(backupBuildStarted)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
		removeImage: func(_ context.Context, tag string) error {
			imageMu.Lock()
			delete(images, tag)
			imageMu.Unlock()
			return nil
		},
		processAlive: func(int) bool { return false },
	}

	done := make(chan error, 1)
	go func() {
		done <- captureSeedImageCacheWithOps(ctx, set, nil, nntpSeedCacheCaptureConfig{
			Project:   "test-project",
			StageRoot: stageRoot,
			LockRoot:  lockRoot,
			OwnerPID:  123,
			Progress: func(current, _ int, _ string) {
				progressMu.Lock()
				progress = append(progress, current)
				progressMu.Unlock()
			},
		}, ops)
	}()
	<-backupBuildStarted
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("capture error = %v, want context cancellation", err)
	}

	imageMu.Lock()
	if len(images) != 0 {
		t.Fatalf("partial images survived cancellation: %#v", images)
	}
	imageMu.Unlock()
	entries, err := os.ReadDir(stageRoot)
	if err != nil {
		t.Fatalf("read stage root: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging survived cancellation: %v", entries)
	}
	lockPath := filepath.Join(lockRoot, "weaver-e2e-nntp-seed-image-"+set.Fingerprint+".lock")
	if _, err := os.Stat(lockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lock survived cancellation: %v", err)
	}
	progressMu.Lock()
	defer progressMu.Unlock()
	if !slices.Equal(progress, []int{0, 1, 2, 3}) {
		t.Fatalf("progress = %v, want [0 1 2 3]", progress)
	}
}

func TestNntpSeedImageLockReclaimsDeadOwnerAndPreservesLiveOwner(t *testing.T) {
	root := t.TempDir()
	set := nntpSeedImageSet{Profile: "functional", Fingerprint: "lock-fingerprint"}

	release, acquired, err := tryAcquireNntpSeedImageLock(set, root, 41, func(pid int) bool { return pid == 41 })
	if err != nil || !acquired {
		t.Fatalf("first lock acquisition: acquired=%t err=%v", acquired, err)
	}
	_, acquired, err = tryAcquireNntpSeedImageLock(set, root, 42, func(pid int) bool { return pid == 41 })
	if err != nil {
		t.Fatalf("contended lock: %v", err)
	}
	if acquired {
		t.Fatal("live owner's lock was stolen")
	}
	release()

	lockPath := filepath.Join(root, "weaver-e2e-nntp-seed-image-"+set.Fingerprint+".lock")
	if err := os.WriteFile(lockPath, []byte(`{"pid":41,"profile":"functional","fingerprint":"lock-fingerprint"}`), 0o644); err != nil {
		t.Fatalf("write stale lock: %v", err)
	}
	release, acquired, err = tryAcquireNntpSeedImageLock(set, root, 42, func(int) bool { return false })
	if err != nil || !acquired {
		t.Fatalf("reclaim stale lock: acquired=%t err=%v", acquired, err)
	}
	release()
}
