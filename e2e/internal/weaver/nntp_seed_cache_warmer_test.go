package weaver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestNntpSeedCacheWarmerReusesCompletePair(t *testing.T) {
	captures := 0
	warmer := newNntpSeedCacheWarmerWith(
		context.Background(),
		t.TempDir(),
		newFullDashboard("test", []string{"Functional SQLite"}, 0),
		func(nntpSeedImageSet) bool { return true },
		func(context.Context, nntpSeedImageSet, []string, nntpSeedCacheCaptureConfig) error {
			captures++
			return nil
		},
	)

	job, owner := warmer.start(&fullPhaseContext{SeedProfile: "functional", Project: "sqlite"})
	if job != nil || owner {
		t.Fatalf("cache hit scheduled a warm: job=%v owner=%t", job, owner)
	}
	if captures != 0 {
		t.Fatalf("cache hit captured %d times", captures)
	}
	if warmer.dashboard.cache.Status != "pass" {
		t.Fatalf("cache hit dashboard status = %q, want pass", warmer.dashboard.cache.Status)
	}
}

func TestNntpSeedCacheWarmerSharesOneBackgroundWarm(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var captures int
	var captureMu sync.Mutex
	warmer := newNntpSeedCacheWarmerWith(
		context.Background(),
		t.TempDir(),
		newFullDashboard("test", []string{"Functional SQLite", "Functional Postgres"}, 0),
		func(nntpSeedImageSet) bool { return false },
		func(_ context.Context, _ nntpSeedImageSet, _ []string, config nntpSeedCacheCaptureConfig) error {
			captureMu.Lock()
			captures++
			captureMu.Unlock()
			close(started)
			config.Progress(0, nntpSeedCacheStageCount, "blocked test capture")
			<-release
			config.Progress(nntpSeedCacheStageCount, nntpSeedCacheStageCount, "cache images ready")
			return nil
		},
	)

	first, firstOwner := warmer.start(&fullPhaseContext{SeedProfile: "functional", Project: "sqlite"})
	if first == nil || !firstOwner {
		t.Fatal("first seeded phase did not own the cache warm")
	}
	<-started
	second, secondOwner := warmer.start(&fullPhaseContext{SeedProfile: "functional", Project: "postgres"})
	if second != first || secondOwner {
		t.Fatalf("matching phase did not share warm: same=%t owner=%t", second == first, secondOwner)
	}

	waited := make(chan error, 1)
	go func() { waited <- first.wait() }()
	select {
	case <-waited:
		t.Fatal("cache wait completed while background capture was blocked")
	default:
		// Phase execution is free to start while final cleanup still waits.
	}
	close(release)
	if err := <-waited; err != nil {
		t.Fatalf("cache warm failed: %v", err)
	}
	captureMu.Lock()
	defer captureMu.Unlock()
	if captures != 1 {
		t.Fatalf("matching phases captured %d times, want 1", captures)
	}
	if warmer.dashboard.cache.Status != "pass" {
		t.Fatalf("completed cache dashboard status = %q, want pass", warmer.dashboard.cache.Status)
	}
}

func TestNntpSeedCacheWarmerReportsFailureAsWarning(t *testing.T) {
	wantErr := errors.New("cache build failed")
	dashboard := newFullDashboard("test", []string{"Functional SQLite"}, 0)
	warmer := newNntpSeedCacheWarmerWith(
		context.Background(),
		t.TempDir(),
		dashboard,
		func(nntpSeedImageSet) bool { return false },
		func(context.Context, nntpSeedImageSet, []string, nntpSeedCacheCaptureConfig) error {
			return wantErr
		},
	)

	job, owner := warmer.start(&fullPhaseContext{SeedProfile: "functional", Project: "sqlite"})
	if !owner {
		t.Fatal("missing cache did not assign a warm owner")
	}
	if err := job.wait(); !errors.Is(err, wantErr) {
		t.Fatalf("cache error = %v, want %v", err, wantErr)
	}
	if dashboard.cache.Status != "warning" {
		t.Fatalf("failed cache dashboard status = %q, want warning", dashboard.cache.Status)
	}
}

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

func TestNntpSeedCacheProgressRendersAllStages(t *testing.T) {
	dashboard := newFullDashboard("test", nil, 0)
	for current, detail := range []string{
		"snapshotting primary article store",
		"snapshotting backup article store",
		"building primary cache image",
		"building backup cache image",
		"cache images ready",
	} {
		dashboard.updateNntpCache("fingerprint", "functional", current, nntpSeedCacheStageCount, "running", detail)
	}
	if dashboard.cache.Current != nntpSeedCacheStageCount || dashboard.cache.Total != nntpSeedCacheStageCount {
		t.Fatalf("cache progress = %d/%d, want %d/%d", dashboard.cache.Current, dashboard.cache.Total, nntpSeedCacheStageCount, nntpSeedCacheStageCount)
	}
	if dashboard.cache.Status != "pass" {
		t.Fatalf("cache status = %q, want pass", dashboard.cache.Status)
	}
	if dashboard.cache.Detail != "functional: cache images ready" {
		t.Fatalf("cache detail = %q", dashboard.cache.Detail)
	}
}

func TestNntpSeedCacheWaitDoesNotCompleteBeforeWarm(t *testing.T) {
	job := &nntpSeedCacheWarmJob{done: make(chan struct{})}
	waited := make(chan struct{})
	go func() {
		_ = job.wait()
		close(waited)
	}()
	select {
	case <-waited:
		t.Fatal("wait returned before cache completion")
	case <-time.After(10 * time.Millisecond):
	}
	close(job.done)
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("wait did not return after cache completion")
	}
}
