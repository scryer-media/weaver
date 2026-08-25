package weaver

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type nntpSeedCacheCaptureFunc func(
	context.Context,
	nntpSeedImageSet,
	[]string,
	nntpSeedCacheCaptureConfig,
) error

type nntpSeedCacheWarmJob struct {
	done chan struct{}
	err  error
}

func (job *nntpSeedCacheWarmJob) wait() error {
	if job == nil {
		return nil
	}
	<-job.done
	return job.err
}

type nntpSeedCacheWarmer struct {
	ctx       context.Context
	tempRoot  string
	dashboard *fullDashboard
	ready     func(nntpSeedImageSet) bool
	capture   nntpSeedCacheCaptureFunc

	mu    sync.Mutex
	jobs  map[string]*nntpSeedCacheWarmJob
	logMu sync.Mutex
}

func newNntpSeedCacheWarmer(ctx context.Context, tempRoot string, dashboard *fullDashboard) *nntpSeedCacheWarmer {
	return newNntpSeedCacheWarmerWith(ctx, tempRoot, dashboard, func(set nntpSeedImageSet) bool {
		return set.ready()
	}, captureSeedImageCache)
}

func newNntpSeedCacheWarmerWith(
	ctx context.Context,
	tempRoot string,
	dashboard *fullDashboard,
	ready func(nntpSeedImageSet) bool,
	capture nntpSeedCacheCaptureFunc,
) *nntpSeedCacheWarmer {
	return &nntpSeedCacheWarmer{
		ctx:       ctx,
		tempRoot:  tempRoot,
		dashboard: dashboard,
		ready:     ready,
		capture:   capture,
		jobs:      make(map[string]*nntpSeedCacheWarmJob),
	}
}

// start returns owner=true only to the phase whose seeded stack supplies the
// snapshot. Other phases with the same corpus continue without waiting.
func (warmer *nntpSeedCacheWarmer) start(phase *fullPhaseContext) (*nntpSeedCacheWarmJob, bool) {
	if warmer == nil || phase == nil || !nntpSeedImageCacheEnabled() {
		return nil, false
	}
	profile := strings.TrimSpace(phase.SeedProfile)
	if profile == "" {
		return nil, false
	}
	slugs := fixtureSlugsForSeedProfile(profile)
	set, err := nntpSeedImageSetForProfile(profile, slugs)
	if err != nil {
		warmer.warn(profile, "fingerprint cache corpus", err)
		return nil, false
	}
	if warmer.ready(set) {
		if warmer.dashboard != nil {
			warmer.dashboard.noteNntpCacheHit(profile)
		}
		return nil, false
	}

	warmer.mu.Lock()
	if job := warmer.jobs[set.Fingerprint]; job != nil {
		warmer.mu.Unlock()
		return job, false
	}
	job := &nntpSeedCacheWarmJob{done: make(chan struct{})}
	warmer.jobs[set.Fingerprint] = job
	warmer.mu.Unlock()

	if warmer.dashboard != nil {
		warmer.dashboard.updateNntpCache(
			set.Fingerprint,
			profile,
			0,
			nntpSeedCacheStageCount,
			"running",
			"waiting for snapshot",
		)
	}
	warmer.logf("profile=%s corpus=%s cache warm started from project=%s", profile, set.Fingerprint[:12], phase.Project)

	go func() {
		started := time.Now()
		job.err = warmer.capture(warmer.ctx, set, slugs, nntpSeedCacheCaptureConfig{
			Project:   phase.Project,
			StageRoot: filepath.Join(warmer.tempRoot, "nntp-seed-cache"),
			LockRoot:  os.TempDir(),
			OwnerPID:  os.Getpid(),
			Progress: func(current, total int, detail string) {
				warmer.logf(
					"profile=%s corpus=%s stage=%d/%d detail=%s elapsed=%s",
					profile,
					set.Fingerprint[:12],
					current,
					total,
					detail,
					time.Since(started).Round(time.Second),
				)
				if warmer.dashboard != nil {
					warmer.dashboard.updateNntpCache(
						set.Fingerprint,
						profile,
						current,
						total,
						"running",
						detail,
					)
				}
			},
		})
		status := "pass"
		detail := "cache images ready"
		if job.err != nil {
			status = "warning"
			detail = job.err.Error()
			warmer.logf(
				"profile=%s corpus=%s cache warm warning after %s: %v",
				profile,
				set.Fingerprint[:12],
				time.Since(started).Round(time.Second),
				job.err,
			)
		} else {
			warmer.logf(
				"profile=%s corpus=%s cache warm complete elapsed=%s",
				profile,
				set.Fingerprint[:12],
				time.Since(started).Round(time.Second),
			)
		}
		if warmer.dashboard != nil {
			warmer.dashboard.updateNntpCache(
				set.Fingerprint,
				profile,
				nntpSeedCacheStageCount,
				nntpSeedCacheStageCount,
				status,
				detail,
			)
		}
		close(job.done)
	}()

	return job, true
}

func (warmer *nntpSeedCacheWarmer) warn(profile, action string, err error) {
	message := fmt.Sprintf("%s: %v", action, err)
	warmer.logf("profile=%s warning=%s", profile, message)
	if warmer.dashboard != nil {
		warmer.dashboard.updateNntpCache(profile, profile, 0, 0, "warning", message)
	}
}

func (warmer *nntpSeedCacheWarmer) logf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	log.Printf("NNTP cache: %s", message)
	if strings.TrimSpace(warmer.tempRoot) == "" {
		return
	}
	warmer.logMu.Lock()
	defer warmer.logMu.Unlock()
	path := filepath.Join(warmer.tempRoot, "nntp-cache.log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = fmt.Fprintf(file, "%s %s\n", time.Now().Format(time.RFC3339), message)
}
