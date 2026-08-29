package weaver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/fixturegen"
)

// A cache image is intentionally keyed only by the generated corpus. The
// normal E2E images may be rebuilt independently; a cache hit must still mean
// that the NNTP article bytes are exactly those from this corpus revision.
const (
	nntpSeedImageCacheEnv   = "E2E_NNTP_SEED_IMAGE_CACHE"
	nntpSeedImageCaptureEnv = "E2E_NNTP_SEED_IMAGE_CAPTURE"
	nntpSeedImageActiveEnv  = "E2E_NNTP_SEED_IMAGE_ACTIVE"
	nntpSeedImageRepository = "weaver-e2e-nntp"
	nntpSeedFixtureRoot     = "/e2e-seed-fixtures"
	nntpSeedImageLabel      = "org.scryer-media.weaver.e2e.corpus-sha256"
)

type nntpSeedImageSet struct {
	Profile     string
	Fingerprint string
	Primary     string
	Backup      string
}

func nntpSeedImageCacheEnabled() bool {
	return envBool(nntpSeedImageCacheEnv, true)
}

func nntpSeedImageCaptureEnabled() bool {
	return nntpSeedImageCacheEnabled() && envBool(nntpSeedImageCaptureEnv, true)
}

func nntpSeedImageSetForProfile(profile string, slugs []string) (nntpSeedImageSet, error) {
	fingerprint, err := nntpSeedCorpusFingerprint(profile, slugs)
	if err != nil {
		return nntpSeedImageSet{}, err
	}
	return nntpSeedImageSet{
		Profile:     profile,
		Fingerprint: fingerprint,
		Primary:     nntpSeedImageTag(profile, "primary", fingerprint),
		Backup:      nntpSeedImageTag(profile, "backup", fingerprint),
	}, nil
}

func nntpSeedImageTag(profile, role, fingerprint string) string {
	return fmt.Sprintf("%s:corpus-%s-%s-%s", nntpSeedImageRepository, profile, role, fingerprint)
}

// nntpSeedCorpusFingerprint uses the ledger and the selected scenarios rather
// than walking payload bytes. sources.json carries the generated payload
// digests, making this check quick even when the corpus itself is large.
func nntpSeedCorpusFingerprint(profile string, slugs []string) (string, error) {
	hash := sha256.New()
	writeNntpSeedFingerprintInput(hash, "format", []byte("nntp-seed-image-v2"))
	writeNntpSeedFingerprintInput(hash, "profile", []byte(profile))

	for _, relative := range []string{
		"test-corpus/sources.json",
		"test-corpus/profiles.json",
	} {
		contents, err := os.ReadFile(repoPath(relative))
		if err != nil {
			return "", fmt.Errorf("read corpus input %s: %w", relative, err)
		}
		writeNntpSeedFingerprintInput(hash, relative, contents)
	}

	for _, slug := range slugs {
		for _, relative := range []string{
			filepath.Join(slug, "scenario.json"),
			filepath.Join(slug, filepath.FromSlash(fixturegen.UUPlanFile)),
		} {
			contents, err := os.ReadFile(filepath.Join(testdataDir(), relative))
			if os.IsNotExist(err) && strings.HasSuffix(relative, filepath.FromSlash(fixturegen.UUPlanFile)) {
				continue
			}
			if err != nil {
				return "", fmt.Errorf("read fixture input %s: %w", relative, err)
			}
			writeNntpSeedFingerprintInput(hash, filepath.ToSlash(relative), contents)
		}
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func writeNntpSeedFingerprintInput(writer io.Writer, label string, contents []byte) {
	_, _ = fmt.Fprintf(writer, "%s\x00%d\x00", label, len(contents))
	_, _ = writer.Write(contents)
	_, _ = writer.Write([]byte{'\x00'})
}

func (set nntpSeedImageSet) ready() bool {
	return dockerImageExists(set.Primary) && dockerImageExists(set.Backup)
}

func (set nntpSeedImageSet) apply() {
	setEnv("E2E_NNTP_IMAGE", set.Primary)
	setEnv("E2E_NNTP2_IMAGE", set.Backup)
	setEnv(nntpSeedImageActiveEnv, "1")
}

func (set nntpSeedImageSet) applyToPhaseEnv(env map[string]string, ready bool) {
	env[nntpSeedImageActiveEnv] = "1"
	if ready {
		env["E2E_NNTP_IMAGE"] = set.Primary
		env["E2E_NNTP2_IMAGE"] = set.Backup
	}
}

func applyNntpSeedImageCacheForProfile(profile string) error {
	if !nntpSeedImageCacheEnabled() || strings.TrimSpace(profile) == "" {
		return nil
	}
	set, err := nntpSeedImageSetForProfile(profile, fixtureSlugsForSeedProfile(profile))
	if err != nil {
		return err
	}
	if set.ready() {
		set.apply()
	}
	return nil
}

// restoreSeedImageCache starts an isolated stack from the pre-seeded NNTP
// images. The article stores and generated NZBs are already baked into those
// images, so this path deliberately does no per-fixture reposting.
func restoreSeedImageCache(set nntpSeedImageSet) error {
	set.apply()
	if err := dockerComposeUp("nntp", "nntp2"); err != nil {
		return fmt.Errorf("start pre-seeded NNTP images: %w", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		return fmt.Errorf("refresh runtime ports for pre-seeded NNTP images: %w", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForTCP("localhost:"+backupNntpPort(), 30*time.Second)

	log.Printf("started pre-seeded runtime for profile=%s (corpus=%s)", set.Profile, set.Fingerprint[:12])
	return nil
}

// restoreSeededNZBBundle copies the complete generated-NZB tree from an image
// once into the requested fixture root. Full-suite phases then only read it.
func restoreSeededNZBBundle(image, destination string) error {
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return fmt.Errorf("create shared NZB fixture directory: %w", err)
	}
	create := exec.Command("docker", "create", image)
	create.Dir = e2eDir()
	containerID, err := create.Output()
	if err != nil {
		return fmt.Errorf("create pre-seeded NNTP fixture container: %w", err)
	}
	id := strings.TrimSpace(string(containerID))
	defer func() {
		remove := exec.Command("docker", "rm", "-f", id)
		remove.Dir = e2eDir()
		_ = runExternalCommand(remove, "remove pre-seeded NNTP fixture container")
	}()
	copy := exec.Command("docker", "cp", id+":"+nntpSeedFixtureRoot+"/.", destination)
	copy.Dir = e2eDir()
	if err := runExternalCommand(copy, "restore pre-seeded NZB bundle"); err != nil {
		return fmt.Errorf("restore pre-seeded NZB bundle: %w", err)
	}
	return nil
}

func seedPayloadBytes(absDir string, scenario *Scenario) (int64, error) {
	var total int64
	stagesForNyuu, err := scenarioStagesPostableFiles(absDir, scenario)
	if err != nil {
		return 0, err
	}
	if stagesForNyuu {
		staged, err := collectFixtureStagingFiles(
			absDir,
			true,
			filepath.Join(testdataDir(), "shared"),
			scenario.SharedAssets,
			testdataDir(),
			scenario.FixtureAssets,
		)
		if err != nil {
			return 0, err
		}
		for _, file := range staged {
			total += file.size
		}
	}
	plan, err := loadUUPlan(absDir)
	if err != nil {
		return 0, err
	}
	if plan != nil {
		for _, file := range plan.Files {
			total += file.Size
		}
	}
	return total, nil
}

const nntpSeedCacheStageCount = 4

type nntpSeedCacheProgressFunc func(current, total int, detail string)

type nntpSeedCacheCaptureConfig struct {
	Project          string
	FixturesDir      string
	StageRoot        string
	LockRoot         string
	OwnerPID         int
	CommitContainers bool
	Progress         nntpSeedCacheProgressFunc
}

type nntpSeedCacheCaptureOps struct {
	imageExists      func(string) bool
	resolveContainer func(context.Context, string, string) (string, error)
	usesDataMount    func(context.Context, string) (bool, error)
	stageFixtures    func(context.Context, string, string) error
	commit           func(context.Context, string, string, nntpSeedImageSet) error
	snapshot         func(context.Context, string, string) error
	build            func(context.Context, string, string, string, nntpSeedImageSet) error
	removeImage      func(context.Context, string) error
	processAlive     func(int) bool
}

func defaultNntpSeedCacheCaptureOps() nntpSeedCacheCaptureOps {
	return nntpSeedCacheCaptureOps{
		imageExists:      dockerImageExists,
		resolveContainer: dockerComposeServiceContainerIDForProject,
		usesDataMount:    dockerContainerUsesDataMount,
		stageFixtures:    stageSeededNZBsInContainer,
		commit:           commitSeededNntpImage,
		snapshot:         snapshotNntpData,
		build:            buildSeededNntpImage,
		removeImage:      removeNntpSeedImage,
		processAlive:     processAlive,
	}
}

// captureSeedImageCache bakes the already-seeded article stores and generated
// NZBs into two local images. Docker commit cannot see named-volume contents,
// so the snapshot is deliberately rebuilt from a temporary Docker build
// context instead.
func captureSeedImageCache(
	ctx context.Context,
	set nntpSeedImageSet,
	slugs []string,
	config nntpSeedCacheCaptureConfig,
) error {
	return captureSeedImageCacheWithOps(ctx, set, slugs, config, defaultNntpSeedCacheCaptureOps())
}

func captureSeedImageCacheWithOps(
	ctx context.Context,
	set nntpSeedImageSet,
	slugs []string,
	config nntpSeedCacheCaptureConfig,
	ops nntpSeedCacheCaptureOps,
) error {
	if ops.imageExists(set.Primary) && ops.imageExists(set.Backup) {
		return nil
	}
	if config.OwnerPID <= 0 {
		config.OwnerPID = os.Getpid()
	}
	if strings.TrimSpace(config.StageRoot) == "" {
		config.StageRoot = os.TempDir()
	}
	if strings.TrimSpace(config.LockRoot) == "" {
		config.LockRoot = os.TempDir()
	}

	release, acquired, err := tryAcquireNntpSeedImageLock(
		set,
		config.LockRoot,
		config.OwnerPID,
		ops.processAlive,
	)
	if err != nil {
		return err
	}
	if !acquired {
		log.Printf("pre-seeded NNTP image build already in progress for profile=%s (corpus=%s)", set.Profile, set.Fingerprint[:12])
		return nil
	}
	defer release()
	if ops.imageExists(set.Primary) && ops.imageExists(set.Backup) {
		return nil
	}

	if err := removeIncompleteNntpSeedImagePair(ctx, set, ops); err != nil {
		return err
	}

	if err := os.MkdirAll(config.StageRoot, 0o755); err != nil {
		return fmt.Errorf("create NNTP image staging root: %w", err)
	}
	stage, err := os.MkdirTemp(
		config.StageRoot,
		fmt.Sprintf("weaver-e2e-nntp-seed-image-%s-%s-", set.Profile, set.Fingerprint[:12]),
	)
	if err != nil {
		return fmt.Errorf("create NNTP image staging directory: %w", err)
	}
	defer os.RemoveAll(stage)

	started := time.Now()
	report := func(current int, detail string) {
		log.Printf(
			"NNTP cache profile=%s corpus=%s elapsed=%s %s",
			set.Profile,
			set.Fingerprint[:12],
			time.Since(started).Round(time.Second),
			detail,
		)
		if config.Progress != nil {
			config.Progress(current, nntpSeedCacheStageCount, detail)
		}
	}
	if config.CommitContainers {
		return commitSeedImageCacheFromContainers(ctx, set, slugs, config, stage, report, ops)
	}
	primaryContext := filepath.Join(stage, "primary")
	backupContext := filepath.Join(stage, "backup")

	report(0, "snapshotting primary article store")
	primaryID, err := ops.resolveContainer(ctx, config.Project, "nntp")
	if err != nil {
		return fmt.Errorf("resolve primary NNTP snapshot source: %w", err)
	}
	if err := ops.snapshot(ctx, primaryID, primaryContext); err != nil {
		return err
	}
	report(1, "snapshotting backup article store")
	backupID, err := ops.resolveContainer(ctx, config.Project, "nntp2")
	if err != nil {
		return fmt.Errorf("resolve backup NNTP snapshot source: %w", err)
	}
	if err := ops.snapshot(ctx, backupID, backupContext); err != nil {
		return err
	}
	if err := snapshotSeededNZBs(primaryContext, config.FixturesDir, slugs); err != nil {
		return err
	}
	if err := snapshotSeededNZBs(backupContext, config.FixturesDir, slugs); err != nil {
		return err
	}

	complete := false
	defer func() {
		if complete {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = ops.removeImage(cleanupCtx, set.Primary)
		_ = ops.removeImage(cleanupCtx, set.Backup)
	}()

	report(2, "building primary cache image")
	if err := ops.build(ctx, nntpSeedBaseImage("E2E_NNTP_IMAGE"), set.Primary, primaryContext, set); err != nil {
		return err
	}
	report(3, "building backup cache image")
	if err := ops.build(ctx, nntpSeedBaseImage("E2E_NNTP2_IMAGE"), set.Backup, backupContext, set); err != nil {
		return err
	}
	complete = true
	report(4, "cache images ready")
	return nil
}

func commitSeedImageCacheFromContainers(
	ctx context.Context,
	set nntpSeedImageSet,
	slugs []string,
	config nntpSeedCacheCaptureConfig,
	stage string,
	report func(int, string),
	ops nntpSeedCacheCaptureOps,
) error {
	manifestContext := filepath.Join(stage, "manifests")
	if err := snapshotSeededNZBs(manifestContext, config.FixturesDir, slugs); err != nil {
		return err
	}

	complete := false
	defer func() {
		if complete {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = ops.removeImage(cleanupCtx, set.Primary)
		_ = ops.removeImage(cleanupCtx, set.Backup)
	}()

	for index, source := range []struct {
		service string
		tag     string
		role    string
	}{
		{service: "nntp", tag: set.Primary, role: "primary"},
		{service: "nntp2", tag: set.Backup, role: "backup"},
	} {
		report(index*2, "preparing "+source.role+" cache container")
		containerID, err := ops.resolveContainer(ctx, config.Project, source.service)
		if err != nil {
			return fmt.Errorf("resolve %s NNTP cache source: %w", source.role, err)
		}
		mounted, err := ops.usesDataMount(ctx, containerID)
		if err != nil {
			return fmt.Errorf("inspect %s NNTP cache source: %w", source.role, err)
		}
		if mounted {
			return fmt.Errorf("%s NNTP cache source still mounts /data; refusing an article-free commit", source.role)
		}
		if err := ops.stageFixtures(ctx, containerID, manifestContext); err != nil {
			return fmt.Errorf("stage %s NNTP cache manifests: %w", source.role, err)
		}
		report(index*2+1, "committing "+source.role+" cache image")
		if err := ops.commit(ctx, containerID, source.tag, set); err != nil {
			return fmt.Errorf("commit %s NNTP cache image: %w", source.role, err)
		}
	}

	complete = true
	report(4, "cache images ready")
	return nil
}

func removeIncompleteNntpSeedImagePair(ctx context.Context, set nntpSeedImageSet, ops nntpSeedCacheCaptureOps) error {
	primaryExists := ops.imageExists(set.Primary)
	backupExists := ops.imageExists(set.Backup)
	if primaryExists == backupExists {
		return nil
	}
	if primaryExists {
		return ops.removeImage(ctx, set.Primary)
	}
	return ops.removeImage(ctx, set.Backup)
}

func nntpSeedBaseImage(envKey string) string {
	if image := strings.TrimSpace(os.Getenv(envKey)); image != "" {
		return image
	}
	return weaverNNTPDefaultImage
}

func dockerComposeServiceContainerIDForProject(ctx context.Context, project, service string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", project, "ps", "-q", service)
	cmd.Dir = e2eDir()
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("resolve container for service %s: %w", service, err)
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		return "", fmt.Errorf("service %s is not running", service)
	}
	return id, nil
}

func snapshotNntpData(ctx context.Context, containerID, contextDir string) error {
	dest := filepath.Join(contextDir, "data", "articles")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("create NNTP data image context: %w", err)
	}
	cmd := exec.CommandContext(ctx, "docker", "cp", containerID+":/data/articles/.", dest)
	cmd.Dir = e2eDir()
	if err := runExternalCommand(cmd, "snapshot seeded NNTP article store"); err != nil {
		return err
	}
	return nil
}

func dockerContainerUsesDataMount(ctx context.Context, containerID string) (bool, error) {
	cmd := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{json .Mounts}}", containerID)
	cmd.Dir = e2eDir()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("docker inspect %s: %w: %s", containerID, err, strings.TrimSpace(string(output)))
	}
	var mounts []struct {
		Destination string `json:"Destination"`
	}
	if err := json.Unmarshal(output, &mounts); err != nil {
		return false, fmt.Errorf("decode docker mounts for %s: %w", containerID, err)
	}
	for _, mount := range mounts {
		if filepath.Clean(mount.Destination) == "/data" {
			return true, nil
		}
	}
	return false, nil
}

func stageSeededNZBsInContainer(ctx context.Context, containerID, contextDir string) error {
	source := filepath.Join(contextDir, "e2e-seed-fixtures")
	cmd := exec.CommandContext(ctx, "docker", "cp", source, containerID+":/")
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "stage generated NZBs in seeded NNTP container")
}

func commitSeededNntpImage(ctx context.Context, containerID, tag string, set nntpSeedImageSet) error {
	cmd := exec.CommandContext(
		ctx,
		"docker", "commit", "--pause=false",
		"--change", fmt.Sprintf("LABEL %s=%s", nntpSeedImageLabel, set.Fingerprint),
		"--change", fmt.Sprintf("LABEL org.scryer-media.weaver.e2e.seed-profile=%s", set.Profile),
		containerID, tag,
	)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "commit pre-seeded NNTP image")
}

func snapshotSeededNZBs(contextDir, fixturesRoot string, slugs []string) error {
	if fixturesRoot == "" {
		fixturesRoot = fixturesDir()
	}
	for _, slug := range slugs {
		source := filepath.Join(fixturesRoot, slug, slug+".nzb")
		dest := filepath.Join(contextDir, "e2e-seed-fixtures", slug, slug+".nzb")
		if err := copyFile(source, dest); err != nil {
			return fmt.Errorf("stage generated NZB for %s: %w", slug, err)
		}
	}
	return nil
}

func buildSeededNntpImage(ctx context.Context, baseImage, tag, contextDir string, set nntpSeedImageSet) error {
	dockerfile := strings.Join([]string{
		"ARG BASE_IMAGE",
		"FROM ${BASE_IMAGE}",
		"COPY data/articles/ /data/articles/",
		"COPY e2e-seed-fixtures/ " + nntpSeedFixtureRoot + "/",
		fmt.Sprintf("LABEL %s=%q", nntpSeedImageLabel, set.Fingerprint),
		fmt.Sprintf("LABEL org.scryer-media.weaver.e2e.seed-profile=%q", set.Profile),
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(contextDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		return fmt.Errorf("write pre-seeded NNTP Dockerfile: %w", err)
	}
	cmd := exec.CommandContext(
		ctx,
		"docker", "build",
		"--build-arg", "BASE_IMAGE="+baseImage,
		"--tag", tag,
		"--label", nntpSeedImageLabel+"="+set.Fingerprint,
		contextDir,
	)
	cmd.Dir = e2eDir()
	if err := runExternalCommand(cmd, "build pre-seeded NNTP image"); err != nil {
		return fmt.Errorf("build %s: %w", tag, err)
	}
	return nil
}

func removeNntpSeedImage(ctx context.Context, tag string) error {
	cmd := exec.CommandContext(ctx, "docker", "image", "rm", tag)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "remove incomplete pre-seeded NNTP image")
}

type nntpSeedImageLockOwner struct {
	PID         int    `json:"pid"`
	Profile     string `json:"profile"`
	Fingerprint string `json:"fingerprint"`
}

func tryAcquireNntpSeedImageLock(
	set nntpSeedImageSet,
	root string,
	ownerPID int,
	alive func(int) bool,
) (func(), bool, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, false, fmt.Errorf("create pre-seeded NNTP image lock root: %w", err)
	}
	path := filepath.Join(root, "weaver-e2e-nntp-seed-image-"+set.Fingerprint+".lock")
	owner := nntpSeedImageLockOwner{PID: ownerPID, Profile: set.Profile, Fingerprint: set.Fingerprint}
	body, err := json.Marshal(owner)
	if err != nil {
		return nil, false, fmt.Errorf("encode pre-seeded NNTP image lock owner: %w", err)
	}

	for attempts := 0; attempts < 2; attempts++ {
		candidate, err := os.CreateTemp(root, filepath.Base(path)+".candidate-")
		if err != nil {
			return nil, false, fmt.Errorf("create pre-seeded NNTP image lock candidate: %w", err)
		}
		candidatePath := candidate.Name()
		if _, err := candidate.Write(body); err != nil {
			_ = candidate.Close()
			_ = os.Remove(candidatePath)
			return nil, false, fmt.Errorf("write pre-seeded NNTP image lock candidate: %w", err)
		}
		if err := candidate.Close(); err != nil {
			_ = os.Remove(candidatePath)
			return nil, false, fmt.Errorf("close pre-seeded NNTP image lock candidate: %w", err)
		}
		err = os.Link(candidatePath, path)
		_ = os.Remove(candidatePath)
		if err == nil {
			return func() { _ = os.Remove(path) }, true, nil
		}
		if !os.IsExist(err) {
			return nil, false, fmt.Errorf("acquire pre-seeded NNTP image lock: %w", err)
		}

		existingBody, readErr := os.ReadFile(path)
		var existing nntpSeedImageLockOwner
		if readErr == nil && json.Unmarshal(existingBody, &existing) == nil && alive(existing.PID) {
			return func() {}, false, nil
		}
		if err := os.RemoveAll(path); err != nil {
			return nil, false, fmt.Errorf("remove stale pre-seeded NNTP image lock: %w", err)
		}
	}
	return nil, false, fmt.Errorf("acquire pre-seeded NNTP image lock after reclaim")
}
