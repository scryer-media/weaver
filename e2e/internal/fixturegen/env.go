package fixturegen

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// stageDir holds the files an oracle is pointed at. A container's working
	// directory is set here, so a member's name in the archive is exactly its
	// path under the stage.
	stageDir = "stage"
	// outputDir holds what the recipe will publish into testdata/<slug>/.
	outputDir = "out"

	// SevenZipToolchain, PAR2Toolchain and VideoToolchain are the ids the
	// non-RAR oracles are pinned under.
	SevenZipToolchain = "sevenzip-26.02"
	PAR2Toolchain     = "par2cmdline-turbo-1.4.0"
	VideoToolchain    = "ffmpeg-7.1-ubuntu2404"

	// RAR5Writer and RAR4Writer are the RARLAB releases the general corpus is
	// written with. The direct-store family keeps the older pair its own
	// recipe has always used.
	RAR5Writer = "rarlab-7.23"
	RAR4Writer = "rarlab-6.24"

	// GeneratorID is the name every regenerated ledger entry credits.
	GeneratorID = "fixturegen"
)

// Env is one scenario's build environment: a private stage and output
// directory, the pinned toolchains, and the shared artifact cache.
type Env struct {
	// Root is the harness root (the directory holding go.mod).
	Root string
	// Work is the scenario's private working directory.
	Work string
	// Slug is the scenario directory name under testdata/.
	Slug string

	Lock      Lock
	Docker    *Docker
	Artifacts *ArtifactCache

	mu            sync.Mutex
	used          map[string]struct{}
	usedArtifacts map[string]string
}

func (env *Env) usedToolchain(id string) {
	env.mu.Lock()
	defer env.mu.Unlock()
	if env.used == nil {
		env.used = map[string]struct{}{}
	}
	env.used[id] = struct{}{}
}

// usedArtifact records an artifact this scenario consumed, and the cache
// identity it had at the time. Written into the scenario's stamp so a salted
// output — which the ledger accepts on presence alone — can still be rebuilt
// when the artifact underneath it changes.
func (env *Env) usedArtifact(name, identity string) {
	env.mu.Lock()
	defer env.mu.Unlock()
	if env.usedArtifacts == nil {
		env.usedArtifacts = map[string]string{}
	}
	env.usedArtifacts[name] = identity
}

// UsedArtifacts is the artifact-to-identity map this scenario was built from.
func (env *Env) UsedArtifacts() map[string]string {
	env.mu.Lock()
	defer env.mu.Unlock()
	out := make(map[string]string, len(env.usedArtifacts))
	for name, identity := range env.usedArtifacts {
		out[name] = identity
	}
	return out
}

// UsedToolchains lists, sorted, every pinned toolchain this scenario actually
// invoked. It is what lands in the ledger entry rather than a hand-kept list.
func (env *Env) UsedToolchains() []string {
	env.mu.Lock()
	defer env.mu.Unlock()
	ids := make([]string, 0, len(env.used))
	for id := range env.used {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// StagePath resolves a stage-relative name, creating its parent.
func (env *Env) StagePath(name string) string {
	full := filepath.Join(env.Work, stageDir, filepath.FromSlash(name))
	_ = os.MkdirAll(filepath.Dir(full), 0o755)
	return full
}

// OutputPath resolves a name in the scenario's output directory.
func (env *Env) OutputPath(name string) string {
	full := filepath.Join(env.Work, outputDir, filepath.FromSlash(name))
	_ = os.MkdirAll(filepath.Dir(full), 0o755)
	return full
}

// Outputs lists what the recipe produced, relative to the output directory.
func (env *Env) Outputs() ([]string, error) {
	root := filepath.Join(env.Work, outputDir)
	var names []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		names = append(names, filepath.ToSlash(relative))
		return nil
	})
	sort.Strings(names)
	return names, err
}

// Stage copies a cached artifact into the stage under a member name, which is
// the name the archive will record.
func (env *Env) Stage(ctx context.Context, artifact, member string) error {
	source, err := env.Artifacts.Path(ctx, env, artifact)
	if err != nil {
		return err
	}
	return CopyFile(source, env.StagePath(member))
}

// Publish copies a cached artifact straight into the output directory. Sets
// that several scenarios must agree on byte for byte — a RAR volume set and
// the PAR2 sidecars computed over it, for instance — are built once as an
// artifact and published from there.
func (env *Env) Publish(ctx context.Context, artifact, name string) error {
	source, err := env.Artifacts.Path(ctx, env, artifact)
	if err != nil {
		return err
	}
	return CopyFile(source, env.OutputPath(name))
}

// PublishAll copies every file of an artifact into the output directory under
// its own name.
func (env *Env) PublishAll(ctx context.Context, artifact string) error {
	files, err := env.Artifacts.Files(ctx, env, artifact)
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := CopyFile(file, env.OutputPath(filepath.Base(file))); err != nil {
			return err
		}
	}
	return nil
}

// StageIndexed stages one file of a multi-file artifact by position.
func (env *Env) StageIndexed(ctx context.Context, artifact string, index int, member string) error {
	files, err := env.Artifacts.Files(ctx, env, artifact)
	if err != nil {
		return err
	}
	if index >= len(files) {
		return fmt.Errorf("artifact %s has %d files, wanted index %d", artifact, len(files), index)
	}
	return CopyFile(files[index], env.StagePath(member))
}

// ArtifactPath resolves a cached artifact's primary file without copying it.
func (env *Env) ArtifactPath(ctx context.Context, artifact string) (string, error) {
	return env.Artifacts.Path(ctx, env, artifact)
}

// ArtifactFile resolves one named file of a cached artifact.
func (env *Env) ArtifactFile(ctx context.Context, artifact, name string) (string, error) {
	files, err := env.Artifacts.Files(ctx, env, artifact)
	if err != nil {
		return "", err
	}
	for _, file := range files {
		if filepath.Base(file) == name {
			return file, nil
		}
	}
	return "", fmt.Errorf("artifact %s has no file %q", artifact, name)
}

// Artifact is a payload or intermediate archive built once and reused by every
// recipe that needs those exact bytes.
type Artifact struct {
	// Name identifies the artifact to recipes.
	Name string
	// Files are the artifact's file names inside its cache directory. The
	// first is what Path returns.
	Files []string
	// Toolchains are the pinned ids the builder invokes; they are credited to
	// every scenario that consumes the artifact.
	Toolchains []string
	// Notes explains the shape for reviewers.
	Notes string
	// Resumable keeps a failed build's partial output so the next attempt can
	// carry on from it. It is set only where a step is both expensive and
	// self-checking — video encoding, which takes the better part of an hour
	// and validates every clip against a size floor. An archive artifact is
	// never resumable: a half-written volume set must be rebuilt whole.
	Resumable bool
	// Build populates the artifact's own Env output directory.
	Build func(ctx context.Context, env *Env) error
}

// ArtifactCache builds each artifact at most once per run and keeps the result
// on disk so a single-scenario rebuild still agrees with the rest of the
// corpus.
type ArtifactCache struct {
	Dir   string
	Table map[string]Artifact
	// Lock and Root are what an artifact's cache identity is computed from:
	// the toolchain pins it builds with, and the generator source that builds
	// it. Zero values fall back to name-keyed directories, which is only what
	// a test that never builds anything wants.
	Lock    Lock
	Root    string
	mu      sync.Mutex
	pending map[string]*sync.WaitGroup
	failed  map[string]error
	// identities memoises the per-artifact key so a fan-out of scenarios does
	// not re-hash the generator source once per lookup.
	identities map[string]string
}

// NewArtifactCache prepares a cache rooted at dir.
func NewArtifactCache(dir string, table map[string]Artifact) *ArtifactCache {
	return &ArtifactCache{
		Dir: dir, Table: table,
		pending: map[string]*sync.WaitGroup{}, failed: map[string]error{},
		identities: map[string]string{},
	}
}

// WithBuildIdentity supplies what the cache keys on. Called once, before use.
func (cache *ArtifactCache) WithBuildIdentity(lock Lock, root string) *ArtifactCache {
	cache.Lock = lock
	cache.Root = root
	pruneLegacyArtifactDirs(cache.Dir, cache.Table)
	return cache
}

// Identity is the cache key for one artifact: what its bytes depend on.
func (cache *ArtifactCache) Identity(name string) string {
	artifact, ok := cache.Table[name]
	if !ok {
		return ""
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cached, ok := cache.identities[name]; ok {
		return cached
	}
	identity := artifactIdentity(artifact, cache.Lock, cache.Root)
	cache.identities[name] = identity
	return identity
}

// dirFor is where an artifact's files live: keyed by identity, so a build that
// changed never reads what the old one left behind.
func (cache *ArtifactCache) dirFor(name string) string {
	identity := cache.Identity(name)
	if identity == "" {
		return filepath.Join(cache.Dir, name)
	}
	return filepath.Join(cache.Dir, name+"@"+identity)
}

// Files returns every file of an artifact, in declaration order.
func (cache *ArtifactCache) Files(ctx context.Context, env *Env, name string) ([]string, error) {
	artifact, ok := cache.Table[name]
	if !ok {
		return nil, fmt.Errorf("unknown artifact %q", name)
	}
	if err := cache.ensure(ctx, env, artifact); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(artifact.Files))
	for _, file := range artifact.Files {
		paths = append(paths, filepath.Join(cache.dirFor(name), filepath.FromSlash(file)))
	}
	return paths, nil
}

// Path returns an artifact's primary file.
func (cache *ArtifactCache) Path(ctx context.Context, env *Env, name string) (string, error) {
	files, err := cache.Files(ctx, env, name)
	if err != nil {
		return "", err
	}
	return files[0], nil
}

func (cache *ArtifactCache) ensure(ctx context.Context, env *Env, artifact Artifact) error {
	for _, id := range artifact.Toolchains {
		env.usedToolchain(id)
	}
	env.usedArtifact(artifact.Name, cache.Identity(artifact.Name))
	target := cache.dirFor(artifact.Name)
	cache.mu.Lock()
	if err, done := cache.failed[artifact.Name]; done {
		cache.mu.Unlock()
		return err
	}
	if wait, running := cache.pending[artifact.Name]; running {
		cache.mu.Unlock()
		wait.Wait()
		cache.mu.Lock()
		err := cache.failed[artifact.Name]
		cache.mu.Unlock()
		return err
	}
	if complete(target, artifact.Files) {
		cache.mu.Unlock()
		return nil
	}
	wait := &sync.WaitGroup{}
	wait.Add(1)
	cache.pending[artifact.Name] = wait
	cache.mu.Unlock()

	err := cache.build(ctx, env, artifact, target)

	cache.mu.Lock()
	cache.failed[artifact.Name] = err
	delete(cache.pending, artifact.Name)
	cache.mu.Unlock()
	wait.Done()
	return err
}

func (cache *ArtifactCache) build(ctx context.Context, env *Env, artifact Artifact, target string) error {
	work := filepath.Join(cache.Dir, ".build-"+artifact.Name+"@"+cache.Identity(artifact.Name))
	if !artifact.Resumable {
		if err := os.RemoveAll(work); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Join(work, stageDir), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(work, outputDir), 0o755); err != nil {
		return err
	}
	inner := &Env{Root: env.Root, Work: work, Slug: "artifact:" + artifact.Name, Lock: env.Lock, Docker: env.Docker, Artifacts: cache}
	if err := artifact.Build(ctx, inner); err != nil {
		return fmt.Errorf("build artifact %s: %w", artifact.Name, err)
	}
	produced, err := inner.Outputs()
	if err != nil {
		return err
	}
	missing := make([]string, 0)
	for _, want := range artifact.Files {
		found := false
		for _, got := range produced {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, want)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("artifact %s did not produce %s (it produced %s)",
			artifact.Name, strings.Join(missing, ", "), strings.Join(produced, ", "))
	}
	if err := os.RemoveAll(target); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(work, outputDir), target); err != nil {
		return err
	}
	return os.RemoveAll(work)
}

func complete(dir string, files []string) bool {
	for _, file := range files {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(file))); err != nil {
			return false
		}
	}
	return len(files) > 0
}
