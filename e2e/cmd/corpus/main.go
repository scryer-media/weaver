// Command corpus manages the harness's signed, content-addressed fixture
// corpus: it builds and verifies the manifest, hydrates fixtures from the
// published bucket, and (in the publish workflow) signs and uploads a corpus
// revision.
//
//	go run ./cmd/corpus ensure --profile functional
//	go run ./cmd/corpus verify --all-present --offline
//	go run ./cmd/corpus hydrate --profile functional
//	go run ./cmd/corpus build --out target/test-corpus/build
//
// Exit codes: 0 success, 1 findings or failure, 2 usage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
	"github.com/scryer-media/weaver/e2e/internal/fixturegen"
)

const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

// moduleLine identifies the harness's own go.mod, so root discovery cannot
// wander into a parent module.
const moduleLine = "module github.com/scryer-media/weaver/e2e"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:]))
}

func run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		usage()
		return exitUsage
	}
	command, rest := args[0], args[1:]
	switch command {
	case "build":
		return dispatch(buildCommand(rest))
	case "verify":
		return dispatch(verifyCommand(ctx, rest))
	case "fetch", "hydrate":
		return dispatch(fetchCommand(ctx, command, rest))
	case "ensure":
		return dispatch(ensureCommand(ctx, rest))
	case "sign":
		return dispatch(signCommand(ctx, rest))
	case "publish":
		return dispatch(publishCommand(ctx, rest))
	case "profiles":
		return dispatch(profilesCommand(rest))
	case "-h", "--help", "help":
		usage()
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", command)
		usage()
		return exitUsage
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `corpus — the harness fixture corpus

  build     --out <dir> [--update-ledger]      build the manifest and provenance from the tree
  verify    [--all-present] [--require-signature] [--offline]
                                               check ledger, tree, lock and signature
  ensure    --profile <name> | --slug <slug> [--no-fetch] [--no-generate] [--quick]
                                               make the fixtures present: reuse what matches the
                                               ledger, fetch the rest from the published corpus,
                                               generate only what is still missing
  hydrate   --profile <name> [--profile …]     fetch the named profiles from the published corpus
  fetch     --profile <name> [--profile …]     the same thing, under its transport name
  sign      --dir <build-dir>                  cosign the manifest and provenance (keyless)
  publish   --dir <build-dir> [--dry-run]      upload a corpus revision and print the lock entry
  profiles                                     list the profiles and what they resolve to

Every command accepts --root <dir> to point at a harness checkout other than
the one containing the working directory.
`)
}

// dispatch turns a command's error into an exit code: a usage error is 2,
// anything else is 1.
func dispatch(err error) int {
	if err == nil {
		return exitOK
	}
	if errors.Is(err, flag.ErrHelp) {
		return exitOK
	}
	var usageErr *usageError
	if errors.As(err, &usageErr) {
		fmt.Fprintf(os.Stderr, "error: %v\n", usageErr.cause)
		return exitUsage
	}
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	return exitFailure
}

type usageError struct{ cause error }

func (err *usageError) Error() string { return err.cause.Error() }
func (err *usageError) Unwrap() error { return err.cause }

func badUsage(format string, args ...any) error {
	return &usageError{cause: fmt.Errorf(format, args...)}
}

// repeatedFlag collects a flag given more than once (--profile a --profile b).
type repeatedFlag []string

func (values *repeatedFlag) String() string { return strings.Join(*values, ",") }

func (values *repeatedFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("empty value")
	}
	*values = append(*values, value)
	return nil
}

func newFlagSet(name string) (*flag.FlagSet, *string) {
	flags := flag.NewFlagSet("corpus "+name, flag.ContinueOnError)
	root := flags.String("root", "", "harness root (the directory holding go.mod); defaults to the enclosing checkout")
	return flags, root
}

func parse(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return flag.ErrHelp
		}
		return &usageError{cause: err}
	}
	if flags.NArg() > 0 {
		return badUsage("unexpected argument %q", flags.Arg(0))
	}
	return nil
}

// harnessRoot finds the directory holding the harness's go.mod, walking up
// from the working directory and then from the executable.
func harnessRoot(override string) (string, error) {
	if override != "" {
		absolute, err := filepath.Abs(override)
		if err != nil {
			return "", err
		}
		if !isHarnessRoot(absolute) {
			return "", fmt.Errorf("--root %s does not hold the harness go.mod", absolute)
		}
		return absolute, nil
	}
	var starts []string
	if working, err := os.Getwd(); err == nil {
		starts = append(starts, working)
	}
	if executable, err := os.Executable(); err == nil {
		starts = append(starts, filepath.Dir(executable))
	}
	for _, start := range starts {
		if root, ok := walkUp(start); ok {
			return root, nil
		}
	}
	return "", errors.New("no harness go.mod found above the working directory; pass --root")
}

func walkUp(start string) (string, bool) {
	directory, err := filepath.Abs(start)
	if err != nil {
		return "", false
	}
	for {
		if isHarnessRoot(directory) {
			return directory, true
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", false
		}
		directory = parent
	}
}

func isHarnessRoot(directory string) bool {
	contents, err := os.ReadFile(filepath.Join(directory, "go.mod"))
	if err != nil {
		return false
	}
	return strings.Contains(string(contents), moduleLine)
}

// loaded is everything the checked-in corpus files describe.
type loaded struct {
	root       string
	ledger     *corpus.Ledger
	toolchains corpus.ToolchainLock
	profiles   *corpus.Profiles
	lock       *corpus.Lock
}

func load(rootFlag string) (*loaded, error) {
	root, err := harnessRoot(rootFlag)
	if err != nil {
		return nil, err
	}
	ledger, toolchains, err := corpus.LoadLedger(root)
	if err != nil {
		return nil, err
	}
	profiles, err := corpus.LoadProfiles(root)
	if err != nil {
		return nil, err
	}
	lock, err := corpus.LoadLock(root)
	if err != nil {
		return nil, err
	}
	return &loaded{root: root, ledger: ledger, toolchains: toolchains, profiles: profiles, lock: lock}, nil
}

// ---------------------------------------------------------------- build ----

func buildCommand(args []string) error {
	flags, root := newFlagSet("build")
	out := flags.String("out", "", "directory to write manifest.json, manifest.blake3 and provenance.json into")
	updateLedger := flags.Bool("update-ledger", false,
		"refresh the sizes and digests of paths already in the ledger (never adds or removes entries)")
	if err := parse(flags, args); err != nil {
		return err
	}
	if strings.TrimSpace(*out) == "" {
		return badUsage("build needs --out <dir>")
	}
	state, err := load(*root)
	if err != nil {
		return err
	}

	// A build reads the whole tree: every listed path must be there, and
	// nothing unlisted may be, or the manifest would describe a corpus the
	// harness cannot reproduce.
	digests := make(map[string]corpus.FileDigest, len(state.ledger.Files))
	var missing, changed []string
	for _, entry := range state.ledger.Files {
		digest, err := corpus.DigestFile(corpus.HostPath(state.root, entry.Path))
		if os.IsNotExist(err) {
			missing = append(missing, entry.Path)
			continue
		}
		if err != nil {
			return err
		}
		digests[entry.Path] = digest
		if digest.BLAKE3 != entry.BLAKE3 || digest.Size != entry.Size {
			changed = append(changed, fmt.Sprintf("%s: %s/%d bytes on disk, ledger says %s/%d",
				entry.Path, digest.BLAKE3, digest.Size, entry.BLAKE3, entry.Size))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("a build describes the whole corpus, and %d ledger paths are not in the tree:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
	report, err := corpus.VerifyTree(state.root, state.ledger, corpus.VerifyOptions{AllPresent: true})
	if err != nil {
		return err
	}
	if len(report.Unledgered) > 0 {
		sort.Strings(report.Unledgered)
		return fmt.Errorf("%d fixture files are not in %s; adding an entry is a provenance decision, so edit the ledger by hand:\n  %s",
			len(report.Unledgered), corpus.LedgerFile, strings.Join(report.Unledgered, "\n  "))
	}

	if len(changed) > 0 {
		sort.Strings(changed)
		if !*updateLedger {
			return fmt.Errorf("%d fixtures differ from the ledger; regeneration is a corpus revision, so re-run with --update-ledger:\n  %s",
				len(changed), strings.Join(changed, "\n  "))
		}
		for index := range state.ledger.Files {
			entry := &state.ledger.Files[index]
			digest := digests[entry.Path]
			entry.BLAKE3, entry.Size = digest.BLAKE3, digest.Size
		}
		if err := state.ledger.Save(state.root); err != nil {
			return err
		}
		fmt.Printf("updated %d ledger entries:\n  %s\n", len(changed), strings.Join(changed, "\n  "))
	}

	manifest, err := corpus.BuildManifest(state.ledger, state.profiles, state.toolchains)
	if err != nil {
		return err
	}
	contents, err := manifest.Encode()
	if err != nil {
		return err
	}
	digest := corpus.DigestBytes(contents)
	provenance := corpus.NewProvenance(digest, state.toolchains.BLAKE3, time.Now())
	provenanceBytes, err := json.MarshalIndent(provenance, "", "  ")
	if err != nil {
		return err
	}
	provenanceBytes = append(provenanceBytes, '\n')

	outDir, err := filepath.Abs(*out)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	for name, document := range map[string][]byte{
		corpus.BuildManifestFile:       contents,
		corpus.BuildManifestDigestFile: []byte(digest + "\n"),
		corpus.BuildProvenanceFile:     provenanceBytes,
	} {
		if err := os.WriteFile(filepath.Join(outDir, name), document, 0o644); err != nil {
			return err
		}
	}

	blocked := state.ledger.Blocked()
	fmt.Printf("manifest %s\n", digest)
	fmt.Printf("  %d files, %d profiles, toolchain lock %s\n", len(manifest.Files), len(manifest.Profiles), state.toolchains.BLAKE3)
	fmt.Printf("  %d blocked entries (publication is refused while any remain)\n", len(blocked))
	fmt.Printf("  written to %s\n", outDir)
	return nil
}

// --------------------------------------------------------------- verify ----

func verifyCommand(ctx context.Context, args []string) error {
	flags, root := newFlagSet("verify")
	allPresent := flags.Bool("all-present", false, "require every ledger path to exist in the tree")
	requireSignature := flags.Bool("require-signature", false, "fail unless the published manifest's Sigstore bundle verifies")
	offline := flags.Bool("offline", false, "check the ledger, the tree and the lock without any network access")
	if err := parse(flags, args); err != nil {
		return err
	}
	if *offline && *requireSignature {
		return badUsage("--offline and --require-signature contradict each other: a signature check needs the published bundle")
	}
	state, err := load(*root)
	if err != nil {
		return err
	}
	options := corpus.VerifyOptions{AllPresent: *allPresent}
	report, err := corpus.VerifyTree(state.root, state.ledger, options)
	if err != nil {
		return err
	}
	fmt.Printf("%d of %d ledger paths present and matching\n", report.Present, len(state.ledger.Files))
	if len(report.Missing) > 0 && !*allPresent {
		fmt.Printf("%d not hydrated (not a failure without --all-present)\n", len(report.Missing))
	}
	// Blocked entries are the generator backlog. They verify like any other
	// file; what they block is publication, not verification.
	if len(report.Blocked) > 0 {
		fmt.Printf("%d blocked entries — no generator yet, so publication is refused:\n", len(report.Blocked))
		for _, path := range report.Blocked {
			fmt.Printf("  %s\n", path)
		}
	}
	if err := report.Err(options); err != nil {
		return err
	}

	if !state.lock.Pinned() {
		fmt.Printf("%s pins no manifest yet: nothing published, nothing to fetch\n", corpus.LockFile)
		return nil
	}
	if err := corpus.VerifyLock(state.root, state.ledger, state.profiles, state.lock, state.toolchains); err != nil {
		return err
	}
	fmt.Printf("the manifest this checkout produces is the one %s pins (%s)\n", corpus.LockFile, state.lock.Manifest.BLAKE3)
	if *offline {
		return nil
	}
	if err := corpus.VerifyPublished(ctx, state.lock, corpus.PublishedOptions{RequireSignature: *requireSignature}); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------- fetch ----

func fetchCommand(ctx context.Context, name string, args []string) error {
	flags, root := newFlagSet(name)
	var profiles repeatedFlag
	flags.Var(&profiles, "profile", "profile to hydrate; repeat for more than one")
	concurrency := flags.Int("concurrency", corpus.DefaultConcurrency, "in-flight downloads")
	if err := parse(flags, args); err != nil {
		return err
	}
	if len(profiles) == 0 {
		return badUsage("%s needs at least one --profile (run `corpus profiles` to list them)", name)
	}
	state, err := load(*root)
	if err != nil {
		return err
	}
	// Fail on an unknown profile before touching the network; the manifest
	// freezes the same names.
	for _, profile := range profiles {
		if _, err := state.profiles.Resolve(profile, state.ledger.Paths()); err != nil {
			return err
		}
	}
	err = corpus.Fetch(ctx, state.root, state.lock, profiles, corpus.FetchOptions{
		Concurrency: *concurrency,
		Progress:    func(line string) { fmt.Println(line) },
	})
	if errors.Is(err, corpus.ErrNotPinned) {
		return fmt.Errorf(
			"%w\n  Until then the fixtures come from a generator run: dispatch the e2e-corpus-publish workflow from main to publish the first revision, then pin it in %s through a reviewed PR",
			err, corpus.LockFile)
	}
	return err
}

// --------------------------------------------------------------- ensure ----

func ensureCommand(ctx context.Context, args []string) error {
	flags, root := newFlagSet("ensure")
	var profiles, slugs repeatedFlag
	flags.Var(&profiles, "profile", "corpus profile to make present; repeat for more than one")
	flags.Var(&slugs, "slug", "scenario directory to make present; repeat for more than one")
	noFetch := flags.Bool("no-fetch", false, "do not consult the published corpus; generate straight away")
	noGenerate := flags.Bool("no-generate", false, "stop after the fetch; anything still missing is an error")
	quick := flags.Bool("quick", false, "trust a present file by size alone instead of re-hashing it")
	workers := flags.Int("workers", 4, "concurrent scenario generation")
	concurrency := flags.Int("concurrency", corpus.DefaultConcurrency, "in-flight downloads")
	verbose := flags.Bool("verbose", false, "echo oracle output during generation")
	if err := parse(flags, args); err != nil {
		return err
	}
	if len(profiles) == 0 && len(slugs) == 0 {
		return badUsage("ensure needs at least one --profile or --slug (run `corpus profiles` to list the profiles)")
	}
	state, err := load(*root)
	if err != nil {
		return err
	}
	var paths []string
	for _, slug := range slugs {
		owned := fixturegen.ScenarioPaths(state.ledger, slug)
		if len(owned) == 0 {
			if reason, only := fixturegen.ScenarioOnly[slug]; only {
				return fmt.Errorf("%s owns no fixture bytes: it %s", slug, reason)
			}
			return fmt.Errorf("no ledger path is under testdata/%s/", slug)
		}
		paths = append(paths, owned...)
	}
	report, err := fixturegen.Ensure(ctx, fixturegen.EnsureConfig{
		Root:        state.root,
		Profiles:    profiles,
		Paths:       paths,
		NoFetch:     *noFetch,
		NoGenerate:  *noGenerate,
		Digest:      !*quick,
		Workers:     *workers,
		Concurrency: *concurrency,
		Verbose:     *verbose,
		Log:         os.Stdout,
	})
	fmt.Printf("ensure: %d wanted, %d present, %d fetched, %d generated",
		len(report.Wanted), len(report.Present), len(report.Fetched), len(report.Generated))
	if len(report.GeneratedSlugs) > 0 {
		fmt.Printf(" (%s)", strings.Join(report.GeneratedSlugs, ", "))
	}
	fmt.Println()
	if report.LedgerChanged {
		fmt.Printf("the tree is now a local corpus revision: %s carries the regenerated digests; publish and pin before committing that change\n", corpus.LedgerFile)
	}
	return err
}

// ----------------------------------------------------------------- sign ----

func signCommand(ctx context.Context, args []string) error {
	flags, _ := newFlagSet("sign")
	dir := flags.String("dir", "", "build directory produced by `corpus build`")
	if err := parse(flags, args); err != nil {
		return err
	}
	if strings.TrimSpace(*dir) == "" {
		return badUsage("sign needs --dir <build-dir>")
	}
	if !corpus.CosignAvailable() {
		return errors.New("cosign is not on PATH; signing is keyless Sigstore and cosign is the only signer")
	}
	for _, name := range []string{corpus.BuildManifestFile, corpus.BuildProvenanceFile} {
		path := filepath.Join(*dir, name)
		bundle, err := corpus.SignBlob(ctx, path)
		if err != nil {
			return err
		}
		fmt.Printf("signed %s -> %s\n", path, bundle)
	}
	return nil
}

// -------------------------------------------------------------- publish ----

func publishCommand(ctx context.Context, args []string) error {
	flags, root := newFlagSet("publish")
	dir := flags.String("dir", "", "build directory produced by `corpus build` and signed by `corpus sign`")
	baseURL := flags.String("base-url", os.Getenv("R2_CORPUS_PUBLIC_URL"), "public read base recorded in the lock entry")
	endpoint := flags.String("s3-endpoint", os.Getenv("R2_CORPUS_S3_ENDPOINT"), "bucket S3 endpoint, https://<account>.r2.cloudflarestorage.com")
	bucket := flags.String("bucket", os.Getenv("R2_CORPUS_BUCKET"), "corpus bucket name")
	dryRun := flags.Bool("dry-run", false, "report what would be uploaded without uploading anything")
	if err := parse(flags, args); err != nil {
		return err
	}
	if strings.TrimSpace(*dir) == "" {
		return badUsage("publish needs --dir <build-dir>")
	}
	state, err := load(*root)
	if err != nil {
		return err
	}
	publisher := &corpus.Publisher{
		Endpoint:        strings.TrimSpace(*endpoint),
		Bucket:          strings.TrimSpace(*bucket),
		BaseURL:         strings.TrimRight(strings.TrimSpace(*baseURL), "/"),
		AccessKeyID:     os.Getenv("R2_CORPUS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("R2_CORPUS_SECRET_ACCESS_KEY"),
	}
	lock, err := publisher.Publish(ctx, state.root, *dir, corpus.PublishOptions{
		DryRun:   *dryRun,
		Progress: func(line string) { fmt.Fprintln(os.Stderr, line) },
	})
	if err != nil {
		return err
	}
	entry, err := lock.Render()
	if err != nil {
		return err
	}
	// The lock entry goes to stdout so a workflow can capture it; everything
	// else went to stderr.
	if err := os.WriteFile(filepath.Join(*dir, corpus.BuildLockFile), entry, 0o644); err != nil {
		return err
	}
	fmt.Print(string(entry))
	return nil
}

// ------------------------------------------------------------- profiles ----

func profilesCommand(args []string) error {
	flags, root := newFlagSet("profiles")
	if err := parse(flags, args); err != nil {
		return err
	}
	state, err := load(*root)
	if err != nil {
		return err
	}
	paths := state.ledger.Paths()
	blocked := map[string]struct{}{}
	for _, entry := range state.ledger.Blocked() {
		blocked[entry.Path] = struct{}{}
	}
	sizes := map[string]int64{}
	for _, entry := range state.ledger.Files {
		sizes[entry.Path] = entry.Size
	}
	for _, name := range state.profiles.Names() {
		members, err := state.profiles.Resolve(name, paths)
		if err != nil {
			return err
		}
		var bytes int64
		blockedCount := 0
		for _, member := range members {
			bytes += sizes[member]
			if _, ok := blocked[member]; ok {
				blockedCount++
			}
		}
		fmt.Printf("%-14s %4d files  %8.1f MiB  %4d blocked  %s\n",
			name, len(members), float64(bytes)/(1<<20), blockedCount, state.profiles.Profiles[name].Description)
	}
	return nil
}
