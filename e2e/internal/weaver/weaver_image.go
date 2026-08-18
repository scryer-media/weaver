package weaver

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/moby/patternmatcher"
	"github.com/moby/patternmatcher/ignorefile"
)

// The builder stage checks weaver out at this path. Relative
// `[patch.crates-io]` paths in weaver's Cargo.toml resolve against the
// directory holding that manifest, so an in-repository path resolves under
// this workdir exactly as it does under the repo root on the host; anything
// that would climb out of it is refused by rejectOutOfTreePatches.
const weaverImageBuilderWorkdir = "/app"

// Bootstrap base image. It exists only to supply rustup, cargo and a Debian
// userland; the compiler that actually builds weaver is the one
// rust-toolchain.toml pins, installed explicitly below. This tag deliberately
// does not track the repo pin -- it just has to exist on Docker Hub.
const weaverImageBootstrapBase = "rust:1.96-slim-bookworm"

// Used when weaver has no readable rust-toolchain.toml. rustup would still pick
// up a toolchain file copied into the image, so this only covers the case where
// there is genuinely no pin to honour.
const weaverImageFallbackToolchain = "stable"

const weaverLocalImageTag = "weaver-e2e-weaver:local"

// The local image is reusable only when this label matches the complete set of
// inputs used by buildLocalWeaverImage. A fixed tag by itself says nothing
// about which working tree produced it.
const weaverImageFingerprintLabel = "org.scryer-media.weaver-e2e.source-fingerprint"

const weaverImageFingerprintSchema = "weaver-e2e-image-v1"

// weaverImagePlan is everything about the image build that depends on the state
// of the weaver working tree rather than on the harness.
type weaverImagePlan struct {
	// Toolchain is the channel from weaver's rust-toolchain.toml.
	Toolchain string
}

// newWeaverImagePlan reads the weaver working tree and decides how the image has
// to be built for the source that is actually there right now. The build is
// always `--locked` and the build context is always the weaver repo root: the
// harness has no notion of sibling checkouts, so a `[patch.crates-io]` entry
// that points outside the repository is refused rather than accommodated.
func newWeaverImagePlan(weaverRoot string) (weaverImagePlan, error) {
	manifestPath := filepath.Join(weaverRoot, "Cargo.toml")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return weaverImagePlan{}, fmt.Errorf("read %s: %w", manifestPath, err)
	}
	if err := rejectOutOfTreePatches(weaverRoot, string(manifest)); err != nil {
		return weaverImagePlan{}, err
	}
	return weaverImagePlan{Toolchain: weaverPinnedRustToolchain(weaverRoot)}, nil
}

// weaverPinnedRustToolchain returns the channel weaver's rust-toolchain.toml
// pins. The builder installs exactly this rather than a hardcoded version:
// the rarpar crates already refuse to build on anything older than the pin
// ("rustc 1.96.0 is not supported by the following packages: par2-rs@0.3.0
// requires rustc 1.97.1"), and the next bump must not need a harness edit.
func weaverPinnedRustToolchain(weaverRoot string) string {
	toolchainPath := filepath.Join(weaverRoot, "rust-toolchain.toml")
	raw, err := os.ReadFile(toolchainPath)
	if err != nil {
		log.Printf("warning: read %s: %v; falling back to rust %q", toolchainPath, err, weaverImageFallbackToolchain)
		return weaverImageFallbackToolchain
	}
	channel := parseRustToolchainChannel(string(raw))
	if channel == "" {
		log.Printf("warning: %s declares no [toolchain] channel; falling back to rust %q", toolchainPath, weaverImageFallbackToolchain)
		return weaverImageFallbackToolchain
	}
	return channel
}

var rustToolchainChannelPattern = regexp.MustCompile(`(?m)^\s*channel\s*=\s*["']([^"']+)["']`)

func parseRustToolchainChannel(text string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if match := rustToolchainChannelPattern.FindStringSubmatch(line); match != nil {
			return strings.TrimSpace(match[1])
		}
	}
	return ""
}

var (
	// par2-rs = { path = "../rarpar/crates/weaver-par2" }
	cargoPatchInlinePathPattern = regexp.MustCompile(`^\s*(?:"[^"]+"|'[^']+'|[A-Za-z0-9_.\-]+)\s*=\s*\{[^}]*\bpath\s*=\s*["']([^"']+)["']`)
	// path = "../rarpar/crates/weaver-par2"   (under [patch.crates-io.par2-rs])
	cargoPatchBarePathPattern = regexp.MustCompile(`^\s*path\s*=\s*["']([^"']+)["']`)
)

// cargoCratesIoPatchPaths returns the `path = "..."` values declared in a
// Cargo.toml's `[patch.crates-io]` table, in declaration order.
//
// A nil return means the manifest carries no such table. That is the intended
// end state once the rarpar crates are published, and every conditional in this
// file keys off it, so this parse has to be exact rather than "close enough":
// a false positive would add a bogus build context, a false negative would
// silently restore --locked against a patched tree and fail the build.
func cargoCratesIoPatchPaths(manifest string) []string {
	var paths []string
	seen := map[string]bool{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		paths = append(paths, value)
	}

	// Both TOML spellings of a patch entry have to be recognised. Missing one
	// would report an unpatched tree, restore --locked and fail the build; the
	// error would at least be loud, but the point is that it should not happen.
	const (
		outsidePatchTable = iota
		inPatchTable      // [patch.crates-io] with inline entries
		inPatchEntryTable // [patch.crates-io.<crate>] with a bare path key
	)
	state := outsidePatchTable

	for _, line := range strings.Split(manifest, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			key, ok := parseTomlTableHeader(trimmed)
			switch {
			case !ok:
				state = outsidePatchTable
			case len(key) == 2 && key[0] == "patch" && key[1] == "crates-io":
				state = inPatchTable
			case len(key) == 3 && key[0] == "patch" && key[1] == "crates-io":
				state = inPatchEntryTable
			default:
				state = outsidePatchTable
			}
			continue
		}
		switch state {
		case inPatchTable:
			if match := cargoPatchInlinePathPattern.FindStringSubmatch(line); match != nil {
				add(match[1])
			}
		case inPatchEntryTable:
			if match := cargoPatchBarePathPattern.FindStringSubmatch(line); match != nil {
				add(match[1])
			}
		}
	}
	return paths
}

// parseTomlTableHeader splits a `[a.b."c"]` table header into its key segments.
// Array-of-table headers ([[a]]) and anything malformed report false, which the
// caller treats as "not a patch table".
func parseTomlTableHeader(line string) ([]string, bool) {
	if strings.HasPrefix(line, "[[") {
		return nil, false
	}
	// A trailing comment after a table header is legal TOML.
	if hash := strings.Index(line, "#"); hash >= 0 {
		line = strings.TrimSpace(line[:hash])
	}
	if !strings.HasPrefix(line, "[") || !strings.HasSuffix(line, "]") {
		return nil, false
	}
	inner := strings.TrimSpace(line[1 : len(line)-1])
	if inner == "" {
		return nil, false
	}

	var segments []string
	var current strings.Builder
	var quote rune
	for _, char := range inner {
		switch {
		case quote != 0:
			if char == quote {
				quote = 0
				continue
			}
			current.WriteRune(char)
		case char == '"' || char == '\'':
			quote = char
		case char == '.':
			segments = append(segments, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteRune(char)
		}
	}
	if quote != 0 {
		return nil, false
	}
	segments = append(segments, strings.TrimSpace(current.String()))
	for _, segment := range segments {
		if segment == "" {
			return nil, false
		}
	}
	return segments, true
}

// rejectOutOfTreePatches enforces the harness's standalone-repository rule:
// every `[patch.crates-io]` path in weaver's Cargo.toml must resolve inside the
// weaver repository (and therefore inside the docker build context). A path
// that climbs out of the tree, or an absolute path, would require a checkout
// the harness cannot know about; the image build refuses so a green gate never
// depends on what happens to sit beside the repo on one machine.
func rejectOutOfTreePatches(weaverRoot string, manifest string) error {
	root, err := filepath.Abs(weaverRoot)
	if err != nil {
		return fmt.Errorf("resolve weaver root: %w", err)
	}
	root = filepath.Clean(root)
	for _, raw := range cargoCratesIoPatchPaths(manifest) {
		if filepath.IsAbs(raw) {
			return fmt.Errorf(
				"weaver Cargo.toml [patch.crates-io] path %q is absolute; the e2e image build supports only "+
					"published crates or paths inside the repository — no sibling or machine-local checkouts",
				raw,
			)
		}
		resolved := filepath.Clean(filepath.Join(root, filepath.FromSlash(raw)))
		if resolved != root && !strings.HasPrefix(resolved, root+string(filepath.Separator)) {
			return fmt.Errorf(
				"weaver Cargo.toml [patch.crates-io] path %q resolves outside the repository (%s); the e2e image "+
					"build supports only published crates or paths inside the repository — no sibling checkouts",
				raw, resolved,
			)
		}
	}
	return nil
}

func (plan weaverImagePlan) dockerfile() string {
	return fmt.Sprintf(`# syntax=docker/dockerfile:1.7
FROM %s AS builder
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev curl ca-certificates gnupg musl-tools && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*
WORKDIR %s
RUN arch="${TARGETARCH:-$(uname -m)}" && \
    case "$arch" in \
        amd64|x86_64) target="x86_64-unknown-linux-musl" ;; \
        arm64|aarch64) target="aarch64-unknown-linux-musl" ;; \
        *) echo "unsupported TARGETARCH: $arch" >&2; exit 1 ;; \
    esac && \
    rustup toolchain install %s --profile minimal --target "$target" && \
    rustup default %s && \
    echo "$target" > /tmp/weaver-target
COPY apps/weaver-web/package.json apps/weaver-web/package-lock.json ./apps/weaver-web/
RUN --mount=type=cache,target=/root/.npm \
    cd apps/weaver-web && npm ci --legacy-peer-deps
COPY . .
# Belt and braces: if rust-toolchain.toml resolves to something other than the
# channel the harness parsed, rustup installs it here and the musl target is
# added to whichever toolchain actually ends up active.
RUN rustup show active-toolchain && rustup target add "$(cat /tmp/weaver-target)"
RUN --mount=type=cache,target=/root/.npm \
    cd apps/weaver-web && npm run build
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked -p weaver --target "$(cat /tmp/weaver-target)" && \
    cp "target/$(cat /tmp/weaver-target)/release/weaver" /tmp/weaver-portable

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata util-linux wget && \
    rm -rf /var/lib/apt/lists/*
COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/runtime-select.sh /runtime-select.sh
COPY --from=builder /tmp/weaver-portable /opt/weaver/weaver-portable
RUN chmod +x /entrypoint.sh /runtime-select.sh /opt/weaver/weaver-portable && mkdir -p /config /data
EXPOSE 9090
VOLUME /config
VOLUME /data
ENV PUID=1000
ENV PGID=1000
ENTRYPOINT ["/entrypoint.sh"]
CMD ["--config", "/config", "serve", "--port", "9090"]
`,
		weaverImageBootstrapBase,
		weaverImageBuilderWorkdir,
		plan.Toolchain,
		plan.Toolchain,
	)
}

// buildArgs renders the docker CLI invocation. buildx is what plain `docker
// build` already delegates to on every Docker version this harness supports.
func (plan weaverImagePlan) buildArgs(dockerfilePath string, image string, contextDir string, fingerprint string) []string {
	// --provenance=false keeps the result a plain single-platform image rather
	// than a manifest list with an attestation, which is what plain `docker
	// build` produced before and what `docker history` and compose expect here.
	return []string{
		"buildx", "build", "--load", "--provenance=false",
		"--label", weaverImageFingerprintLabel + "=" + fingerprint,
		"-f", dockerfilePath, "-t", image,
		contextDir,
	}
}

func weaverImageFingerprint(weaverRoot string, plan weaverImagePlan) (string, error) {
	digest := sha256.New()
	writeFingerprintField(digest, weaverImageFingerprintSchema)
	writeFingerprintField(digest, plan.dockerfile())

	matcher, err := dockerIgnoreMatcher(weaverRoot)
	if err != nil {
		return "", err
	}
	if err := hashBuildContext(digest, "weaver", weaverRoot, matcher, nil); err != nil {
		return "", fmt.Errorf("fingerprint Weaver build context %s: %w", weaverRoot, err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func dockerIgnoreMatcher(root string) (*patternmatcher.PatternMatcher, error) {
	ignorePath := filepath.Join(root, ".dockerignore")
	file, err := os.Open(ignorePath)
	if os.IsNotExist(err) {
		return patternmatcher.New(nil)
	}
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", ignorePath, err)
	}
	defer file.Close()
	patterns, err := ignorefile.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", ignorePath, err)
	}
	matcher, err := patternmatcher.New(patterns)
	if err != nil {
		return nil, fmt.Errorf("compile %s: %w", ignorePath, err)
	}
	return matcher, nil
}

func hashBuildContext(
	digest io.Writer,
	prefix string,
	root string,
	ignore *patternmatcher.PatternMatcher,
	skipDir func(relative string, name string) bool,
) error {
	root = filepath.Clean(root)
	return filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		if relative == "." {
			writeFingerprintField(digest, prefix)
			return nil
		}
		relative = filepath.ToSlash(relative)
		if entry.IsDir() && skipDir != nil && skipDir(relative, entry.Name()) {
			return filepath.SkipDir
		}
		if ignore != nil {
			ignored, err := ignore.MatchesOrParentMatches(relative)
			if err != nil {
				return err
			}
			if ignored {
				if entry.IsDir() && !ignore.Exclusions() {
					return filepath.SkipDir
				}
				return nil
			}
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		writeFingerprintField(digest, relative)
		var mode [4]byte
		binary.BigEndian.PutUint32(mode[:], uint32(info.Mode()))
		_, _ = digest.Write(mode[:])
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(current)
			if err != nil {
				return err
			}
			writeFingerprintField(digest, target)
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		file, err := os.Open(current)
		if err != nil {
			return err
		}
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(info.Size()))
		_, _ = digest.Write(size[:])
		_, copyErr := io.Copy(digest, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func writeFingerprintField(digest io.Writer, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = io.WriteString(digest, value)
}

func shortFingerprint(fingerprint string) string {
	if len(fingerprint) <= 12 {
		return fingerprint
	}
	return fingerprint[:12]
}

func ensureLocalWeaverImage() error {
	if override := strings.TrimSpace(os.Getenv("E2E_WEAVER_IMAGE")); override != "" {
		return nil
	}

	weaverImageOnce.Do(func() {
		image := weaverLocalImageTag
		weaverRoot := weaverRepoPath()
		plan, err := newWeaverImagePlan(weaverRoot)
		if err != nil {
			weaverImageErr = err
			return
		}
		fingerprint, err := weaverImageFingerprint(weaverRoot, plan)
		if err != nil {
			weaverImageErr = err
			return
		}
		if !envBool("E2E_FORCE_REBUILD_WEAVER_IMAGE", false) && dockerImageLabel(image, weaverImageFingerprintLabel) == fingerprint {
			log.Printf(
				"reusing current local weaver image: %s (source fingerprint %s, built %s)",
				image, shortFingerprint(fingerprint), dockerImageCreated(image),
			)
			setEnv("E2E_WEAVER_IMAGE", image)
			return
		}

		weaverImageErr = buildLocalWeaverImage(image, weaverRoot, plan, fingerprint)
		if weaverImageErr == nil {
			setEnv("E2E_WEAVER_IMAGE", image)
		}
	})
	if weaverImageErr != nil {
		return fmt.Errorf("build local weaver image: %w", weaverImageErr)
	}
	return nil
}

func buildLocalWeaverImage(image string, weaverRoot string, plan weaverImagePlan, fingerprint string) error {
	log.Printf("building local weaver image from %s (rust %s, --locked)", weaverRoot, plan.Toolchain)
	cmd := newWeaverImageBuildCommand(image, weaverRoot, plan, fingerprint)
	return runExternalCommand(cmd, "docker build weaver image")
}

func newWeaverImageBuildCommand(image string, weaverRoot string, plan weaverImagePlan, fingerprint string) *exec.Cmd {
	cmd := exec.Command("docker", plan.buildArgs("-", image, weaverRoot, fingerprint)...)
	cmd.Dir = e2eDir()
	cmd.Stdin = strings.NewReader(plan.dockerfile())
	return cmd
}

func dockerImageCreated(image string) string {
	cmd := exec.Command("docker", "image", "inspect", "-f", "{{.Created}}", image)
	cmd.Dir = e2eDir()
	out, err := cmd.Output()
	if err != nil {
		return "unknown build date"
	}
	created := strings.TrimSpace(string(out))
	if created == "" {
		return "unknown build date"
	}
	return created
}

func dockerImageLabel(image string, label string) string {
	template := fmt.Sprintf("{{ index .Config.Labels %q }}", label)
	cmd := exec.Command("docker", "image", "inspect", "-f", template, image)
	cmd.Dir = e2eDir()
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
