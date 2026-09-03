// Package fixturegen rebuilds the Weaver e2e fixture corpus from declarative
// recipes.
//
// Everything a general-purpose language can do is done in Go: HTTPS downloads,
// SHA-256 and BLAKE3 digests, deterministic payload synthesis, the zip, tar,
// gzip, DEFLATE, zstd, bzip2 and brotli containers, and every format-agnostic
// byte edit (split, concatenate, truncate, zero or overwrite a range, rename).
// The only external processes are the pinned oracle containers, because RAR,
// PAR2, 7z and video encoding exist only as binaries:
//
//   - RAR archives are written exclusively by RARLAB's own `rar`. UnRAR's
//     licence forbids using UnRAR code to create RAR archives, so no Go code,
//     no third-party library and no hand-assembled header ever authors or
//     edits a RAR structure here. Go may only move the resulting bytes around.
//   - PAR2 recovery material comes from par2cmdline-turbo.
//   - 7z containers come from the official 7-Zip console binary.
//   - Video comes from the digest-pinned FFmpeg image.
//   - uuencoding, and the split across multi-part postings, come from
//     UUDeview's uuenview — and every encoding is decoded back by uudeview
//     before it is published, so no fixture can ship a shape a real decoder
//     rejects.
//
// Every one of those is pinned by URL and SHA-256, or by image digest, in
// test-corpus/toolchains.json.
package fixturegen

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// Toolchain is one pinned oracle: a container image built from a Dockerfile in
// this package over an archive fixed by URL and SHA-256, or an image already
// fixed by its own digest.
type Toolchain struct {
	ID         string `json:"id"`
	Image      string `json:"image"`
	Platform   string `json:"platform"`
	URL        string `json:"url"`
	SHA256     string `json:"sha256"`
	Binary     string `json:"binary"`
	Dockerfile string `json:"dockerfile"`
	// UTF8Locale runs the container under C.UTF-8. Without it RARLAB's writer
	// cannot decode a non-ASCII member name and stores each byte as a
	// private-use codepoint, which would make the unicode-filename fixture
	// mojibake rather than Unicode. It is off for the 3.x/4.x/5.0 releases:
	// those ship a 32-bit x86 binary, and glibc's locale loader aborts inside
	// qemu-i386 when it is asked for C.UTF-8. Those writers only ever see
	// ASCII member names.
	UTF8Locale bool `json:"utf8_locale"`
}

// GoWriter is a format Go itself writes. It has no image; the pin is the Go
// toolchain and, for third-party writers, the module version in go.mod.
type GoWriter struct {
	ID      string `json:"id"`
	Go      string `json:"go"`
	Module  string `json:"module"`
	Version string `json:"version"`
	Package string `json:"package"`
	Writes  string `json:"writes"`
}

// Lock is test-corpus/toolchains.json in the shape the generator needs.
type Lock struct {
	DockerBase    string      `json:"docker_base"`
	RARWriters    []Toolchain `json:"rar_writers"`
	VideoEncoder  Toolchain   `json:"video_encoder"`
	PAR2Generator Toolchain   `json:"par2_generator"`
	UUCodec       Toolchain   `json:"uu_codec"`
	Archivers     []Toolchain `json:"archivers"`
	GoWriters     []GoWriter  `json:"go_writers"`
}

// LoadLock reads and validates the toolchain lock under root.
func LoadLock(root string) (Lock, error) {
	contents, err := os.ReadFile(filepath.Join(root, "test-corpus", "toolchains.json"))
	if err != nil {
		return Lock{}, fmt.Errorf("read toolchain lock: %w", err)
	}
	var lock Lock
	if err := json.Unmarshal(contents, &lock); err != nil {
		return Lock{}, fmt.Errorf("decode toolchain lock: %w", err)
	}
	seen := map[string]struct{}{}
	for _, toolchain := range lock.all() {
		if err := toolchain.validate(); err != nil {
			return Lock{}, err
		}
		if _, duplicate := seen[toolchain.ID]; duplicate {
			return Lock{}, fmt.Errorf("toolchain lock has duplicate id %q", toolchain.ID)
		}
		seen[toolchain.ID] = struct{}{}
	}
	for _, writer := range lock.GoWriters {
		if strings.TrimSpace(writer.ID) == "" {
			return Lock{}, fmt.Errorf("every go_writers entry needs an id")
		}
		if _, duplicate := seen[writer.ID]; duplicate {
			return Lock{}, fmt.Errorf("toolchain lock has duplicate id %q", writer.ID)
		}
		seen[writer.ID] = struct{}{}
	}
	return lock, nil
}

func (lock Lock) all() []Toolchain {
	toolchains := make([]Toolchain, 0, len(lock.RARWriters)+len(lock.Archivers)+3)
	toolchains = append(toolchains, lock.RARWriters...)
	toolchains = append(toolchains, lock.Archivers...)
	toolchains = append(toolchains, lock.VideoEncoder, lock.PAR2Generator, lock.UUCodec)
	return toolchains
}

// Find resolves a container-backed toolchain id.
func (lock Lock) Find(id string) (Toolchain, error) {
	for _, toolchain := range lock.all() {
		if toolchain.ID == id {
			return toolchain, nil
		}
	}
	return Toolchain{}, fmt.Errorf("toolchain %q is not pinned in test-corpus/toolchains.json", id)
}

// IDs lists every pinned id, container-backed and Go alike.
func (lock Lock) IDs() []string {
	ids := make([]string, 0, len(lock.all())+len(lock.GoWriters))
	for _, toolchain := range lock.all() {
		ids = append(ids, toolchain.ID)
	}
	for _, writer := range lock.GoWriters {
		ids = append(ids, writer.ID)
	}
	return ids
}

func (toolchain Toolchain) validate() error {
	if strings.TrimSpace(toolchain.ID) == "" || strings.TrimSpace(toolchain.Image) == "" {
		return fmt.Errorf("every pinned toolchain needs an id and an image")
	}
	if toolchain.Platform == "" {
		return fmt.Errorf("toolchain %q has no platform", toolchain.ID)
	}
	if toolchain.URL == "" {
		// An image pinned by its own digest carries no source archive.
		if !strings.Contains(toolchain.Image, "@sha256:") {
			return fmt.Errorf("toolchain %q must pin either a source URL or an image digest", toolchain.ID)
		}
		return nil
	}
	parsed, err := url.Parse(toolchain.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("toolchain %q must use an https URL", toolchain.ID)
	}
	if len(toolchain.SHA256) != 64 || strings.Trim(toolchain.SHA256, "0123456789abcdefABCDEF") != "" {
		return fmt.Errorf("toolchain %q has an invalid SHA-256", toolchain.ID)
	}
	if toolchain.Dockerfile == "" {
		return fmt.Errorf("toolchain %q must name the Dockerfile that installs it", toolchain.ID)
	}
	return nil
}

// buildArgs maps a toolchain onto the build arguments its Dockerfile declares.
func (toolchain Toolchain) buildArgs() []string {
	switch {
	case strings.Contains(toolchain.Dockerfile, "/rarlab/"):
		return []string{"RAR_URL=" + toolchain.URL, "RAR_SHA256=" + toolchain.SHA256, "RAR_BINARY=" + toolchain.Binary}
	case strings.Contains(toolchain.Dockerfile, "/par2/"):
		return []string{"PAR2_URL=" + toolchain.URL, "PAR2_SHA256=" + toolchain.SHA256}
	case strings.Contains(toolchain.Dockerfile, "/sevenzip/"):
		return []string{"SEVENZIP_URL=" + toolchain.URL, "SEVENZIP_SHA256=" + toolchain.SHA256}
	case strings.Contains(toolchain.Dockerfile, "/uudeview/"):
		return []string{"UUDEVIEW_URL=" + toolchain.URL, "UUDEVIEW_SHA256=" + toolchain.SHA256}
	default:
		return nil
	}
}

// Docker drives the pinned oracle images. Image preparation is memoised, so a
// parallel run builds each image once.
type Docker struct {
	Binary   string
	Root     string
	Verbose  bool
	prepared sync.Map
}

// Prepare makes a pinned image available: an image pinned by digest is pulled,
// an image built from one of this package's Dockerfiles has its source archive
// fetched and digest-checked in Go before `docker build` verifies it again.
func (docker *Docker) Prepare(ctx context.Context, toolchain Toolchain) error {
	// One image, one build, however many scenarios ask for it at once: two
	// concurrent `docker build` runs against the same tag would race on the
	// tagging step.
	entry, _ := docker.prepared.LoadOrStore(toolchain.ID, &imagePreparation{})
	preparation := entry.(*imagePreparation)
	preparation.once.Do(func() { preparation.err = docker.prepare(ctx, toolchain) })
	return preparation.err
}

type imagePreparation struct {
	once sync.Once
	err  error
}

func (docker *Docker) prepare(ctx context.Context, toolchain Toolchain) error {
	if toolchain.URL == "" {
		if err := docker.run(ctx, "pull", "--platform", toolchain.Platform, toolchain.Image); err != nil {
			return fmt.Errorf("pull %s: %w", toolchain.ID, err)
		}
		return nil
	}
	if err := VerifyPin(ctx, toolchain.URL, toolchain.SHA256); err != nil {
		return fmt.Errorf("toolchain %s: %w", toolchain.ID, err)
	}
	dockerfile := filepath.Join(docker.Root, filepath.FromSlash(toolchain.Dockerfile))
	args := []string{"build", "--platform", toolchain.Platform, "--tag", toolchain.Image, "--file", dockerfile}
	for _, argument := range toolchain.buildArgs() {
		args = append(args, "--build-arg", argument)
	}
	args = append(args, filepath.Dir(dockerfile))
	if err := docker.run(ctx, args...); err != nil {
		return fmt.Errorf("build %s: %w", toolchain.ID, err)
	}
	return nil
}

// Run executes the image's entrypoint with mount bound at /work and the
// container's working directory at /work/<relative>. Nothing is passed through
// a shell: the argument vector is the command.
func (docker *Docker) Run(ctx context.Context, toolchain Toolchain, mount, relative string, arguments ...string) error {
	args, err := docker.containerArgs(toolchain, mount, relative, false)
	if err != nil {
		return err
	}
	return docker.run(ctx, append(args, arguments...)...)
}

// Capture is Run over a read-only mount with the oracle's output returned, for
// listings and verification.
func (docker *Docker) Capture(ctx context.Context, toolchain Toolchain, mount, relative string, arguments ...string) (string, error) {
	args, err := docker.containerArgs(toolchain, mount, relative, true)
	if err != nil {
		return "", err
	}
	command := exec.CommandContext(ctx, docker.binary(), append(args, arguments...)...)
	output, err := command.CombinedOutput()
	return string(output), err
}

func (docker *Docker) containerArgs(toolchain Toolchain, mount, relative string, readOnly bool) ([]string, error) {
	absolute, err := filepath.Abs(mount)
	if err != nil {
		return nil, err
	}
	bind := "type=bind,src=" + absolute + ",dst=/work"
	if readOnly {
		bind += ",readonly"
	}
	workdir := "/work"
	if relative != "" {
		workdir += "/" + filepath.ToSlash(relative)
	}
	args := []string{
		"run", "--rm", "--platform", toolchain.Platform,
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"--mount", bind,
		"--workdir", workdir,
	}
	if toolchain.UTF8Locale {
		args = append(args, "--env", "LANG=C.UTF-8", "--env", "LC_ALL=C.UTF-8")
	}
	return append(args, toolchain.Image), nil
}

func (docker *Docker) binary() string {
	if docker.Binary != "" {
		return docker.Binary
	}
	return "docker"
}

func (docker *Docker) run(ctx context.Context, args ...string) error {
	command := exec.CommandContext(ctx, docker.binary(), args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w\n%s", docker.binary(), strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	if docker.Verbose && len(output) > 0 {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(string(output)))
	}
	return nil
}

// VerifyPin streams an https URL through SHA-256 and fails unless it matches
// the pin. It is the Go half of the two-sided check: the Dockerfile repeats it
// inside the build so a changed upstream download can never be installed.
func VerifyPin(ctx context.Context, source, want string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", source, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: HTTP %d", source, response.StatusCode)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, response.Body); err != nil {
		return fmt.Errorf("read %s: %w", source, err)
	}
	got := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(got, want) {
		return fmt.Errorf("%s has SHA-256 %s, the lock pins %s", source, got, want)
	}
	return nil
}

// Pin is the immutable identity of a toolchain: the archive digest it installs
// from, or its image when it has none. Used in the artifact cache key so a
// toolchain bump invalidates everything built with the old one.
func (lock Lock) Pin(id string) string {
	toolchain, err := lock.Find(id)
	if err != nil {
		// A go writer, or an id the lock does not carry. Its own version lives
		// in the id itself (`go-klauspost-zstd@v1.19.2`), so the id is the pin.
		return id
	}
	if toolchain.SHA256 != "" {
		return toolchain.SHA256
	}
	return toolchain.Image
}
