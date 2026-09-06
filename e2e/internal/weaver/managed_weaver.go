package weaver

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func findWeaverBin() string {
	if configured := strings.TrimSpace(os.Getenv("WEAVER_BIN")); configured != "" {
		return absolutePath(configured)
	}
	bin, err := ensureE2EWeaverBinary()
	if err != nil {
		log.Fatalf("build e2e weaver binary: %v", err)
	}
	return bin
}

// rustupCargo returns the cargo the weaver build should use: rustup's shim,
// by absolute path, whenever one is installed.
//
// The shim is what reads `rust-toolchain.toml`. A bare `cargo` only reaches it
// if the shim directory wins on PATH — and it does not have to. A toolchain's
// own bin directory placed ahead of it resolves `cargo` straight to that
// toolchain's real binary, which has no idea the repository pinned anything,
// and no amount of `rustup run` or `RUSTUP_TOOLCHAIN` changes that because
// both are read by the shim being bypassed.
//
// The failure is not subtle when it lands, but it is very indirect: a bare
// `cargo` resolves to an older toolchain than the tree pins, `cargo build`
// refuses with "rustc X is not supported by the following packages", and
// *every* phase dies at `ensureE2EWeaverBinary` — the phases that skip
// seeding instantly and the rest a few minutes later, which reads like several
// broken phases rather than one broken build.
//
// So ask rustup instead of guessing. `rustup which cargo`, evaluated in the
// weaver repository, resolves `rust-toolchain.toml` and prints the absolute
// cargo for the pinned toolchain — no assumption about where the shim lives
// (a package-manager rustup may put it under its own prefix rather than
// `~/.cargo/bin`) and none about PATH order.
//
// Returns that cargo *and* the directory holding it, because naming the cargo
// is not sufficient on its own: cargo finds `rustc` by searching PATH, so the
// pinned 1.97.1 cargo invoked with this PATH still drove the 1.96.0 rustc and
// failed identically. The caller puts the returned directory at the front of
// the child's PATH so both halves of the toolchain agree.
//
// Empty strings when rustup is absent or cannot answer: the build then uses a
// bare `cargo` and an unmodified PATH, exactly as it did before this existed,
// which is correct for a machine that installed Rust some other way.
func rustupPinnedToolchain() (cargoPath string, binDir string) {
	rustup, err := exec.LookPath("rustup")
	if err != nil {
		return "", ""
	}
	probe := exec.Command(rustup, "which", "cargo")
	probe.Dir = weaverRepoPath()
	out, err := probe.Output()
	if err != nil {
		return "", ""
	}
	resolved := strings.TrimSpace(string(out))
	if resolved == "" {
		return "", ""
	}
	if info, statErr := os.Stat(resolved); statErr != nil || info.IsDir() {
		return "", ""
	}
	return resolved, filepath.Dir(resolved)
}

// prependPathEnv returns env with dir at the front of PATH.
func prependPathEnv(env []string, dir string) []string {
	if strings.TrimSpace(dir) == "" {
		return env
	}
	out := make([]string, 0, len(env))
	replaced := false
	for _, entry := range env {
		if strings.HasPrefix(entry, "PATH=") {
			out = append(out, "PATH="+dir+string(os.PathListSeparator)+strings.TrimPrefix(entry, "PATH="))
			replaced = true
			continue
		}
		out = append(out, entry)
	}
	if !replaced {
		out = append(out, "PATH="+dir)
	}
	return out
}

func ensureE2EWeaverBinary() (string, error) {
	// Stable across runs, deliberately. A per-PID dir made every run a cold
	// optimized build and orphaned ~1.4 GB of artifacts each time. The only
	// thing it has to stay clear of is the dev `weaver/target`,
	// which a stable name outside the repo keeps just as well. If the cache
	// ever goes bad the retry below wipes it and rebuilds clean.
	targetDir := filepath.Join(os.TempDir(), "weaver-e2e-target")
	weaverBin := filepath.Join(targetDir, "e2e", "weaver")

	weaverBuildOnce.Do(func() {
		build := func() error {
			cargoPath, toolchainBin := rustupPinnedToolchain()
			if cargoPath == "" {
				cargoPath = "cargo"
			}
			cmd := exec.Command(cargoPath, "build", "--profile", "e2e", "-p", "weaver", "--locked")
			cmd.Dir = weaverRepoPath()
			cmd.Env = append(prependPathEnv(os.Environ(), toolchainBin), "CARGO_TARGET_DIR="+targetDir)
			return runExternalCommand(cmd, "cargo build --profile e2e -p weaver --locked")
		}

		log.Printf("building optimized e2e weaver binary from %s at %s", weaverRepoPath(), targetDir)
		weaverBuildErr = build()
		if weaverBuildErr == nil {
			weaverBuildPath = weaverBin
			return
		}

		if removeErr := os.RemoveAll(targetDir); removeErr != nil {
			weaverBuildErr = fmt.Errorf("%w (also failed to reset e2e target dir %s: %v)", weaverBuildErr, targetDir, removeErr)
			return
		}

		log.Printf("e2e weaver build failed; retrying with a clean target dir %s", targetDir)
		weaverBuildErr = build()
		if weaverBuildErr == nil {
			weaverBuildPath = weaverBin
		}
	})

	if weaverBuildErr != nil {
		return "", weaverBuildErr
	}
	if weaverBuildPath == "" {
		weaverBuildPath = weaverBin
	}
	if _, err := os.Stat(weaverBuildPath); err != nil {
		return "", fmt.Errorf("e2e weaver binary missing at %s: %w", weaverBuildPath, err)
	}
	return weaverBuildPath, nil
}

func ensureStandardManagedWeaver() error {
	return startStandardManagedWeaver(false)
}

func restartStandardManagedWeaverPreservingState() error {
	return startStandardManagedWeaver(true)
}

func startStandardManagedWeaver(preserveState bool) error {
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForTCP("localhost:"+backupNntpPort(), 30*time.Second)
	if weaverUsesPostgresDatastore() {
		if err := waitForWeaverPostgresReady(30 * time.Second); err != nil {
			return err
		}
	}

	weaverBin := findWeaverBin()
	weaverURL := fmt.Sprintf("http://localhost:%s", localWeaverPort())

	killWeaver()
	if !preserveState {
		cleanWeaverState()
		writeWeaverConfig(
			localWeaverConfigPath(),
			mustPortInt("NNTP_PORT", nntpPort()),
			mustPortInt("NNTP_BACKUP_PORT", backupNntpPort()),
		)
	}
	// Weaver imports the TOML into its datastore on first startup and renames it
	// to `.migrated`. Passing the original path on restart is intentional: the
	// database at that derived location remains authoritative even when the TOML
	// itself no longer exists.

	_ = os.MkdirAll(filepath.Dir(localWeaverLogPath()), 0o755)
	logFlags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if preserveState {
		logFlags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	logFile, err := os.OpenFile(localWeaverLogPath(), logFlags, 0o644)
	if err != nil {
		return fmt.Errorf("open managed weaver log: %w", err)
	}

	cmd := exec.Command(weaverBin, "--config", localWeaverConfigPath(), "serve", "--port", localWeaverPort())
	// `weaver::pipeline` matches nothing — the pipeline lives in the
	// `weaver_server_core` crate, so every `debug!` on this path has been
	// silently dropped for as long as the filter has existed. That is why three
	// separate diagnoses of the PAR2 repair guards had to be inferred from info
	// lines. Scoped to `completion` rather than the whole pipeline on purpose:
	// the full pipeline at debug is a firehose that perturbs the timing of the
	// very starvation behaviour these runs are trying to measure.
	cmd.Env = managedWeaverEnv(os.Environ(), localRunDir(), "info,weaver::pipeline=debug,weaver_server_core::pipeline::completion=debug")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start managed weaver: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(localWeaverPIDPath()), 0o755); err == nil {
		if err := os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644); err != nil {
			log.Printf("warning: write local weaver pid file: %v", err)
		}
	}
	generation := armManagedWeaverExitWatch()
	go func() {
		waitErr := cmd.Wait()
		noteManagedWeaverExit(generation, waitErr)
		_ = logFile.Close()
	}()

	setEnv("WEAVER_URL", weaverURL)
	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)
	return nil
}

// Managed-weaver liveness.
//
// Weaver's release profile is `panic = "abort"`, so one panic anywhere in the
// pipeline takes the entire server down. Every scenario still in flight then
// fails its GraphQL poll with `connection refused`, and the harness scores each
// one as `timeout` — which reads as "weaver was slow" and hides the crash
// behind the symptom of every *other* scenario. That once turned a
// 14-second abort into a 20-minute run reported as 68 timeouts.
//
// Watching the process directly is what distinguishes "slow" from "dead".
var (
	managedWeaverMu   sync.Mutex
	managedWeaverGen  int
	managedWeaverDead bool
	managedWeaverErr  error
)

// armManagedWeaverExitWatch marks a freshly started weaver as the live one and
// returns its generation. The generation is what keeps a superseded weaver —
// one killWeaver just stopped, whose Wait may not have returned yet — from
// reporting its own shutdown as the death of its replacement.
func armManagedWeaverExitWatch() int {
	managedWeaverMu.Lock()
	defer managedWeaverMu.Unlock()
	managedWeaverGen++
	managedWeaverDead = false
	managedWeaverErr = nil
	return managedWeaverGen
}

func noteManagedWeaverExit(generation int, waitErr error) {
	managedWeaverMu.Lock()
	defer managedWeaverMu.Unlock()
	if generation != managedWeaverGen {
		return
	}
	managedWeaverDead = true
	managedWeaverErr = waitErr
}

// managedWeaverDied reports whether the live managed weaver has exited, and the
// wait error if it did. A deliberate shutdown also trips this, so callers must
// only consult it while they still expect weaver to be serving.
func managedWeaverDied() (bool, error) {
	managedWeaverMu.Lock()
	defer managedWeaverMu.Unlock()
	return managedWeaverDead, managedWeaverErr
}

// managedWeaverDeathReport returns the tail of weaver's log, preferring the
// panic if there is one. The harness's own logs cannot explain an abort — only
// weaver's can — so the reason is surfaced at the point of detection rather
// than left for someone to find in an artifacts directory later.
func managedWeaverDeathReport() string {
	data, err := os.ReadFile(localWeaverLogPath())
	if err != nil {
		return fmt.Sprintf("(could not read %s: %v)", localWeaverLogPath(), err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	for i, line := range lines {
		if strings.Contains(line, "panicked at") || strings.Contains(line, "unexpected panic") {
			end := i + 6
			if end > len(lines) {
				end = len(lines)
			}
			return strings.Join(lines[i:end], "\n")
		}
	}
	const tail = 15
	if len(lines) > tail {
		lines = lines[len(lines)-tail:]
	}
	return strings.Join(lines, "\n")
}

func killWeaver() {
	pidData, err := os.ReadFile(localWeaverPIDPath())
	if err == nil {
		if pid, parseErr := strconv.Atoi(strings.TrimSpace(string(pidData))); parseErr == nil && pid > 0 {
			if process, findErr := os.FindProcess(pid); findErr == nil {
				_ = process.Signal(os.Interrupt)
				if !waitForPIDExit(pid, 10*time.Second) {
					_ = process.Kill()
				}
			}
		}
	}
	_ = os.Remove(localWeaverPIDPath())
	killWeaverListenersOnPort(localWeaverPort())
	time.Sleep(time.Second)
}

func stopManagedWeaverAfterProfileCollection() {
	if strings.TrimSpace(os.Getenv("E2E_WEAVER_PROFILE_DIR")) == "" {
		return
	}
	pidData, err := os.ReadFile(localWeaverPIDPath())
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil || pid <= 0 {
		return
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	_ = process.Signal(os.Interrupt)
	if !waitForPIDExit(pid, 30*time.Second) {
		log.Printf("warning: managed Weaver did not exit after profile collection signal; forcing shutdown")
		_ = process.Kill()
		_ = waitForPIDExit(pid, 5*time.Second)
	}
	_ = os.Remove(localWeaverPIDPath())
}

func stopManagedWeaverCommand(cmd *exec.Cmd, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		_, _ = cmd.Process.Wait()
		close(done)
	}()
	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case <-done:
		return
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
	}
}

func waitForPIDExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if !pidExists(pid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func pidExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

func killWeaverListenersOnPort(port string) {
	port = strings.TrimSpace(port)
	if port == "" {
		return
	}

	out, err := exec.Command("lsof", "-tiTCP:"+port, "-sTCP:LISTEN").Output()
	if err != nil || len(out) == 0 {
		return
	}

	for _, field := range strings.Fields(string(out)) {
		pid, parseErr := strconv.Atoi(strings.TrimSpace(field))
		if parseErr != nil || pid <= 0 {
			continue
		}
		if process, findErr := os.FindProcess(pid); findErr == nil {
			_ = process.Kill()
		}
	}
}

func cleanWeaverState() {
	root := localWeaverDir()
	_ = os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	_ = os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	os.Remove(localWeaverConfigPath() + ".migrated")
	os.Remove(filepath.Join(root, "weaver.db"))
	os.Remove(filepath.Join(root, "weaver.db-shm"))
	os.Remove(filepath.Join(root, "weaver.db-wal"))
	filepath.Walk(filepath.Join(root, "intermediate"), func(path string, info os.FileInfo, err error) error {
		if err != nil || path == filepath.Join(root, "intermediate") {
			return nil
		}
		os.RemoveAll(path)
		return nil
	})
	filepath.Walk(filepath.Join(root, "complete"), func(path string, info os.FileInfo, err error) error {
		if err != nil || path == filepath.Join(root, "complete") {
			return nil
		}
		os.RemoveAll(path)
		return nil
	})
	if weaverUsesPostgresDatastore() {
		if err := resetWeaverPostgresDatabase(); err != nil {
			log.Fatalf("reset Weaver Postgres state: %v", err)
		}
	}
}

func writeWeaverConfig(path string, port1, port2 int) {
	root := localWeaverDir()
	os.MkdirAll(filepath.Dir(path), 0o755)
	os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	config := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true

[[servers]]
id = 1
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = 8
active = true
priority = 0

[[servers]]
id = 2
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = 4
active = true
priority = 1

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "series"
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), port1, port2)
	_ = os.WriteFile(path, []byte(config), 0o644)
}

func writeAdaptiveDispatchWeaverConfig(path string, latentPort, directPort, connections int) {
	root := localWeaverDir()
	os.MkdirAll(filepath.Dir(path), 0o755)
	os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	config := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true
max_retries = 3

[[servers]]
id = 1
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = %d
active = true
priority = 0

[[servers]]
id = 2
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = %d
active = true
priority = 0

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "series"
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), latentPort, connections, directPort, connections)
	_ = os.WriteFile(path, []byte(config), 0o644)
}
