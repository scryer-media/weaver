package containerengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
)

// System is what resolution needs from the host. Tests replace it with fakes;
// the harness uses HostSystem.
type System struct {
	Getenv   func(string) string
	LookPath func(string) (string, error)
	// Run executes name with args and returns stdout and stderr.
	Run func(ctx context.Context, name string, args ...string) (stdout, stderr []byte, err error)
}

// HostSystem is the real process environment, PATH and exec.
func HostSystem() System {
	return System{
		Getenv:   os.Getenv,
		LookPath: exec.LookPath,
		Run: func(ctx context.Context, name string, args ...string) ([]byte, []byte, error) {
			var stdout, stderr bytes.Buffer
			cmd := exec.CommandContext(ctx, name, args...)
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err := cmd.Run()
			return stdout.Bytes(), stderr.Bytes(), err
		},
	}
}

// Choice parses E2E_CONTAINER_ENGINE.
func Choice(getenv func(string) string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(getenv(EnvVar)))
	switch value {
	case "", "auto":
		return "auto", nil
	case string(Docker), string(Podman):
		return value, nil
	}
	return "", fmt.Errorf("%s=%q: want docker, podman or auto", EnvVar, getenv(EnvVar))
}

// Resolve probes the host and returns the engine this run uses. With auto,
// Docker wins when its CLI is on PATH and its daemon answers; otherwise Podman
// is used. An explicit choice that cannot be used is an error naming what is
// missing.
func Resolve(ctx context.Context, system System) (*Engine, error) {
	choice, err := Choice(system.Getenv)
	if err != nil {
		return nil, err
	}
	switch choice {
	case string(Docker):
		return resolveDocker(ctx, system)
	case string(Podman):
		return resolvePodman(ctx, system)
	}
	engine, dockerErr := resolveDocker(ctx, system)
	if dockerErr == nil {
		return engine, nil
	}
	engine, podmanErr := resolvePodman(ctx, system)
	if podmanErr == nil {
		return engine, nil
	}
	return nil, fmt.Errorf("no usable container engine (%s=auto): docker: %v; podman: %v", EnvVar, dockerErr, podmanErr)
}

func resolveDocker(ctx context.Context, system System) (*Engine, error) {
	if _, err := system.LookPath("docker"); err != nil {
		return nil, errors.New("the docker CLI is not on PATH")
	}
	// A `docker` that is Podman's compatibility wrapper is Podman.
	if out, _, err := system.Run(ctx, "docker", "--version"); err == nil &&
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(string(out))), "podman") {
		return nil, errors.New("the docker CLI on PATH is Podman's docker wrapper; set " + EnvVar + "=podman")
	}
	out, stderr, err := system.Run(ctx, "docker", "version", "--format", "{{.Server.Version}}")
	if err != nil {
		return nil, fmt.Errorf("the Docker daemon is not answering (docker version: %v: %s)", err, firstLine(stderr))
	}
	engine := &Engine{Kind: Docker, Binary: "docker", Version: strings.TrimSpace(string(out)), Probed: true}
	out, stderr, err = system.Run(ctx, "docker", "compose", "version", "--short")
	if err != nil {
		return nil, fmt.Errorf("the Docker Compose v2 plugin is missing (docker compose version: %v: %s)", err, firstLine(stderr))
	}
	engine.ComposeProvider = ProviderDockerCompose
	engine.ComposeVersion = strings.TrimPrefix(strings.TrimSpace(string(out)), "v")
	return engine, nil
}

func resolvePodman(ctx context.Context, system System) (*Engine, error) {
	if _, err := system.LookPath("podman"); err != nil {
		return nil, errors.New("the podman CLI is not on PATH")
	}
	out, stderr, err := system.Run(ctx, "podman", "info", "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("podman is not answering (podman info: %v: %s); on macOS or Windows start the machine with `podman machine start`", err, firstLine(stderr))
	}
	engine := &Engine{Kind: Podman, Binary: "podman", Probed: true}
	if err := applyPodmanInfo(engine, out); err != nil {
		return nil, err
	}
	out, stderr, err = system.Run(ctx, "podman", "compose", "version")
	if err != nil {
		return nil, fmt.Errorf("podman compose has no provider (podman compose version: %v: %s); install docker-compose or podman-compose", err, firstLine(append(stderr, out...)))
	}
	provider, version := parseComposeVersion(string(out) + "\n" + string(stderr))
	if provider == "" {
		return nil, fmt.Errorf("unrecognised podman compose provider: %s", firstLine(append(out, stderr...)))
	}
	engine.ComposeProvider, engine.ComposeVersion = provider, version
	return engine, nil
}

type podmanInfo struct {
	Host struct {
		CgroupManager string `json:"cgroupManager"`
		IDMappings    struct {
			UIDMap []IDMap `json:"uidmap"`
			GIDMap []IDMap `json:"gidmap"`
		} `json:"idMappings"`
		Security struct {
			Rootless       bool `json:"rootless"`
			SELinuxEnabled bool `json:"selinuxEnabled"`
		} `json:"security"`
	} `json:"host"`
	Version struct {
		Version string `json:"Version"`
	} `json:"version"`
}

func applyPodmanInfo(engine *Engine, raw []byte) error {
	var info podmanInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return fmt.Errorf("decode podman info: %w", err)
	}
	engine.Version = info.Version.Version
	engine.Rootless = info.Host.Security.Rootless
	engine.SELinux = info.Host.Security.SELinuxEnabled
	engine.CgroupManager = info.Host.CgroupManager
	engine.UIDMap = info.Host.IDMappings.UIDMap
	engine.GIDMap = info.Host.IDMappings.GIDMap
	return nil
}

var (
	dockerComposeVersionPattern = regexp.MustCompile(`(?i)docker compose version:?\s+v?([0-9][0-9A-Za-z.+-]*)`)
	podmanComposeVersionPattern = regexp.MustCompile(`(?i)podman-compose version:?\s+v?([0-9][0-9A-Za-z.+-]*)`)
	providerPathPattern         = regexp.MustCompile(`(?i)executing external compose provider "([^"]+)"`)
)

// parseComposeVersion identifies the provider behind `podman compose version`
// from the provider's own banner, falling back to the "Executing external
// compose provider" notice Podman prints.
func parseComposeVersion(output string) (provider, version string) {
	if match := podmanComposeVersionPattern.FindStringSubmatch(output); match != nil {
		return ProviderPodmanCompose, match[1]
	}
	if match := dockerComposeVersionPattern.FindStringSubmatch(output); match != nil {
		return ProviderDockerCompose, match[1]
	}
	if match := providerPathPattern.FindStringSubmatch(output); match != nil {
		base := match[1]
		if slash := strings.LastIndexAny(base, `/\`); slash >= 0 {
			base = base[slash+1:]
		}
		base = strings.TrimSuffix(strings.ToLower(base), ".exe")
		switch {
		case strings.Contains(base, "podman-compose"):
			return ProviderPodmanCompose, ""
		case strings.Contains(base, "docker-compose"):
			return ProviderDockerCompose, ""
		}
	}
	return "", ""
}

// Select picks an engine from E2E_CONTAINER_ENGINE and PATH alone, without
// asking a daemon anything. It serves commands built before (or without) a
// preflight; Resolve is the authoritative choice.
func Select(system System) *Engine {
	choice, err := Choice(system.Getenv)
	if err != nil || choice == "auto" {
		choice = string(Docker)
		if _, err := system.LookPath("docker"); err != nil {
			if _, err := system.LookPath("podman"); err == nil {
				choice = string(Podman)
			}
		}
	}
	engine := &Engine{Kind: Kind(choice), Binary: choice}
	if engine.Kind == Docker {
		engine.ComposeProvider = ProviderDockerCompose
	}
	return engine
}

var (
	currentMu sync.Mutex
	current   *Engine
)

// Current is the engine for this process: the one installed by Use, or else
// a PATH-only selection made once.
func Current() *Engine {
	currentMu.Lock()
	defer currentMu.Unlock()
	if current == nil {
		system := HostSystem()
		current = Select(system)
		if current.Kind == Podman {
			// Rootlessness changes `run` flags, so a Podman selection asks
			// for it; a failure leaves the rootful defaults and the command
			// that follows reports the real problem.
			if out, _, err := system.Run(context.Background(), current.Binary, "info", "--format", "json"); err == nil {
				_ = applyPodmanInfo(current, out)
			}
		}
	}
	return current
}

// Use installs engine as this process's engine and pins the choice in the
// environment so child harness processes resolve the same engine.
func Use(engine *Engine) {
	currentMu.Lock()
	current = engine
	currentMu.Unlock()
	_ = os.Setenv(EnvVar, string(engine.Kind))
	if engine.Kind == Podman {
		// `podman compose` otherwise prefixes every run with a provider
		// notice on stderr, which would pollute captured combined output.
		_ = os.Setenv("PODMAN_COMPOSE_WARNING_LOGS", "false")
	}
}

// Command is Current().Command.
func Command(args ...string) *exec.Cmd { return Current().Command(args...) }

// CommandContext is Current().CommandContext.
func CommandContext(ctx context.Context, args ...string) *exec.Cmd {
	return Current().CommandContext(ctx, args...)
}

func firstLine(raw []byte) string {
	text := strings.TrimSpace(string(raw))
	if index := strings.IndexByte(text, '\n'); index >= 0 {
		text = text[:index]
	}
	return text
}
