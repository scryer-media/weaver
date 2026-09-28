// Package containerengine resolves the container engine the e2e harness
// drives (Docker or Podman) and builds every engine and Compose command line.
// Nothing else in the harness names an engine binary.
package containerengine

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// EnvVar selects the engine: docker, podman, or auto (the default).
const EnvVar = "E2E_CONTAINER_ENGINE"

// Kind names a container engine family.
type Kind string

const (
	Docker Kind = "docker"
	Podman Kind = "podman"
)

// Compose providers. Docker always runs its Compose v2 plugin; `podman
// compose` delegates to whichever external provider it finds.
const (
	ProviderDockerCompose = "docker-compose"
	ProviderPodmanCompose = "podman-compose"
)

// Host names a container uses to reach the host through the engine's gateway.
const (
	DockerHostGateway = "host.docker.internal"
	PodmanHostGateway = "host.containers.internal"
)

// IDMap is one user-namespace ID range: container IDs
// [ContainerID, ContainerID+Size) map to host IDs starting at HostID.
type IDMap struct {
	ContainerID int `json:"container_id"`
	HostID      int `json:"host_id"`
	Size        int `json:"size"`
}

// Engine is the resolved container engine for this process.
type Engine struct {
	Kind Kind
	// Binary is the CLI invoked for every engine command. It is a bare name
	// resolved through PATH so command lines read the same as before.
	Binary string
	// Version is the engine server version, when probed.
	Version string
	// ComposeProvider is ProviderDockerCompose or ProviderPodmanCompose.
	ComposeProvider string
	// ComposeVersion is the provider's own version, when probed.
	ComposeVersion string
	Rootless       bool
	SELinux        bool
	// CgroupManager is Podman's cgroup manager (systemd or cgroupfs).
	CgroupManager string
	UIDMap        []IDMap
	GIDMap        []IDMap
	// Probed reports whether the fields above came from the live engine
	// rather than a PATH-only selection.
	Probed bool

	mu sync.RWMutex
	// composeBaseFile is the Compose file used when a command names none and
	// COMPOSE_FILE is unset.
	composeBaseFile string
	// composeOverlay is appended after every Compose file when set. It is
	// only ever set for Podman.
	composeOverlay string
}

// HostGateway is the in-container name the engine itself provides for the
// host. The Compose file additionally maps host.docker.internal to the host
// gateway on both engines.
func (engine *Engine) HostGateway() string {
	if engine.Kind == Podman {
		return PodmanHostGateway
	}
	return DockerHostGateway
}

// UsesPodmanCompose reports whether Compose commands run through
// podman-compose rather than a Docker Compose binary.
func (engine *Engine) UsesPodmanCompose() bool {
	return engine.Kind == Podman && engine.ComposeProvider == ProviderPodmanCompose
}

// SetComposeFiles records the default Compose file and the engine overlay (""
// for none). The overlay is ignored for Docker, so Docker command lines never
// change.
func (engine *Engine) SetComposeFiles(base, overlay string) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.composeBaseFile = base
	if engine.Kind == Podman {
		engine.composeOverlay = overlay
	} else {
		engine.composeOverlay = ""
	}
}

// ComposeOverlay returns the overlay file in effect, or "".
func (engine *Engine) ComposeOverlay() string {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return engine.composeOverlay
}

// Args rewrites an engine argument vector for this engine. A vector starting
// with "compose" gets the engine overlay; a `buildx build` becomes Podman's
// native build.
func (engine *Engine) Args(args ...string) []string {
	if len(args) == 0 {
		return args
	}
	switch {
	case args[0] == "compose":
		return engine.composeArgs(args, os.Getenv)
	case engine.Kind == Podman && len(args) >= 2 && args[0] == "buildx" && args[1] == "build":
		return podmanBuildArgs(args[2:])
	}
	return append([]string(nil), args...)
}

// Command is exec.Command for the engine CLI.
func (engine *Engine) Command(args ...string) *exec.Cmd {
	return exec.Command(engine.Binary, engine.Args(args...)...)
}

// CommandContext is exec.CommandContext for the engine CLI.
func (engine *Engine) CommandContext(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, engine.Binary, engine.Args(args...)...)
}

// CommandLine renders the command a caller would type, for messages.
func (engine *Engine) CommandLine(args ...string) string {
	return strings.Join(append([]string{engine.Binary}, args...), " ")
}

// RunUserArgs are extra `run` flags needed when a container runs as the
// invoking host user and writes into a bind mount. Rootless Podman maps the
// host user to container root, so a `--user uid:gid` container would write
// files owned by a subordinate ID; keep-id maps the host user onto itself.
func (engine *Engine) RunUserArgs() []string {
	if engine.Kind == Podman && engine.Rootless {
		return []string{"--userns=keep-id"}
	}
	return nil
}

// NetworkSubnetTemplate is the `network inspect --format` template printing
// one subnet per line.
func (engine *Engine) NetworkSubnetTemplate() string {
	if engine.Kind == Podman {
		return `{{range .Subnets}}{{.Subnet}}{{"\n"}}{{end}}`
	}
	return `{{range .IPAM.Config}}{{.Subnet}}{{"\n"}}{{end}}`
}

// ServiceContainerArgs lists the IDs of one Compose service's containers.
// Docker and the docker-compose provider ask Compose; podman-compose does not
// filter `ps` by service, so it filters on the labels it stamps instead.
func (engine *Engine) ServiceContainerArgs(project, service string, includeStopped bool) []string {
	if engine.UsesPodmanCompose() {
		args := []string{"ps", "-q"}
		if includeStopped {
			args = append(args, "-a")
		}
		return append(args,
			"--filter", "label=com.docker.compose.project="+project,
			"--filter", "label=com.docker.compose.service="+service,
		)
	}
	args := []string{"compose", "-p", project, "ps"}
	if includeStopped {
		args = append(args, "-a")
	}
	return append(args, "-q", service)
}

// composeArgs inserts the overlay after the Compose files a command already
// uses. Naming any -f replaces COMPOSE_FILE and the default file, so the
// files in effect are made explicit first.
func (engine *Engine) composeArgs(args []string, getenv func(string) string) []string {
	out := append([]string(nil), args...)
	engine.mu.RLock()
	overlay, base := engine.composeOverlay, engine.composeBaseFile
	engine.mu.RUnlock()
	subcommand, named, profiled := composeGlobalFlags(args)
	var insert []string
	if overlay != "" {
		if !named {
			for _, file := range composeFilesFromEnv(getenv, base) {
				insert = append(insert, "-f", file)
			}
		}
		insert = append(insert, "-f", overlay)
	}
	// Podman refuses to remove a container while another still joins its
	// network namespace (`network_mode: service:`), and Compose only stops
	// and removes the services of active profiles, in dependency order,
	// before it sweeps orphans. A `down` without every profile active would
	// therefore reach the namespace owner first and fail. Docker removes the
	// owner regardless, so it keeps its argument vector.
	if engine.Kind == Podman && subcommand < len(args) && args[subcommand] == "down" &&
		!profiled && strings.TrimSpace(getenv("COMPOSE_PROFILES")) == "" {
		insert = append(insert, "--profile", "*")
	}
	if len(insert) == 0 {
		return out
	}
	result := make([]string, 0, len(out)+len(insert))
	result = append(result, out[:subcommand]...)
	result = append(result, insert...)
	return append(result, out[subcommand:]...)
}

// composeGlobalFlags returns the index of the Compose subcommand in args
// (args[0] is "compose"), whether a -f/--file flag precedes it, and whether
// a --profile flag does.
func composeGlobalFlags(args []string) (subcommand int, named, profiled bool) {
	valued := map[string]bool{
		"-p": true, "--project-name": true, "-f": true, "--file": true,
		"--project-directory": true, "--env-file": true, "--profile": true,
		"--ansi": true, "--progress": true, "--parallel": true,
	}
	index := 1
	for index < len(args) {
		arg := args[index]
		if !strings.HasPrefix(arg, "-") {
			return index, named, profiled
		}
		name := arg
		if eq := strings.IndexByte(arg, '='); eq >= 0 {
			name = arg[:eq]
		}
		switch name {
		case "-f", "--file":
			named = true
		case "--profile":
			profiled = true
		}
		if valued[name] && !strings.Contains(arg, "=") {
			index += 2
			continue
		}
		index++
	}
	return len(args), named, profiled
}

func composeFilesFromEnv(getenv func(string) string, base string) []string {
	value := strings.TrimSpace(getenv("COMPOSE_FILE"))
	if value == "" {
		if base == "" {
			return nil
		}
		return []string{base}
	}
	separator := getenv("COMPOSE_PATH_SEPARATOR")
	if separator == "" {
		separator = string(os.PathListSeparator)
	}
	var files []string
	for _, file := range strings.Split(value, separator) {
		if file = strings.TrimSpace(file); file != "" {
			files = append(files, file)
		}
	}
	return files
}

// podmanBuildArgs maps `docker buildx build` flags onto `podman build`:
// Podman builds straight into local storage, so --load has no meaning, and it
// never attaches provenance attestations.
func podmanBuildArgs(rest []string) []string {
	out := []string{"build"}
	for _, arg := range rest {
		switch {
		case arg == "--load":
		case arg == "--provenance" || strings.HasPrefix(arg, "--provenance="):
		default:
			out = append(out, arg)
		}
	}
	return out
}
