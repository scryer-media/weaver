package clientadapter

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type dockerClient struct {
	binary string
}

type runningContainer struct {
	docker    dockerClient
	name      string
	endpoint  string
	configDir string
}

func startContainer(ctx context.Context, cfg Config, spec ProductSpec) (*runningContainer, error) {
	incompleteDir := filepath.Join(cfg.ConfigDir, "incomplete")
	if err := os.MkdirAll(incompleteDir, 0o755); err != nil {
		return nil, fmt.Errorf("create incomplete directory: %w", err)
	}
	for _, path := range []string{cfg.ConfigDir, cfg.OutputDir, incompleteDir} {
		if strings.Contains(path, ",") {
			return nil, fmt.Errorf("Docker bind path must not contain a comma: %s", path)
		}
	}
	if spec.NeedsNZBMount && strings.Contains(cfg.NZBPath, ",") {
		return nil, fmt.Errorf("Docker NZB bind path must not contain a comma: %s", cfg.NZBPath)
	}
	name := containerNameFor(cfg.RunID, string(cfg.Client))
	docker := dockerClient{binary: cfg.DockerBinary}
	args := []string{
		"run", "--detach",
		"--name", name,
		"--network", cfg.Network,
		"--label", "com.scryer-media.weaver.nntp-bench.run=" + cfg.RunID,
		"--mount", mount(cfg.ConfigDir, "/config", false),
		"--mount", mount(incompleteDir, "/downloads/incomplete", false),
		"--mount", mount(cfg.OutputDir, "/downloads/complete", false),
	}
	if spec.ExposeAPI {
		if spec.APIPort < 1 {
			return nil, fmt.Errorf("%s API port must be positive when an API is exposed", cfg.Client)
		}
		args = append(args, "--publish", "127.0.0.1::"+strconv.Itoa(spec.APIPort))
	}
	if spec.NeedsNZBMount {
		args = append(args, "--mount", mount(cfg.NZBPath, "/benchmark-input/"+filepath.Base(cfg.NZBPath), true))
	}
	if spec.NeedsCAMount {
		if strings.Contains(cfg.NNTPCAFile, ",") {
			return nil, fmt.Errorf("Docker CA bind path must not contain a comma: %s", cfg.NNTPCAFile)
		}
		args = append(args, "--mount", mount(cfg.NNTPCAFile, "/benchmark-ca/nntp-ca.pem", true))
	}
	if cfg.Platform != "" {
		args = append(args, "--platform", cfg.Platform)
	}
	for _, variable := range spec.Environment {
		args = append(args, "--env", variable)
	}
	args = append(args, cfg.Image)
	args = append(args, spec.Command...)
	if _, err := docker.run(ctx, args...); err != nil {
		return nil, fmt.Errorf("start %s container: %w", cfg.Client, err)
	}
	return &runningContainer{docker: docker, name: name, configDir: cfg.ConfigDir}, nil
}

// resolveEndpoint is intentionally separate from startContainer. The runner
// starts the container-scoped counters in the interval between these calls, so
// port inspection never creates a product-specific startup accounting gap.
func (container *runningContainer) resolveEndpoint(ctx context.Context, containerPort int) error {
	endpoint, err := container.docker.publishedEndpoint(ctx, container.name, containerPort)
	if err != nil {
		return err
	}
	container.endpoint = endpoint
	return nil
}

func mount(source, destination string, readOnly bool) string {
	value := "type=bind,src=" + source + ",dst=" + destination
	if readOnly {
		value += ",readonly"
	}
	return value
}

func containerNameFor(runID string, client string) string {
	var builder strings.Builder
	for _, character := range runID + "-" + client {
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9':
			builder.WriteRune(character)
		default:
			builder.WriteByte('-')
		}
	}
	base := strings.Trim(builder.String(), "-")
	if base == "" {
		base = "run"
	}
	if len(base) > 44 {
		base = base[:44]
	}
	seed := fmt.Sprintf("%s:%d", base, time.Now().UnixNano())
	digest := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("nntpbench-%s-%x", base, digest[:4])
}

func (d dockerClient) run(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, d.binary, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		preview := strings.TrimSpace(string(output))
		if len(preview) > 2_000 {
			preview = preview[:2_000] + "…"
		}
		if preview == "" {
			return "", fmt.Errorf("Docker command failed: %w", err)
		}
		return "", fmt.Errorf("Docker command failed: %w: %s", err, preview)
	}
	return strings.TrimSpace(string(output)), nil
}

func (d dockerClient) publishedEndpoint(ctx context.Context, name string, containerPort int) (string, error) {
	output, err := d.run(ctx, "port", name, strconv.Itoa(containerPort)+"/tcp")
	if err != nil {
		return "", fmt.Errorf("inspect published client API port: %w", err)
	}
	return endpointFromDockerPort(output, name)
}

func endpointFromDockerPort(output, name string) (string, error) {
	for _, line := range strings.Split(output, "\n") {
		address := strings.TrimSpace(line)
		if address == "" {
			continue
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			continue
		}
		switch host {
		case "0.0.0.0", "":
			host = "127.0.0.1"
		case "::":
			host = "::1"
		}
		return "http://" + net.JoinHostPort(host, port), nil
	}
	return "", fmt.Errorf("Docker did not return a usable published port for %s", name)
}

func (d dockerClient) containerPID(ctx context.Context, name string) (int, error) {
	output, err := d.run(ctx, "inspect", "--format", "{{.State.Pid}}", name)
	if err != nil {
		return 0, fmt.Errorf("inspect client container PID: %w", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil || pid < 1 {
		return 0, fmt.Errorf("client container has no usable host PID")
	}
	return pid, nil
}

func (d dockerClient) containerCgroup(ctx context.Context, name string) (string, error) {
	pid, err := d.containerPID(ctx, name)
	if err != nil {
		return "", err
	}
	contents, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "", fmt.Errorf("read client container cgroup: %w", err)
	}
	cgroup, err := parseContainerCgroup(string(contents))
	if err != nil {
		return "", fmt.Errorf("parse client container cgroup: %w", err)
	}
	return cgroup, nil
}

func parseContainerCgroup(contents string) (string, error) {
	var perfEvent string
	for _, line := range strings.Split(contents, "\n") {
		fields := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(fields) != 3 {
			continue
		}
		path := strings.TrimPrefix(strings.TrimSpace(fields[2]), "/")
		if path == "" {
			continue
		}
		if fields[0] == "0" && fields[1] == "" {
			return path, nil
		}
		for _, controller := range strings.Split(fields[1], ",") {
			if controller == "perf_event" {
				perfEvent = path
			}
		}
	}
	if perfEvent != "" {
		return perfEvent, nil
	}
	return "", fmt.Errorf("no non-root cgroup v2 or perf_event path found")
}

func (d dockerClient) containerRunning(ctx context.Context, name string) (bool, error) {
	output, err := d.run(ctx, "inspect", "--format", "{{.State.Running}}", name)
	if err != nil {
		return false, fmt.Errorf("inspect client container state: %w", err)
	}
	running, err := strconv.ParseBool(strings.TrimSpace(output))
	if err != nil {
		return false, fmt.Errorf("parse client container running state %q: %w", output, err)
	}
	return running, nil
}

func (d dockerClient) imageVersion(ctx context.Context, image string) string {
	for _, label := range []string{"org.opencontainers.image.version", "build_version"} {
		output, err := d.run(ctx, "image", "inspect", "--format", "{{index .Config.Labels \""+label+"\"}}", image)
		if err == nil {
			value := strings.TrimSpace(output)
			if value != "" && value != "<no value>" {
				return value
			}
		}
	}
	return image
}

func (container *runningContainer) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if output, err := container.docker.run(ctx, "logs", container.name); err == nil {
		// Product entrypoints may chown /config to their runtime UID. The suite
		// directory itself is never mounted, so it remains writable by the
		// benchmark controller even after a failed container startup.
		path := filepath.Join(filepath.Dir(container.configDir), "client-container.log")
		file, createErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if createErr == nil {
			_, _ = file.WriteString(output + "\n")
			_ = file.Close()
		}
	}
	_, _ = container.docker.run(ctx, "rm", "--force", container.name)
}
