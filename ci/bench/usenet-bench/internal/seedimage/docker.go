package seedimage

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// CLI drives the real Docker command line. Every method is a single
// invocation, so a failure names exactly which step could not run.
type CLI struct {
	Binary string
}

func (c CLI) binary() string {
	if strings.TrimSpace(c.Binary) == "" {
		return "docker"
	}
	return c.Binary
}

func (c CLI) output(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, c.binary(), args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w\n%s", c.binary(), strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func (c CLI) run(ctx context.Context, args ...string) error {
	_, err := c.output(ctx, args...)
	return err
}

// ImageID returns "" rather than an error when the image is simply absent,
// because absence is a normal cache miss rather than a failure.
func (c CLI) ImageID(ctx context.Context, ref string) (string, error) {
	command := exec.CommandContext(ctx, c.binary(), "image", "inspect", "--format", "{{.Id}}", ref)
	output, err := command.CombinedOutput()
	if err != nil {
		text := strings.ToLower(string(output))
		if strings.Contains(text, "no such image") || strings.Contains(text, "not found") {
			return "", nil
		}
		return "", fmt.Errorf("docker image inspect %s: %w\n%s", ref, err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func (c CLI) ImageLabels(ctx context.Context, ref string) (map[string]string, error) {
	raw, err := c.output(ctx, "image", "inspect", "--format", "{{json .Config.Labels}}", ref)
	if err != nil {
		return nil, err
	}
	if raw == "" || raw == "null" {
		return map[string]string{}, nil
	}
	labels := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &labels); err != nil {
		return nil, fmt.Errorf("decode labels of %s: %w", ref, err)
	}
	return labels, nil
}

func (c CLI) ContainerID(ctx context.Context, project, service string) (string, error) {
	raw, err := c.output(ctx,
		"ps", "--quiet",
		"--filter", "label=com.docker.compose.project="+project,
		"--filter", "label=com.docker.compose.service="+service,
	)
	if err != nil {
		return "", err
	}
	ids := strings.Fields(raw)
	if len(ids) == 0 {
		return "", nil
	}
	if len(ids) > 1 {
		return "", fmt.Errorf("Compose service %q in project %q has %d running containers; capture needs exactly one", service, project, len(ids))
	}
	return ids[0], nil
}

func (c CLI) CopyFromContainer(ctx context.Context, container, containerPath, destination string) error {
	return c.run(ctx, "cp", container+":"+containerPath, destination)
}

// CopyFromImage never starts the image: a created-but-not-started container is
// enough for docker cp, and starting the NNTP server here could bind ports the
// benchmark stack owns.
func (c CLI) CopyFromImage(ctx context.Context, image, imagePath, destination string) error {
	id, err := c.output(ctx, "create", image)
	if err != nil {
		return err
	}
	id = lastLine(id)
	defer func() {
		_ = c.run(context.WithoutCancel(ctx), "rm", "--force", id)
	}()
	return c.run(ctx, "cp", id+":"+imagePath, destination)
}

func (c CLI) Build(ctx context.Context, contextDir, baseImage, tag string, labels map[string]string) error {
	args := []string{"build", "--build-arg", "BASE_IMAGE=" + baseImage, "--tag", tag}
	for _, key := range []string{FormatLabel, FingerprintLabel, ManifestLabel, RunIDLabel, CreatedLabel} {
		args = append(args, "--label", key+"="+labels[key])
	}
	args = append(args, contextDir)
	return c.run(ctx, args...)
}

func (c CLI) RemoveImage(ctx context.Context, ref string) error {
	return c.run(ctx, "image", "rm", "--force", ref)
}

// lastLine keeps the container id from output that may carry a pull progress
// preamble.
func lastLine(value string) string {
	lines := strings.Split(strings.TrimSpace(value), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}
