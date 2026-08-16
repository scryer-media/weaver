package composeutil

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ListNetworkSubnets returns every subnet currently claimed by Docker.
func ListNetworkSubnets(ctx context.Context, workDir string) ([]string, error) {
	list := exec.CommandContext(ctx, "docker", "network", "ls", "-q")
	list.Dir = workDir
	output, err := list.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker network ls: %w: %s", err, strings.TrimSpace(string(output)))
	}
	ids := strings.Fields(string(output))
	if len(ids) == 0 {
		return nil, nil
	}
	args := append(
		[]string{"network", "inspect", "--format", "{{range .IPAM.Config}}{{.Subnet}}{{\"\\n\"}}{{end}}"},
		ids...,
	)
	inspect := exec.CommandContext(ctx, "docker", args...)
	inspect.Dir = workDir
	output, err = inspect.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker network inspect: %w: %s", err, strings.TrimSpace(string(output)))
	}
	var subnets []string
	for _, line := range strings.Split(string(output), "\n") {
		if subnet := strings.TrimSpace(line); subnet != "" && subnet != "<no value>" {
			subnets = append(subnets, subnet)
		}
	}
	return subnets, nil
}

// SelectNonOverlappingSubnets chooses the requested number of candidates
// without overlapping live or already selected networks.
func SelectNonOverlappingSubnets(
	count int,
	candidates []string,
	used []string,
) ([]string, error) {
	if count < 0 {
		return nil, fmt.Errorf("subnet count must be non-negative")
	}
	claimed := parsePrefixes(used)
	selected := make([]string, 0, count)
	for _, candidate := range candidates {
		if len(selected) == count {
			break
		}
		prefix, err := netip.ParsePrefix(strings.TrimSpace(candidate))
		if err != nil {
			return nil, fmt.Errorf("parse subnet candidate %q: %w", candidate, err)
		}
		if overlapsAny(prefix, claimed) {
			continue
		}
		selected = append(selected, candidate)
		claimed = append(claimed, prefix)
	}
	if len(selected) != count {
		return nil, fmt.Errorf(
			"allocate %d isolated Compose subnets: only %d candidates remain",
			count,
			len(selected),
		)
	}
	return selected, nil
}

func WriteNetworkOverride(path, subnet string) error {
	if _, err := netip.ParsePrefix(strings.TrimSpace(subnet)); err != nil {
		return fmt.Errorf("invalid network subnet %q: %w", subnet, err)
	}
	body := fmt.Sprintf(`networks:
  default:
    ipam:
      config:
        - subnet: %q
`, subnet)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		return fmt.Errorf("write Compose network override %s: %w", path, err)
	}
	return nil
}

func ComposeFileValue(paths ...string) string {
	return strings.Join(paths, string(os.PathListSeparator))
}

func parsePrefixes(values []string) []netip.Prefix {
	prefixes := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		if prefix, err := netip.ParsePrefix(strings.TrimSpace(value)); err == nil {
			prefixes = append(prefixes, prefix)
		}
	}
	return prefixes
}

func overlapsAny(candidate netip.Prefix, claimed []netip.Prefix) bool {
	for _, prefix := range claimed {
		if candidate.Overlaps(prefix) {
			return true
		}
	}
	return false
}
