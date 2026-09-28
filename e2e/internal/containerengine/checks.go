package containerengine

import (
	"fmt"
	"strconv"
	"strings"
)

// Check is one capability the harness relies on.
type Check struct {
	Name   string
	OK     bool
	Hard   bool // a failed hard check stops the run before the stack starts
	Detail string
	Remedy string
}

// Minimum versions for the Compose-file features the harness uses.
var (
	// extra_hosts "host-gateway" is resolved by Podman from 5.3.
	minPodmanVersion = []int{5, 3}
	// The pre-seeded override uses the `!override` merge tag and the NNTP
	// secret is environment-sourced.
	minDockerComposeVersion = []int{2, 24, 4}
	// depends_on health conditions are honoured from podman-compose 1.3.
	minPodmanComposeVersion = []int{1, 3}
)

// Checks evaluates the capabilities of a probed engine.
func (engine *Engine) Checks() []Check {
	checks := []Check{{
		Name:   "engine answers",
		OK:     engine.Probed,
		Hard:   true,
		Detail: fmt.Sprintf("%s %s", engine.Kind, orUnknown(engine.Version)),
		Remedy: "start the engine (Docker daemon, or `podman machine start` on macOS/Windows)",
	}}
	switch engine.Kind {
	case Docker:
		checks = append(checks, Check{
			Name:   "compose v2",
			OK:     versionAtLeast(engine.ComposeVersion, []int{2}),
			Hard:   true,
			Detail: "docker compose " + orUnknown(engine.ComposeVersion),
			Remedy: "install the Docker Compose v2 plugin",
		})
	case Podman:
		checks = append(checks, engine.podmanChecks()...)
	}
	return checks
}

func (engine *Engine) podmanChecks() []Check {
	checks := []Check{{
		Name:   "host-gateway in extra_hosts",
		OK:     versionAtLeast(engine.Version, minPodmanVersion),
		Hard:   true,
		Detail: "podman " + orUnknown(engine.Version) + ", needs 5.3+",
		Remedy: "upgrade Podman to 5.3 or newer",
	}}
	switch engine.ComposeProvider {
	case ProviderDockerCompose:
		checks = append(checks, Check{
			Name:   "compose provider",
			OK:     versionAtLeast(engine.ComposeVersion, minDockerComposeVersion),
			Hard:   true,
			Detail: "docker-compose " + orUnknown(engine.ComposeVersion) + ", needs 2.24.4+ (!override, environment secrets)",
			Remedy: "upgrade docker-compose to 2.24.4 or newer",
		})
	case ProviderPodmanCompose:
		checks = append(checks,
			Check{
				Name:   "compose provider",
				OK:     versionAtLeast(engine.ComposeVersion, minPodmanComposeVersion),
				Hard:   true,
				Detail: "podman-compose " + orUnknown(engine.ComposeVersion) + ", needs 1.3+ (depends_on health conditions)",
				Remedy: "upgrade podman-compose to 1.3 or newer, or set PODMAN_COMPOSE_PROVIDER=docker-compose",
			},
			Check{
				Name:   "environment secrets",
				OK:     true,
				Detail: "podman-compose cannot source a secret from the environment; the harness writes it to a 0600 file secret",
			},
		)
	default:
		checks = append(checks, Check{
			Name:   "compose provider",
			Hard:   true,
			Detail: "no recognised provider behind `podman compose`",
			Remedy: "install docker-compose (preferred) or podman-compose",
		})
	}
	healthcheckOK := engine.CgroupManager == "" || engine.CgroupManager == "systemd"
	checks = append(checks, Check{
		Name:   "scheduled healthchecks",
		OK:     healthcheckOK,
		Hard:   true,
		Detail: "cgroup manager " + orUnknown(engine.CgroupManager) + "; Podman schedules healthchecks with systemd timers",
		Remedy: "use the systemd cgroup manager (and `loginctl enable-linger $USER` for rootless)",
	})
	if engine.Rootless {
		checks = append(checks, Check{
			Name:   "rootless ID mapping",
			OK:     idMapCoversAll(engine.UIDMap, serviceIDs) && idMapCoversAll(engine.GIDMap, serviceIDs),
			Hard:   true,
			Detail: "service entrypoints chown to uid/gid 1000 (and PostgreSQL to 999) inside the user namespace",
			Remedy: "add subordinate ranges: `sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 $USER && podman system migrate`",
		})
	}
	selinuxDetail := "disabled"
	if engine.SELinux {
		selinuxDetail = "enabled; the harness overlay runs services with label=disable so repository bind mounts stay readable"
	}
	checks = append(checks, Check{Name: "SELinux bind mounts", OK: true, Detail: selinuxDetail})
	return checks
}

// HardFailures returns the failed hard checks.
func HardFailures(checks []Check) []Check {
	var failed []Check
	for _, check := range checks {
		if check.Hard && !check.OK {
			failed = append(failed, check)
		}
	}
	return failed
}

// serviceIDs are the in-container user and group IDs the services take
// ownership as: 1000 for the service entrypoints, 999 for PostgreSQL.
var serviceIDs = []int{999, 1000}

func idMapCoversAll(maps []IDMap, ids []int) bool {
	for _, id := range ids {
		if !idMapCovers(maps, id) {
			return false
		}
	}
	return true
}

func idMapCovers(maps []IDMap, id int) bool {
	for _, m := range maps {
		if id >= m.ContainerID && id < m.ContainerID+m.Size {
			return true
		}
	}
	return false
}

func orUnknown(value string) string {
	if strings.TrimSpace(value) == "" {
		return "(unknown version)"
	}
	return value
}

// versionAtLeast compares the leading numeric components of version against
// want. An unparseable version fails.
func versionAtLeast(version string, want []int) bool {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	if version == "" {
		return false
	}
	fields := strings.FieldsFunc(version, func(r rune) bool { return r == '.' || r == '-' || r == '+' })
	for index, minimum := range want {
		if index >= len(fields) {
			return minimum == 0
		}
		value, err := strconv.Atoi(fields[index])
		if err != nil {
			return false
		}
		if value != minimum {
			return value > minimum
		}
	}
	return true
}
