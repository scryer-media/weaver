package containerengine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// fakeHost is a scripted host: which CLIs are on PATH and what each command
// line prints.
type fakeHost struct {
	env      map[string]string
	onPath   map[string]bool
	replies  map[string]fakeReply
	commands []string
}

type fakeReply struct {
	stdout, stderr string
	fail           bool
}

func (host *fakeHost) system() System {
	return System{
		Getenv: func(key string) string { return host.env[key] },
		LookPath: func(name string) (string, error) {
			if host.onPath[name] {
				return "/fake/bin/" + name, nil
			}
			return "", errors.New("not found")
		},
		Run: func(_ context.Context, name string, args ...string) ([]byte, []byte, error) {
			line := strings.Join(append([]string{name}, args...), " ")
			host.commands = append(host.commands, line)
			reply, ok := host.replies[line]
			if !ok {
				return nil, []byte("unexpected command " + line), errors.New("exit status 127")
			}
			var err error
			if reply.fail {
				err = errors.New("exit status 1")
			}
			return []byte(reply.stdout), []byte(reply.stderr), err
		},
	}
}

const podmanInfoRootless = `{
  "host": {
    "cgroupManager": "systemd",
    "idMappings": {
      "uidmap": [{"container_id": 0, "host_id": 1000, "size": 1}, {"container_id": 1, "host_id": 100000, "size": 65536}],
      "gidmap": [{"container_id": 0, "host_id": 1000, "size": 1}, {"container_id": 1, "host_id": 100000, "size": 65536}]
    },
    "security": {"rootless": true, "selinuxEnabled": true}
  },
  "version": {"Version": "5.4.2"}
}`

func dockerReplies() map[string]fakeReply {
	return map[string]fakeReply{
		"docker --version": {stdout: "Docker version 29.8.0, build 88096ef\n"},
		"docker version --format {{.Server.Version}}": {stdout: "29.8.0\n"},
		"docker compose version --short":              {stdout: "5.5.1\n"},
		"podman info --format json":                   {stdout: podmanInfoRootless},
		"podman compose version":                      {stdout: "Docker Compose version v2.39.1\n", stderr: ">>>> Executing external compose provider \"/usr/local/bin/docker-compose\". Please see podman-compose(1) for how to disable this message. <<<<\n"},
	}
}

func TestResolveAutoPrefersAnsweringDocker(t *testing.T) {
	host := &fakeHost{onPath: map[string]bool{"docker": true, "podman": true}, replies: dockerReplies()}
	engine, err := Resolve(context.Background(), host.system())
	if err != nil {
		t.Fatal(err)
	}
	if engine.Kind != Docker || engine.Binary != "docker" || engine.Version != "29.8.0" || engine.ComposeVersion != "5.5.1" {
		t.Fatalf("engine = %+v", engine)
	}
	for _, line := range host.commands {
		if strings.HasPrefix(line, "podman") {
			t.Fatalf("auto probed podman although docker answered: %v", host.commands)
		}
	}
}

func TestResolveAutoFallsBackToPodmanWhenDockerDaemonIsDown(t *testing.T) {
	replies := dockerReplies()
	replies["docker version --format {{.Server.Version}}"] = fakeReply{stderr: "Cannot connect to the Docker daemon", fail: true}
	host := &fakeHost{onPath: map[string]bool{"docker": true, "podman": true}, replies: replies}
	engine, err := Resolve(context.Background(), host.system())
	if err != nil {
		t.Fatal(err)
	}
	if engine.Kind != Podman || engine.Binary != "podman" {
		t.Fatalf("engine = %+v", engine)
	}
	if !engine.Rootless || !engine.SELinux || engine.Version != "5.4.2" || engine.CgroupManager != "systemd" {
		t.Fatalf("podman info not applied: %+v", engine)
	}
	if engine.ComposeProvider != ProviderDockerCompose || engine.ComposeVersion != "2.39.1" {
		t.Fatalf("compose provider = %q %q", engine.ComposeProvider, engine.ComposeVersion)
	}
	if engine.HostGateway() != PodmanHostGateway {
		t.Fatalf("host gateway = %q", engine.HostGateway())
	}
}

func TestResolveAutoWithoutEitherEngineNamesBoth(t *testing.T) {
	host := &fakeHost{onPath: map[string]bool{}, replies: map[string]fakeReply{}}
	_, err := Resolve(context.Background(), host.system())
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"docker CLI is not on PATH", "podman CLI is not on PATH"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

func TestResolveExplicitChoiceNamesTheMissingPiece(t *testing.T) {
	cases := []struct {
		name   string
		choice string
		onPath map[string]bool
		mutate func(map[string]fakeReply)
		want   string
	}{
		{"docker cli", "docker", map[string]bool{"podman": true}, nil, "docker CLI is not on PATH"},
		{"docker daemon", "docker", map[string]bool{"docker": true}, func(r map[string]fakeReply) {
			r["docker version --format {{.Server.Version}}"] = fakeReply{fail: true}
		}, "Docker daemon is not answering"},
		{"docker compose", "docker", map[string]bool{"docker": true}, func(r map[string]fakeReply) {
			r["docker compose version --short"] = fakeReply{fail: true}
		}, "Docker Compose v2 plugin is missing"},
		{"docker is podman", "docker", map[string]bool{"docker": true}, func(r map[string]fakeReply) {
			r["docker --version"] = fakeReply{stdout: "podman version 5.4.2\n"}
		}, "Podman's docker wrapper"},
		{"podman cli", "podman", map[string]bool{"docker": true}, nil, "podman CLI is not on PATH"},
		{"podman machine", "podman", map[string]bool{"podman": true}, func(r map[string]fakeReply) {
			r["podman info --format json"] = fakeReply{fail: true}
		}, "podman machine start"},
		{"podman compose provider", "podman", map[string]bool{"podman": true}, func(r map[string]fakeReply) {
			r["podman compose version"] = fakeReply{fail: true}
		}, "podman compose has no provider"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replies := dockerReplies()
			if tc.mutate != nil {
				tc.mutate(replies)
			}
			host := &fakeHost{env: map[string]string{EnvVar: tc.choice}, onPath: tc.onPath, replies: replies}
			_, err := Resolve(context.Background(), host.system())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want mention of %q", err, tc.want)
			}
		})
	}
}

func TestResolveExplicitPodmanIgnoresDocker(t *testing.T) {
	host := &fakeHost{env: map[string]string{EnvVar: " Podman "}, onPath: map[string]bool{"docker": true, "podman": true}, replies: dockerReplies()}
	engine, err := Resolve(context.Background(), host.system())
	if err != nil {
		t.Fatal(err)
	}
	if engine.Kind != Podman {
		t.Fatalf("kind = %s", engine.Kind)
	}
	for _, line := range host.commands {
		if strings.HasPrefix(line, "docker") {
			t.Fatalf("explicit podman probed docker: %v", host.commands)
		}
	}
}

func TestChoiceRejectsUnknownEngine(t *testing.T) {
	if _, err := Choice(func(string) string { return "containerd" }); err == nil {
		t.Fatal("expected an error for an unknown engine")
	}
}

func TestSelectUsesPathOnly(t *testing.T) {
	host := &fakeHost{onPath: map[string]bool{"podman": true}}
	engine := Select(host.system())
	if engine.Kind != Podman || engine.Binary != "podman" || len(host.commands) != 0 {
		t.Fatalf("engine = %+v, commands = %v", engine, host.commands)
	}
	host = &fakeHost{onPath: map[string]bool{"docker": true, "podman": true}}
	if engine := Select(host.system()); engine.Kind != Docker {
		t.Fatalf("auto with both on PATH = %s", engine.Kind)
	}
}

func TestParseComposeVersion(t *testing.T) {
	cases := []struct {
		output, provider, version string
	}{
		{"Docker Compose version v2.39.1", ProviderDockerCompose, "2.39.1"},
		{"podman-compose version 1.3.0\npodman version 5.4.2", ProviderPodmanCompose, "1.3.0"},
		{"podman-compose version: 1.0.6\n['podman', '--version', '']\nusing podman version: 4.9.3", ProviderPodmanCompose, "1.0.6"},
		{`>>>> Executing external compose provider "/usr/bin/podman-compose". <<<<`, ProviderPodmanCompose, ""},
		{"something else", "", ""},
	}
	for _, tc := range cases {
		provider, version := parseComposeVersion(tc.output)
		if provider != tc.provider || version != tc.version {
			t.Fatalf("parseComposeVersion(%q) = %q %q, want %q %q", tc.output, provider, version, tc.provider, tc.version)
		}
	}
}

func TestChecksFlagPodmanIncompatibilities(t *testing.T) {
	engine := &Engine{
		Kind: Podman, Binary: "podman", Probed: true, Version: "5.2.1",
		ComposeProvider: ProviderPodmanCompose, ComposeVersion: "1.2.0",
		CgroupManager: "cgroupfs", Rootless: true,
		UIDMap: []IDMap{{ContainerID: 0, HostID: 1000, Size: 1}},
		GIDMap: []IDMap{{ContainerID: 0, HostID: 1000, Size: 1}},
	}
	failed := map[string]bool{}
	for _, check := range HardFailures(engine.Checks()) {
		failed[check.Name] = true
		if check.Remedy == "" {
			t.Fatalf("failed check %q has no remedy", check.Name)
		}
	}
	want := map[string]bool{
		"host-gateway in extra_hosts": true,
		"compose provider":            true,
		"scheduled healthchecks":      true,
		"rootless ID mapping":         true,
	}
	if !reflect.DeepEqual(failed, want) {
		t.Fatalf("hard failures = %v, want %v", failed, want)
	}

	engine.Version, engine.ComposeVersion, engine.CgroupManager = "5.3.0", "1.3.0", "systemd"
	engine.UIDMap = append(engine.UIDMap, IDMap{ContainerID: 1, HostID: 100000, Size: 65536})
	engine.GIDMap = append(engine.GIDMap, IDMap{ContainerID: 1, HostID: 100000, Size: 65536})
	if failed := HardFailures(engine.Checks()); len(failed) != 0 {
		t.Fatalf("unexpected failures: %+v", failed)
	}
}

func TestChecksDocker(t *testing.T) {
	engine := &Engine{Kind: Docker, Binary: "docker", Probed: true, Version: "29.8.0", ComposeProvider: ProviderDockerCompose, ComposeVersion: "5.5.1"}
	if failed := HardFailures(engine.Checks()); len(failed) != 0 {
		t.Fatalf("unexpected failures: %+v", failed)
	}
	engine.ComposeVersion = "1.29.2"
	if failed := HardFailures(engine.Checks()); len(failed) != 1 {
		t.Fatalf("compose v1 must fail: %+v", failed)
	}
}

func TestVersionAtLeast(t *testing.T) {
	cases := []struct {
		version string
		want    []int
		ok      bool
	}{
		{"5.3.0", []int{5, 3}, true},
		{"5.2.9", []int{5, 3}, false},
		{"6.0", []int{5, 3}, true},
		{"v2.24.4", []int{2, 24, 4}, true},
		{"2.24.3", []int{2, 24, 4}, false},
		{"2.24", []int{2, 24, 4}, false},
		{"5.3.0-rc1", []int{5, 3}, true},
		{"", []int{1}, false},
		{"dev", []int{1}, false},
	}
	for _, tc := range cases {
		if got := versionAtLeast(tc.version, tc.want); got != tc.ok {
			t.Fatalf("versionAtLeast(%q, %v) = %t", tc.version, tc.want, got)
		}
	}
}
