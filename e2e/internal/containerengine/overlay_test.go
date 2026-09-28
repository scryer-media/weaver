package containerengine

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const sampleCompose = `services:
  # comment
  nntp:
    image: ${E2E_NNTP_IMAGE:-weaver-e2e-nntp:local}
    secrets:
      - nntp-password
    extra_hosts:
      - "host.docker.internal:host-gateway"
  weaver:
    command:
      - mkdir -p /config
  nyuu:
    network_mode: "service:nntp"

volumes:
  nntp-data:

secrets:
  nntp-password:
    environment: E2E_NNTP_PASSWORD
`

func TestParseComposeLayout(t *testing.T) {
	layout := ParseComposeLayout([]byte(sampleCompose))
	if want := []string{"nntp", "weaver", "nyuu"}; !reflect.DeepEqual(layout.Services, want) {
		t.Fatalf("services = %v, want %v", layout.Services, want)
	}
	if want := map[string]string{"nntp-password": "E2E_NNTP_PASSWORD"}; !reflect.DeepEqual(layout.EnvironmentSecrets, want) {
		t.Fatalf("secrets = %v, want %v", layout.EnvironmentSecrets, want)
	}
}

func TestParseComposeLayoutOfHarnessComposeFile(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	layout := ParseComposeLayout(content)
	for _, service := range []string{"nntp", "nntp2", "toxiproxy", "weaver", "weaver-postgres", "weaver-playwright", "nyuu"} {
		found := false
		for _, name := range layout.Services {
			found = found || name == service
		}
		if !found {
			t.Fatalf("service %s missing from %v", service, layout.Services)
		}
	}
	if layout.EnvironmentSecrets["nntp-password"] != "E2E_NNTP_PASSWORD" {
		t.Fatalf("environment secret not found: %v", layout.EnvironmentSecrets)
	}
}

func TestOverlayIsNeverWrittenForDocker(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "docker-compose.yml")
	writeTestFile(t, base, sampleCompose)
	engine := &Engine{Kind: Docker, Binary: "docker", SELinux: true}
	files, err := engine.WriteOverlay(filepath.Join(dir, "state"), base, func(string) string { return "secret" })
	if err != nil {
		t.Fatal(err)
	}
	if files.Compose != "" || len(files.Secrets) != 0 {
		t.Fatalf("docker overlay = %+v", files)
	}
	if _, err := os.Stat(filepath.Join(dir, "state")); !os.IsNotExist(err) {
		t.Fatalf("docker created overlay state: %v", err)
	}
}

func TestOverlayForDockerComposeProviderWithoutSELinuxIsEmpty(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "docker-compose.yml")
	writeTestFile(t, base, sampleCompose)
	engine := &Engine{Kind: Podman, Binary: "podman", ComposeProvider: ProviderDockerCompose}
	files, err := engine.WriteOverlay(filepath.Join(dir, "state"), base, func(string) string { return "secret" })
	if err != nil {
		t.Fatal(err)
	}
	if files.Compose != "" || len(files.Secrets) != 0 {
		t.Fatalf("overlay = %+v; docker-compose implements environment secrets itself", files)
	}
}

func TestOverlayFallsBackToFileSecretForPodmanCompose(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "docker-compose.yml")
	writeTestFile(t, base, sampleCompose)
	state := filepath.Join(dir, "state dir")
	engine := &Engine{Kind: Podman, Binary: "podman", ComposeProvider: ProviderPodmanCompose}
	files, err := engine.WriteOverlay(state, base, func(key string) string {
		if key == "E2E_NNTP_PASSWORD" {
			return "fixture-pass"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(state, "secret-nntp-password")
	if !reflect.DeepEqual(files.Secrets, []string{secret}) {
		t.Fatalf("secrets = %v", files.Secrets)
	}
	info, err := os.Stat(secret)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("secret mode = %o, want 600", mode)
	}
	if got := readTestFile(t, secret); got != "fixture-pass" {
		t.Fatalf("secret content = %q", got)
	}
	overlay := readTestFile(t, files.Compose)
	if !strings.Contains(overlay, "secrets:\n  nntp-password:\n    file: \""+secret+"\"\n") {
		t.Fatalf("overlay does not point the secret at its file:\n%s", overlay)
	}
	if strings.Contains(overlay, "security_opt") {
		t.Fatalf("overlay relabels without SELinux:\n%s", overlay)
	}

	files.Remove()
	if _, err := os.Stat(secret); !os.IsNotExist(err) {
		t.Fatalf("secret survived Remove: %v", err)
	}
}

func TestOverlayDisablesLabelsUnderSELinux(t *testing.T) {
	engine := &Engine{Kind: Podman, Binary: "podman", ComposeProvider: ProviderDockerCompose, SELinux: true}
	yaml := engine.OverlayYAML(ParseComposeLayout([]byte(sampleCompose)), nil)
	for _, service := range []string{"nntp", "weaver", "nyuu"} {
		if !strings.Contains(yaml, "  "+service+":\n    security_opt:\n      - label=disable\n") {
			t.Fatalf("service %s not unlabeled:\n%s", service, yaml)
		}
	}
	if strings.Contains(yaml, "secrets:") {
		t.Fatalf("docker-compose provider got a file secret:\n%s", yaml)
	}
}

func TestComposeArgsUnchangedWithoutOverlay(t *testing.T) {
	for _, engine := range []*Engine{
		{Kind: Docker, Binary: "docker"},
		{Kind: Podman, Binary: "podman"},
	} {
		engine.SetComposeFiles("/e2e/docker-compose.yml", "")
		args := []string{"compose", "-p", "e2e", "up", "-d", "nntp"}
		if got := engine.Args(args...); !reflect.DeepEqual(got, args) {
			t.Fatalf("%s args = %v", engine.Kind, got)
		}
	}
	down := []string{"compose", "-p", "e2e", "down", "-v", "--remove-orphans"}
	docker := &Engine{Kind: Docker, Binary: "docker"}
	if got := docker.Args(down...); !reflect.DeepEqual(got, down) {
		t.Fatalf("docker down args = %v", got)
	}
	podman := &Engine{Kind: Podman, Binary: "podman"}
	podman.SetComposeFiles("/e2e/docker-compose.yml", "")
	want := []string{"compose", "-p", "e2e", "--profile", "*", "down", "-v", "--remove-orphans"}
	if got := podman.composeArgs(down, func(string) string { return "" }); !reflect.DeepEqual(got, want) {
		t.Fatalf("podman down args = %v", got)
	}
	docker.SetComposeFiles("/e2e/docker-compose.yml", "/state/overlay.yml")
	if docker.ComposeOverlay() != "" {
		t.Fatal("docker accepted an overlay")
	}
}

func TestComposeArgsAppendOverlay(t *testing.T) {
	engine := &Engine{Kind: Podman, Binary: "podman"}
	engine.SetComposeFiles("/e2e/docker-compose.yml", "/state/overlay.yml")
	env := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	cases := []struct {
		name string
		args []string
		env  map[string]string
		want []string
	}{
		{
			name: "default file",
			args: []string{"compose", "-p", "e2e", "up", "-d", "nntp"},
			want: []string{"compose", "-p", "e2e", "-f", "/e2e/docker-compose.yml", "-f", "/state/overlay.yml", "up", "-d", "nntp"},
		},
		{
			name: "explicit files",
			args: []string{"compose", "-p", "e2e", "-f", "/e2e/docker-compose.yml", "-f", "/e2e/docker-compose.preseeded-nntp.yml", "ps", "-q", "nntp"},
			want: []string{"compose", "-p", "e2e", "-f", "/e2e/docker-compose.yml", "-f", "/e2e/docker-compose.preseeded-nntp.yml", "-f", "/state/overlay.yml", "ps", "-q", "nntp"},
		},
		{
			name: "COMPOSE_FILE",
			args: []string{"compose", "--project-name=e2e", "down", "-v"},
			env:  map[string]string{"COMPOSE_FILE": "/e2e/docker-compose.yml" + string(os.PathListSeparator) + "/run/network.yml"},
			want: []string{"compose", "--project-name=e2e", "-f", "/e2e/docker-compose.yml", "-f", "/run/network.yml", "-f", "/state/overlay.yml", "--profile", "*", "down", "-v"},
		},
		{
			name: "down keeps an explicit profile",
			args: []string{"compose", "-p", "e2e", "--profile", "cli", "down"},
			want: []string{"compose", "-p", "e2e", "--profile", "cli", "-f", "/e2e/docker-compose.yml", "-f", "/state/overlay.yml", "down"},
		},
		{
			name: "down keeps COMPOSE_PROFILES",
			args: []string{"compose", "-p", "e2e", "down"},
			env:  map[string]string{"COMPOSE_PROFILES": "cli"},
			want: []string{"compose", "-p", "e2e", "-f", "/e2e/docker-compose.yml", "-f", "/state/overlay.yml", "down"},
		},
		{
			name: "COMPOSE_PATH_SEPARATOR",
			args: []string{"compose", "config"},
			env:  map[string]string{"COMPOSE_FILE": "a.yml,b.yml", "COMPOSE_PATH_SEPARATOR": ","},
			want: []string{"compose", "-f", "a.yml", "-f", "b.yml", "-f", "/state/overlay.yml", "config"},
		},
		{
			name: "subcommand flags that look global are untouched",
			args: []string{"compose", "-p", "e2e", "run", "--rm", "-f", "x", "weaver"},
			want: []string{"compose", "-p", "e2e", "-f", "/e2e/docker-compose.yml", "-f", "/state/overlay.yml", "run", "--rm", "-f", "x", "weaver"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := engine.composeArgs(tc.args, env(tc.env)); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got  %v\nwant %v", got, tc.want)
			}
		})
	}
}

func TestBuildxBuildBecomesPodmanBuild(t *testing.T) {
	args := []string{"buildx", "build", "--load", "--provenance=false", "--label", "a=b", "-f", "-", "-t", "img:local", "/ctx"}
	podman := &Engine{Kind: Podman, Binary: "podman"}
	if got, want := podman.Args(args...), []string{"build", "--label", "a=b", "-f", "-", "-t", "img:local", "/ctx"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("podman build args = %v", got)
	}
	docker := &Engine{Kind: Docker, Binary: "docker"}
	if got := docker.Args(args...); !reflect.DeepEqual(got, args) {
		t.Fatalf("docker build args changed: %v", got)
	}
}

func TestServiceContainerArgs(t *testing.T) {
	podmanCompose := &Engine{Kind: Podman, Binary: "podman", ComposeProvider: ProviderPodmanCompose}
	want := []string{"ps", "-q", "-a", "--filter", "label=com.docker.compose.project=e2e", "--filter", "label=com.docker.compose.service=weaver"}
	if got := podmanCompose.ServiceContainerArgs("e2e", "weaver", true); !reflect.DeepEqual(got, want) {
		t.Fatalf("podman-compose lookup = %v", got)
	}
	dockerCompose := &Engine{Kind: Podman, Binary: "podman", ComposeProvider: ProviderDockerCompose}
	want = []string{"compose", "-p", "e2e", "ps", "-q", "weaver"}
	if got := dockerCompose.ServiceContainerArgs("e2e", "weaver", false); !reflect.DeepEqual(got, want) {
		t.Fatalf("docker-compose lookup = %v", got)
	}
}

func TestNetworkSubnetTemplate(t *testing.T) {
	if got := (&Engine{Kind: Docker}).NetworkSubnetTemplate(); !strings.Contains(got, ".IPAM.Config") {
		t.Fatalf("docker template = %q", got)
	}
	if got := (&Engine{Kind: Podman}).NetworkSubnetTemplate(); !strings.Contains(got, ".Subnets") {
		t.Fatalf("podman template = %q", got)
	}
}

func TestRunUserArgsKeepIDOnlyForRootlessPodman(t *testing.T) {
	if got := (&Engine{Kind: Podman, Rootless: true}).RunUserArgs(); !reflect.DeepEqual(got, []string{"--userns=keep-id"}) {
		t.Fatalf("rootless podman = %v", got)
	}
	if got := (&Engine{Kind: Podman}).RunUserArgs(); got != nil {
		t.Fatalf("rootful podman = %v", got)
	}
	if got := (&Engine{Kind: Docker, Rootless: true}).RunUserArgs(); got != nil {
		t.Fatalf("docker = %v", got)
	}
}

func TestHostGatewayNames(t *testing.T) {
	if got := (&Engine{Kind: Docker}).HostGateway(); got != "host.docker.internal" {
		t.Fatalf("docker = %q", got)
	}
	if got := (&Engine{Kind: Podman}).HostGateway(); got != "host.containers.internal" {
		t.Fatalf("podman = %q", got)
	}
}

func TestDockerCLIEnvShimsPodman(t *testing.T) {
	bin := t.TempDir()
	podman := filepath.Join(bin, "podman")
	writeTestFile(t, podman, "#!/bin/sh\n")
	if err := os.Chmod(podman, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	root := t.TempDir()
	engine := &Engine{Kind: Podman, Binary: "podman"}
	environ, err := engine.DockerCLIEnv([]string{"HOME=/h", "PATH=" + bin}, root)
	if err != nil {
		t.Fatal(err)
	}
	shimDir := filepath.Join(root, "docker-cli-shim")
	if want := []string{"HOME=/h", "PATH=" + shimDir + string(os.PathListSeparator) + bin}; !reflect.DeepEqual(environ, want) {
		t.Fatalf("environ = %v, want %v", environ, want)
	}
	script := readTestFile(t, filepath.Join(shimDir, "docker"))
	if !strings.Contains(script, "exec '"+podman+"' \"$@\"") {
		t.Fatalf("shim script = %q", script)
	}

	docker := &Engine{Kind: Docker, Binary: "docker"}
	original := []string{"PATH=/usr/bin"}
	if got, err := docker.DockerCLIEnv(original, root); err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("docker environ = %v, %v", got, err)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

const ownedVolumesCompose = `services:
  owner:
    volumes:
      - shared:/data
  reader:
    volumes:
      - shared:/shared:ro
    x-e2e-owner: puid

volumes:
  # a comment between volumes
  shared:
    x-e2e-owner: puid
  scratch:
  quoted:
    x-e2e-owner: "puid"
  other:
    x-e2e-owner: root
`

func TestParseComposeLayoutFindsPUIDOwnedVolumes(t *testing.T) {
	layout := ParseComposeLayout([]byte(ownedVolumesCompose))
	if want := []string{"shared", "quoted"}; !reflect.DeepEqual(layout.PUIDOwnedVolumes, want) {
		t.Fatalf("PUID-owned volumes = %v, want %v", layout.PUIDOwnedVolumes, want)
	}
}

func TestHarnessComposeFileMarksVolumesWeaverShares(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	layout := ParseComposeLayout(content)
	want := []string{"weaver-data", "weaver-downloads", "weaver-watch-folder"}
	if !reflect.DeepEqual(layout.PUIDOwnedVolumes, want) {
		t.Fatalf("PUID-owned volumes = %v, want %v", layout.PUIDOwnedVolumes, want)
	}
}

func TestPodmanOverlayPinsPUIDOwnedVolumeOwnership(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "docker-compose.yml")
	writeTestFile(t, base, ownedVolumesCompose)
	engine := &Engine{Kind: Podman, Binary: "podman", ComposeProvider: ProviderDockerCompose}
	files, err := engine.WriteOverlay(filepath.Join(dir, "state"), base, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if files.Compose == "" {
		t.Fatal("Podman wrote no overlay for PUID-owned volumes")
	}
	overlay := readTestFile(t, files.Compose)
	want := "volumes:\n" +
		"  shared:\n    driver_opts:\n      o: \"uid=${PUID:-1000},gid=${PGID:-1000}\"\n" +
		"  quoted:\n    driver_opts:\n      o: \"uid=${PUID:-1000},gid=${PGID:-1000}\"\n"
	if !strings.Contains(overlay, want) {
		t.Fatalf("overlay does not pin volume ownership:\n%s", overlay)
	}
	for _, unowned := range []string{"scratch", "other"} {
		if strings.Contains(overlay, "  "+unowned+":") {
			t.Fatalf("overlay pinned unmarked volume %s:\n%s", unowned, overlay)
		}
	}

	docker := &Engine{Kind: Docker, Binary: "docker"}
	if yaml := docker.OverlayYAML(ParseComposeLayout([]byte(ownedVolumesCompose)), nil); yaml != "" {
		t.Fatalf("docker got an overlay:\n%s", yaml)
	}
}
