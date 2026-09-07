package nativeadapter

import (
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/clientadapter"
)

// The benchmark's whole cross-target claim is that a native run and a
// containerized one measure the same client. A container is a bare install by
// construction -- every run starts from a pristine image -- so the Docker
// lane's rendered configuration is the reference for what a never-configured
// product needs. Where the two lanes disagree about a setting that changes
// behaviour, one of the lanes is measuring a different client.
//
// Only host-shaped settings may differ: where files live, what address the
// control API binds, and whether the process daemonizes.
var dockerOnlySettings = map[benchmark.Client]map[string]string{
	benchmark.SABnzbd: {
		// LinuxServer image plumbing, not a client setting.
		"misc.PATH": "the image's own interpreter path, which has no native equivalent",
	},
	benchmark.NZBGet: {},
}

// hostResolvedSettings must be set by both lanes, but their values name a file
// on the host: the pinned image knows an absolute path, a native install
// resolves the same tool from the install's own bundle or from PATH. Both
// still have to state it rather than inherit whatever the package defaults to.
var hostResolvedSettings = map[benchmark.Client][]string{
	benchmark.SABnzbd: {},
	benchmark.NZBGet:  {"UnrarCmd", "SevenZipCmd"},
}

var nativeOnlySettings = map[benchmark.Client]map[string]string{
	benchmark.SABnzbd: {},
	benchmark.NZBGet: {
		"DaemonMode": "a native run keeps the process in the foreground so the launcher can collect its CPU time",
	},
}

// behaviourSettings must carry the same value in both lanes: each one changes
// what the client does on the wire or on disk, so a difference is a different
// measurement rather than a different host.
var behaviourSettings = map[benchmark.Client][]string{
	benchmark.SABnzbd: {
		"misc.enable_unrar",
		"misc.direct_unpack",
		"misc.pre_check",
		"misc.pause_on_post_processing",
		// Without the conversion stamp SABnzbd treats the rendered server as
		// legacy and fetches one article per round trip.
		"misc.config_conversion_version",
		"servers.benchmark.connections",
		"servers.benchmark.pipelining_requests",
		"servers.benchmark.ssl",
		"servers.benchmark.ssl_verify",
	},
	benchmark.NZBGet: {
		"DirectWrite",
		"DirectUnpack",
		"ParCheck",
		"ParRepair",
		"Unpack",
		"Extensions",
		"Server1.Active",
		"Server1.Connections",
		"Server1.Encryption",
		"Server1.CertVerification",
		"CertCheck",
	},
}

func TestNativeAndDockerLanesRenderTheSameClient(t *testing.T) {
	for _, client := range []benchmark.Client{benchmark.SABnzbd, benchmark.NZBGet} {
		t.Run(string(client), func(t *testing.T) {
			native := parseSettings(t, renderNativeConfig(t, client))
			docker := parseSettings(t, renderDockerConfig(t, client))

			for _, name := range missing(docker, native) {
				if _, allowed := dockerOnlySettings[client][name]; !allowed {
					t.Errorf("the Docker lane sets %s and the native lane does not; a bare native install is then configured differently", name)
				}
			}
			for _, name := range missing(native, docker) {
				if _, allowed := nativeOnlySettings[client][name]; !allowed {
					t.Errorf("the native lane sets %s and the Docker lane does not", name)
				}
			}
			for _, name := range hostResolvedSettings[client] {
				if value, ok := native[name]; !ok || value == "" {
					t.Errorf("the native lane leaves %s to the install's own default", name)
				}
				if value, ok := docker[name]; !ok || value == "" {
					t.Errorf("the Docker lane leaves %s to the image's own default", name)
				}
			}
			for _, name := range behaviourSettings[client] {
				nativeValue, ok := native[name]
				if !ok {
					t.Errorf("the native lane does not set %s at all", name)
					continue
				}
				dockerValue, ok := docker[name]
				if !ok {
					t.Errorf("the Docker lane does not set %s at all", name)
					continue
				}
				if nativeValue != dockerValue {
					t.Errorf("%s is %q natively and %q in Docker; that is a different client, not a different host", name, nativeValue, dockerValue)
				}
			}
		})
	}
}

// A never-configured NZBGet resolves its unpackers from the configuration, not
// from whatever the package happened to default to, and runs no extension it
// was not told about.
func TestNativeNZBGetStatesWhatABareInstallWouldLeaveToTheHost(t *testing.T) {
	settings := parseSettings(t, renderNativeConfig(t, benchmark.NZBGet))
	if value, ok := settings["Extensions"]; !ok || value != "" {
		t.Errorf("Extensions = %q (set: %t), want it stated and empty", value, ok)
	}
	// The unpackers name a file on the host, so what a bare install needs
	// stated is that the setting is there and names one of the binaries
	// NZBGet can actually run -- not any particular path.
	for name, names := range map[string][]string{
		"UnrarCmd":    NZBGetUnrarNames,
		"SevenZipCmd": NZBGetSevenZipNames,
	} {
		value, ok := settings[name]
		if !ok {
			t.Errorf("%s is left to the install's own default", name)
			continue
		}
		if !slices.Contains(names, filepath.Base(value)) {
			t.Errorf("%s = %q, which is none of %v", name, value, names)
		}
	}
}

func renderNativeConfig(t *testing.T, client benchmark.Client) []byte {
	t.Helper()
	cfg := testConfig(client)
	cfg.Profile = benchmark.ProfileEquivalentThroughput
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return spec.Content
}

func renderDockerConfig(t *testing.T, client benchmark.Client) []byte {
	t.Helper()
	root := t.TempDir()
	cfg := clientadapter.Config{
		RunID:            "run-0001",
		Client:           client,
		ArchiveToolchain: benchmark.VanillaArchiveToolchain,
		ExecutionTarget:  benchmark.DockerLinux,
		Transport:        benchmark.Plaintext,
		TransportLabel:   string(benchmark.Plaintext),
		TLSValidation:    benchmark.TLSNotApplicable,
		ServerLink:       benchmark.DefaultServerLinkProfile(),
		StorageProfile:   benchmark.DefaultStorageProfile(),
		FixtureDir:       root,
		NZBPath:          filepath.Join(root, "fixture.nzb"),
		NNTPHost:         "nntp",
		NNTPPort:         "119",
		NNTPUsername:     "user",
		NNTPPassword:     "password",
		Connections:      8,
		Profile:          benchmark.ProfileEquivalentThroughput,
	}
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	return spec.ConfigContent
}

// parseSettings reads both rendered formats into one shape. SABnzbd's ini
// repeats names across sections -- `host` is the web UI in [misc] and the news
// server under [servers] -- so a setting is identified by its section too.
func parseSettings(t *testing.T, content []byte) map[string]string {
	t.Helper()
	settings := map[string]string{}
	section := ""
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			name := strings.Trim(line, "[]")
			if strings.HasPrefix(section, "servers") {
				section = "servers." + name
			} else {
				section = name
			}
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			t.Fatalf("rendered line %q is neither a section nor a setting", line)
		}
		key = strings.TrimSpace(key)
		if section != "" {
			key = section + "." + key
		}
		settings[key] = strings.TrimSpace(value)
	}
	return settings
}

func missing(from, in map[string]string) []string {
	var names []string
	for name := range from {
		if _, ok := in[name]; !ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}
