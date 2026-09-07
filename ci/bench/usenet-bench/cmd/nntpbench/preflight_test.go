package main

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nativeadapter"
)

// A check aimed at retyped directories can pass for a stack no session will
// ever run. Taking it from the chain description is what keeps the two the
// same stack.
func TestPreflightTakesTheRawStackFromTheChainItWillRun(t *testing.T) {
	if _, err := rawExecutionTarget(); err != nil {
		t.Skipf("no native execution target on this host: %v", err)
	}
	declared := rawChainConfig(t)
	path := writeChainConfigFile(t, declared)
	settings, wanted, err := preflightRawStack(path, "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !wanted {
		t.Fatal("a raw chain produced no stack to check")
	}
	raw := declared["raw"].(map[string]any)
	if settings.BinDir != raw["bin_dir"] || settings.DataDir != raw["data_dir"] {
		t.Fatalf("checked %s/%s, chain runs %s/%s", settings.BinDir, settings.DataDir, raw["bin_dir"], raw["data_dir"])
	}
	if settings.PasswordFile != declared["password_file"] {
		t.Fatalf("password file %q, chain declares %q", settings.PasswordFile, declared["password_file"])
	}
}

func TestPreflightRefusesAChainWithNoLocalServerSide(t *testing.T) {
	path := writeChainConfigFile(t, minimalChainConfig())
	if _, _, err := preflightRawStack(path, "", "", "", ""); err == nil {
		t.Fatal("accepted a docker chain as a raw stack to check")
	}
}

// Two descriptions of one stack is the drift this is here to prevent, so
// giving both is refused rather than silently resolved in favour of one.
func TestPreflightRefusesAChainAndFlagsTogether(t *testing.T) {
	path := writeChainConfigFile(t, rawChainConfig(t))
	_, _, err := preflightRawStack(path, t.TempDir(), t.TempDir(), "", "")
	if err == nil {
		t.Fatal("accepted a chain and raw flags describing the stack twice")
	}
	if !strings.Contains(err.Error(), "twice") {
		t.Fatalf("error %q does not say the stack was described twice", err)
	}
}

func TestPreflightWithoutARawStackChecksNothingExtra(t *testing.T) {
	_, wanted, err := preflightRawStack("", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if wanted {
		t.Fatal("a client-only preflight grew a raw stack")
	}
}

// Staging a host before its session exists is the case the flags are for; half
// a stack is not enough to check one.
func TestPreflightFlagsNeedBothDirectories(t *testing.T) {
	if _, _, err := preflightRawStack("", t.TempDir(), "", "", ""); err == nil {
		t.Fatal("accepted a bin directory with no article store")
	}
	binDir := t.TempDir()
	settings, wanted, err := preflightRawStack("", binDir, t.TempDir(), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !wanted {
		t.Fatal("the flags produced no stack to check")
	}
	// The certificate and log directories are made at startup, so the check
	// only needs them named; an unnamed one fails for the wrong reason.
	if settings.CertDir != filepath.Join(filepath.Dir(binDir), "certs") {
		t.Fatalf("certificate directory %q is not beside the staged binaries", settings.CertDir)
	}
	if settings.LogDir == "" {
		t.Fatal("log directory was left unnamed, which fails a check that has nothing to do with the host")
	}
}

func writeAdapterCatalog(t *testing.T, adapters ...map[string]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "adapters.json")
	body, err := json.Marshal(map[string]any{"schema_version": 4, "adapters": adapters})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func nativeAdapterEntry(client, adapter, launch string) map[string]any {
	return map[string]any{
		"client":            client,
		"archive_toolchain": "vanilla",
		"target":            string(benchmark.MacOSNative),
		"command":           []string{adapter},
		"environment": map[string]string{
			"NATIVE_CLIENT_VERSION": "1.2.3",
			"NATIVE_API_ENDPOINT":   "http://127.0.0.1:19090",
			"NATIVE_LAUNCH_COMMAND": launch,
		},
	}
}

func binaryByName(t *testing.T, binaries []preflightBinary, name string) preflightBinary {
	t.Helper()
	for _, binary := range binaries {
		if binary.Name == name {
			return binary
		}
	}
	t.Fatalf("no binary named %q in %+v", name, binaries)
	return preflightBinary{}
}

// The products are installed by hand, so the only thing the harness needs is
// where they are -- and the place it is told is the catalog, not a flag.
func TestPreflightChecksTheClientTheCatalogWillLaunch(t *testing.T) {
	root := t.TempDir()
	installed := filepath.Join(root, "weaver")
	if err := os.WriteFile(installed, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	adapter := filepath.Join(root, "nativeadapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	launch, err := json.Marshal([]string{installed, "serve", "--port", "{{api_port}}"})
	if err != nil {
		t.Fatal(err)
	}
	path := writeAdapterCatalog(t, nativeAdapterEntry("weaver", adapter, string(launch)))

	binaries, _, err := preflightCatalogBinaries(path, benchmark.MacOSNative)
	if err != nil {
		t.Fatal(err)
	}
	if got := binaryByName(t, binaries, "weaver"); got.Status != "present" || got.Path != installed {
		t.Fatalf("checked %+v, the catalog launches %s", got, installed)
	}
	if got := binaryByName(t, binaries, "adapter"); got.Status != "present" {
		t.Fatalf("adapter %+v", got)
	}
}

func TestPreflightNamesTheClientAProductPathIsMissingFor(t *testing.T) {
	adapter := filepath.Join(t.TempDir(), "nativeadapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	launch, err := json.Marshal([]string{"/absolute/path/to/nzbget", "-s"})
	if err != nil {
		t.Fatal(err)
	}
	binaries, _, err := preflightCatalogBinaries(
		writeAdapterCatalog(t, nativeAdapterEntry("nzbget", adapter, string(launch))),
		benchmark.MacOSNative)
	if err != nil {
		t.Fatal(err)
	}
	if got := binaryByName(t, binaries, "nzbget"); got.Status == "present" {
		t.Fatal("an unreplaced catalog placeholder passed as an installed product")
	}
}

// A launch command that is not an argv array fails every run of that client,
// and it fails identically whether or not the product is installed.
func TestPreflightReportsAnUnusableLaunchCommand(t *testing.T) {
	adapter := filepath.Join(t.TempDir(), "nativeadapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, launch := range map[string]string{
		"a shell string": "/usr/bin/weaver serve",
		"an empty argv":  "[]",
		"nothing at all": "",
	} {
		t.Run(name, func(t *testing.T) {
			binaries, _, err := preflightCatalogBinaries(
				writeAdapterCatalog(t, nativeAdapterEntry("weaver", adapter, launch)),
				benchmark.MacOSNative)
			if err != nil {
				t.Fatal(err)
			}
			check := binaryByName(t, binaries, "weaver")
			if check.Status == "present" {
				t.Fatalf("%s passed as a launchable client", name)
			}
			if check.Reason == "" {
				t.Fatal("no reason given for an unusable launch command")
			}
		})
	}
}

// One staged launcher is the normal case; reporting it once per client turns
// a short report into three copies of the same line.
func TestPreflightReportsOneSharedAdapterOnce(t *testing.T) {
	adapter := filepath.Join(t.TempDir(), "nativeadapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	launch, err := json.Marshal([]string{"/absolute/path/to/product"})
	if err != nil {
		t.Fatal(err)
	}
	shared := writeAdapterCatalog(t,
		nativeAdapterEntry("weaver", adapter, string(launch)),
		nativeAdapterEntry("sabnzbd", adapter, string(launch)),
	)
	binaries, _, err := preflightCatalogBinaries(shared, benchmark.MacOSNative)
	if err != nil {
		t.Fatal(err)
	}
	if got := binaryByName(t, binaries, "adapter"); got.Status != "present" {
		t.Fatalf("shared adapter %+v", got)
	}

	// Two different launchers really are two files, so they are reported apart.
	other := filepath.Join(t.TempDir(), "nativeadapter")
	if err := os.WriteFile(other, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	split := writeAdapterCatalog(t,
		nativeAdapterEntry("weaver", adapter, string(launch)),
		nativeAdapterEntry("sabnzbd", other, string(launch)),
	)
	binaries, _, err = preflightCatalogBinaries(split, benchmark.MacOSNative)
	if err != nil {
		t.Fatal(err)
	}
	binaryByName(t, binaries, "weaver adapter")
	binaryByName(t, binaries, "sabnzbd adapter")
}

// Checking a catalog for the wrong target would report every client as fine
// while checking none of them.
func TestPreflightRefusesACatalogWithNothingForTheTarget(t *testing.T) {
	adapter := filepath.Join(t.TempDir(), "nativeadapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	launch, err := json.Marshal([]string{adapter})
	if err != nil {
		t.Fatal(err)
	}
	path := writeAdapterCatalog(t, nativeAdapterEntry("weaver", adapter, string(launch)))
	if _, _, err := preflightCatalogBinaries(path, benchmark.WindowsNative); err == nil {
		t.Fatal("accepted a macOS catalog as a Windows host check")
	}
}

// A product needs more from a host than its own executable, and none of it
// arrives with the install. Both gaps below surface late and badly: a missing
// unpacker fails output verification after a full download, and a Weaver with
// no key of its own blocks on a keychain prompt nobody is there to answer.
func TestPreflightChecksWhatAClientNeedsBeyondItsExecutable(t *testing.T) {
	adapter := filepath.Join(t.TempDir(), "nativeadapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	launch, err := json.Marshal([]string{adapter})
	if err != nil {
		t.Fatal(err)
	}
	path := writeAdapterCatalog(t,
		nativeAdapterEntry("nzbget", adapter, string(launch)),
		nativeAdapterEntry("weaver", adapter, string(launch)),
		nativeAdapterEntry("sabnzbd", adapter, string(launch)),
	)
	_, clients, err := preflightCatalogBinaries(path, benchmark.MacOSNative)
	if err != nil {
		t.Fatal(err)
	}

	// NZBGet shells out to both unpackers by name and ships neither.
	for _, tool := range []string{nativeadapter.NZBGetUnrarCommand, nativeadapter.NZBGetSevenZipCommand} {
		check := clientCheck(t, clients, "nzbget", tool)
		if _, err := exec.LookPath(tool); err != nil {
			if check.Status != "missing" {
				t.Fatalf("%s is not on this host but the check says %+v", tool, check)
			}
		} else if check.Status != "present" {
			t.Fatalf("%s resolves on this host but the check says %+v", tool, check)
		}
	}

	// The example catalogs ship without a key, which is the case worth
	// catching: the run does not fail, it hangs.
	if check := clientCheck(t, clients, "weaver", "WEAVER_ENCRYPTION_KEY"); check.Status != "missing" {
		t.Fatalf("a catalog with no encryption key passed: %+v", check)
	}

	// SABnzbd bundles its own unpackers and needs no key, so the address the
	// adapter will poll is its only requirement; inventing another would fail
	// a host that is in fact ready.
	var sab []string
	for _, check := range clients {
		if check.Client == "sabnzbd" {
			sab = append(sab, check.Name)
		}
	}
	if len(sab) != 1 || sab[0] != "API address" {
		t.Fatalf("sabnzbd requirements are %v", sab)
	}

	// Every client is polled over HTTP, so every client has that check.
	for _, client := range []string{"weaver", "nzbget", "sabnzbd"} {
		clientCheck(t, clients, client, "API address")
	}
}

// A filled-in catalog has to be able to pass, or the check is just an
// unconditional failure and preflight can never report a host as ready.
func TestPreflightAcceptsAWeaverEntryCarryingItsOwnKey(t *testing.T) {
	adapter := filepath.Join(t.TempDir(), "nativeadapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	launch, err := json.Marshal([]string{adapter})
	if err != nil {
		t.Fatal(err)
	}
	entry := nativeAdapterEntry("weaver", adapter, string(launch))
	entry["environment"].(map[string]string)["WEAVER_ENCRYPTION_KEY"] = base64.StdEncoding.EncodeToString(make([]byte, 32))
	_, clients, err := preflightCatalogBinaries(writeAdapterCatalog(t, entry), benchmark.MacOSNative)
	if err != nil {
		t.Fatal(err)
	}
	if check := clientCheck(t, clients, "weaver", "WEAVER_ENCRYPTION_KEY"); check.Status != "present" {
		t.Fatalf("a catalog carrying a key was reported as %+v", check)
	}
}

// A key Weaver will not parse is as bad as no key: it exits while starting up
// and the run reports only a client that never became ready. Both shapes of
// bad key are worth naming, because neither is visible from the failure.
func TestPreflightRefusesAWeaverKeyWeaverWouldReject(t *testing.T) {
	adapter := filepath.Join(t.TempDir(), "nativeadapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	launch, err := json.Marshal([]string{adapter})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"nntpbench-native-lane-key",                         // not base64 at all
		base64.StdEncoding.EncodeToString(make([]byte, 16)), // well-formed, wrong length
	} {
		entry := nativeAdapterEntry("weaver", adapter, string(launch))
		entry["environment"].(map[string]string)["WEAVER_ENCRYPTION_KEY"] = key
		_, clients, err := preflightCatalogBinaries(writeAdapterCatalog(t, entry), benchmark.MacOSNative)
		if err != nil {
			t.Fatal(err)
		}
		if check := clientCheck(t, clients, "weaver", "WEAVER_ENCRYPTION_KEY"); check.Status != "unusable" {
			t.Fatalf("key %q was accepted as %+v", key, check)
		}
	}
}

// The shipped catalogs are the first thing an operator copies, so a key that
// does not parse ships a host that cannot run Weaver.
func TestExampleCatalogsCarryAKeyWeaverAccepts(t *testing.T) {
	for _, path := range []string{
		filepath.Join("..", "..", "configs", "adapters.macos.example.json"),
		filepath.Join("..", "..", "configs", "adapters.windows.example.json"),
	} {
		catalog, err := benchmark.LoadAdapterCatalog(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, adapter := range catalog.Adapters {
			if adapter.Client != benchmark.Weaver {
				continue
			}
			check := clientCheck(t, inspectClientRequirements(adapter), "weaver", "WEAVER_ENCRYPTION_KEY")
			if check.Status != "present" {
				t.Fatalf("%s: %+v", path, check)
			}
		}
	}
}

func clientCheck(t *testing.T, checks []preflightClientCheck, client, name string) preflightClientCheck {
	t.Helper()
	for _, check := range checks {
		if check.Client == client && check.Name == name {
			return check
		}
	}
	t.Fatalf("no %s check named %q in %+v", client, name, checks)
	return preflightClientCheck{}
}

// A busy API port is the one condition no configuration settles, and the
// product does not refuse it: SABnzbd relocates and rewrites its ini, leaving
// the adapter polling whatever else answers there.
func TestPreflightReportsAnAPIPortSomethingElseHolds(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	check := apiPortCheck("sabnzbd", "http://"+listener.Addr().String())
	if check.Status != "in use" {
		t.Fatalf("an occupied port was reported as %+v", check)
	}

	// The same address, once nothing holds it, has to pass.
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if check := apiPortCheck("sabnzbd", "http://"+address); check.Status != "present" {
		t.Fatalf("a free port was reported as %+v", check)
	}
}
