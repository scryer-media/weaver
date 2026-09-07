package main

import (
	"path/filepath"
	"strings"
	"testing"
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
