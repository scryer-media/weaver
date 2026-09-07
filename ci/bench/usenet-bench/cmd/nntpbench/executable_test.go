package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeExecutable(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestAnExecutableNamedExactlyIsPresent(t *testing.T) {
	program := filepath.Join(t.TempDir(), "nzbget")
	writeExecutable(t, program)
	result := inspectExecutable("nzbget", program)
	if result.Status != "present" {
		t.Fatalf("status = %q (%s), want present", result.Status, result.Reason)
	}
}

// A catalog that differs from the filesystem only in case launches a program
// nobody named, because macOS matches the path anyway.
func TestAnExecutableFoundOnlyByCaseFoldingIsRefused(t *testing.T) {
	directory := t.TempDir()
	writeExecutable(t, filepath.Join(directory, "NZBGet"))
	asked := filepath.Join(directory, "nzbget")
	if _, err := os.Stat(asked); err != nil {
		t.Skip("this filesystem is case-sensitive, so the mismatch cannot arise")
	}
	result := inspectExecutable("nzbget", asked)
	if result.Status != "misnamed" {
		t.Fatalf("status = %q, want misnamed", result.Status)
	}
	if !strings.Contains(result.Reason, "NZBGet") {
		t.Fatalf("reason does not name the file on disk: %s", result.Reason)
	}
}

// NZBGet's macOS bundle ships a launcher in Contents/MacOS and the daemon
// itself deeper in. Launching the former measures the host user's own setup.
func TestABundleLauncherWithTheProgramBehindItIsRefused(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "NZBGet.app")
	launcher := filepath.Join(bundle, "Contents", "MacOS", "nzbget")
	daemon := filepath.Join(bundle, "Contents", "Resources", "daemon", "usr", "local", "bin", "nzbget")
	writeExecutable(t, launcher)
	writeExecutable(t, daemon)
	result := inspectExecutable("nzbget", launcher)
	if result.Status != "launcher" {
		t.Fatalf("status = %q (%s), want launcher", result.Status, result.Reason)
	}
	if !strings.Contains(result.Reason, daemon) {
		t.Fatalf("reason does not name the real program: %s", result.Reason)
	}
}

// A bundle holding only the one executable is the program, which is how
// SABnzbd ships; refusing it would refuse a client that runs correctly.
func TestABundleHoldingOnlyItsOwnProgramIsPresent(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "SABnzbd.app")
	program := filepath.Join(bundle, "Contents", "MacOS", "SABnzbd")
	writeExecutable(t, program)
	writeExecutable(t, filepath.Join(bundle, "Contents", "Resources", "helper"))
	result := inspectExecutable("sabnzbd", program)
	if result.Status != "present" {
		t.Fatalf("status = %q (%s), want present", result.Status, result.Reason)
	}
}
