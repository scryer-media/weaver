package nativeadapter

import (
	"os"
	"path/filepath"
	"testing"
)

func writeUnpacker(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// A packaged install ships the unpacker it was built against beside the
// daemon; that copy is the one to run, not whatever the host has on PATH.
func TestAnUnpackerBesideTheProgramWins(t *testing.T) {
	directory := t.TempDir()
	program := filepath.Join(directory, "nzbget")
	bundled := filepath.Join(directory, "unrar")
	writeUnpacker(t, program)
	writeUnpacker(t, bundled)
	if resolved := NZBGetUnpacker(program, NZBGetUnrarNames); resolved != bundled {
		t.Fatalf("resolved %q, want the bundled %q", resolved, bundled)
	}
}

// NZBGet's macOS bundle names 7-Zip "7za"; a config saying "7z" would send it
// after a program that is not there.
func TestABundledSevenZipUnderAnAlternateNameIsFound(t *testing.T) {
	directory := t.TempDir()
	program := filepath.Join(directory, "nzbget")
	bundled := filepath.Join(directory, "7za")
	writeUnpacker(t, program)
	writeUnpacker(t, bundled)
	if resolved := NZBGetUnpacker(program, NZBGetSevenZipNames); resolved != bundled {
		t.Fatalf("resolved %q, want the bundled %q", resolved, bundled)
	}
}

// With nothing beside the program and nothing on PATH the canonical name is
// returned unchanged, which is the signal preflight reports as missing.
func TestAnUnresolvableUnpackerFallsBackToItsCanonicalName(t *testing.T) {
	directory := t.TempDir()
	program := filepath.Join(directory, "nzbget")
	writeUnpacker(t, program)
	t.Setenv("PATH", directory)
	if resolved := NZBGetUnpacker(program, NZBGetUnrarNames); resolved != NZBGetUnrarCommand {
		t.Fatalf("resolved %q, want the canonical %q", resolved, NZBGetUnrarCommand)
	}
}

// Rendering has to agree with resolution, or preflight vouches for a binary
// the run never invokes.
func TestTheRenderedConfigNamesTheResolvedUnpackers(t *testing.T) {
	directory := t.TempDir()
	program := filepath.Join(directory, "nzbget")
	unrar := filepath.Join(directory, "unrar")
	sevenZip := filepath.Join(directory, "7za")
	writeUnpacker(t, program)
	writeUnpacker(t, unrar)
	writeUnpacker(t, sevenZip)
	spec := renderNZBGet(Config{APIEndpoint: "http://127.0.0.1:16789", LaunchCommand: []string{program}}, false)
	for _, want := range []string{"UnrarCmd=" + unrar, "SevenZipCmd=" + sevenZip} {
		if !containsLine(string(spec.Content), want) {
			t.Fatalf("rendered config does not carry %q", want)
		}
	}
}

// NZBGet's Windows install ships "unrar.exe" and "7za.exe" beside the
// daemon, and Windows keeps no execute bit to test; resolved by extension
// there, and never mistaken for a program anywhere else.
func TestAWindowsBundleNamesItsUnpackersByExtension(t *testing.T) {
	directory := t.TempDir()
	program := filepath.Join(directory, "nzbget.exe")
	bundled := filepath.Join(directory, "7za.exe")
	for _, path := range []string{program, bundled} {
		if err := os.WriteFile(path, []byte("MZ"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	// PATH holds nothing, so a hit can only be the beside-the-program check
	// (a Windows host's PATH lookup would otherwise find 7za.exe itself).
	t.Setenv("PATH", t.TempDir())
	if resolved := resolveNZBGetUnpacker("windows", program, NZBGetSevenZipNames); resolved != bundled {
		t.Fatalf("resolved %q on windows, want the bundled %q", resolved, bundled)
	}
	if resolved := resolveNZBGetUnpacker("linux", program, NZBGetSevenZipNames); resolved != NZBGetSevenZipCommand {
		t.Fatalf("resolved %q on linux, want the canonical %q", resolved, NZBGetSevenZipCommand)
	}
}

func containsLine(content, want string) bool {
	for _, line := range splitLines(content) {
		if line == want {
			return true
		}
	}
	return false
}

func splitLines(content string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(content); i++ {
		if content[i] == '\n' {
			lines = append(lines, content[start:i])
			start = i + 1
		}
	}
	return append(lines, content[start:])
}
