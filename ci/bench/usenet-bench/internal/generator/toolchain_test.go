package generator

import (
	"path/filepath"
	"testing"
)

func TestSourceLockedToolchainsCoverRAR3ThroughRAR7Writers(t *testing.T) {
	lock, err := LoadToolchainLock(filepath.Join("..", "..", "docker", "rarlab", "toolchains.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"rarlab-3.93": "rar_static",
		"rarlab-4.20": "rar_static",
		"rarlab-5.00": "rar_static",
		"rarlab-6.24": "rar",
		"rarlab-7.23": "rar",
	}
	if got := len(lock.Toolchains); got != len(want) {
		t.Fatalf("locked toolchains = %d, want %d", got, len(want))
	}
	for id, binary := range want {
		toolchain, found := lock.Find(id)
		if !found {
			t.Fatalf("missing source-locked toolchain %q", id)
		}
		if toolchain.Binary != binary {
			t.Errorf("%s binary = %q, want %q", id, toolchain.Binary, binary)
		}
	}
}
