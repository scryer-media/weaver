package benchmark

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteServerLinkEnvironmentIsImmutableAndExact(t *testing.T) {
	profile, err := ResolveServerLinkProfile(Link1Gbit, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "server.env")
	if err := WriteServerLinkEnvironment(path, profile); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"nntpbench server link profile: 1gbit", "NNTP_EGRESS_BITS_PER_SECOND=1000000000", "NNTP_EGRESS_BURST_BYTES=1048576"} {
		if !strings.Contains(string(contents), want) {
			t.Fatalf("environment missing %q: %s", want, contents)
		}
	}
	if err := WriteServerLinkEnvironment(path, profile); err == nil {
		t.Fatal("server environment must not overwrite prior evidence")
	}
}
