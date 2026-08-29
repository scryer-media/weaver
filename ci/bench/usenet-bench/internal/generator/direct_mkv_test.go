package generator

import (
	"path/filepath"
	"testing"
)

func TestDirectMKVPathIsARealMediaFilename(t *testing.T) {
	if got, want := filepath.Base(directMKVPath), "direct-200mb.mkv"; got != want {
		t.Fatalf("direct MKV path = %q, want %q", got, want)
	}
}
