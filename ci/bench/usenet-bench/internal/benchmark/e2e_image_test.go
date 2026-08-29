package benchmark

import (
	"path/filepath"
	"testing"
)

func TestValidateE2EImageBuildOptions(t *testing.T) {
	provenance := filepath.Join(t.TempDir(), "provenance.json")
	if err := validateE2EImageBuildOptions(E2EImageBuildOptions{
		Version:        "v0.1.0",
		Tag:            "e2e-nntp:v0.1.0",
		ProvenancePath: provenance,
	}); err != nil {
		t.Fatalf("valid version build: %v", err)
	}
	for _, options := range []E2EImageBuildOptions{
		{Tag: "e2e-nntp:local", ProvenancePath: provenance},
		{Version: "v0.1.0", SourceDir: t.TempDir(), Tag: "e2e-nntp:local", ProvenancePath: provenance},
		{Version: "latest", Tag: "e2e-nntp:local", ProvenancePath: provenance},
		{Version: "v0.1.0", Tag: "", ProvenancePath: provenance},
		{Version: "v0.1.0", Tag: "e2e-nntp:local"},
	} {
		if err := validateE2EImageBuildOptions(options); err == nil {
			t.Fatalf("expected validation failure for %#v", options)
		}
	}
}
