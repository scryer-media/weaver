package weaver

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestWeaverNNTPImageBuildCommandUsesCurrentSource(t *testing.T) {
	source := t.TempDir()
	writeFile(t, filepath.Join(source, "go.mod"), "module github.com/scryer-media/e2e-nntp\n")
	t.Setenv("E2E_NNTP_SOURCE_DIR", source)
	t.Setenv("E2E_NNTP_MODULE_VERSION", "")

	cmd, err := weaverNNTPImageBuildCommand("test-nntp:local")
	if err != nil {
		t.Fatalf("build command: %v", err)
	}
	absoluteSource, err := filepath.Abs(source)
	if err != nil {
		t.Fatalf("absolute source: %v", err)
	}
	want := []string{
		"go", "-C", absoluteSource, "run", "./cmd/e2e-nntp",
		"image", "build", "--source-dir", absoluteSource,
		"--tag", "test-nntp:local",
	}
	if !slices.Equal(cmd.Args, want) {
		t.Fatalf("command args = %#v, want %#v", cmd.Args, want)
	}
	if cmd.Dir != e2eDir() {
		t.Fatalf("command dir = %q, want %q", cmd.Dir, e2eDir())
	}
}

func TestWeaverNNTPImageBuildCommandUsesPinnedModule(t *testing.T) {
	t.Setenv("E2E_NNTP_SOURCE_DIR", "")
	t.Setenv("E2E_NNTP_MODULE_VERSION", "v1.2.3")

	cmd, err := weaverNNTPImageBuildCommand("test-nntp:pinned")
	if err != nil {
		t.Fatalf("build command: %v", err)
	}
	want := []string{
		"go", "run", "github.com/scryer-media/e2e-nntp/cmd/e2e-nntp@v1.2.3",
		"image", "build", "--version", "v1.2.3",
		"--tag", "test-nntp:pinned",
	}
	if !slices.Equal(cmd.Args, want) {
		t.Fatalf("command args = %#v, want %#v", cmd.Args, want)
	}
	if _, ok := os.LookupEnv("E2E_NNTP_MODULE_VERSION"); !ok {
		t.Fatal("pinned module version unexpectedly cleared")
	}
}

func TestWeaverNNTPImageBuildCommandDefaultsToThePublishedModule(t *testing.T) {
	t.Setenv("E2E_NNTP_SOURCE_DIR", "")
	t.Setenv("E2E_NNTP_MODULE_VERSION", "")

	cmd, err := weaverNNTPImageBuildCommand("test-nntp:default")
	if err != nil {
		t.Fatalf("build command: %v", err)
	}
	want := []string{
		"go", "run", "github.com/scryer-media/e2e-nntp/cmd/e2e-nntp@" + weaverNNTPDefaultModuleVersion,
		"image", "build", "--version", weaverNNTPDefaultModuleVersion,
		"--tag", "test-nntp:default",
	}
	if !slices.Equal(cmd.Args, want) {
		t.Fatalf("command args = %#v, want the published module default %#v", cmd.Args, want)
	}
	if weaverNNTPModuleVersion() != weaverNNTPDefaultModuleVersion {
		t.Fatalf("module version = %q, want %q", weaverNNTPModuleVersion(), weaverNNTPDefaultModuleVersion)
	}
}

func TestWeaverNNTPImageBuildCommandNeverGuessesASourceCheckout(t *testing.T) {
	// An explicit override must name a real module root; a missing directory
	// is an error, never a silent fallback to some sibling checkout.
	t.Setenv("E2E_NNTP_SOURCE_DIR", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("E2E_NNTP_MODULE_VERSION", "")
	if _, err := weaverNNTPImageBuildCommand("test-nntp:missing"); err == nil {
		t.Fatal("a missing E2E_NNTP_SOURCE_DIR must be an error")
	}
}
