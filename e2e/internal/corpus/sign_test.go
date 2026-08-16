package corpus

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSignArgsAndVerifyArgsPinTheExactIdentity(t *testing.T) {
	signing := SignArgs("/build/manifest.json", "/build/manifest.json.sigstore.json")
	want := []string{"sign-blob", "--yes", "--bundle", "/build/manifest.json.sigstore.json", "/build/manifest.json"}
	if strings.Join(signing, " ") != strings.Join(want, " ") {
		t.Fatalf("sign argv = %v, want %v", signing, want)
	}

	verifying := VerifyArgs("/build/manifest.json", "/build/manifest.json.sigstore.json",
		PublishWorkflowIdentity, GitHubOIDCIssuer)
	if verifying[0] != "verify-blob" {
		t.Fatalf("verify argv starts with %q", verifying[0])
	}
	if verifying[len(verifying)-1] != "/build/manifest.json" {
		t.Fatalf("the blob must be the last argument: %v", verifying)
	}
	joined := strings.Join(verifying, " ")
	for _, want := range []string{
		"--certificate-identity " + PublishWorkflowIdentity,
		"--certificate-oidc-issuer " + GitHubOIDCIssuer,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("verify argv is missing %q: %v", want, verifying)
		}
	}
	// The identity is matched literally: a regexp match would let a fork or a
	// branch satisfy the lock.
	if strings.Contains(joined, "regexp") {
		t.Fatal("the identity must never be matched by regexp")
	}
	if !strings.HasSuffix(BundleSuffix, ".sigstore.json") {
		t.Fatalf("bundle suffix is %q", BundleSuffix)
	}
}

// A stub cosign proves the process plumbing without requiring the real signer:
// `go test` never needs cosign installed.
func TestSignBlobAndVerifyBlobSurfaceCosignOutcomes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stub is a shell script")
	}
	directory := t.TempDir()
	blob := filepath.Join(directory, BuildManifestFile)
	if err := os.WriteFile(blob, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(directory, "cosign")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + filepath.Join(directory, "argv.txt") +
		"\ncase \"$1\" in sign-blob) printf 'bundle' > \"$4\";; esac\nexit ${STUB_EXIT:-0}\n"
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(CosignBinaryEnv, stub)
	if !CosignAvailable() {
		t.Fatal("a stub on an absolute path should be found")
	}
	bundle, err := SignBlob(context.Background(), blob)
	if err != nil {
		t.Fatal(err)
	}
	if bundle != blob+BundleSuffix {
		t.Fatalf("bundle path %s", bundle)
	}
	recorded, err := os.ReadFile(filepath.Join(directory, "argv.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(recorded), "sign-blob") || !strings.Contains(string(recorded), "--yes") {
		t.Fatalf("cosign was called with %q", recorded)
	}
	if err := VerifyBlob(context.Background(), blob, bundle, PublishWorkflowIdentity, GitHubOIDCIssuer); err != nil {
		t.Fatal(err)
	}

	t.Setenv("STUB_EXIT", "1")
	if err := VerifyBlob(context.Background(), blob, bundle, PublishWorkflowIdentity, GitHubOIDCIssuer); err == nil {
		t.Fatal("a cosign failure must be surfaced")
	}
	if _, err := SignBlob(context.Background(), blob); err == nil {
		t.Fatal("a cosign failure must be surfaced")
	}

	t.Setenv(CosignBinaryEnv, filepath.Join(directory, "does-not-exist"))
	if CosignAvailable() {
		t.Fatal("a missing cosign must not report as available")
	}
}
