package corpus

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// CosignBinaryEnv points the tooling at a specific cosign (the tests use a
// stub; nothing here requires cosign to be installed).
const CosignBinaryEnv = "WEAVER_E2E_COSIGN"

// BundleSuffix is what cosign sign-blob writes beside a signed document.
const BundleSuffix = ".sigstore.json"

// CosignBinary is the cosign executable to run.
func CosignBinary() string {
	if override := strings.TrimSpace(os.Getenv(CosignBinaryEnv)); override != "" {
		return override
	}
	return "cosign"
}

// CosignAvailable reports whether cosign is on PATH. `verify` uses it to decide
// whether signature checking is possible; --require-signature turns its absence
// into a failure.
func CosignAvailable() bool {
	_, err := exec.LookPath(CosignBinary())
	return err == nil
}

// SignArgs is the exact `cosign sign-blob` invocation. Pure, so a test can
// assert it without a signer.
func SignArgs(path, bundlePath string) []string {
	return []string{"sign-blob", "--yes", "--bundle", bundlePath, path}
}

// VerifyArgs is the exact `cosign verify-blob` invocation. The identity is
// matched literally (`--certificate-identity`), never as a regexp, so a
// workflow on another branch or in a fork can never satisfy it.
func VerifyArgs(path, bundlePath, identity, issuer string) []string {
	return []string{
		"verify-blob",
		"--bundle", bundlePath,
		"--certificate-identity", identity,
		"--certificate-oidc-issuer", issuer,
		path,
	}
}

// SignBlob signs one document keyless under the ambient OIDC identity (GitHub
// Actions supplies it) and returns the bundle it wrote. No crypto is
// implemented here; cosign is the only signer.
func SignBlob(ctx context.Context, path string) (string, error) {
	bundlePath := path + BundleSuffix
	command := exec.CommandContext(ctx, CosignBinary(), SignArgs(path, bundlePath)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("cosign sign-blob %s: %w: %s", path, err, strings.TrimSpace(string(output)))
	}
	info, err := os.Stat(bundlePath)
	if err != nil || info.Size() == 0 {
		return "", fmt.Errorf("cosign sign-blob wrote no bundle at %s", bundlePath)
	}
	return bundlePath, nil
}

// VerifyBlob checks a Sigstore bundle against the exact identity and issuer
// the lock pins.
func VerifyBlob(ctx context.Context, path, bundlePath, identity, issuer string) error {
	command := exec.CommandContext(ctx, CosignBinary(), VerifyArgs(path, bundlePath, identity, issuer)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("cosign verify-blob rejected %s (bundle %s, identity %s, issuer %s): %w: %s",
			path, bundlePath, identity, issuer, err, strings.TrimSpace(string(output)))
	}
	return nil
}
