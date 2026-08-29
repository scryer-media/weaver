package weaver

import (
	"os"
	"testing"
)

// The compose stack reads `E2E_NNTP_PASSWORD` as an environment-backed secret,
// which has no default syntax — so the variable has to actually exist in the
// process environment, not merely be defaulted by the Go accessors. Running the
// suite must not require exporting it by hand.
func TestDefaultNNTPCredentialEnvIsExportedForCompose(t *testing.T) {
	t.Setenv("E2E_NNTP_USERNAME", "")
	t.Setenv("E2E_NNTP_PASSWORD", "")

	applyDefaultNNTPCredentialEnv()

	if got := os.Getenv("E2E_NNTP_PASSWORD"); got != defaultNNTPPassword {
		t.Fatalf("password env = %q, want the fixture default so the compose secret resolves", got)
	}
	if got := os.Getenv("E2E_NNTP_USERNAME"); got != defaultNNTPUsername {
		t.Fatalf("username env = %q", got)
	}
}

// A real credential must survive the defaulting.
func TestDefaultNNTPCredentialEnvDoesNotClobberOverrides(t *testing.T) {
	t.Setenv("E2E_NNTP_USERNAME", "real-user")
	t.Setenv("E2E_NNTP_PASSWORD", "real-password")

	applyDefaultNNTPCredentialEnv()

	if got := os.Getenv("E2E_NNTP_USERNAME"); got != "real-user" {
		t.Fatalf("username env = %q, want the caller's override", got)
	}
	if got := os.Getenv("E2E_NNTP_PASSWORD"); got != "real-password" {
		t.Fatalf("password override was clobbered by the default")
	}
}

func TestNNTPCredentialsUseExplicitE2EOverrides(t *testing.T) {
	t.Setenv("E2E_NNTP_USERNAME", "fixture-override-user")
	t.Setenv("E2E_NNTP_PASSWORD", "fixture-override-password")
	if got := nntpUsername(); got != "fixture-override-user" {
		t.Fatalf("username = %q", got)
	}
	if got := nntpPassword(); got != "fixture-override-password" {
		t.Fatalf("password override was not used")
	}
}

func TestNNTPCredentialsDefaultWhenOverridesAreEmpty(t *testing.T) {
	t.Setenv("E2E_NNTP_USERNAME", "")
	t.Setenv("E2E_NNTP_PASSWORD", "")
	if got := nntpUsername(); got != defaultNNTPUsername {
		t.Fatalf("username = %q", got)
	}
	if got := nntpPassword(); got != defaultNNTPPassword {
		t.Fatalf("password = %q", got)
	}
}
