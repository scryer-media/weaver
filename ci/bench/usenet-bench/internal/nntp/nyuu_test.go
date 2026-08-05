package nntp

import (
	"strings"
	"testing"
)

func TestNyuuSeedConfigDefaults(t *testing.T) {
	config := (NyuuSeedConfig{FixtureDir: "fixture", RunID: "run", Network: "bench", Username: "user", Password: "pass"}).withDefaults()
	if config.Image != "weaver-nntp-bench-nyuu:0.4.2" || config.NNTPHost != "nntp" || config.NNTPPort != "119" {
		t.Fatalf("unexpected defaults: %#v", config)
	}
	if err := config.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestRedactedCommandDoesNotExposePasswords(t *testing.T) {
	const secret = "definitely-not-for-output"
	got := redactedCommand("docker", []string{
		"run", "-p", secret, "--password=" + secret, "--nzb-password", secret,
		"-hp" + secret, "-p" + secret,
	})
	if strings.Contains(got, secret) {
		t.Fatalf("password leaked in command preview: %s", got)
	}
}

func TestNyuuSeedConfigRejectsMissingNetwork(t *testing.T) {
	config := NyuuSeedConfig{FixtureDir: "fixture", RunID: "run", Username: "user", Password: "pass"}.withDefaults()
	if err := config.validate(); err == nil {
		t.Fatal("missing network should fail validation")
	}
}

func TestSafeID(t *testing.T) {
	if got, want := safeID("run name/1"), "run-name-1"; got != want {
		t.Fatalf("safeID() = %q, want %q", got, want)
	}
}

func TestMessageIDSeparatesFixtures(t *testing.T) {
	first := messageID("corpus", "rar4-store")
	second := messageID("corpus", "rar5-store")
	if first == second {
		t.Fatalf("fixture message IDs collided: %q", first)
	}
	if want := "bench-corpus-rar4-store-{0filenum}-{0part}@nntp-bench"; first != want {
		t.Fatalf("first message ID = %q, want %q", first, want)
	}
}
