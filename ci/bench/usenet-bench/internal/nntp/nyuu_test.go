package nntp

import "testing"

func TestNyuuSeedConfigDefaults(t *testing.T) {
	config := (NyuuSeedConfig{FixtureDir: "fixture", RunID: "run", Network: "bench", Username: "user", Password: "pass"}).withDefaults()
	if config.Image != "weaver-nntp-bench-nyuu:0.4.2" || config.NNTPHost != "nntp" || config.NNTPPort != "119" {
		t.Fatalf("unexpected defaults: %#v", config)
	}
	if err := config.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
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
