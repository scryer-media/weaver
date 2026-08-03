package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolvePassword(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("fixture-password\n"), 0o600); err != nil {
		t.Fatalf("write password: %v", err)
	}
	password, err := resolvePassword("", path)
	if err != nil {
		t.Fatalf("resolve password file: %v", err)
	}
	if password != "fixture-password" {
		t.Fatalf("password = %q", password)
	}
	if _, err := resolvePassword("inline", path); err == nil {
		t.Fatal("expected mutually exclusive password source failure")
	}
}
