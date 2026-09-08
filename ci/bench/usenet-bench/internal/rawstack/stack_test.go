package rawstack

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func usableConfig(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	dataDir := filepath.Join(root, "articles")
	for _, directory := range []string{binDir, dataDir} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, binary := range []string{serverBinary, shaperBinary} {
		if err := os.WriteFile(filepath.Join(binDir, executableName(binary)), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dataDir, "alt.binaries.test"), []byte("article"), 0o644); err != nil {
		t.Fatal(err)
	}
	password := filepath.Join(root, "password")
	if err := os.WriteFile(password, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	return Config{
		BinDir:       binDir,
		DataDir:      dataDir,
		CertDir:      filepath.Join(root, "certs"),
		LogDir:       filepath.Join(root, "logs"),
		Username:     "fixture-user",
		PasswordFile: password,
		Pipelining:   true,
	}
}

func TestNewFillsInThePortDefaults(t *testing.T) {
	stack, err := New(usableConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if stack.PlaintextPort() != "8119" || stack.TLSPort() != "8563" {
		t.Fatalf("front ports %s/%s, want the defaults", stack.PlaintextPort(), stack.TLSPort())
	}
	if stack.ControlURL() != "http://127.0.0.1:8080" {
		t.Fatalf("control URL %s", stack.ControlURL())
	}
	if !strings.HasSuffix(stack.CAFile(), filepath.Join("certs", "ca.pem")) {
		t.Fatalf("CA file %s", stack.CAFile())
	}
}

func TestNewRefusesAStackThatWouldMismeasure(t *testing.T) {
	cases := map[string]func(*Config){
		"no username": func(c *Config) { c.Username = "" },
		"no password file": func(c *Config) {
			c.PasswordFile = filepath.Join(c.BinDir, "absent")
		},
		"no binaries": func(c *Config) { c.BinDir = t.TempDir() },
		"no article store": func(c *Config) {
			c.DataDir = filepath.Join(c.DataDir, "absent")
		},
		// An empty spool answers 430 to every article, and the run reads as a
		// client failure rather than as a missing corpus.
		"empty article store": func(c *Config) { c.DataDir = t.TempDir() },
		"no log directory":    func(c *Config) { c.LogDir = "" },
		// Two services on one port means one of them is not the one being
		// measured through.
		"clashing ports":     func(c *Config) { c.PlaintextPort = 11119 },
		"port out of range":  func(c *Config) { c.ControlPort = 70000 },
		"negative port":      func(c *Config) { c.TLSPort = -1 },
		"front on the front": func(c *Config) { c.TLSPort = 8119 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			config := usableConfig(t)
			mutate(&config)
			if _, err := New(config); err == nil {
				t.Fatalf("accepted a stack with %s", name)
			}
		})
	}
}

func TestServerEnvironmentPinsTheStore(t *testing.T) {
	config := usableConfig(t)
	stack, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{}
	for _, assignment := range stack.serverEnvironment() {
		key, value, _ := strings.Cut(assignment, "=")
		environment[key] = value
	}
	if environment["NNTP_DATA_DIR"] != config.DataDir {
		t.Fatalf("article store %q", environment["NNTP_DATA_DIR"])
	}
	if environment["NNTP_LISTEN_ADDR"] != "127.0.0.1:11119" {
		t.Fatalf("the server must listen upstream of the shaper, got %q", environment["NNTP_LISTEN_ADDR"])
	}
	if environment["NNTP_PIPELINING"] != "1" {
		t.Fatalf("pipelining %q, want it advertised", environment["NNTP_PIPELINING"])
	}
	// A verified-TLS client dials the host the stack binds, so that name has
	// to be in the certificate the server generates.
	if !strings.Contains(environment["NNTP_TLS_IP_ADDRESSES"], "127.0.0.1") {
		t.Fatalf("certificate addresses %q do not cover the bound host", environment["NNTP_TLS_IP_ADDRESSES"])
	}

	config.Pipelining = false
	quiet, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range quiet.serverEnvironment() {
		if assignment == "NNTP_PIPELINING=1" {
			t.Fatal("pipelining stayed on after it was turned off")
		}
	}
}

func TestShaperEnvironmentCarriesTheLinkAndTheMechanism(t *testing.T) {
	stack, err := New(usableConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := benchmark.ResolveServerLinkProfile("1gbit", 0, 0, 100_000)
	if err != nil {
		t.Fatal(err)
	}
	assignments, err := stack.shaperEnvironment(profile)
	if err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{}
	for _, assignment := range assignments {
		key, value, _ := strings.Cut(assignment, "=")
		environment[key] = value
	}
	if environment["NNTP_RTT_MECHANISM"] != "userspace-delay" {
		t.Fatalf("mechanism %q; a raw stack has no tc", environment["NNTP_RTT_MECHANISM"])
	}
	if environment["NNTP_RTT_MICROS"] != "100000" {
		t.Fatalf("round trip %q", environment["NNTP_RTT_MICROS"])
	}
	if environment["NNTP_EGRESS_BITS_PER_SECOND"] != "1000000000" {
		t.Fatalf("egress rate %q", environment["NNTP_EGRESS_BITS_PER_SECOND"])
	}
	// Clients reach the front ports; the server's own ports are upstream of
	// the shaper so no run can measure around the link.
	if environment["LISTEN_ADDR"] != "127.0.0.1:8119" || environment["UPSTREAM_ADDR"] != "127.0.0.1:11119" {
		t.Fatalf("proxy wiring %q -> %q", environment["LISTEN_ADDR"], environment["UPSTREAM_ADDR"])
	}
	if _, stated := environment["NNTP_DELAY_QUEUE_BYTES"]; stated {
		t.Fatal("a rated link states a queue size it should derive")
	}
}

func TestShaperEnvironmentPassesAnExplicitQueueSize(t *testing.T) {
	config := usableConfig(t)
	config.DelayQueueBytes = 64 << 20
	stack, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	// An unlimited link has no bandwidth-delay product, so the queue size is
	// the operator's to state and the shaper's to enforce.
	profile, err := benchmark.ResolveServerLinkProfile("unlimited", 0, 0, 50_000)
	if err != nil {
		t.Fatal(err)
	}
	assignments, err := stack.shaperEnvironment(profile)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, assignment := range assignments {
		if assignment == "NNTP_DELAY_QUEUE_BYTES=67108864" {
			found = true
		}
	}
	if !found {
		t.Fatalf("queue size was not passed through: %v", assignments)
	}
}

func TestExecutableNameFollowsTheHost(t *testing.T) {
	name := executableName("nntpshaper")
	if runtime.GOOS == "windows" && name != "nntpshaper.exe" {
		t.Fatalf("name %q on windows", name)
	}
	if runtime.GOOS != "windows" && name != "nntpshaper" {
		t.Fatalf("name %q on %s", name, runtime.GOOS)
	}
}
