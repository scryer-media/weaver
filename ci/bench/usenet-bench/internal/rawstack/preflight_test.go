package rawstack

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func checkByName(t *testing.T, checks []Check, name string) Check {
	t.Helper()
	for _, check := range checks {
		if check.Name == name {
			return check
		}
	}
	t.Fatalf("no check named %q in %d checks", name, len(checks))
	return Check{}
}

func TestPreflightReportsEveryConditionAtOnce(t *testing.T) {
	config := usableConfig(t)
	settled, checks := Preflight(config)
	if settled.ControlPort != DefaultControlPort {
		t.Fatalf("preflight must settle the defaults it checked, got control port %d", settled.ControlPort)
	}
	for _, check := range checks {
		if check.Status != CheckOK {
			t.Fatalf("check %q failed on a usable stack: %s", check.Name, check.Reason)
		}
	}
	if len(FailedChecks(checks)) != 0 {
		t.Fatalf("a usable stack reported failures")
	}
	// The report is what an operator staging a host reads, so it has to name
	// the two executables and the store rather than only their directory.
	for _, name := range []string{serverBinary, shaperBinary, "article store", "password file", "control port is free"} {
		checkByName(t, checks, name)
	}
}

// New stops at the first problem; a report that did the same would send an
// operator round the loop once per missing piece.
func TestPreflightReportsAllTheFailuresNewWouldHideBehindTheFirst(t *testing.T) {
	config := usableConfig(t)
	config.BinDir = t.TempDir()
	config.PasswordFile = filepath.Join(config.BinDir, "absent")
	_, checks := Preflight(config)
	failed := FailedChecks(checks)
	if len(failed) < 3 {
		t.Fatalf("expected the two binaries and the password file to fail, got %d failures", len(failed))
	}
	for _, name := range []string{serverBinary, shaperBinary, "password file"} {
		if check := checkByName(t, checks, name); check.Status == CheckOK {
			t.Fatalf("check %q passed with nothing behind it", name)
		}
	}
	if _, err := New(config); err == nil {
		t.Fatal("New accepted a stack Preflight rejected")
	}
}

func TestPreflightReportsAnEmptyArticleStoreAsTheCorpusProblemItIs(t *testing.T) {
	config := usableConfig(t)
	config.DataDir = t.TempDir()
	_, checks := Preflight(config)
	check := checkByName(t, checks, "article store")
	if check.Status == CheckOK {
		t.Fatal("an empty spool passed; it answers 430 to every article")
	}
	if !strings.Contains(check.Reason, "empty") || !strings.Contains(check.Reason, "seeded spool") {
		t.Fatalf("reason %q does not say what to do about it", check.Reason)
	}
}

// A port free in the configuration can still be taken on the host, and that is
// the failure the configuration alone can never catch.
func TestPreflightAsksTheHostWhetherAPortIsActuallyFree(t *testing.T) {
	config := usableConfig(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind a probe port: %v", err)
	}
	defer listener.Close()
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	config.ControlPort = port

	// The configuration is perfectly valid; only the host disagrees.
	if _, err := New(config); err != nil {
		t.Fatalf("New rejected a configuration whose only problem is the running host: %v", err)
	}
	_, checks := Preflight(config)
	if check := checkByName(t, checks, "control port is free"); check.Status == CheckOK {
		t.Fatal("an occupied port passed the probe")
	}
	if check := checkByName(t, checks, "control port"); check.Status != CheckOK {
		t.Fatalf("the assignment check must stay separate from the probe: %s", check.Reason)
	}
}

// The probe must leave the port exactly as it found it: a preflight that held
// a port would stop the very stack it just cleared.
func TestPreflightReleasesEveryPortItProbed(t *testing.T) {
	config := usableConfig(t)
	settled, checks := Preflight(config)
	for _, check := range checks {
		if check.Status != CheckOK {
			t.Skipf("host cannot run the probe cleanly: %s", check.Reason)
		}
	}
	address := net.JoinHostPort(settled.Host, strconv.Itoa(settled.ControlPort))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("preflight kept %s: %v", address, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStackPreflightChecksTheStackItWillRun(t *testing.T) {
	config := usableConfig(t)
	stack, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	checks := stack.Preflight()
	store := checkByName(t, checks, "article store")
	if store.Detail == "" || store.Status != CheckOK {
		t.Fatalf("stack preflight did not report its own article store: %+v", store)
	}
	// Removing the binaries under a built stack must show up: the report reads
	// the host, it does not replay what New decided.
	if err := os.Remove(filepath.Join(config.BinDir, executableName(shaperBinary))); err != nil {
		t.Fatal(err)
	}
	if check := checkByName(t, stack.Preflight(), shaperBinary); check.Status == CheckOK {
		t.Fatal("a deleted shaper still reported present")
	}
}
