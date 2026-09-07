package rawstack

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
)

// Check statuses. A check is either satisfied or it names, in Reason, exactly
// what has to change on the host before a run can start.
const (
	CheckOK     = "ok"
	CheckFailed = "failed"
)

// Check is one condition the raw stack needs from its host. Preflight reports
// every one of them rather than returning the first as an error, because an
// operator staging a new machine would otherwise discover them one failed run
// at a time.
type Check struct {
	Name   string `json:"name"`
	Detail string `json:"detail,omitempty"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

func satisfied(name, detail string) Check {
	return Check{Name: name, Detail: detail, Status: CheckOK}
}

func unsatisfied(name, detail, reason string) Check {
	return Check{Name: name, Detail: detail, Status: CheckFailed, Reason: reason}
}

// Preflight settles a configuration and reports everything the stack needs
// from the host: the two executables, a seeded article store, the password
// file, and five ports that are actually free. It starts nothing, writes
// nothing and changes nothing, so it is safe to run against a host that is
// about to measure.
func Preflight(config Config) (Config, []Check) {
	config = settle(config)
	checks := configChecks(config)
	return config, append(checks, portChecks(config)...)
}

// Preflight re-checks a stack that has already been accepted. Its
// configuration checks pass by construction; what it adds is the state of the
// host right now, which is what a dry run wants to know.
func (s *Stack) Preflight() []Check {
	return append(configChecks(s.config), portChecks(s.config)...)
}

// FailedChecks keeps only the checks that have to be fixed, which is what a
// caller reporting into a running log wants.
func FailedChecks(checks []Check) []Check {
	var failed []Check
	for _, check := range checks {
		if check.Status != CheckOK {
			failed = append(failed, check)
		}
	}
	return failed
}

// configChecks is everything decidable from the configuration and the
// filesystem. New returns the first failure here as its error, so a stack that
// exists has already passed all of them.
func configChecks(config Config) []Check {
	checks := []Check{usernameCheck(config)}
	checks = append(checks, directoryChecks(config)...)
	checks = append(checks, binaryChecks(config)...)
	checks = append(checks, articleStoreCheck(config), passwordFileCheck(config))
	return append(checks, portAssignmentChecks(config)...)
}

func usernameCheck(config Config) Check {
	if config.Username == "" {
		return unsatisfied("username", "", "raw stack needs an NNTP username")
	}
	return satisfied("username", config.Username)
}

func directoryChecks(config Config) []Check {
	var checks []Check
	for _, directory := range []struct {
		label string
		path  string
	}{
		{"binary", config.BinDir},
		{"article", config.DataDir},
		{"certificate", config.CertDir},
		{"log", config.LogDir},
	} {
		name := directory.label + " directory"
		if directory.path == "" {
			checks = append(checks, unsatisfied(name, "", fmt.Sprintf("raw stack needs a %s directory", directory.label)))
			continue
		}
		checks = append(checks, satisfied(name, directory.path))
	}
	return checks
}

func binaryChecks(config Config) []Check {
	var checks []Check
	for _, binary := range []string{serverBinary, shaperBinary} {
		path := filepath.Join(config.BinDir, executableName(binary))
		if _, err := os.Stat(path); err != nil {
			checks = append(checks, unsatisfied(binary, path, fmt.Sprintf("locate %s: %v", binary, err)))
			continue
		}
		checks = append(checks, satisfied(binary, path))
	}
	return checks
}

// An empty spool serves 430 to every article and produces a run that looks
// like a client failure. Catch it here, before the shaper is configured.
func articleStoreCheck(config Config) Check {
	entries, err := os.ReadDir(config.DataDir)
	if err != nil {
		return unsatisfied("article store", config.DataDir, fmt.Sprintf("read the article store: %v", err))
	}
	if len(entries) == 0 {
		return unsatisfied("article store", config.DataDir,
			fmt.Sprintf("article store %s is empty; restore a seeded spool before running a raw stack", config.DataDir))
	}
	entryLabel := "entries"
	if len(entries) == 1 {
		entryLabel = "entry"
	}
	return Check{
		Name:   "article store",
		Detail: fmt.Sprintf("%s (%d %s)", config.DataDir, len(entries), entryLabel),
		Status: CheckOK,
	}
}

func passwordFileCheck(config Config) Check {
	if config.PasswordFile == "" {
		return unsatisfied("password file", "", "raw stack needs a password file")
	}
	if _, err := os.Stat(config.PasswordFile); err != nil {
		return unsatisfied("password file", config.PasswordFile, fmt.Sprintf("locate the NNTP password file: %v", err))
	}
	return satisfied("password file", config.PasswordFile)
}

// portAssignmentChecks decides the ports against each other. Two services on
// one port means one of them is not the one being measured through.
func portAssignmentChecks(config Config) []Check {
	var checks []Check
	taken := map[int]string{}
	for _, port := range configuredPorts(config) {
		name := port.label + " port"
		detail := strconv.Itoa(port.value)
		if port.value < 1 || port.value > 65535 {
			checks = append(checks, unsatisfied(name, detail, fmt.Sprintf("%s port %d is out of range", port.label, port.value)))
			continue
		}
		if previous, clash := taken[port.value]; clash {
			checks = append(checks, unsatisfied(name, detail,
				fmt.Sprintf("%s and %s ports are both %d", previous, port.label, port.value)))
			continue
		}
		taken[port.value] = port.label
		checks = append(checks, satisfied(name, detail))
	}
	return checks
}

// portChecks asks the host whether the ports are free. It is the one condition
// that cannot be decided from the configuration, and the one most likely to
// fail on a developer's machine, where the control plane's 8080 is a popular
// port. A port free now can be taken by the time the stack starts; this
// catches the standing occupant, which is the case worth catching.
func portChecks(config Config) []Check {
	var checks []Check
	for _, port := range configuredPorts(config) {
		if port.value < 1 || port.value > 65535 {
			continue
		}
		address := net.JoinHostPort(config.Host, strconv.Itoa(port.value))
		name := port.label + " port is free"
		listener, err := net.Listen("tcp", address)
		if err != nil {
			checks = append(checks, unsatisfied(name, address,
				fmt.Sprintf("cannot bind the %s port: %v", port.label, err)))
			continue
		}
		if err := listener.Close(); err != nil {
			checks = append(checks, unsatisfied(name, address, fmt.Sprintf("release the probe on %s: %v", address, err)))
			continue
		}
		checks = append(checks, satisfied(name, address))
	}
	return checks
}

type labelledPort struct {
	label string
	value int
}

func configuredPorts(config Config) []labelledPort {
	return []labelledPort{
		{"upstream plaintext", config.UpstreamPlaintextPort},
		{"upstream TLS", config.UpstreamTLSPort},
		{"plaintext", config.PlaintextPort},
		{"TLS", config.TLSPort},
		{"control", config.ControlPort},
	}
}
