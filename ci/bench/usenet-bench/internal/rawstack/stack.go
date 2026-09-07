// Package rawstack runs the benchmark's server side as ordinary local
// processes: the synthetic NNTP server and the shaper in front of it, with no
// Docker, no Compose and no tc. It exists because two of the hosts this
// benchmark has to run on cannot have the containerized stack. Windows has no
// netem, and on an ARM Mac every container runs inside a Linux virtual machine
// whose scheduling and networking are exactly the things a download benchmark
// measures.
package rawstack

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nntpshaper"
)

// DefaultStartTimeout bounds how long a process has to answer its own health
// probe before the stack gives up on it.
const DefaultStartTimeout = 30 * time.Second

// Config describes one raw stack. Ports are explicit rather than chosen at
// random: a benchmark host's firewall rules, and the operator reading a
// netstat, both need them to be the same on every run.
type Config struct {
	// BinDir holds the e2e-nntp and nntpshaper executables.
	BinDir string
	// DataDir is the seeded article spool. It is copied onto the host, never
	// posted there: seeding needs Nyuu, which is a container.
	DataDir string
	// CertDir receives the server's generated test TLS material.
	CertDir string
	// LogDir receives each process's stdout and stderr.
	LogDir string

	Username     string
	PasswordFile string
	// Pipelining advertises RFC 4644 PIPELINING, as commercial providers do.
	Pipelining bool

	// Host is the address the stack binds and the clients connect to. It is
	// also the certificate's subject, so a verified-TLS client validates the
	// same name it dialled.
	Host string

	// The server listens on the upstream ports; clients only ever reach the
	// shaper's front ports, so no run can bypass the link being modelled.
	UpstreamPlaintextPort int
	UpstreamTLSPort       int
	PlaintextPort         int
	TLSPort               int
	ControlPort           int

	// DelayQueueBytes overrides the delay line size the shaper derives from
	// the link. An unlimited link with a round trip has no bandwidth-delay
	// product to derive from and must state it.
	DelayQueueBytes uint64

	StartTimeout time.Duration
}

// Default ports. They sit above 1024 so the stack needs no privileges on any
// host, and apart from the control plane they are deliberately unlike the
// well-known NNTP ports, so nothing on the host mistakes them for a real news
// service.
const (
	DefaultUpstreamPlaintextPort = 11119
	DefaultUpstreamTLSPort       = 11563
	DefaultPlaintextPort         = 8119
	DefaultTLSPort               = 8563
	DefaultControlPort           = 8080
)

// Stack is a running server and shaper pair.
type Stack struct {
	config Config
	server *process
	shaper *process
}

// New validates a configuration and fills in its defaults. Everything it
// checks is something that would otherwise surface as a run full of 430s or a
// silently unshaped link. It reports the first failure; Preflight reports them
// all, from the same list of checks.
func New(config Config) (*Stack, error) {
	config = settle(config)
	for _, check := range configChecks(config) {
		if check.Status != CheckOK {
			return nil, errors.New(check.Reason)
		}
	}
	return &Stack{config: config}, nil
}

// settle fills in every value that has a default. It never touches the host,
// so a report and a run start from exactly the same configuration.
func settle(config Config) Config {
	if config.Host == "" {
		config.Host = "127.0.0.1"
	}
	if config.StartTimeout <= 0 {
		config.StartTimeout = DefaultStartTimeout
	}
	if config.UpstreamPlaintextPort == 0 {
		config.UpstreamPlaintextPort = DefaultUpstreamPlaintextPort
	}
	if config.UpstreamTLSPort == 0 {
		config.UpstreamTLSPort = DefaultUpstreamTLSPort
	}
	if config.PlaintextPort == 0 {
		config.PlaintextPort = DefaultPlaintextPort
	}
	if config.TLSPort == 0 {
		config.TLSPort = DefaultTLSPort
	}
	if config.ControlPort == 0 {
		config.ControlPort = DefaultControlPort
	}
	return config
}

// CAFile is the certificate authority the server generated, which a
// verified-TLS client validates against.
func (s *Stack) CAFile() string { return filepath.Join(s.config.CertDir, "ca.pem") }

// ControlURL is the shaper's control plane, which the benchmark controller
// leases and takes its before and after attestations from.
func (s *Stack) ControlURL() string {
	return "http://" + net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.ControlPort))
}

// Username is the account the server was started with, which is the account
// every client has to present. It is settled here rather than by each caller
// so a stack cannot be listening for one name while the clients send another.
func (s *Stack) Username() string { return s.config.Username }

func (s *Stack) Host() string          { return s.config.Host }
func (s *Stack) PlaintextPort() string { return strconv.Itoa(s.config.PlaintextPort) }
func (s *Stack) TLSPort() string       { return strconv.Itoa(s.config.TLSPort) }

// Start brings the server up once and the shaper up for the first link. The
// server is started once per session on purpose: restarting it would reopen
// the article store between phases and charge one phase for the other's cold
// cache.
func (s *Stack) Start(ctx context.Context, profile benchmark.ServerLinkProfile) error {
	if s.server != nil {
		return fmt.Errorf("raw stack is already running")
	}
	for _, directory := range []string{s.config.CertDir, s.config.LogDir} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("create %s: %w", directory, err)
		}
	}
	server, err := start(ctx, processConfig{
		name:    serverBinary,
		path:    filepath.Join(s.config.BinDir, executableName(serverBinary)),
		args:    []string{"serve"},
		env:     s.serverEnvironment(),
		logPath: filepath.Join(s.config.LogDir, "raw-nntp.log"),
		health: []string{
			filepath.Join(s.config.BinDir, executableName(serverBinary)),
			"health", "--addr", net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.UpstreamPlaintextPort)),
		},
		timeout: s.config.StartTimeout,
	})
	if err != nil {
		return err
	}
	s.server = server
	if err := s.startShaper(ctx, profile); err != nil {
		_ = s.server.stop()
		s.server = nil
		return err
	}
	return nil
}

// Reshape replaces the shaper with one configured for a different link. The
// process is restarted rather than reconfigured because its link contract is
// immutable for the life of the process, which is what lets a run's before and
// after attestations prove the conditions never moved underneath it.
func (s *Stack) Reshape(ctx context.Context, profile benchmark.ServerLinkProfile) error {
	if s.server == nil {
		return fmt.Errorf("raw stack is not running")
	}
	if s.shaper != nil {
		if err := s.shaper.stop(); err != nil {
			return fmt.Errorf("stop the running shaper: %w", err)
		}
		s.shaper = nil
	}
	return s.startShaper(ctx, profile)
}

func (s *Stack) startShaper(ctx context.Context, profile benchmark.ServerLinkProfile) error {
	environment, err := s.shaperEnvironment(profile)
	if err != nil {
		return err
	}
	shaper, err := start(ctx, processConfig{
		name:    shaperBinary,
		path:    filepath.Join(s.config.BinDir, executableName(shaperBinary)),
		env:     environment,
		logPath: filepath.Join(s.config.LogDir, "raw-shaper.log"),
		health: []string{
			filepath.Join(s.config.BinDir, executableName(shaperBinary)),
			"health", "--addr", net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.ControlPort)),
		},
		timeout: s.config.StartTimeout,
	})
	if err != nil {
		return err
	}
	s.shaper = shaper
	return nil
}

// Stop tears the stack down. It stops the shaper first so no client can reach
// an unshaped server during the shutdown.
func (s *Stack) Stop() error {
	var failure error
	if s.shaper != nil {
		if err := s.shaper.stop(); err != nil {
			failure = err
		}
		s.shaper = nil
	}
	if s.server != nil {
		if err := s.server.stop(); err != nil && failure == nil {
			failure = err
		}
		s.server = nil
	}
	return failure
}

func (s *Stack) serverEnvironment() []string {
	pipelining := "0"
	if s.config.Pipelining {
		pipelining = "1"
	}
	return []string{
		"NNTP_LISTEN_ADDR=" + net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.UpstreamPlaintextPort)),
		"NNTP_TLS_LISTEN_ADDR=" + net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.UpstreamTLSPort)),
		"NNTP_DATA_DIR=" + s.config.DataDir,
		"NNTP_USERNAME=" + s.config.Username,
		"NNTP_PASSWORD_FILE=" + s.config.PasswordFile,
		"NNTP_GENERATE_TEST_TLS=1",
		"NNTP_TLS_DIR=" + s.config.CertDir,
		"NNTP_TLS_DNS_NAMES=localhost",
		"NNTP_TLS_IP_ADDRESSES=127.0.0.1,::1",
		"NNTP_PIPELINING=" + pipelining,
	}
}

func (s *Stack) shaperEnvironment(profile benchmark.ServerLinkProfile) ([]string, error) {
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	upstream := net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.UpstreamPlaintextPort))
	upstreamTLS := net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.UpstreamTLSPort))
	environment := []string{
		"UPSTREAM_ADDR=" + upstream,
		"TLS_UPSTREAM_ADDR=" + upstreamTLS,
		"LISTEN_ADDR=" + net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.PlaintextPort)),
		"TLS_LISTEN_ADDR=" + net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.TLSPort)),
		"CONTROL_LISTEN_ADDR=" + net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.ControlPort)),
		// There is no tc here, so the proxy carries the round trip itself and
		// says so in every attestation it serves.
		"NNTP_RTT_MECHANISM=" + nntpshaper.LinkDelayUserspace,
	}
	environment = append(environment, benchmark.ServerLinkEnvironment(profile)...)
	if s.config.DelayQueueBytes > 0 {
		environment = append(environment, "NNTP_DELAY_QUEUE_BYTES="+strconv.FormatUint(s.config.DelayQueueBytes, 10))
	}
	return environment, nil
}

const (
	serverBinary = "e2e-nntp"
	shaperBinary = "nntpshaper"
)

func executableName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

type processConfig struct {
	name    string
	path    string
	args    []string
	env     []string
	logPath string
	health  []string
	timeout time.Duration
}

type process struct {
	name    string
	command *exec.Cmd
	log     *os.File
}

// start launches one process and waits for it to answer its own health probe.
// The probe is the binary's own health subcommand rather than a reimplemented
// check here, so what the stack waits for is exactly what the container
// healthcheck waits for.
func start(ctx context.Context, config processConfig) (*process, error) {
	logFile, err := os.OpenFile(config.logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s log: %w", config.name, err)
	}
	command := exec.Command(config.path, config.args...)
	// A benchmark process inherits nothing: an operator's stray NNTP_ or
	// shaper variable in the shell would silently change the conditions.
	command.Env = config.env
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("start %s: %w", config.name, err)
	}
	running := &process{name: config.name, command: command, log: logFile}
	if err := waitHealthy(ctx, config, running); err != nil {
		_ = running.stop()
		return nil, err
	}
	return running, nil
}

func waitHealthy(ctx context.Context, config processConfig, running *process) error {
	deadline := time.Now().Add(config.timeout)
	var lastErr error
	for {
		if exited, state := running.exited(); exited {
			return fmt.Errorf("%s exited during startup (%s); see %s", config.name, state, config.logPath)
		}
		probe := exec.CommandContext(ctx, config.health[0], config.health[1:]...)
		output, err := probe.CombinedOutput()
		if err == nil {
			return nil
		}
		lastErr = fmt.Errorf("%w: %s", err, string(output))
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not become healthy within %s: %v; see %s", config.name, config.timeout, lastErr, config.logPath)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (p *process) exited() (bool, string) {
	if p.command.ProcessState != nil {
		return true, p.command.ProcessState.String()
	}
	// Signal 0 is not portable, so ask the OS whether the child has been
	// reaped instead. Wait is not used here because it would consume the
	// process; a non-blocking check is all that is wanted.
	if p.command.Process == nil {
		return true, "not started"
	}
	return false, ""
}

// stop asks the process to exit and escalates if it will not. Windows has no
// interrupt to send a child, so there it goes straight to a kill.
func (p *process) stop() error {
	defer p.log.Close()
	if p.command.Process == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- p.command.Wait() }()
	if runtime.GOOS != "windows" {
		_ = p.command.Process.Signal(os.Interrupt)
		select {
		case <-done:
			return nil
		case <-time.After(5 * time.Second):
		}
	}
	if err := p.command.Process.Kill(); err != nil {
		return fmt.Errorf("kill %s: %w", p.name, err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("%s did not exit after a kill", p.name)
	}
	return nil
}
