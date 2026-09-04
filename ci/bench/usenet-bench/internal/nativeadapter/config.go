// Package nativeadapter runs a product through its normal native executable
// on macOS or Windows. Launch commands are supplied by an audited adapter
// catalog because distribution layouts differ across product releases.
package nativeadapter

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

const (
	apiKey          = "nntpbench-api-key"
	controlUsername = "nntpbench"
	reportName      = "weaver-cli-report.json"
	reportAckName   = "weaver-cli-report.ack"
)

type Config struct {
	RunID            string
	Client           benchmark.Client
	ArchiveToolchain benchmark.ArchiveToolchain
	ExecutionTarget  benchmark.ExecutionTarget
	Transport        benchmark.Transport
	TransportLabel   string
	TLSValidation    benchmark.TLSValidation
	ServerLink       benchmark.ServerLinkProfile
	StorageProfile   benchmark.StorageProfile
	FixtureDir       string
	NZBPath          string
	QueueInput       *benchmark.QueueInput
	OutputDir        string
	ConfigDir        string
	ResultPath       string
	NNTPHost         string
	NNTPPort         string
	NNTPUsername     string
	NNTPPassword     string
	NNTPUseTLS       bool
	NNTPCAFile       string
	ArchivePassword  string
	Connections      int
	Profile          string
	LaunchCommand    []string
	APIEndpoint      string
	ClientVersion    string
	WorkingDir       string
	StartupTimeout   time.Duration
	PollInterval     time.Duration
}

func LoadConfigFromEnvironment() (Config, error) {
	return LoadConfig(os.Getenv)
}

func LoadConfig(getenv func(string) string) (Config, error) {
	connections, err := parsePositiveInt(getenv("BENCH_CONNECTIONS"), "BENCH_CONNECTIONS")
	if err != nil {
		return Config{}, err
	}
	nntpTLS, err := strconv.ParseBool(required(getenv, "BENCH_NNTP_TLS"))
	if err != nil {
		return Config{}, fmt.Errorf("parse BENCH_NNTP_TLS: %w", err)
	}
	egress, err := parseUint(getenv("BENCH_SERVER_EGRESS_BITS_PER_SECOND"), "BENCH_SERVER_EGRESS_BITS_PER_SECOND")
	if err != nil {
		return Config{}, err
	}
	burst, err := parseUint(getenv("BENCH_SERVER_EGRESS_BURST_BYTES"), "BENCH_SERVER_EGRESS_BURST_BYTES")
	if err != nil {
		return Config{}, err
	}
	link, err := benchmark.ResolveServerLinkProfile(required(getenv, "BENCH_SERVER_LINK_ID"), egress, burst)
	if err != nil {
		return Config{}, err
	}
	if scope := required(getenv, "BENCH_SERVER_LINK_SCOPE"); scope != link.Scope {
		return Config{}, fmt.Errorf("BENCH_SERVER_LINK_SCOPE %q does not match profile scope %q", scope, link.Scope)
	}
	storage, err := parseStorageProfile(required(getenv, "BENCH_STORAGE_PROFILE"))
	if err != nil {
		return Config{}, err
	}
	startupTimeout, err := parseDurationDefault(getenv("NATIVE_STARTUP_TIMEOUT"), 3*time.Minute, "NATIVE_STARTUP_TIMEOUT")
	if err != nil {
		return Config{}, err
	}
	pollInterval, err := parseDurationDefault(getenv("NATIVE_POLL_INTERVAL"), 10*time.Millisecond, "NATIVE_POLL_INTERVAL")
	if err != nil {
		return Config{}, err
	}
	var launchCommand []string
	if err := json.Unmarshal([]byte(required(getenv, "NATIVE_LAUNCH_COMMAND")), &launchCommand); err != nil {
		return Config{}, fmt.Errorf("decode NATIVE_LAUNCH_COMMAND JSON array: %w", err)
	}
	cfg := Config{
		RunID:            required(getenv, "BENCH_RUN_ID"),
		Client:           benchmark.Client(required(getenv, "BENCH_CLIENT")),
		ArchiveToolchain: benchmark.ArchiveToolchain(required(getenv, "BENCH_ARCHIVE_TOOLCHAIN")),
		ExecutionTarget:  benchmark.ExecutionTarget(required(getenv, "BENCH_EXECUTION_TARGET")),
		Transport:        benchmark.Transport(required(getenv, "BENCH_TRANSPORT")),
		TransportLabel:   required(getenv, "BENCH_TRANSPORT_LABEL"),
		TLSValidation:    benchmark.TLSValidation(required(getenv, "BENCH_TLS_VALIDATION")),
		ServerLink:       link,
		StorageProfile:   storage,
		FixtureDir:       required(getenv, "BENCH_FIXTURE_DIR"),
		NZBPath:          required(getenv, "BENCH_NZB_PATH"),
		OutputDir:        required(getenv, "BENCH_OUTPUT_DIR"),
		ConfigDir:        required(getenv, "BENCH_CONFIG_DIR"),
		ResultPath:       required(getenv, "BENCH_RESULT_PATH"),
		NNTPHost:         required(getenv, "BENCH_NNTP_HOST"),
		NNTPPort:         required(getenv, "BENCH_NNTP_PORT"),
		NNTPUsername:     required(getenv, "BENCH_NNTP_USERNAME"),
		NNTPPassword:     getenv("BENCH_NNTP_PASSWORD"),
		NNTPUseTLS:       nntpTLS,
		NNTPCAFile:       getenv("BENCH_NNTP_CA_FILE"),
		ArchivePassword:  getenv("BENCH_ARCHIVE_PASSWORD"),
		Connections:      connections,
		Profile:          required(getenv, "BENCH_PROFILE"),
		LaunchCommand:    launchCommand,
		APIEndpoint:      required(getenv, "NATIVE_API_ENDPOINT"),
		ClientVersion:    required(getenv, "NATIVE_CLIENT_VERSION"),
		WorkingDir:       strings.TrimSpace(getenv("NATIVE_WORKING_DIR")),
		StartupTimeout:   startupTimeout,
		PollInterval:     pollInterval,
	}
	for field, value := range map[string]*string{
		"BENCH_FIXTURE_DIR":  &cfg.FixtureDir,
		"BENCH_NZB_PATH":     &cfg.NZBPath,
		"BENCH_OUTPUT_DIR":   &cfg.OutputDir,
		"BENCH_CONFIG_DIR":   &cfg.ConfigDir,
		"BENCH_RESULT_PATH":  &cfg.ResultPath,
		"BENCH_NNTP_CA_FILE": &cfg.NNTPCAFile,
		"NATIVE_WORKING_DIR": &cfg.WorkingDir,
	} {
		if strings.TrimSpace(*value) == "" {
			continue
		}
		absolute, err := filepath.Abs(*value)
		if err != nil {
			return Config{}, fmt.Errorf("resolve %s: %w", field, err)
		}
		*value = absolute
	}
	if queuePath := strings.TrimSpace(getenv("BENCH_QUEUE_PATH")); queuePath != "" {
		absolute, err := filepath.Abs(queuePath)
		if err != nil {
			return Config{}, fmt.Errorf("resolve BENCH_QUEUE_PATH: %w", err)
		}
		input, err := benchmark.LoadQueueInput(absolute)
		if err != nil {
			return Config{}, err
		}
		for index := range input.Jobs {
			jobPath, err := filepath.Abs(input.Jobs[index].NZBPath)
			if err != nil {
				return Config{}, fmt.Errorf("resolve queue NZB %s: %w", input.Jobs[index].RunID, err)
			}
			input.Jobs[index].NZBPath = jobPath
		}
		cfg.QueueInput = &input
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Client != benchmark.Weaver && c.Client != benchmark.SABnzbd && c.Client != benchmark.NZBGet {
		return fmt.Errorf("unsupported client %q", c.Client)
	}
	if c.ArchiveToolchain != benchmark.VanillaArchiveToolchain {
		return fmt.Errorf("native adapter only supports the vanilla archive toolchain, got %q", c.ArchiveToolchain)
	}
	if c.ExecutionTarget != benchmark.MacOSNative && c.ExecutionTarget != benchmark.WindowsNative {
		return fmt.Errorf("nativeadapter requires macOS-native or Windows-native execution target, got %q", c.ExecutionTarget)
	}
	if c.ExecutionTarget == benchmark.MacOSNative && runtime.GOOS != "darwin" {
		return fmt.Errorf("macOS-native target must run on darwin, not %s", runtime.GOOS)
	}
	if c.ExecutionTarget == benchmark.WindowsNative && runtime.GOOS != "windows" {
		return fmt.Errorf("Windows-native target must run on windows, not %s", runtime.GOOS)
	}
	if c.Transport != benchmark.Plaintext && c.Transport != benchmark.TLS {
		return fmt.Errorf("unsupported transport %q", c.Transport)
	}
	if (c.Transport == benchmark.TLS) != c.NNTPUseTLS {
		return fmt.Errorf("BENCH_NNTP_TLS does not match benchmark transport %q", c.Transport)
	}
	if c.Transport == benchmark.Plaintext {
		if c.TLSValidation != benchmark.TLSNotApplicable || c.TransportLabel != string(benchmark.Plaintext) {
			return fmt.Errorf("plaintext runs must report not_applicable TLS validation and plaintext label")
		}
	} else {
		if c.TLSValidation != benchmark.TLSCAVerified && c.TLSValidation != benchmark.TLSDisabled {
			return fmt.Errorf("TLS run has unsupported TLS validation %q", c.TLSValidation)
		}
		if c.TLSValidation == benchmark.TLSDisabled && c.Client != benchmark.SABnzbd {
			return fmt.Errorf("only SABnzbd may run with TLS validation disabled")
		}
		if c.TLSValidation == benchmark.TLSCAVerified {
			if strings.TrimSpace(c.NNTPCAFile) == "" {
				return fmt.Errorf("CA-verified TLS requires BENCH_NNTP_CA_FILE")
			}
			if _, err := os.Stat(c.NNTPCAFile); err != nil {
				return fmt.Errorf("inspect BENCH_NNTP_CA_FILE: %w", err)
			}
		}
	}
	if c.Profile != benchmark.ProfileStock && c.Profile != benchmark.ProfileEquivalentThroughput {
		return fmt.Errorf("unsupported benchmark profile %q", c.Profile)
	}
	if err := c.ServerLink.Validate(); err != nil {
		return err
	}
	if err := c.StorageProfile.Validate(); err != nil {
		return err
	}
	// The native lanes are local-storage only. Mounting an NFS export needs
	// the host kernel and root on the operator's own machine, which this
	// harness will not do; the plan refuses such a combination first, and this
	// is the second gate in case a plan is executed by hand.
	if c.StorageProfile.Kind != benchmark.StorageLocal {
		return fmt.Errorf("native adapter only supports the %q storage profile, got %q", benchmark.StorageProfileLocal, c.StorageProfile.ID)
	}
	if c.Connections < 1 || c.StartupTimeout <= 0 || c.PollInterval <= 0 {
		return fmt.Errorf("connections, startup timeout, and poll interval must be positive")
	}
	if len(c.LaunchCommand) == 0 || strings.TrimSpace(c.LaunchCommand[0]) == "" {
		return fmt.Errorf("NATIVE_LAUNCH_COMMAND must contain a program path")
	}
	if strings.TrimSpace(c.ClientVersion) == "" {
		return fmt.Errorf("NATIVE_CLIENT_VERSION is required")
	}
	if _, _, err := nativeAPIAddress(c.APIEndpoint); err != nil {
		return err
	}
	if c.QueueInput != nil {
		if err := c.QueueInput.Validate(); err != nil {
			return err
		}
		if c.QueueInput.SubmissionMode != benchmark.SubmissionModeSequential {
			return fmt.Errorf("native adapter only supports sequential queue input, got %q", c.QueueInput.SubmissionMode)
		}
		if len(c.QueueInput.Jobs) != 1 {
			return fmt.Errorf("native sequential queue input must contain exactly one job, got %d", len(c.QueueInput.Jobs))
		}
		for _, job := range c.QueueInput.Jobs {
			if _, err := os.Stat(job.NZBPath); err != nil {
				return fmt.Errorf("inspect queue NZB %s: %w", job.RunID, err)
			}
		}
	}
	if _, err := os.Stat(c.NZBPath); err != nil {
		return fmt.Errorf("inspect NZB: %w", err)
	}
	for field, value := range map[string]string{
		"run id":           c.RunID,
		"NNTP host":        c.NNTPHost,
		"NNTP port":        c.NNTPPort,
		"NNTP username":    c.NNTPUsername,
		"NNTP password":    c.NNTPPassword,
		"archive password": c.ArchivePassword,
		"client version":   c.ClientVersion,
	} {
		if strings.TrimSpace(value) == "" && field == "run id" {
			return fmt.Errorf("%s is required", field)
		}
		if strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("%s must not contain a line break", field)
		}
	}
	return nil
}

// parseStorageProfile decodes the plan's exact storage contract. Unknown
// fields are refused so a native lane can never quietly ignore part of a
// declared storage layout.
func parseStorageProfile(raw string) (benchmark.StorageProfile, error) {
	var profile benchmark.StorageProfile
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return benchmark.StorageProfile{}, fmt.Errorf("decode BENCH_STORAGE_PROFILE: %w", err)
	}
	if err := profile.Validate(); err != nil {
		return benchmark.StorageProfile{}, err
	}
	return profile, nil
}

func nativeAPIAddress(endpoint string) (string, int, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" {
		return "", 0, fmt.Errorf("NATIVE_API_ENDPOINT must be an http URL with an explicit host and port")
	}
	host, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil || host == "" {
		return "", 0, fmt.Errorf("NATIVE_API_ENDPOINT must include an explicit host and port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("NATIVE_API_ENDPOINT has invalid port %q", portText)
	}
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return "", 0, fmt.Errorf("NATIVE_API_ENDPOINT must bind locally, got host %q", host)
	}
	return host, port, nil
}

func required(getenv func(string) string, key string) string {
	return strings.TrimSpace(getenv(key))
}

func parsePositiveInt(value, name string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func parseUint(value, name string) (uint64, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an unsigned integer: %w", name, err)
	}
	return parsed, nil
}

func parseDurationDefault(value string, fallback time.Duration, name string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return parsed, nil
}
