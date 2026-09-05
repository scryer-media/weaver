// Package clientadapter runs one product-specific client in an isolated Docker
// container and returns the product-neutral benchmark adapter result.
package clientadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

const (
	apiKey             = "nntpbench-api-key"
	controlUsername    = "nntpbench"
	weaverCLIReport    = "weaver-cli-report.json"
	weaverCLIReportAck = "weaver-cli-report.ack"
)

// Config is the complete one-run adapter contract. BENCH_* values are set by
// the neutral runner; CLIENT_* values are deliberately adapter-local so a
// catalog can select digest-pinned images without mutating the saved plan.
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
	CompleteVolume   string
	IncompleteVolume string
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
	Image            string
	Network          string
	DockerBinary     string
	Platform         string
	PerfBinary       string
	RarparBinary     string
	RarparVersion    string
	RarparSHA256     string
	StartupTimeout   time.Duration
	PollInterval     time.Duration
	// JobTimeout bounds how long a submitted job may go without reaching a
	// terminal state. A client that never finishes is a did-not-finish result,
	// not a reason for the whole pass to wait forever.
	JobTimeout time.Duration
}

// ProductSpec is the rendered, client-specific portion of an otherwise
// product-neutral run. The rendered document is preserved before a client can
// rewrite its own working configuration.
type ProductSpec struct {
	APIPort              int
	ExposeAPI            bool
	Command              []string
	NeedsNZBMount        bool
	CompletionReportName string
	CompletionAckName    string
	ConfigName           string
	ConfigContent        []byte
	Environment          []string
	Rendered             []byte
	ConfigSHA256         string
	NeedsCAMount         bool
	ExtraFiles           []ProductFile
}

type ProductFile struct {
	RelativePath string
	Content      []byte
	Mode         os.FileMode
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

	startupTimeout, err := parseDurationDefault(getenv("CLIENT_STARTUP_TIMEOUT"), 3*time.Minute, "CLIENT_STARTUP_TIMEOUT")
	if err != nil {
		return Config{}, err
	}
	pollInterval, err := parseDurationDefault(getenv("CLIENT_POLL_INTERVAL"), 10*time.Millisecond, "CLIENT_POLL_INTERVAL")
	if err != nil {
		return Config{}, err
	}
	jobTimeout, err := parseDurationDefault(getenv("CLIENT_JOB_TIMEOUT"), DefaultJobTimeout, "CLIENT_JOB_TIMEOUT")
	if err != nil {
		return Config{}, err
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
		CompleteVolume:   required(getenv, "BENCH_STORAGE_COMPLETE_VOLUME"),
		IncompleteVolume: required(getenv, "BENCH_STORAGE_INCOMPLETE_VOLUME"),
		FixtureDir:       required(getenv, "BENCH_FIXTURE_DIR"),
		NZBPath:          required(getenv, "BENCH_NZB_PATH"),
		OutputDir:        required(getenv, "BENCH_OUTPUT_DIR"),
		ConfigDir:        required(getenv, "BENCH_CONFIG_DIR"),
		ResultPath:       required(getenv, "BENCH_RESULT_PATH"),
		NNTPHost:         required(getenv, "BENCH_NNTP_HOST"),
		NNTPPort:         required(getenv, "BENCH_NNTP_PORT"),
		NNTPUsername:     required(getenv, "BENCH_NNTP_USERNAME"),
		// Passwords are opaque values; unlike identifiers and paths, leading or
		// trailing spaces must not be silently rewritten by the adapter.
		NNTPPassword:    getenv("BENCH_NNTP_PASSWORD"),
		NNTPUseTLS:      nntpTLS,
		NNTPCAFile:      getenv("BENCH_NNTP_CA_FILE"),
		ArchivePassword: getenv("BENCH_ARCHIVE_PASSWORD"),
		Connections:     connections,
		Profile:         required(getenv, "BENCH_PROFILE"),
		Image:           required(getenv, "CLIENT_IMAGE"),
		Network:         required(getenv, "CLIENT_NETWORK"),
		DockerBinary:    defaultString(getenv("CLIENT_DOCKER_BINARY"), "docker"),
		Platform:        strings.TrimSpace(getenv("CLIENT_PLATFORM")),
		PerfBinary:      defaultString(getenv("CLIENT_PERF_BINARY"), "perf"),
		RarparBinary:    strings.TrimSpace(getenv("CLIENT_RARPAR_BINARY")),
		RarparVersion:   strings.TrimSpace(getenv("CLIENT_RARPAR_VERSION")),
		RarparSHA256:    strings.TrimSpace(getenv("CLIENT_RARPAR_SHA256")),
		StartupTimeout:  startupTimeout,
		PollInterval:    pollInterval,
		JobTimeout:      jobTimeout,
	}
	for field, value := range map[string]*string{
		"BENCH_FIXTURE_DIR":    &cfg.FixtureDir,
		"BENCH_NZB_PATH":       &cfg.NZBPath,
		"BENCH_OUTPUT_DIR":     &cfg.OutputDir,
		"BENCH_CONFIG_DIR":     &cfg.ConfigDir,
		"BENCH_RESULT_PATH":    &cfg.ResultPath,
		"BENCH_NNTP_CA_FILE":   &cfg.NNTPCAFile,
		"CLIENT_RARPAR_BINARY": &cfg.RarparBinary,
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
	if strings.TrimSpace(c.RunID) == "" || strings.TrimSpace(c.Network) == "" {
		return fmt.Errorf("run id and Docker network are required")
	}
	if c.Client != benchmark.Weaver && c.Client != benchmark.SABnzbd && c.Client != benchmark.NZBGet {
		return fmt.Errorf("unsupported client %q", c.Client)
	}
	if c.ArchiveToolchain != benchmark.VanillaArchiveToolchain && c.ArchiveToolchain != benchmark.RarparArchiveToolchain {
		return fmt.Errorf("unsupported archive toolchain %q", c.ArchiveToolchain)
	}
	if c.ArchiveToolchain == benchmark.RarparArchiveToolchain && (c.Client != benchmark.SABnzbd && c.Client != benchmark.NZBGet) {
		return fmt.Errorf("Rarpar is only configured for SABnzbd and NZBGet Docker lanes")
	}
	if c.ExecutionTarget != benchmark.DockerLinux {
		return fmt.Errorf("clientadapter only supports execution target %q, got %q", benchmark.DockerLinux, c.ExecutionTarget)
	}
	if c.Transport != benchmark.Plaintext && c.Transport != benchmark.TLS {
		return fmt.Errorf("unsupported transport %q", c.Transport)
	}
	if c.Connections < 1 || c.StartupTimeout <= 0 || c.PollInterval <= 0 || c.JobTimeout <= 0 {
		return fmt.Errorf("connections, startup timeout, poll interval, and job timeout must be positive")
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
	if c.QueueInput != nil {
		if err := c.QueueInput.Validate(); err != nil {
			return err
		}
		for _, job := range c.QueueInput.Jobs {
			if _, err := os.Stat(job.NZBPath); err != nil {
				return fmt.Errorf("inspect queue NZB %s: %w", job.RunID, err)
			}
		}
	}
	if c.Profile != benchmark.ProfileStock && c.Profile != benchmark.ProfileEquivalentThroughput {
		return fmt.Errorf("unsupported benchmark profile %q", c.Profile)
	}
	if err := c.ServerLink.Validate(); err != nil {
		return err
	}
	if err := c.validateStorage(); err != nil {
		return err
	}
	if !digestPinnedImage(c.Image) {
		return fmt.Errorf("CLIENT_IMAGE must be digest-pinned (image@sha256:<64 hex characters>)")
	}
	if c.ArchiveToolchain == benchmark.RarparArchiveToolchain {
		if err := validateRarparInput(c.RarparBinary, c.RarparVersion, c.RarparSHA256); err != nil {
			return err
		}
	}
	if _, err := os.Stat(c.NZBPath); err != nil {
		return fmt.Errorf("inspect NZB: %w", err)
	}
	for field, value := range map[string]string{
		"NNTP host":        c.NNTPHost,
		"NNTP port":        c.NNTPPort,
		"NNTP username":    c.NNTPUsername,
		"NNTP password":    c.NNTPPassword,
		"archive password": c.ArchivePassword,
		"Docker network":   c.Network,
		"Docker image":     c.Image,
	} {
		if strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("%s must not contain a line break", field)
		}
	}
	return nil
}

// parseStorageProfile decodes the plan's exact storage contract. Unknown
// fields are refused: an adapter that silently ignored part of the declared
// layout would report a run the plan did not describe.
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

// validateStorage keeps the adapter honest about where the client's
// directories live. The controller owns the export, the volumes, and the
// attestation; the adapter may only mount exactly what it was given.
func (c Config) validateStorage() error {
	if err := c.StorageProfile.Validate(); err != nil {
		return err
	}
	if c.StorageProfile.Kind == benchmark.StorageLocal {
		if c.CompleteVolume != "" || c.IncompleteVolume != "" {
			return fmt.Errorf("local storage profile must not receive NFS volumes")
		}
		return nil
	}
	if c.CompleteVolume == "" {
		return fmt.Errorf("storage profile %q requires BENCH_STORAGE_COMPLETE_VOLUME", c.StorageProfile.ID)
	}
	if c.StorageProfile.IntermediateOnNFS != (c.IncompleteVolume != "") {
		return fmt.Errorf("storage profile %q does not match the supplied intermediate volume", c.StorageProfile.ID)
	}
	for _, volume := range []string{c.CompleteVolume, c.IncompleteVolume} {
		if strings.ContainsAny(volume, ",\r\n") {
			return fmt.Errorf("Docker volume name must not contain a comma or line break: %q", volume)
		}
	}
	return nil
}

func (c Config) RenderProductConfig() (ProductSpec, error) {
	directUnpack := c.Profile == benchmark.ProfileEquivalentThroughput
	var spec ProductSpec
	switch c.Client {
	case benchmark.Weaver:
		spec = renderWeaver(c, directUnpack)
	case benchmark.SABnzbd:
		spec = renderSABnzbd(c, directUnpack)
	case benchmark.NZBGet:
		spec = renderNZBGet(c, directUnpack)
	default:
		return ProductSpec{}, fmt.Errorf("unsupported client %q", c.Client)
	}
	spec.NeedsCAMount = c.Transport == benchmark.TLS && c.TLSValidation == benchmark.TLSCAVerified
	spec.Rendered = renderAuditConfig(c, spec)
	digest := sha256.Sum256(spec.Rendered)
	spec.ConfigSHA256 = hex.EncodeToString(digest[:])
	return spec, nil
}

func renderWeaver(c Config, _ bool) ProductSpec {
	env := []string{
		"WEAVER_HTTP_BIND_ADDRESS=0.0.0.0",
		"WEAVER_DATA_DIR=/config/data",
		"WEAVER_INTERMEDIATE_DIR=/downloads/incomplete",
		"WEAVER_COMPLETE_DIR=/downloads/complete",
		"WEAVER_CLEANUP_AFTER_EXTRACT=false",
		// Direct unpack (in-stream extraction of stored archives) is Weaver's
		// shipping default from the release these benches accompany. It is
		// rendered explicitly in BOTH profiles so the pinned image benches the
		// product as shipped, and so the audit record shows it.
		"WEAVER_DIRECT_UNPACK=on",
		// Weaver holds fresh posts for five minutes before downloading them
		// (propagation delay). SABnzbd and NZBGet ship with that delay at zero,
		// and every benchmark NZB is minutes old by construction, so the hold
		// would measure the poster's clock rather than the client. Disabled
		// through the documented environment gate; the audit record shows it.
		"WEAVER_PROPAGATION_DELAY_SECS=0",
		"WEAVER_SERVER_1_HOSTNAME=" + c.NNTPHost,
		"WEAVER_SERVER_1_PORT=" + c.NNTPPort,
		"WEAVER_SERVER_1_TLS=" + strconv.FormatBool(c.NNTPUseTLS),
		"WEAVER_SERVER_1_USERNAME=" + c.NNTPUsername,
		"WEAVER_SERVER_1_PASSWORD=" + c.NNTPPassword,
		"WEAVER_SERVER_1_CONNECTIONS=" + strconv.Itoa(c.Connections),
		"WEAVER_SERVER_1_ACTIVE=true",
		// Weaver's first-run access policy hands an anonymous browser session
		// only to peers on its trusted-network list; without one, an install
		// with no login serves a setup notice and refuses every GraphQL call.
		// The adapter reaches the container through a port published on the
		// host's loopback, so inside the container its peer is the bridge
		// gateway of the private benchmark network — never loopback. Pinning
		// the list from the environment settles the policy at startup (no
		// wizard, no bootstrap login) and is confined to that isolated network.
		"WEAVER_TRUSTED_CIDRS=0.0.0.0/0,::/0",
	}
	// The pinned Weaver image honors PUID/PGID just like the LinuxServer
	// client images. Keeping its bind mounts owned by the invoking benchmark
	// user lets the adapter retain logs and immutable telemetry artifacts.
	env = append(env, linuxServerEnvironment()...)
	if c.Transport == benchmark.TLS && c.TLSValidation == benchmark.TLSCAVerified {
		env = append(env, "WEAVER_SERVER_1_TLS_CA_CERT=/benchmark-ca/nntp-ca.pem")
	}
	// Keep the selected TLS implementation visible in the rendered product
	// environment when an operator explicitly supplies one for a diagnostic.
	// Normal benchmark runs leave this unset and use the product default.
	if tlsBackend := os.Getenv("WEAVER_NNTP_TLS_BACKEND"); tlsBackend != "" {
		env = append(env, "WEAVER_NNTP_TLS_BACKEND="+tlsBackend)
	}
	if rustLog := os.Getenv("RUST_LOG"); rustLog != "" {
		env = append(env, "RUST_LOG="+rustLog)
	}
	// Pin the startup random-read IOPS so the server skips its startup disk
	// probe — a 4 MB write + fsync + 200 random preads that otherwise lands
	// inside the measured process lifetime and varies with the bench host's
	// storage — and tunes identically on every run. An operator can override
	// the pinned value for a diagnostic; the probe itself only runs when the
	// variable is unset, which no benchmark run should want.
	startupIops := os.Getenv("WEAVER_STARTUP_IOPS")
	if startupIops == "" {
		startupIops = "50000"
	}
	env = append(env, "WEAVER_STARTUP_IOPS="+startupIops)
	if c.QueueInput != nil {
		return ProductSpec{
			APIPort:       9090,
			ExposeAPI:     true,
			Command:       []string{"--config", "/config", "serve", "--port", "9090"},
			ConfigName:    "weaver.env",
			ConfigContent: []byte(strings.Join(env, "\n") + "\n"),
			Environment:   env,
		}
	}
	command := []string{
		"--config", "/config",
		"download", "/benchmark-input/" + filepath.Base(c.NZBPath),
		"--report", "/config/" + weaverCLIReport,
		"--report-ack", "/config/" + weaverCLIReportAck,
	}
	if c.ArchivePassword != "" {
		command = append(command, "--password", c.ArchivePassword)
	}
	return ProductSpec{
		APIPort:              0,
		Command:              command,
		NeedsNZBMount:        true,
		CompletionReportName: weaverCLIReport,
		CompletionAckName:    weaverCLIReportAck,
		ConfigName:           "weaver.env",
		ConfigContent:        []byte(strings.Join(env, "\n") + "\n"),
		Environment:          env,
	}
}

func renderSABnzbd(c Config, directUnpack bool) ProductSpec {
	ssl := "0"
	if c.NNTPUseTLS {
		ssl = "1"
	}
	direct := "0"
	if directUnpack {
		direct = "1"
	}
	content := strings.Join([]string{
		"[misc]",
		"host = 0.0.0.0",
		"port = 8080",
		"api_key = " + apiKey,
		"download_dir = /downloads/incomplete",
		"complete_dir = /downloads/complete",
		"enable_unrar = 1",
		"direct_unpack = " + direct,
		"pre_check = 0",
		"pause_on_post_processing = 0",
		"",
		"[servers]",
		"[[benchmark]]",
		"host = " + c.NNTPHost,
		"port = " + c.NNTPPort,
		"username = " + c.NNTPUsername,
		"password = " + c.NNTPPassword,
		"connections = " + strconv.Itoa(c.Connections),
		"ssl = " + ssl,
		// This is intentional and policy-labelled by the plan. SAB's local CA
		// support is not reliable in this harness, so verified TLS is never
		// claimed for SABnzbd.
		"ssl_verify = 0",
		"",
	}, "\n")
	environment := linuxServerEnvironment()
	if c.ArchiveToolchain == benchmark.RarparArchiveToolchain {
		// Keep LinuxServer's /lsiopy Python environment ahead of the system
		// interpreter while allowing SABnzbd to discover the Rarpar shims first.
		environment = append(environment, "PATH=/config/toolchain:/lsiopy/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return ProductSpec{APIPort: 8080, ExposeAPI: true, ConfigName: "sabnzbd.ini", ConfigContent: []byte(content), Environment: environment}
}

// nzbgetSevenZipCommand names the official 7-Zip console build the pinned
// LinuxServer NZBGet image ships. The 7z corpus lane depends on it, and
// stating the absolute path keeps that lane from silently picking up whichever
// `7z` happens to come first on PATH inside the image.
const nzbgetSevenZipCommand = "/usr/bin/7zz"

func renderNZBGet(c Config, directUnpack bool) ProductSpec {
	encryption := "no"
	verification := "none"
	certStore := ""
	certCheck := "no"
	if c.NNTPUseTLS {
		encryption = "yes"
		if c.TLSValidation == benchmark.TLSCAVerified {
			verification = "strict"
			certStore = "/benchmark-ca/nntp-ca.pem"
			certCheck = "yes"
		}
	}
	direct := "no"
	directWrite := "no"
	if directUnpack {
		direct = "yes"
		directWrite = "yes"
	}
	unpack := "yes"
	parRepair := "yes"
	unrarCommand := "unrar"
	if c.ArchiveToolchain == benchmark.RarparArchiveToolchain {
		// NZBGet's PAR2 engine is built in. Keep its normal PAR2 and unpack
		// pipeline enabled, changing only the externally configurable UnRAR tool.
		unrarCommand = "/config/toolchain/unrar"
	}
	content := strings.Join([]string{
		"MainDir=/config",
		"DestDir=/downloads/complete",
		"InterDir=/downloads/incomplete",
		"NzbDir=/config/nzb",
		"QueueDir=/config/queue",
		"TempDir=/config/tmp",
		"ScriptDir=/config/scripts",
		"LogFile=/config/nzbget.log",
		"ControlIP=0.0.0.0",
		"ControlPort=6789",
		"ControlUsername=" + controlUsername,
		"ControlPassword=" + apiKey,
		"OutputMode=log",
		"DirectWrite=" + directWrite,
		"DirectUnpack=" + direct,
		"ParCheck=auto",
		"ParRepair=" + parRepair,
		"Unpack=" + unpack,
		"UnrarCmd=" + unrarCommand,
		"SevenZipCmd=" + nzbgetSevenZipCommand,
		"Extensions=",
		"Server1.Active=yes",
		"Server1.Name=benchmark",
		"Server1.Level=0",
		"Server1.Optional=no",
		"Server1.Group=0",
		"Server1.Host=" + c.NNTPHost,
		"Server1.Port=" + c.NNTPPort,
		"Server1.Username=" + c.NNTPUsername,
		"Server1.Password=" + c.NNTPPassword,
		"Server1.Connections=" + strconv.Itoa(c.Connections),
		"Server1.Encryption=" + encryption,
		"Server1.CertVerification=" + verification,
		"CertStore=" + certStore,
		"CertCheck=" + certCheck,
		"",
	}, "\n")
	return ProductSpec{APIPort: 6789, ExposeAPI: true, ConfigName: "nzbget.conf", ConfigContent: []byte(content), Environment: linuxServerEnvironment()}
}

func linuxServerEnvironment() []string {
	// The LinuxServer images honor these values before dropping privileges. It
	// keeps per-run bind mounts writable without making artifact directories
	// world-writable on native Linux hosts.
	environment := []string{"TZ=UTC"}
	if uid, gid := os.Getuid(), os.Getgid(); uid >= 0 && gid >= 0 {
		environment = append(environment, "PUID="+strconv.Itoa(uid), "PGID="+strconv.Itoa(gid))
	}
	return environment
}

func renderAuditConfig(c Config, spec ProductSpec) []byte {
	env := append([]string(nil), spec.Environment...)
	// Product env is a set. Sort this audit view to make the checksum stable
	// even if the renderer's append order changes for an equivalent config.
	sortStrings(env)
	execution := "one_shot_cli"
	if spec.ExposeAPI {
		execution = "api_service"
	}
	return []byte(strings.Join([]string{
		"schema_version=2",
		"client=" + string(c.Client),
		"archive_toolchain=" + string(c.ArchiveToolchain),
		"archive_toolchain_identity=" + c.archiveToolchainIdentity(),
		"execution_target=" + string(c.ExecutionTarget),
		"execution=" + execution,
		"profile=" + c.Profile,
		"image=" + c.Image,
		"network=" + c.Network,
		"transport=" + string(c.Transport),
		"transport_label=" + c.TransportLabel,
		"tls_validation=" + string(c.TLSValidation),
		"server_link_id=" + c.ServerLink.ID,
		"server_link_scope=" + c.ServerLink.Scope,
		"server_link_egress_bits_per_second=" + strconv.FormatUint(c.ServerLink.EgressBitsPerSecond, 10),
		"server_link_burst_bytes=" + strconv.FormatUint(c.ServerLink.BurstBytes, 10),
		"storage_profile_id=" + c.StorageProfile.ID,
		"storage_kind=" + string(c.StorageProfile.Kind),
		"storage_nfs_link_id=" + c.StorageProfile.NFSLinkID,
		"storage_intermediate_on_nfs=" + strconv.FormatBool(c.StorageProfile.IntermediateOnNFS),
		"storage_complete_on_nfs=" + strconv.FormatBool(c.StorageProfile.CompleteOnNFS),
		"storage_link_bits_per_second=" + strconv.FormatUint(c.StorageProfile.LinkBitsPerSecond, 10),
		"storage_link_burst_bytes=" + strconv.FormatUint(c.StorageProfile.LinkBurstBytes, 10),
		"storage_rtt_micros=" + strconv.FormatUint(c.StorageProfile.RTTMicros, 10),
		"storage_mount_options=" + c.StorageProfile.MountOptions,
		"storage_export_options=" + c.StorageProfile.ExportOptions,
		"storage_shaper=" + c.StorageProfile.Shaper,
		"storage_attestation_scope=" + c.StorageProfile.AttestationScope,
		"archive_password=" + c.ArchivePassword,
		"api_port=" + strconv.Itoa(spec.APIPort),
		"api_exposed=" + strconv.FormatBool(spec.ExposeAPI),
		"nzb_input_mount=" + strconv.FormatBool(spec.NeedsNZBMount),
		"completion_report_name=" + spec.CompletionReportName,
		"completion_ack_name=" + spec.CompletionAckName,
		"config_name=" + spec.ConfigName,
		"--- product environment ---",
		strings.Join(env, "\n"),
		"--- product command ---",
		strings.Join(spec.Command, "\n"),
		"--- product config ---",
		string(spec.ConfigContent),
	}, "\n"))
}

func required(getenv func(string) string, key string) string {
	return strings.TrimSpace(getenv(key))
}

func defaultString(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
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

// DefaultJobTimeout is the default bound on a single job's time to a terminal
// state. It is generous: the largest fixture (a 6 GiB Blu-ray set) completes
// on every client in a few minutes at 1 Gbit, including the slowest
// post-processing path over the throttled NFS profile. It exists to turn a
// client that never finishes into a recorded did-not-finish instead of a
// stalled pass.
const DefaultJobTimeout = 20 * time.Minute

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

func digestPinnedImage(image string) bool {
	parts := strings.Split(image, "@sha256:")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || len(parts[1]) != 64 {
		return false
	}
	for _, character := range parts[1] {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func sortStrings(values []string) {
	for index := 1; index < len(values); index++ {
		for cursor := index; cursor > 0 && values[cursor] < values[cursor-1]; cursor-- {
			values[cursor], values[cursor-1] = values[cursor-1], values[cursor]
		}
	}
}
