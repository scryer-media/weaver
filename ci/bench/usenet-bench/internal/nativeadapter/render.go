package nativeadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

type productSpec struct {
	ConfigName   string
	Content      []byte
	Environment  []string
	Command      []string
	Rendered     []byte
	ConfigSHA256 string
}

func renderProduct(cfg Config) (productSpec, error) {
	directUnpack := cfg.Profile == benchmark.ProfileEquivalentThroughput
	var spec productSpec
	switch cfg.Client {
	case benchmark.Weaver:
		spec = renderWeaver(cfg)
	case benchmark.SABnzbd:
		spec = renderSABnzbd(cfg, directUnpack)
	case benchmark.NZBGet:
		spec = renderNZBGet(cfg, directUnpack)
	default:
		return productSpec{}, fmt.Errorf("unsupported client %q", cfg.Client)
	}
	spec.Rendered = renderAuditConfig(cfg, spec)
	digest := sha256.Sum256(spec.Rendered)
	spec.ConfigSHA256 = hex.EncodeToString(digest[:])
	return spec, nil
}

func renderWeaver(cfg Config) productSpec {
	env := []string{
		"WEAVER_HTTP_BIND_ADDRESS=127.0.0.1",
		"WEAVER_DATA_DIR=" + filepath.Join(cfg.ConfigDir, "data"),
		"WEAVER_INTERMEDIATE_DIR=" + filepath.Join(cfg.ConfigDir, "incomplete"),
		"WEAVER_COMPLETE_DIR=" + cfg.OutputDir,
		"WEAVER_CLEANUP_AFTER_EXTRACT=false",
		// Direct unpack (in-stream extraction of stored archives) is Weaver's
		// shipping default from the release these benches accompany. It is
		// rendered explicitly in BOTH profiles so the pinned client binary
		// benches the product as shipped, and so the audit record shows it.
		"WEAVER_DIRECT_UNPACK=on",
		// Weaver holds fresh posts for five minutes before downloading them
		// (propagation delay). SABnzbd and NZBGet ship with that delay at zero,
		// and every benchmark NZB is minutes old by construction, so the hold
		// would measure the poster's clock rather than the client. Disabled
		// through the documented environment gate; the audit record shows it.
		"WEAVER_PROPAGATION_DELAY_SECS=0",
		"WEAVER_SERVER_1_HOSTNAME=" + cfg.NNTPHost,
		"WEAVER_SERVER_1_PORT=" + cfg.NNTPPort,
		"WEAVER_SERVER_1_TLS=" + strconv.FormatBool(cfg.NNTPUseTLS),
		"WEAVER_SERVER_1_USERNAME=" + cfg.NNTPUsername,
		"WEAVER_SERVER_1_PASSWORD=" + cfg.NNTPPassword,
		"WEAVER_SERVER_1_CONNECTIONS=" + strconv.Itoa(cfg.Connections),
		"WEAVER_SERVER_1_ACTIVE=true",
		// A fresh native install trusts no peer until its first-run wizard is
		// completed from the machine's own browser; loopback is offered the
		// wizard, not a session. Pinning loopback as trusted from the
		// environment settles the access policy at startup so the launcher's
		// anonymous session on 127.0.0.1 is admitted without any wizard step.
		"WEAVER_TRUSTED_CIDRS=127.0.0.0/8,::1/128",
	}
	if cfg.Transport == benchmark.TLS && cfg.TLSValidation == benchmark.TLSCAVerified {
		env = append(env, "WEAVER_SERVER_1_TLS_CA_CERT="+cfg.NNTPCAFile)
	}
	// The native launcher inherits the controller environment, so these would
	// reach Weaver anyway; rendering them explicitly keeps the audit record
	// identical to the Docker lane, which lists every effective product setting.
	if tlsBackend := os.Getenv("WEAVER_NNTP_TLS_BACKEND"); tlsBackend != "" {
		env = append(env, "WEAVER_NNTP_TLS_BACKEND="+tlsBackend)
	}
	if rustLog := os.Getenv("RUST_LOG"); rustLog != "" {
		env = append(env, "RUST_LOG="+rustLog)
	}
	// Match the Docker lane: pin the startup random-read IOPS so Weaver skips
	// its startup disk probe (a write + fsync + random-read burst that would
	// otherwise run inside the measured native process lifetime and vary with
	// the host's storage). An operator override is honoured for diagnostics.
	startupIops := os.Getenv("WEAVER_STARTUP_IOPS")
	if startupIops == "" {
		startupIops = "50000"
	}
	env = append(env, "WEAVER_STARTUP_IOPS="+startupIops)
	return productSpec{
		ConfigName:  "weaver.env",
		Content:     []byte(strings.Join(env, "\n") + "\n"),
		Environment: env,
		Command:     expandCommand(cfg.LaunchCommand, cfg),
	}
}

func renderSABnzbd(cfg Config, directUnpack bool) productSpec {
	_, apiPort, _ := nativeAPIAddress(cfg.APIEndpoint)
	ssl := "0"
	if cfg.NNTPUseTLS {
		ssl = "1"
	}
	direct := "0"
	if directUnpack {
		direct = "1"
	}
	content := strings.Join([]string{
		"[misc]",
		"host = 127.0.0.1",
		"port = " + strconv.Itoa(apiPort),
		"api_key = " + apiKey,
		"download_dir = " + filepath.Join(cfg.ConfigDir, "incomplete"),
		"complete_dir = " + cfg.OutputDir,
		"enable_unrar = 1",
		"direct_unpack = " + direct,
		"pre_check = 0",
		"pause_on_post_processing = 0",
		"",
		"[servers]",
		"[[benchmark]]",
		"host = " + cfg.NNTPHost,
		"port = " + cfg.NNTPPort,
		"username = " + cfg.NNTPUsername,
		"password = " + cfg.NNTPPassword,
		"connections = " + strconv.Itoa(cfg.Connections),
		"ssl = " + ssl,
		// Native SAB follows the same explicitly labelled local TLS policy as
		// Docker. No result may claim CA verification for this product.
		"ssl_verify = 0",
		"",
	}, "\n")
	return productSpec{
		ConfigName: "sabnzbd.ini",
		Content:    []byte(content),
		Command:    expandCommand(cfg.LaunchCommand, cfg),
	}
}

// nzbgetSevenZipCommand is the official 7-Zip console binary a native NZBGet
// install resolves from PATH. The 7z corpus lane needs it, so it is stated
// rather than left to NZBGet's built-in default: a host without it then fails
// loudly instead of quietly skipping every 7z unpack.
const nzbgetSevenZipCommand = "7z"

func renderNZBGet(cfg Config, directUnpack bool) productSpec {
	_, apiPort, _ := nativeAPIAddress(cfg.APIEndpoint)
	encryption := "no"
	verification := "none"
	certStore := ""
	certCheck := "no"
	if cfg.NNTPUseTLS {
		encryption = "yes"
		if cfg.TLSValidation == benchmark.TLSCAVerified {
			verification = "strict"
			certStore = cfg.NNTPCAFile
			certCheck = "yes"
		}
	}
	direct := "no"
	directWrite := "no"
	if directUnpack {
		direct = "yes"
		directWrite = "yes"
	}
	content := strings.Join([]string{
		"MainDir=" + cfg.ConfigDir,
		"DestDir=" + cfg.OutputDir,
		"InterDir=" + filepath.Join(cfg.ConfigDir, "incomplete"),
		"NzbDir=" + filepath.Join(cfg.ConfigDir, "nzb"),
		"QueueDir=" + filepath.Join(cfg.ConfigDir, "queue"),
		"TempDir=" + filepath.Join(cfg.ConfigDir, "tmp"),
		"ScriptDir=" + filepath.Join(cfg.ConfigDir, "scripts"),
		"LogFile=" + filepath.Join(cfg.ConfigDir, "nzbget.log"),
		"ControlIP=127.0.0.1",
		"ControlPort=" + strconv.Itoa(apiPort),
		"ControlUsername=" + controlUsername,
		"ControlPassword=" + apiKey,
		"DaemonMode=no",
		"OutputMode=log",
		"DirectWrite=" + directWrite,
		"DirectUnpack=" + direct,
		"ParCheck=auto",
		"ParRepair=yes",
		"Unpack=yes",
		"SevenZipCmd=" + nzbgetSevenZipCommand,
		"Server1.Active=yes",
		"Server1.Name=benchmark",
		"Server1.Level=0",
		"Server1.Optional=no",
		"Server1.Group=0",
		"Server1.Host=" + cfg.NNTPHost,
		"Server1.Port=" + cfg.NNTPPort,
		"Server1.Username=" + cfg.NNTPUsername,
		"Server1.Password=" + cfg.NNTPPassword,
		"Server1.Connections=" + strconv.Itoa(cfg.Connections),
		"Server1.Encryption=" + encryption,
		"Server1.CertVerification=" + verification,
		"CertStore=" + certStore,
		"CertCheck=" + certCheck,
		"",
	}, "\n")
	return productSpec{
		ConfigName: "nzbget.conf",
		Content:    []byte(content),
		Command:    expandCommand(cfg.LaunchCommand, cfg),
	}
}

func expandCommand(command []string, cfg Config) []string {
	_, apiPort, _ := nativeAPIAddress(cfg.APIEndpoint)
	replacements := map[string]string{
		"{{config_dir}}":  cfg.ConfigDir,
		"{{fixture_dir}}": cfg.FixtureDir,
		"{{nzb_path}}":    cfg.NZBPath,
		"{{output_dir}}":  cfg.OutputDir,
		"{{api_port}}":    strconv.Itoa(apiPort),
	}
	expanded := make([]string, len(command))
	for index, argument := range command {
		expanded[index] = argument
		for token, value := range replacements {
			expanded[index] = strings.ReplaceAll(expanded[index], token, value)
		}
	}
	return expanded
}

func renderAuditConfig(cfg Config, spec productSpec) []byte {
	env := append([]string(nil), spec.Environment...)
	sortStrings(env)
	command, _ := json.Marshal(spec.Command)
	return []byte(strings.Join([]string{
		"schema_version=2",
		"client=" + string(cfg.Client),
		"archive_toolchain=" + string(cfg.ArchiveToolchain),
		"archive_toolchain_identity=stock",
		"execution_target=" + string(cfg.ExecutionTarget),
		"profile=" + cfg.Profile,
		"transport=" + string(cfg.Transport),
		"transport_label=" + cfg.TransportLabel,
		"tls_validation=" + string(cfg.TLSValidation),
		"client_version=" + cfg.ClientVersion,
		"server_link_id=" + cfg.ServerLink.ID,
		"server_link_scope=" + cfg.ServerLink.Scope,
		"server_link_egress_bits_per_second=" + strconv.FormatUint(cfg.ServerLink.EgressBitsPerSecond, 10),
		"server_link_burst_bytes=" + strconv.FormatUint(cfg.ServerLink.BurstBytes, 10),
		"storage_profile_id=" + cfg.StorageProfile.ID,
		"storage_kind=" + string(cfg.StorageProfile.Kind),
		"storage_nfs_link_id=" + cfg.StorageProfile.NFSLinkID,
		"storage_intermediate_on_nfs=" + strconv.FormatBool(cfg.StorageProfile.IntermediateOnNFS),
		"storage_complete_on_nfs=" + strconv.FormatBool(cfg.StorageProfile.CompleteOnNFS),
		"storage_link_bits_per_second=" + strconv.FormatUint(cfg.StorageProfile.LinkBitsPerSecond, 10),
		"storage_link_burst_bytes=" + strconv.FormatUint(cfg.StorageProfile.LinkBurstBytes, 10),
		"storage_rtt_micros=" + strconv.FormatUint(cfg.StorageProfile.RTTMicros, 10),
		"storage_mount_options=" + cfg.StorageProfile.MountOptions,
		"storage_export_options=" + cfg.StorageProfile.ExportOptions,
		"storage_shaper=" + cfg.StorageProfile.Shaper,
		"storage_attestation_scope=" + cfg.StorageProfile.AttestationScope,
		"api_endpoint=" + cfg.APIEndpoint,
		"execution=api_service",
		"archive_password=" + cfg.ArchivePassword,
		"launch_command=" + string(command),
		"--- product environment ---",
		strings.Join(env, "\n"),
		"--- product config ---",
		string(spec.Content),
	}, "\n"))
}

func sortStrings(values []string) {
	for index := 1; index < len(values); index++ {
		for cursor := index; cursor > 0 && values[cursor] < values[cursor-1]; cursor-- {
			values[cursor], values[cursor-1] = values[cursor-1], values[cursor]
		}
	}
}
