package nativeadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	ReportPath   string
	AckPath      string
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
	reportPath := filepath.Join(cfg.ConfigDir, reportName)
	ackPath := filepath.Join(cfg.ConfigDir, reportAckName)
	env := []string{
		"WEAVER_HTTP_BIND_ADDRESS=127.0.0.1",
		"WEAVER_DATA_DIR=" + filepath.Join(cfg.ConfigDir, "data"),
		"WEAVER_INTERMEDIATE_DIR=" + filepath.Join(cfg.ConfigDir, "incomplete"),
		"WEAVER_COMPLETE_DIR=" + cfg.OutputDir,
		"WEAVER_CLEANUP_AFTER_EXTRACT=false",
		// Keep native and container measurements on the same extraction limit.
		"WEAVER_MAX_CONCURRENT_EXTRACTIONS=6",
		"WEAVER_SERVER_1_HOSTNAME=" + cfg.NNTPHost,
		"WEAVER_SERVER_1_PORT=" + cfg.NNTPPort,
		"WEAVER_SERVER_1_TLS=" + strconv.FormatBool(cfg.NNTPUseTLS),
		"WEAVER_SERVER_1_USERNAME=" + cfg.NNTPUsername,
		"WEAVER_SERVER_1_PASSWORD=" + cfg.NNTPPassword,
		"WEAVER_SERVER_1_CONNECTIONS=" + strconv.Itoa(cfg.Connections),
		"WEAVER_SERVER_1_ACTIVE=true",
	}
	if cfg.Transport == benchmark.TLS && cfg.TLSValidation == benchmark.TLSCAVerified {
		env = append(env, "WEAVER_SERVER_1_TLS_CA_CERT="+cfg.NNTPCAFile)
	}
	command := append(expandCommand(cfg.LaunchCommand, cfg, reportPath, ackPath), "--report", reportPath, "--report-ack", ackPath)
	if cfg.ArchivePassword != "" {
		command = append(command, "--password", cfg.ArchivePassword)
	}
	return productSpec{
		ConfigName:  "weaver.env",
		Content:     []byte(strings.Join(env, "\n") + "\n"),
		Environment: env,
		Command:     command,
		ReportPath:  reportPath,
		AckPath:     ackPath,
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
		"auto_disconnect = 0",
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
		Command:    expandCommand(cfg.LaunchCommand, cfg, "", ""),
	}
}

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
		"ArticleCache=0",
		"DirectWrite=" + directWrite,
		"DirectUnpack=" + direct,
		"ParCheck=manual",
		"ParRepair=no",
		"Unpack=yes",
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
		Command:    expandCommand(cfg.LaunchCommand, cfg, "", ""),
	}
}

func expandCommand(command []string, cfg Config, reportPath, ackPath string) []string {
	replacements := map[string]string{
		"{{config_dir}}":  cfg.ConfigDir,
		"{{fixture_dir}}": cfg.FixtureDir,
		"{{nzb_path}}":    cfg.NZBPath,
		"{{output_dir}}":  cfg.OutputDir,
		"{{report_path}}": reportPath,
		"{{report_ack}}":  ackPath,
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
		"schema_version=1",
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
		"api_endpoint=" + cfg.APIEndpoint,
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
