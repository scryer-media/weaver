package weaver

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const realUsenetNZBName = "test-download-100MB.nzb"
const realUsenetNZBSHA256 = "c369cfb5e00ebbba843f145e931d6616c322224a1a7267a3a9b42c1d82114ca7"

var realUsenetWGNames = []string{
	"wg-chain-01.conf", "wg-chain-02.conf", "wg-chain-03.conf",
	"wg-pool-01.conf", "wg-pool-02.conf", "wg-pool-03.conf", "wg-pool-04.conf", "wg-pool-05.conf",
}

type realUsenetConfig struct {
	Root, Host, Username, Password string
	Port, Connections              int
	NZB                            []byte
	Profiles                       []map[string]any
	ChainProfiles                  []string
	Perf                           bool
	WorkerThreads                  int
}

// Values stay in memory and enter Weaver only through its local profile API.
// Neither the private files nor their contents belong in a build context.
func loadRealUsenetConfig(root string) (*realUsenetConfig, error) {
	config := &realUsenetConfig{Root: root, Host: "news.newshosting.com", Port: 563, Connections: 4}
	data, err := readRealUsenetPrivateFile(root, ".env")
	if err != nil {
		return nil, err
	}
	values, err := parseRealUsenetEnv(string(data))
	if err != nil {
		return nil, err
	}
	config.Username, config.Password = values["NEWSHOSTING_USER"], values["NEWSHOSTING_PASS"]
	if config.Username == "" || config.Password == "" {
		return nil, fmt.Errorf("private .env requires NEWSHOSTING_USER and NEWSHOSTING_PASS")
	}
	if host := values["NEWSHOSTING_HOST"]; host != "" {
		config.Host = host
	}
	if strings.ContainsAny(config.Host, " /\\\r\n\t:") {
		return nil, fmt.Errorf("NEWSHOSTING_HOST must be a hostname or IPv4 address")
	}
	for key, target := range map[string]*int{"NEWSHOSTING_PORT": &config.Port, "NEWSHOSTING_CONNECTIONS": &config.Connections} {
		if value := values[key]; value != "" {
			number, err := strconv.Atoi(value)
			if err != nil || number < 1 || number > 65535 {
				return nil, fmt.Errorf("invalid %s", key)
			}
			*target = number
		}
	}
	if config.Connections > 64 {
		return nil, fmt.Errorf("NEWSHOSTING_CONNECTIONS must be between 1 and 64")
	}
	config.NZB, err = readRealUsenetPrivateFile(root, realUsenetNZBName)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(config.NZB)
	if hex.EncodeToString(hash[:]) != realUsenetNZBSHA256 {
		return nil, fmt.Errorf("%s differs from the pinned SABnzbd test NZB", realUsenetNZBName)
	}
	var missing []string
	for _, name := range realUsenetWGNames {
		if _, err := os.Lstat(filepath.Join(root, name)); os.IsNotExist(err) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("add the following private WireGuard files: %s", strings.Join(missing, ", "))
	}
	for _, name := range realUsenetWGNames {
		data, err := readRealUsenetPrivateFile(root, name)
		if err != nil {
			return nil, err
		}
		profile, err := parseRealUsenetWireGuard(string(data), strings.TrimSuffix(name, ".conf"))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		config.Profiles = append(config.Profiles, profile)
	}
	if err := validateRealUsenetEndpoints(config.Profiles); err != nil {
		return nil, err
	}
	return config, nil
}

func validateRealUsenetEndpoints(profiles []map[string]any) error {
	for _, group := range [][]map[string]any{profiles[:3], profiles[3:]} {
		seen := map[string]string{}
		for _, profile := range group {
			host := strings.TrimSuffix(strings.ToLower(profile["host"].(string)), ".")
			if ip := net.ParseIP(host); ip != nil {
				host = ip.String()
			}
			endpoint := net.JoinHostPort(host, strconv.Itoa(profile["port"].(int)))
			if previous, duplicate := seen[endpoint]; duplicate {
				return fmt.Errorf("%s and %s must use different WireGuard endpoints", previous, profile["name"])
			}
			seen[endpoint] = profile["name"].(string)
		}
	}
	return nil
}

func readRealUsenetPrivateFile(root, name string) ([]byte, error) {
	path := filepath.Join(root, name)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("private %s must be a regular file, not a symlink", name)
	}
	if info.Size() > 2<<20 {
		return nil, fmt.Errorf("private %s is too large", name)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read private %s", name)
	}
	return data, nil
}

func parseRealUsenetEnv(text string) (map[string]string, error) {
	values := map[string]string{}
	for index, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid private .env assignment at line %d", index+1)
		}
		if strings.HasPrefix(value, "\"") {
			parsed, err := strconv.Unquote(value)
			if err != nil {
				return nil, fmt.Errorf("invalid quoted .env value at line %d", index+1)
			}
			value = parsed
		} else if strings.HasPrefix(value, "'") {
			if len(value) < 2 || !strings.HasSuffix(value, "'") {
				return nil, fmt.Errorf("invalid quoted .env value at line %d", index+1)
			}
			value = value[1 : len(value)-1]
		} else if start := strings.Index(value, " #"); start >= 0 {
			value = strings.TrimSpace(value[:start])
		}
		if _, duplicate := values[key]; duplicate {
			return nil, fmt.Errorf("duplicate private .env assignment at line %d", index+1)
		}
		values[key] = value // Deliberately no shell execution or variable expansion.
	}
	return values, nil
}

func parseRealUsenetWireGuard(text, name string) (map[string]any, error) {
	sections := map[string]map[string]string{"Interface": {}, "Peer": {}}
	section := ""
	seen := map[string]bool{}
	allowed := map[string]map[string]bool{
		"Interface": {"PrivateKey": true, "Address": true, "DNS": true, "MTU": true},
		"Peer":      {"PublicKey": true, "PresharedKey": true, "Endpoint": true, "AllowedIPs": true, "PersistentKeepalive": true},
	}
	for index, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			section = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
			if sections[section] == nil || seen[section] {
				return nil, fmt.Errorf("requires one Interface and one Peer section (line %d)", index+1)
			}
			seen[section] = true
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || !allowed[section][key] {
			return nil, fmt.Errorf("unsupported WireGuard field at line %d; configuration hooks are not executed", index+1)
		}
		if _, duplicate := sections[section][key]; duplicate {
			return nil, fmt.Errorf("duplicate WireGuard field at line %d", index+1)
		}
		sections[section][key] = value
	}
	iface, peer := sections["Interface"], sections["Peer"]
	for _, field := range []struct{ key, value string }{{"PrivateKey", iface["PrivateKey"]}, {"PublicKey", peer["PublicKey"]}, {"PresharedKey", peer["PresharedKey"]}} {
		if field.key == "PresharedKey" && field.value == "" {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(field.value)
		if err != nil || len(decoded) != 32 {
			return nil, fmt.Errorf("invalid WireGuard %s", field.key)
		}
	}
	host, portText, err := net.SplitHostPort(peer["Endpoint"])
	port, portErr := strconv.Atoi(portText)
	if err != nil || portErr != nil || host == "" || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid WireGuard Endpoint")
	}
	addresses, dns := splitRealUsenetList(iface["Address"]), splitRealUsenetList(iface["DNS"])
	if len(addresses) == 0 || len(dns) == 0 {
		return nil, fmt.Errorf("WireGuard Address and DNS are required")
	}
	for _, address := range addresses {
		if _, _, err := net.ParseCIDR(address); err != nil {
			return nil, fmt.Errorf("invalid WireGuard Address")
		}
	}
	for _, address := range dns {
		if net.ParseIP(address) == nil {
			return nil, fmt.Errorf("WireGuard DNS must contain IP addresses")
		}
	}
	fullIPv4 := false
	for _, prefix := range splitRealUsenetList(peer["AllowedIPs"]) {
		if _, _, err := net.ParseCIDR(prefix); err != nil {
			return nil, fmt.Errorf("invalid WireGuard AllowedIPs")
		}
		fullIPv4 = fullIPv4 || prefix == "0.0.0.0/0"
	}
	if !fullIPv4 {
		return nil, fmt.Errorf("real Usenet requires a full IPv4 WireGuard route")
	}
	profile := map[string]any{"name": name, "kind": "WIRE_GUARD", "enabled": true, "host": host, "port": port,
		"privateKey": iface["PrivateKey"], "peerPublicKey": peer["PublicKey"], "tunnelAddresses": addresses, "dnsServers": dns}
	if peer["PresharedKey"] != "" {
		profile["presharedKey"] = peer["PresharedKey"]
	}
	for _, field := range []struct {
		input, value     string
		minimum, maximum int
	}{
		{"mtu", iface["MTU"], 68, 65535}, {"keepaliveSeconds", peer["PersistentKeepalive"], 0, 65535},
	} {
		if field.value != "" {
			number, err := strconv.Atoi(field.value)
			if err != nil || number < field.minimum || number > field.maximum {
				return nil, fmt.Errorf("invalid WireGuard %s", field.input)
			}
			profile[field.input] = number
		}
	}
	return profile, nil
}

func splitRealUsenetList(text string) []string {
	var values []string
	for _, value := range strings.Split(text, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}
