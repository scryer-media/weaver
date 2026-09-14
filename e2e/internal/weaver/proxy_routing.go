package weaver

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

func proxyRoutingReleaseFlow() weaverReleaseFlowSpec {
	spec := behaviorReleaseFlow("proxy-routing", 10*time.Minute)
	spec.Services = append(spec.Services, "proxy-fixture")
	spec.Artifacts = append(spec.Artifacts, "proxy-routing-evidence.json")
	return spec
}

// The app's resolver points directly at the canary, bypassing Docker's DNS
// cache. Only this isolated flow gets this mount. Literal proxy endpoints
// keep bootstrap lookups out of the destination-DNS assertions.
func writeProxyRoutingNetwork(phase *weaverReleasePhase) error {
	ip, subnet, err := net.ParseCIDR(phase.NetworkSubnet)
	if err != nil || ip.To4() == nil {
		return fmt.Errorf("invalid proxy fixture subnet %q", phase.NetworkSubnet)
	}
	ip = ip.To4()
	ip[3] = 250
	if !subnet.Contains(ip) {
		return fmt.Errorf("proxy fixture address outside subnet %q", phase.NetworkSubnet)
	}
	resolverPath := filepath.Join(phase.RootDir, "proxy-resolv.conf")
	if err := os.WriteFile(resolverPath, []byte("nameserver "+ip.String()+"\noptions timeout:1 attempts:1\n"), 0o644); err != nil {
		return err
	}
	// JSON strings are also YAML strings, including paths containing spaces.
	mount, _ := json.Marshal(resolverPath + ":/etc/resolv.conf:ro")
	addition := fmt.Sprintf(`
services:
  proxy-fixture:
    networks:
      default:
        ipv4_address: %s
    environment:
      PROXY_FIXTURE_IP: %s
  weaver:
    volumes:
      - %s
    depends_on:
      proxy-fixture:
        condition: service_healthy
`, ip, ip, mount)
	f, err := os.OpenFile(phase.ComposeOverride, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := f.WriteString(addition)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
