package weaver

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// --- chaos ---

// errNntpConnectionsSaturated is the provider turning a session away because
// its chaos connection cap is already spoken for.
//
// Worth naming because the cap counts the refused connection itself, so the
// harness meets it whenever every slot is held — including by sockets a
// stopped Weaver has not handed back yet. Callers that can afford to wait for
// a slot need to tell that apart from a provider that is actually broken.
var errNntpConnectionsSaturated = errors.New("NNTP provider connection cap saturated")

func sendNntpCommand(cmd string) string {
	return sendNntpCommandTo(nntpHost(), nntpPort(), cmd)
}

func sendNntpCommandTo(host, port, cmd string) (resp string) {
	resp, _ = sendNntpCommandToWithRetry(host, port, cmd, 1)
	return resp
}

func sendNntpCommandToWithRetry(host, port, cmd string, attempts int) (string, error) {
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		resp, err := sendNntpCommandToOnce(host, port, cmd)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
	return "", lastErr
}

func sendNntpCommandToOnce(host, port, cmd string) (string, error) {
	return sendNntpCommandToOnceWithAuth(host, port, cmd, true)
}

type nntpCommandSession struct {
	addr   string
	conn   net.Conn
	reader *bufio.Reader
}

func openNntpCommandSession(host, port string, authenticate bool) (*nntpCommandSession, error) {
	addr := host + ":" + port
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", addr, err)
	}

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	greeting, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read greeting from %s: %w", addr, err)
	}
	if strings.HasPrefix(greeting, "502") {
		conn.Close()
		return nil, fmt.Errorf("%w: %s answered %s", errNntpConnectionsSaturated, addr, strings.TrimSpace(greeting))
	}
	if !strings.HasPrefix(greeting, "200") && !strings.HasPrefix(greeting, "201") {
		conn.Close()
		return nil, fmt.Errorf("unexpected greeting from %s: %s", addr, strings.TrimSpace(greeting))
	}
	if authenticate {
		if err := authenticateNNTPConnection(conn, reader, addr); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return &nntpCommandSession{addr: addr, conn: conn, reader: reader}, nil
}

func (session *nntpCommandSession) send(cmd string) (string, error) {
	session.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := session.conn.Write([]byte(cmd + "\r\n")); err != nil {
		return "", fmt.Errorf("write command %q to %s: %w", cmd, session.addr, err)
	}

	session.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := session.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read response for %q from %s: %w", cmd, session.addr, err)
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return "", fmt.Errorf("empty response for %q from %s", cmd, session.addr)
	}
	return line, nil
}

func (session *nntpCommandSession) close() {
	_ = session.conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = session.conn.Write([]byte("QUIT\r\n"))
	_ = session.conn.Close()
}

func sendNntpCommandToOnceWithAuth(host, port, cmd string, authenticate bool) (string, error) {
	session, err := openNntpCommandSession(host, port, authenticate)
	if err != nil {
		return "", err
	}
	defer session.close()
	return session.send(cmd)
}

func resetNntpServer(host, port, label string) error {
	resp, err := sendNntpCommandToWithRetry(host, port, "CHAOS off", 10)
	if err != nil {
		return fmt.Errorf("reset %s chaos: %w", label, err)
	}
	if !strings.HasPrefix(resp, "290") {
		return fmt.Errorf("reset %s chaos: unexpected response %q", label, resp)
	}
	log.Printf("%s chaos reset: %s", label, resp)
	if _, err := sendNntpCommandToWithRetry(host, port, "RELOAD", 3); err != nil {
		log.Printf("warning: %s reload failed: %v", label, err)
	}
	return nil
}

func backupNntpRunning() bool {
	return dockerContainerRunning("nntp2")
}

func ensureNntpChaosOff() error {
	if err := resetNntpServer(nntpHost(), nntpPort(), "primary NNTP"); err != nil {
		return err
	}
	if backupNntpRunning() {
		if err := resetNntpServer(nntpHost(), backupNntpPort(), "backup NNTP"); err != nil {
			return err
		}
	}
	return nil
}

type containerEncryptionKeyState struct {
	Fingerprint string
	Mode        string
}

var encryptionKeyFingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

const e2eContainerEncryptionKeyPath = "/data/encryption.key"

func cmdContainerRestartTest() {
	if err := os.Setenv("E2E_WEAVER_ENCRYPTION_KEY", ""); err != nil {
		log.Fatalf("clear fixed Weaver encryption key for container restart test: %v", err)
	}

	emitProgressEvent(progressEvent{Kind: "phase_total", Total: 2, Detail: "Docker boot and restart"})
	if err := dockerComposeUp("nntp", "nntp2", "weaver"); err != nil {
		log.Fatalf("start Docker Weaver restart stack: %v", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		log.Fatalf("refresh runtime ports after starting Docker Weaver: %v", err)
	}

	weaverURL := strings.TrimRight(defaultWeaverURL(), "/") + "/"
	waitForHTTP(weaverURL, 2*time.Minute)
	containerID, err := dockerComposeServiceContainerID("weaver")
	if err != nil {
		log.Fatalf("resolve fresh Weaver container: %v", err)
	}
	before, err := inspectContainerEncryptionKeyState(containerID)
	if err != nil {
		log.Fatalf("inspect fresh Weaver encryption key: %v", err)
	}
	if before.Mode != "600" {
		log.Fatalf("fresh Weaver encryption key mode = %s, want 600", before.Mode)
	}
	freshLogs, err := dockerContainerLogs(containerID)
	if err != nil {
		log.Fatalf("read fresh Weaver container logs: %v", err)
	}
	if !strings.Contains(freshLogs, "persisted encryption master key in key file") {
		log.Fatal("fresh Weaver container did not report persisting its encryption key")
	}
	emitProgressEvent(progressEvent{Kind: "phase_progress", Current: 1, Total: 2, Status: "pass", Detail: "fresh container key persisted"})

	if err := dockerComposeRestart("weaver"); err != nil {
		log.Fatalf("restart Docker Weaver service: %v", err)
	}
	waitForHTTP(weaverURL, 2*time.Minute)

	restartedContainerID, err := dockerComposeServiceContainerID("weaver")
	if err != nil {
		log.Fatalf("resolve restarted Weaver container: %v", err)
	}
	if restartedContainerID != containerID {
		log.Fatalf("Docker restart replaced Weaver container: before=%s after=%s", containerID, restartedContainerID)
	}
	after, err := inspectContainerEncryptionKeyState(restartedContainerID)
	if err != nil {
		log.Fatalf("inspect restarted Weaver encryption key: %v", err)
	}
	if after.Mode != "600" {
		log.Fatalf("restarted Weaver encryption key mode = %s, want 600", after.Mode)
	}
	if after.Fingerprint != before.Fingerprint {
		log.Fatalf("Docker restart changed Weaver encryption key fingerprint: before=%s after=%s", before.Fingerprint, after.Fingerprint)
	}
	restartedLogs, err := dockerContainerLogs(restartedContainerID)
	if err != nil {
		log.Fatalf("read restarted Weaver container logs: %v", err)
	}
	if !strings.Contains(restartedLogs, "using encryption master key from key file") {
		log.Fatal("restarted Weaver container did not report reusing its persisted encryption key")
	}

	emitProgressEvent(progressEvent{Kind: "phase_progress", Current: 2, Total: 2, Status: "pass", Detail: "restarted container reused key"})
	log.Printf("Docker Weaver restart preserved %s (%s, mode %s)", e2eContainerEncryptionKeyPath, after.Fingerprint, after.Mode)
}

func inspectContainerEncryptionKeyState(containerID string) (containerEncryptionKeyState, error) {
	fingerprintOutput, err := dockerExecOutput(containerID, "sha256sum", e2eContainerEncryptionKeyPath)
	if err != nil {
		return containerEncryptionKeyState{}, err
	}
	modeOutput, err := dockerExecOutput(containerID, "stat", "-c", "%a", e2eContainerEncryptionKeyPath)
	if err != nil {
		return containerEncryptionKeyState{}, err
	}
	return parseContainerEncryptionKeyState(fingerprintOutput, modeOutput)
}

func dockerContainerLogs(containerID string) (string, error) {
	cmd := exec.Command("docker", "logs", containerID)
	cmd.Dir = e2eDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker logs %s: %w: %s", containerID, err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func dockerExecOutput(containerID string, args ...string) (string, error) {
	dockerArgs := append([]string{"exec", containerID}, args...)
	cmd := exec.Command("docker", dockerArgs...)
	cmd.Dir = e2eDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker exec %s %s: %w: %s", containerID, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func parseContainerEncryptionKeyState(fingerprintOutput, modeOutput string) (containerEncryptionKeyState, error) {
	fields := strings.Fields(fingerprintOutput)
	if len(fields) == 0 {
		return containerEncryptionKeyState{}, fmt.Errorf("empty encryption key fingerprint output")
	}
	fingerprint := strings.ToLower(fields[0])
	if !encryptionKeyFingerprintPattern.MatchString(fingerprint) {
		return containerEncryptionKeyState{}, fmt.Errorf("invalid encryption key fingerprint %q", fields[0])
	}
	mode := strings.TrimSpace(modeOutput)
	if mode == "" || strings.ContainsAny(mode, "\r\n \t") {
		return containerEncryptionKeyState{}, fmt.Errorf("invalid encryption key mode %q", modeOutput)
	}
	return containerEncryptionKeyState{Fingerprint: fingerprint, Mode: mode}, nil
}

func dockerComposeUp(services ...string) error {
	if requiresWeaverService(services) {
		if err := ensureLocalWeaverImage(); err != nil {
			return err
		}
	}
	const maxPortBindRetries = 2
	for attempt := 0; ; attempt++ {
		log.Printf("starting docker services: %s", strings.Join(services, ", "))
		args := append(dockerComposeArgs("up", "-d", "--quiet-pull"), services...)
		cmd := exec.Command("docker", args...)
		cmd.Dir = e2eDir()
		err := runExternalCommand(cmd, "docker compose up")
		if err == nil || !isDockerHostPortBindCollision(err) || attempt == maxPortBindRetries {
			return err
		}
		if retryErr := reallocateRuntimePortsForDockerRetry(); retryErr != nil {
			return fmt.Errorf("%w; reallocate runtime ports for retry: %v", err, retryErr)
		}
		log.Printf("docker host-port collision; retrying compose with fresh runtime ports (attempt %d/%d)", attempt+1, maxPortBindRetries)
	}
}

func requiresWeaverService(services []string) bool {
	for _, service := range services {
		if service == "weaver" {
			return true
		}
	}
	return false
}

func dockerComposeRestart(services ...string) error {
	log.Printf("restarting docker services: %s", strings.Join(services, ", "))
	args := append(dockerComposeArgs("restart"), services...)
	cmd := exec.Command("docker", args...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "docker compose restart")
}

func dockerImageExists(image string) bool {
	if strings.TrimSpace(image) == "" {
		return false
	}
	cmd := exec.Command("docker", "image", "inspect", image)
	cmd.Dir = e2eDir()
	return cmd.Run() == nil
}

func dockerComposeDown() error {
	cmd := exec.Command("docker", dockerComposeArgs("down", "-v", "--remove-orphans")...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "docker compose down")
}

func ensureBackupNntpReady() error {
	if !backupNntpRunning() {
		log.Println("starting backup NNTP container...")
		if err := dockerComposeUp("nntp2"); err != nil {
			return fmt.Errorf("start backup NNTP: %w", err)
		}
		if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
			return fmt.Errorf("refresh runtime ports after starting backup NNTP: %w", err)
		}
	}
	waitForTCP("localhost:"+backupNntpPort(), 15*time.Second)
	if err := resetNntpServer(nntpHost(), backupNntpPort(), "backup NNTP"); err != nil {
		return err
	}
	if err := syncArticlesToBackup(); err != nil {
		return fmt.Errorf("sync backup NNTP articles: %w", err)
	}
	return nil
}

// --- toxiproxy helpers ---

func toxiproxyURL() string {
	if value := strings.TrimSpace(os.Getenv("TOXIPROXY_URL")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return fmt.Sprintf("http://localhost:%s", os.Getenv("E2E_TOXIPROXY_API_PORT"))
}

// addToxic adds a toxic to a toxiproxy proxy.
// proxyName: "nntp1" or "nntp2"
// toxicName: unique name for this toxic
// toxicType: "latency", "bandwidth", "slow_close", "timeout", "reset_peer", "slicer", "limit_data"
// stream: "downstream" or "upstream"
// attrs: toxic-specific attributes
func addToxic(proxyName, toxicName, toxicType, stream string, attrs map[string]interface{}) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"name":       toxicName,
		"type":       toxicType,
		"stream":     stream,
		"toxicity":   1.0,
		"attributes": attrs,
	})
	url := fmt.Sprintf("%s/proxies/%s/toxics", toxiproxyURL(), proxyName)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("add toxic %s: %w", toxicName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add toxic %s: %s %s", toxicName, resp.Status, string(body))
	}
	return nil
}

// removeToxic removes a named toxic from a proxy.
func removeToxic(proxyName, toxicName string) {
	url := fmt.Sprintf("%s/proxies/%s/toxics/%s", toxiproxyURL(), proxyName, toxicName)
	req, _ := http.NewRequest("DELETE", url, nil)
	http.DefaultClient.Do(req)
}

// removeAllToxics removes all toxics from both proxies.
func removeAllToxics() {
	for _, proxy := range []string{"nntp1", "nntp2"} {
		url := fmt.Sprintf("%s/proxies/%s/toxics", toxiproxyURL(), proxy)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		var toxics []struct{ Name string }
		json.NewDecoder(resp.Body).Decode(&toxics)
		resp.Body.Close()
		for _, t := range toxics {
			removeToxic(proxy, t.Name)
		}
	}
}

// filterChaosScenarios returns a representative subset of scenarios for chaos
// testing. The full suite takes too long with retry delays.
func filterChaosScenarios(all []*Scenario) []*Scenario {
	return filterScenariosBySlug(all, chaosFixtureSlugs)
}

func filterTcpChaosScenarios(all []*Scenario) []*Scenario {
	return filterScenariosBySlug(all, tcpChaosFixtureSlugs)
}

func filterScenariosBySlug(all []*Scenario, slugs []string) []*Scenario {
	bySlug := make(map[string]*Scenario, len(all))
	for _, s := range all {
		bySlug[s.Slug] = s
	}
	var out []*Scenario
	for _, slug := range slugs {
		if s, ok := bySlug[slug]; ok {
			out = append(out, s)
		}
	}
	return out
}

func runChaosStatProbeScenario(
	weaverURL string,
	scenario *Scenario,
	roundName string,
	statChaos string,
) (string, error) {
	if scenario == nil {
		return "", fmt.Errorf("missing STAT probe scenario")
	}
	statChaos = strings.TrimSpace(statChaos)
	if statChaos == "" {
		return "", fmt.Errorf("missing STAT-only chaos config for probe")
	}
	log.Printf("  restarting managed Weaver before isolated STAT probe for %s", roundName)
	if err := restartStandardManagedWeaverPreservingState(); err != nil {
		return "", fmt.Errorf("restart managed weaver before %s STAT probe: %w", roundName, err)
	}

	if err := setNntpChaosOnServer(nntpHost(), nntpPort(), statChaos); err != nil {
		return "", fmt.Errorf("enable primary STAT chaos %q: %w", statChaos, err)
	}
	defer func() {
		if err := setNntpChaosOnServer(nntpHost(), nntpPort(), "off"); err != nil {
			log.Printf("  WARNING: disable primary STAT chaos after probe: %v", err)
		}
	}()

	if backupNntpRunning() {
		if err := setNntpChaosOnServer(nntpHost(), backupNntpPort(), statChaos); err != nil {
			return "", fmt.Errorf("enable backup STAT chaos %q: %w", statChaos, err)
		}
		defer func() {
			if err := setNntpChaosOnServer(nntpHost(), backupNntpPort(), "off"); err != nil {
				log.Printf("  WARNING: disable backup STAT chaos after probe: %v", err)
			}
		}()
	}

	if err := resetNntpMetrics(); err != nil {
		return "", fmt.Errorf("reset NNTP metrics for isolated STAT probe: %w", err)
	}
	log.Printf("  isolated STAT probe for %s with %q after state-preserving restart", roundName, statChaos)

	jobID, err := submitOneNZB(weaverURL, scenario)
	if err != nil {
		return "", fmt.Errorf("submit %s: %w", scenario.Slug, err)
	}
	log.Printf("  probing STAT path with %s — submitted job=%d", scenario.Slug, jobID)

	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
		if err == nil && snapshot.Found && facadeTerminalStatus(snapshot.Status) {
			actual, _ := applyTerminalStateCheck(localWeaverDBPath(), jobID, scenario.Slug, snapshot.Status)
			if actual == "" {
				actual = snapshot.Status
			}
			return actual, nil
		}
		mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("%s STAT probe", roundName))
	}

	reconciled := reconcileTerminalSnapshots(
		weaverURL,
		[]int{jobID},
		20*time.Second,
		fmt.Sprintf("%s STAT probe final reconciliation", roundName),
	)
	if snapshot, ok := reconciled[jobID]; ok {
		actual, _ := applyTerminalStateCheck(localWeaverDBPath(), jobID, scenario.Slug, snapshot.Status)
		if actual == "" {
			actual = snapshot.Status
		}
		return actual, nil
	}

	if err := cancelJobGraphQL(weaverURL, jobID); err != nil {
		log.Printf("  WARNING: cancel timed out STAT probe job %s (%d): %v", scenario.Slug, jobID, err)
	}
	return "TIMEOUT", fmt.Errorf("timeout waiting for STAT probe scenario %s", scenario.Slug)
}

func statOnlyChaosConfig(config string) string {
	var statParts []string
	for _, part := range strings.Split(config, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "stat_bad_code=") || strings.HasPrefix(part, "stat_short=") {
			statParts = append(statParts, part)
		}
	}
	return strings.Join(statParts, ",")
}

func cmdChaos(config string) {
	resp := sendNntpCommand("CHAOS " + config)
	log.Println(resp)
}

type submitNZBOptions struct {
	force bool
	// newsgroup, when set, replaces every <group> element of the fixture NZB
	// before submission. The seeded fixtures all post to one newsgroup, so a
	// round that wants each job leased for a different group rewrites it here.
	newsgroup string
}

var nzbGroupElementPattern = regexp.MustCompile(`<group>[^<]*</group>`)

// overrideNzbNewsgroup points every <group> element of an NZB at newsgroup.
func overrideNzbNewsgroup(nzb []byte, newsgroup string) ([]byte, error) {
	newsgroup = strings.TrimSpace(newsgroup)
	if newsgroup == "" {
		return nzb, nil
	}
	if !nzbGroupElementPattern.Match(nzb) {
		return nil, fmt.Errorf("NZB has no <group> element to override")
	}
	return nzbGroupElementPattern.ReplaceAllLiteral(nzb, []byte("<group>"+newsgroup+"</group>")), nil
}

func submitOneNZB(weaverURL string, scenario *Scenario) (int, error) {
	return submitOneNZBWithOptions(weaverURL, scenario, submitNZBOptions{})
}

func submitOneNZBWithOptions(weaverURL string, scenario *Scenario, options submitNZBOptions) (int, error) {
	slug := scenario.Slug
	nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
	nzbData, err := os.ReadFile(nzbPath)
	if err != nil {
		return 0, fmt.Errorf("read NZB: %w", err)
	}
	if options.newsgroup != "" {
		nzbData, err = overrideNzbNewsgroup(nzbData, options.newsgroup)
		if err != nil {
			return 0, fmt.Errorf("override NZB newsgroup for %s: %w", slug, err)
		}
	}

	nzbB64 := base64.StdEncoding.EncodeToString(nzbData)
	query := `mutation($input: SubmitNzbInput!) {
		submitNzb(input: $input) {
			accepted
			item {
				id
			}
		}
	}`
	input := map[string]interface{}{
		"nzbBase64": nzbB64,
		"filename":  scenario.Title + ".nzb",
		"category":  scenario.Category,
	}
	if scenario.Password != "" {
		input["password"] = scenario.Password
	}
	if options.force {
		input["force"] = true
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"query":     query,
		"variables": map[string]interface{}{"input": input},
	})

	resp, err := postGraphQL(weaverURL, payload)
	if err != nil {
		return 0, fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	var gqlResp struct {
		Data struct {
			SubmitNzb struct {
				Accepted bool `json:"accepted"`
				Item     struct {
					ID int `json:"id"`
				} `json:"item"`
			} `json:"submitNzb"`
		} `json:"data"`
		Errors []struct{ Message string } `json:"errors"`
	}
	json.Unmarshal(respBody, &gqlResp)
	if len(gqlResp.Errors) > 0 {
		return 0, fmt.Errorf("gql: %s", gqlResp.Errors[0].Message)
	}
	if !gqlResp.Data.SubmitNzb.Accepted {
		return 0, fmt.Errorf("submitNzb rejected for %s", slug)
	}
	if gqlResp.Data.SubmitNzb.Item.ID <= 0 {
		return 0, fmt.Errorf("submitNzb returned invalid job id %d for %s", gqlResp.Data.SubmitNzb.Item.ID, slug)
	}
	return gqlResp.Data.SubmitNzb.Item.ID, nil
}

func reconcileTerminalSnapshots(
	weaverURL string,
	jobIDs []int,
	timeout time.Duration,
	detail string,
) map[int]facadeItemSnapshot {
	results := make(map[int]facadeItemSnapshot, len(jobIDs))
	pending := make(map[int]struct{}, len(jobIDs))
	for _, jobID := range jobIDs {
		if jobID > 0 {
			pending[jobID] = struct{}{}
		}
	}
	if len(pending) == 0 {
		return results
	}

	deadline := time.Now().Add(timeout)
	for {
		for jobID := range pending {
			snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
			if err != nil || !snapshot.Found {
				continue
			}
			if facadeTerminalStatus(snapshot.Status) {
				results[jobID] = snapshot
				delete(pending, jobID)
			}
		}
		if len(pending) == 0 || time.Now().After(deadline) {
			return results
		}
		mustSleepWithSuspendDetection(2*time.Second, detail)
	}
}
