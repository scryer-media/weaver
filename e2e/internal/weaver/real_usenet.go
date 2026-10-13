package weaver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/composeutil"
	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

type realUsenetCase struct {
	Name          string
	Kind          string
	Depth, Member int
}

func realUsenetCases() []realUsenetCase {
	cases := []realUsenetCase{
		{Name: "direct-tls", Kind: "direct"},
		{Name: "docker-connect-tls", Kind: "connect"},
		{Name: "docker-socks5-tls", Kind: "socks"},
		{Name: "wireguard-single", Kind: "chain", Depth: 1},
		{Name: "wireguard-chain-2", Kind: "chain", Depth: 2},
		{Name: "wireguard-chain-3", Kind: "chain", Depth: 3},
		{Name: "wireguard-pool-5", Kind: "pool", Member: -1},
	}
	for member := range 5 {
		cases = append(cases, realUsenetCase{Name: fmt.Sprintf("wireguard-pool-member-%02d", member+1), Kind: "pool", Member: member})
	}
	return cases
}

func selectRealUsenetCases(selection string) ([]realUsenetCase, error) {
	all := realUsenetCases()
	if selection == "" {
		return all, nil
	}
	wanted := map[string]bool{}
	for _, name := range strings.Split(selection, ",") {
		name = strings.TrimSpace(name)
		if name == "" || wanted[name] {
			return nil, fmt.Errorf("cases must be distinct case names separated by commas")
		}
		wanted[name] = true
	}
	var selected []realUsenetCase
	for _, testCase := range all {
		if wanted[testCase.Name] {
			selected = append(selected, testCase)
			delete(wanted, testCase.Name)
		}
	}
	if len(wanted) > 0 {
		return nil, fmt.Errorf("unknown real-usenet case; use the names in real-usenet results")
	}
	return selected, nil
}

type realUsenetFile struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type realUsenetResult struct {
	Datastore       string           `json:"datastore"`
	Case            string           `json:"case"`
	ReferenceCase   string           `json:"reference_case,omitempty"`
	Status          string           `json:"status"`
	JobID           int              `json:"job_id,omitempty"`
	SelectedProxyID int              `json:"selected_proxy_id,omitempty"`
	DownloadedBytes uint64           `json:"downloaded_bytes,omitempty"`
	DurationMS      int64            `json:"duration_ms"`
	Perf            *realUsenetPerf  `json:"perf,omitempty"`
	Files           []realUsenetFile `json:"files,omitempty"`
	Error           string           `json:"error,omitempty"`
}

func cmdRealUsenet(args []string) error {
	flags := flag.NewFlagSet("real-usenet", flag.ContinueOnError)
	check := flags.Bool("check", false, "validate private inputs without using Docker or providers")
	datastore := flags.String("datastore", "both", "sqlite, postgres, or both")
	budget := flags.Duration("timeout", time.Hour, "runner budget for the complete selected matrix")
	selection := flags.String("cases", "", "comma-separated case names; omitted cases never run")
	chainProfiles := flags.String("chain-profiles", "", "three known private WG filenames, outermost first, separated by commas")
	perf := flags.Bool("perf", false, "record private progress and container CPU/memory measurements")
	connections := flags.Int("connections", 0, "NNTP connections for this run (1-64); zero keeps the private configuration")
	workers := flags.Int("worker-threads", 0, "Tokio worker threads for the isolated test process; zero keeps the product default")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *budget <= 0 {
		return fmt.Errorf("usage: full real-usenet [--check] [--datastore sqlite|postgres|both] [--cases names] [--timeout 1h]")
	}
	cases, err := selectRealUsenetCases(*selection)
	if err != nil {
		return err
	}
	stores := releaseDatastoreMatrix()
	if *datastore != "both" {
		store, err := parseWeaverDatastore(*datastore)
		if err != nil {
			return fmt.Errorf("datastore must be sqlite, postgres, or both")
		}
		stores = []weaverDatastore{store}
	}
	root := filepath.Join(e2eDir(), "private", "real-network")
	config, err := loadRealUsenetConfig(root)
	if err != nil {
		return err
	}
	if err := overrideRealUsenetChain(config, *chainProfiles); err != nil {
		return err
	}
	config.Perf = *perf
	if *workers < 0 || *workers > 256 {
		return fmt.Errorf("worker-threads must be between 1 and 256, or zero to retain the product default")
	}
	config.WorkerThreads = *workers
	if *connections < 0 || *connections > 64 {
		return fmt.Errorf("connections must be between 1 and 64, or zero to retain configuration")
	}
	if *connections != 0 {
		config.Connections = *connections
	}
	if *check {
		fmt.Printf("real-usenet inputs valid: pinned NZB, eight WG profiles, %d connections; %d cases per datastore\n", config.Connections, len(cases))
		return nil
	}
	// Preflight runs before engine activation: absent credentials never start a
	// fixture stack, and --check works without a container engine.
	cleanup, err := activateContainerEngine()
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureLocalWeaverImage(); err != nil {
		return err
	}
	if err := ensureLocalWeaverPlaywrightImage(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *budget)
	defer cancel()
	runRoot, err := os.MkdirTemp(root, "run-")
	if err != nil {
		return fmt.Errorf("cannot create private run directory")
	}
	redact := realUsenetRedactor(config)
	var results []realUsenetResult
	var failures []error
	for _, store := range stores {
		batch, err := runRealUsenetDatastore(ctx, config, runRoot, store, cases, redact)
		results = append(results, batch...)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %s", store, redact(err.Error())))
		}
		if ctx.Err() != nil {
			break
		}
	}
	report := map[string]any{"nzb_sha256": realUsenetNZBSHA256, "results": results, "chain_profiles": config.ChainProfiles, "connections": config.Connections, "worker_threads_override": config.WorkerThreads}
	report["status"] = "passed"
	if len(failures) > 0 {
		report["status"] = "failed"
		messages := make([]string, len(failures))
		for index, failure := range failures {
			messages[index] = redact(failure.Error())
		}
		report["errors"] = messages
	}
	if err := writeRealUsenetJSON(filepath.Join(runRoot, "results.json"), report); err != nil {
		failures = append(failures, err)
	}
	log.Printf("real-usenet report: private/real-network/%s/results.json", filepath.Base(runRoot))
	return errors.Join(failures...)
}

func realUsenetRedactor(config *realUsenetConfig) func(string) string {
	secrets := []string{config.Username, config.Password, config.Host}
	for _, profile := range config.Profiles {
		for _, field := range []string{"privateKey", "presharedKey", "host"} {
			if value, ok := profile[field].(string); ok {
				secrets = append(secrets, value)
			}
		}
	}
	slices.SortFunc(secrets, func(a, b string) int { return len(b) - len(a) })
	var replacements []string
	for _, value := range secrets {
		if value != "" {
			replacements = append(replacements, value, "[REDACTED]")
		}
	}
	replacer := strings.NewReplacer(replacements...)
	return replacer.Replace
}

func writeRealUsenetJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot encode private run state")
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("cannot write private run state")
	}
	return nil
}

type realUsenetAPI struct {
	ctx    context.Context
	url    string
	client *http.Client
	redact func(string) string
	perf   *realUsenetPerf
}

func (api realUsenetAPI) query(query string, variables any, result any) error {
	data, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return fmt.Errorf("cannot encode local API request")
	}
	req, err := http.NewRequestWithContext(api.ctx, http.MethodPost, api.url+"/graphql", strings.NewReader(string(data)))
	if err != nil {
		return fmt.Errorf("cannot construct local API request")
	}
	req.Header.Set("Content-Type", "application/json")
	operation := "unknown"
	if _, body, ok := strings.Cut(query, "{"); ok {
		operation = strings.TrimSpace(body)
		if end := strings.IndexAny(operation, "({} \r\n\t"); end >= 0 {
			operation = operation[:end]
		}
	}
	response, err := api.client.Do(req)
	if err != nil {
		return fmt.Errorf("local API %s request: %s", api.redact(operation), api.redact(err.Error()))
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("local API HTTP %d", response.StatusCode)
	}
	var envelope struct {
		Data   json.RawMessage
		Errors []struct{ Message string }
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&envelope); err != nil {
		return fmt.Errorf("invalid local API response")
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("local API: %s", api.redact(envelope.Errors[0].Message))
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(envelope.Data, result); err != nil {
		return fmt.Errorf("invalid local API result")
	}
	return nil
}

func (api realUsenetAPI) ready() error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(api.ctx, http.MethodGet, api.url+"/", nil)
		if err != nil {
			return err
		}
		response, err := api.client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				var result struct{ Version string }
				if err := api.query("query { version }", nil, &result); err == nil {
					return nil
				}
			}
		}
		select {
		case <-api.ctx.Done():
			return api.ctx.Err()
		case <-ticker.C:
		}
	}
}

func realUsenetComposeEngine(source *containerengine.Engine) *containerengine.Engine {
	// This stack defines all of its services. The ordinary harness overlay
	// introduces unrelated services and must not accompany its Compose file.
	return &containerengine.Engine{Kind: source.Kind, Binary: source.Binary, ComposeProvider: source.ComposeProvider}
}

func runRealUsenetDatastore(ctx context.Context, config *realUsenetConfig, runRoot string, store weaverDatastore, cases []realUsenetCase, redact func(string) string) (results []realUsenetResult, err error) {
	root := filepath.Join(runRoot, string(store))
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("cannot create private datastore directory")
	}
	ports, err := allocateRuntimePortStates(1)
	if err != nil {
		return nil, err
	}
	used, err := composeutil.ListNetworkSubnets(ctx, e2eDir())
	if err != nil {
		return nil, fmt.Errorf("cannot inspect test network allocations")
	}
	phases := []*weaverReleasePhase{{Project: filepath.Base(runRoot) + "-" + string(store)}}
	subnets, err := composeutil.SelectNonOverlappingSubnets(1, weaverReleaseNetworkCandidates(phases), used)
	if err != nil {
		return nil, err
	}
	ip, _, _ := net.ParseCIDR(subnets[0])
	ip = ip.To4()
	ip[3] = 250
	relayIP := ip.String()
	addresses, err := net.DefaultResolver.LookupIP(ctx, "ip4", config.Host)
	if err != nil || len(addresses) == 0 {
		return nil, fmt.Errorf("cannot resolve configured Usenet hostname")
	}
	var targetAddresses []string
	for _, address := range addresses {
		targetAddresses = append(targetAddresses, address.String())
	}
	var udpTargets []map[string]any
	for index, profile := range config.Profiles {
		if index == 1 || index == 2 {
			continue
		} // Inner peers resolve through their immediate carrier.
		udpTargets = append(udpTargets, map[string]any{"host": profile["host"], "port": profile["port"], "listenPort": 51830 + index})
	}
	if err := writeRealUsenetJSON(filepath.Join(root, "relay.json"), map[string]any{
		"ip": relayIP, "publicTargets": []map[string]any{{"host": config.Host, "port": config.Port, "addresses": targetAddresses}}, "wireguard": udpTargets,
		"api": map[string]any{"host": realUsenetRoutedIP(relayIP), "port": 9090, "listenPort": 9090},
	}); err != nil {
		return nil, err
	}
	compose, err := realUsenetCompose(root, subnets[0], relayIP, store, ports[0])
	if err != nil {
		return nil, err
	}
	if config.WorkerThreads > 0 {
		for _, name := range []string{"baseline", "routed"} {
			compose["services"].(map[string]any)[name].(map[string]any)["environment"].(map[string]string)["WEAVER_TOKIO_WORKER_THREADS"] = strconv.Itoa(config.WorkerThreads)
		}
	}
	composePath := filepath.Join(root, "compose.json")
	if err := writeRealUsenetJSON(composePath, compose); err != nil {
		return nil, err
	}
	project := "real-usenet-" + filepath.Base(runRoot) + "-" + string(store)
	engine := realUsenetComposeEngine(containerengine.Current())
	command := func(commandCtx context.Context, args ...string) error {
		arguments := append([]string{"compose", "-p", project, "-f", composePath}, args...)
		cmd := engine.CommandContext(commandCtx, arguments...)
		cmd.Dir = e2eDir()
		// Compose startup can mention paths; provider files are never mounted and
		// credentials never enter its arguments, environment, or output.
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("isolated compose command failed: %s: %s", redact(err.Error()), redact(string(output)))
		}
		return nil
	}
	defer func() {
		// Stop only this run's containers. Retain them and the private data for
		// diagnosis; never delete containers as part of a test invocation.
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if stopErr := command(stopCtx, "stop"); stopErr != nil {
			err = errors.Join(err, stopErr)
		}
	}()
	services := []string{"relay"}
	if store == weaverDatastorePostgres {
		services = append(services, "postgres")
	}
	if err := command(ctx, append([]string{"up", "--wait"}, services...)...); err != nil {
		return nil, err
	}
	api := realUsenetAPI{ctx: ctx, redact: redact}
	var reference []realUsenetFile
	var referenceCase string
	for _, testCase := range cases {
		service, port := "routed", ports[0].LocalWeaverPort
		caseRoot := root
		if testCase.Kind == "direct" {
			service, port = "baseline", ports[0].WeaverPort
		} else {
			caseRoot = filepath.Join(root, "cases", testCase.Name)
			service = "routed-" + testCase.Name
			isolated, prepareErr := prepareRealUsenetCase(compose["services"].(map[string]any)["routed"].(map[string]any), caseRoot)
			if prepareErr != nil {
				return results, errors.Join(err, prepareErr)
			}
			compose["services"].(map[string]any)[service] = isolated
			if writeErr := writeRealUsenetJSON(composePath, compose); writeErr != nil {
				return results, errors.Join(err, writeErr)
			}
		}
		if startErr := command(ctx, "up", "--wait", service); startErr != nil {
			return results, errors.Join(err, startErr)
		}
		api.url = "http://127.0.0.1:" + strconv.Itoa(port)
		api.client = &http.Client{Jar: weaverCookieJar(api.url)}
		if readyErr := api.ready(); readyErr != nil {
			return results, errors.Join(err, readyErr)
		}
		api.perf = nil
		if config.Perf {
			api.perf = &realUsenetPerf{readCounters: func() (map[string]uint64, error) {
				counters := map[string]uint64{}
				for _, target := range []string{service, "relay"} {
					args := []string{"compose", "-p", project, "-f", composePath, "exec", "-T", target, "sh", "-c", realUsenetCounterScript}
					cmd := engine.CommandContext(ctx, args...)
					cmd.Dir = e2eDir()
					output, readErr := cmd.Output()
					if readErr != nil {
						return nil, fmt.Errorf("cannot read isolated container performance counters")
					}
					values, parseErr := parseRealUsenetCounters(string(output))
					if parseErr != nil {
						return nil, parseErr
					}
					prefix := ""
					if target == "relay" {
						prefix = "relay_"
					}
					for key, value := range values {
						counters[prefix+key] = value
					}
				}
				return counters, nil
			}}
		}
		result, caseErr := runRealUsenetCase(api, config, caseRoot, store, testCase, relayIP, reference)
		if caseErr == nil && reference == nil {
			reference, referenceCase = result.Files, testCase.Name
		}
		result.ReferenceCase = referenceCase
		results = append(results, result)
		if caseErr != nil {
			err = errors.Join(err, fmt.Errorf("%s: %w", testCase.Name, caseErr))
		}
		if reportErr := writeRealUsenetJSON(filepath.Join(root, "results.json"), results); reportErr != nil {
			err = errors.Join(err, reportErr)
		}
		if ctx.Err() != nil {
			break
		}
		if stopErr := command(ctx, "stop", service); stopErr != nil {
			return results, errors.Join(err, stopErr)
		}
		if service == "baseline" {
			continue
		}
		if store == weaverDatastorePostgres {
			// Only this run's newly created fixture database is reset. No later
			// route can reuse an earlier job, article cache, or credentials.
			if resetErr := command(ctx, "exec", "-T", "postgres", "psql", "-U", "fixture", "-d", "fixture", "-v", "ON_ERROR_STOP=1", "-c", "DROP DATABASE routed WITH (FORCE)", "-c", "CREATE DATABASE routed"); resetErr != nil {
				return results, errors.Join(err, resetErr)
			}
		}
	}
	return results, err
}

const realUsenetBootstrapConfig = "data_dir = \"/data\"\nintermediate_dir = \"/data/intermediate\"\ncomplete_dir = \"/data/complete\"\ncleanup_after_extract = true\nmax_retries = 3\n"

func prepareRealUsenetCase(template map[string]any, root string) (map[string]any, error) {
	// A stopped Podman container can retain the inode of a renamed bind
	// directory. Each case therefore owns a new service and stable bind paths.
	service := maps.Clone(template)
	var volumes []string
	for _, dir := range []string{"config", "data"} {
		path := filepath.Join(root, "routed", dir)
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, fmt.Errorf("cannot prepare fresh case %s", dir)
		}
		volumes = append(volumes, path+":/"+dir)
	}
	if err := os.WriteFile(filepath.Join(root, "routed", "config", "weaver.toml"), []byte(realUsenetBootstrapConfig), 0o600); err != nil {
		return nil, fmt.Errorf("cannot write fresh case configuration")
	}
	service["volumes"] = volumes
	return service, nil
}

func realUsenetRoutedIP(relayIP string) string {
	ip := net.ParseIP(relayIP).To4()
	ip[3]--
	return ip.String()
}

func realUsenetCompose(root, subnet, relayIP string, store weaverDatastore, ports runtimePortState) (map[string]any, error) {
	services := map[string]any{}
	for _, name := range []string{"baseline", "routed"} {
		configDir, dataDir := filepath.Join(root, name, "config"), filepath.Join(root, name, "data")
		for _, dir := range []string{configDir, dataDir} {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("cannot prepare private Weaver state")
			}
		}
		if err := os.WriteFile(filepath.Join(configDir, "weaver.toml"), []byte(realUsenetBootstrapConfig), 0o600); err != nil {
			return nil, fmt.Errorf("cannot write isolated Weaver configuration")
		}
		var key [32]byte
		if _, err := rand.Read(key[:]); err != nil {
			return nil, err
		}
		environment := map[string]string{
			"PUID": strconv.Itoa(os.Getuid()), "PGID": strconv.Itoa(os.Getgid()),
			"WEAVER_ENCRYPTION_KEY":    base64.StdEncoding.EncodeToString(key[:]),
			"WEAVER_HTTP_BIND_ADDRESS": "0.0.0.0", "WEAVER_TRUSTED_CIDRS": "0.0.0.0/0,::/0",
			"RUST_LOG": "warn", "WEAVER_E2E_MODE": "0",
		}
		networks := map[string]any{"private": map[string]any{"ipv4_address": realUsenetRoutedIP(relayIP)}}
		if name == "baseline" {
			networks["private"] = map[string]any{}
			networks["outside"] = map[string]any{}
		}
		if store == weaverDatastorePostgres {
			environment["WEAVER_DATABASE_URL"] = "postgres://fixture:fixture@postgres:5432/" + name + "?sslmode=disable"
		}
		service := map[string]any{
			"image": os.Getenv("E2E_WEAVER_IMAGE"), "pull_policy": "never",
			"command":     []string{"--config", "/config/weaver.toml", "serve", "--port", "9090"},
			"environment": environment, "networks": networks,
			"volumes":     []string{configDir + ":/config", dataDir + ":/data"},
			"healthcheck": map[string]any{"test": []string{"CMD-SHELL", "wget -q --spider http://127.0.0.1:9090/"}, "interval": "1s", "timeout": "3s", "retries": 3600},
			"depends_on":  map[string]any{"relay": map[string]any{"condition": "service_healthy"}},
		}
		if name == "baseline" {
			service["ports"] = []string{fmt.Sprintf("127.0.0.1:%d:9090", ports.WeaverPort)}
		}
		if store == weaverDatastorePostgres {
			service["depends_on"].(map[string]any)["postgres"] = map[string]any{"condition": "service_healthy"}
		}
		services[name] = service
	}
	services["relay"] = map[string]any{
		"image": os.Getenv("E2E_WEAVER_PLAYWRIGHT_IMAGE"), "pull_policy": "never",
		"command":     []string{"node", "/work/support/real-network-relay.mjs"},
		"networks":    map[string]any{"outside": map[string]any{}, "private": map[string]any{"ipv4_address": relayIP}},
		"ports":       []string{fmt.Sprintf("127.0.0.1:%d:9090", ports.LocalWeaverPort)},
		"volumes":     []string{filepath.Join(e2eDir(), "playwright-weaver", "tests", "support") + ":/work/support:ro", filepath.Join(root, "relay.json") + ":/run/real-network/relay.json:ro"},
		"healthcheck": map[string]any{"test": []string{"CMD-SHELL", "test -f /tmp/real-network-ready"}, "interval": "1s", "timeout": "3s", "retries": 3600},
	}
	if store == weaverDatastorePostgres {
		initPath := filepath.Join(root, "init.sql")
		if err := os.WriteFile(initPath, []byte("CREATE DATABASE baseline;\nCREATE DATABASE routed;\n"), 0o644); err != nil {
			return nil, fmt.Errorf("cannot initialize isolated databases")
		}
		services["postgres"] = map[string]any{
			"image":       "postgres:18@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280",
			"environment": map[string]string{"POSTGRES_USER": "fixture", "POSTGRES_PASSWORD": "fixture", "POSTGRES_DB": "fixture"},
			"networks":    map[string]any{"private": map[string]any{}},
			"volumes":     []string{initPath + ":/docker-entrypoint-initdb.d/databases.sql:ro"},
			"healthcheck": map[string]any{"test": []string{"CMD-SHELL", "PGPASSWORD=fixture psql -U fixture -d baseline -c 'SELECT 1' && PGPASSWORD=fixture psql -U fixture -d routed -c 'SELECT 1'"}, "interval": "1s", "timeout": "3s", "retries": 3600},
		}
	}
	return map[string]any{"services": services, "networks": map[string]any{
		"outside": map[string]any{}, "private": map[string]any{"internal": true, "ipam": map[string]any{"config": []map[string]string{{"subnet": subnet}}}},
	}}, nil
}

func runRealUsenetCase(api realUsenetAPI, config *realUsenetConfig, root string, store weaverDatastore, testCase realUsenetCase, relayIP string, reference []realUsenetFile) (result realUsenetResult, err error) {
	started := time.Now()
	result = realUsenetResult{Datastore: string(store), Case: testCase.Name, Status: "failed"}
	log.Printf("real-usenet %s/%s: downloading pinned SAB test NZB", store, testCase.Name)
	defer func() {
		result.DurationMS = time.Since(started).Milliseconds()
		if err != nil {
			result.Error = api.redact(err.Error())
		} else {
			result.Status = "passed"
		}
		log.Printf("real-usenet %s/%s: %s", store, testCase.Name, result.Status)
	}()
	// Each case owns its consumer and removes it before the next case. No
	// backup server or connection from an earlier route may serve this job.
	var servers struct{ Servers []struct{ ID int } }
	if err := api.query("query { servers { id } }", nil, &servers); err != nil {
		return result, err
	}
	if len(servers.Servers) != 0 {
		return result, fmt.Errorf("isolated instance contains an unexpected server")
	}
	// Egress zero is the built-in System binding; user-created egresses must
	// bind an interface or address. The Docker namespace defines its capacity.
	egressID := 0
	var profileIDs []int
	var proxyID, poolID int
	var serverID int
	defer func() {
		if serverID != 0 {
			err = errors.Join(err, api.query("mutation($id:Int!) {removeServer(id:$id) {id}}", map[string]any{"id": serverID}, nil))
		}
		if poolID != 0 {
			err = errors.Join(err, api.query("mutation($id:Int!) {deleteProxyPool(id:$id)}", map[string]any{"id": poolID}, nil))
		}
		for _, id := range profileIDs {
			err = errors.Join(err, api.query("mutation($id:Int!) {deleteProxyProfile(id:$id)}", map[string]any{"id": id}, nil))
		}
	}()
	if testCase.Kind == "chain" || testCase.Kind == "pool" {
		indices := []int{0, 1, 2}
		if testCase.Kind == "pool" {
			indices = []int{3, 4, 5, 6, 7}
		} else {
			indices = indices[:testCase.Depth]
		}
		for position, index := range indices {
			input := maps.Clone(config.Profiles[index])
			input["name"] = testCase.Name + "-" + input["name"].(string)
			if position == 0 || testCase.Kind == "pool" {
				input["host"], input["port"] = relayIP, 51830+index
			}
			if testCase.Kind == "pool" && testCase.Member >= 0 {
				input["enabled"] = position == testCase.Member
			}
			var saved struct {
				SaveProxyProfile struct {
					ID  int
					MTU int
				}
			}
			if err := api.query("mutation($input:ProxyProfileInput!) {saveProxyProfile(input:$input) {id mtu}}", map[string]any{"input": input}, &saved); err != nil {
				return result, err
			}
			profileIDs = append(profileIDs, saved.SaveProxyProfile.ID)
			mtu := 1280
			if explicit, ok := input["mtu"].(int); ok {
				mtu = explicit
			}
			if saved.SaveProxyProfile.MTU != mtu {
				return result, fmt.Errorf("configured carrier MTU changed")
			}
		}
		proxyID = profileIDs[len(profileIDs)-1]
		if testCase.Kind == "pool" {
			var saved struct {
				CreateProxyPool struct {
					ID        int
					MemberIDs []int
				}
			}
			if err := api.query("mutation($input:ProxyPoolInput!) {createProxyPool(input:$input) {id memberIds}}", map[string]any{"input": map[string]any{"name": testCase.Name, "kind": "WIRE_GUARD", "memberIds": profileIDs, "enabled": true}}, &saved); err != nil {
				return result, err
			}
			poolID = saved.CreateProxyPool.ID
			if len(saved.CreateProxyPool.MemberIDs) != 5 {
				return result, fmt.Errorf("WG pool must retain five members")
			}
			if testCase.Member >= 0 {
				proxyID = profileIDs[testCase.Member]
			} else {
				proxyID = 0
			}
		}
	} else if testCase.Kind != "direct" {
		kind, port := "HTTP_CONNECT", 8081
		if testCase.Kind == "socks" {
			kind, port = "SOCKS5", 8082
		}
		var saved struct{ SaveProxyProfile struct{ ID int } }
		if err := api.query("mutation($input:ProxyProfileInput!) {saveProxyProfile(input:$input) {id}}", map[string]any{"input": map[string]any{
			"name": testCase.Name, "kind": kind, "enabled": true, "host": relayIP, "port": port,
			"username": "fixture", "password": "fixture", "dnsServers": []string{relayIP},
		}}, &saved); err != nil {
			return result, err
		}
		proxyID = saved.SaveProxyProfile.ID
		profileIDs = append(profileIDs, proxyID)
	}
	serverInput := map[string]any{"host": config.Host, "port": config.Port, "tls": true, "username": config.Username, "password": config.Password,
		"connections": config.Connections, "active": false, "priority": 0, "backfill": false, "retentionDays": 0}
	var server struct{ AddServer struct{ ID int } }
	if err := api.query("mutation($input:ServerInput!) {addServer(input:$input) {id}}", map[string]any{"input": serverInput}, &server); err != nil {
		return result, err
	}
	serverID = server.AddServer.ID
	path := map[string]any{"direct": true}
	if testCase.Kind != "direct" {
		rung := map[string]any{"proxy": proxyID}
		if testCase.Kind == "chain" && testCase.Depth > 1 {
			rung = map[string]any{"chain": profileIDs}
		}
		if testCase.Kind == "pool" {
			rung = map[string]any{"pool": poolID}
		}
		path = map[string]any{"ladder": map[string]any{"rungs": []map[string]any{rung}, "directFallback": false}}
	}
	route := map[string]any{"failover": "HOLD", "legs": []map[string]any{{"egressId": egressID, "weight": 100, "path": path}}}
	if err := api.query("mutation($id:Int!, $input:RouteInput!) {saveNetworkRoute(kind:SERVER,id:$id,input:$input) {consumer}}", map[string]any{"id": serverID, "input": route}, nil); err != nil {
		return result, err
	}
	serverInput["active"] = true
	if err := api.query("mutation($id:Int!, $input:ServerInput!) {updateServer(id:$id,input:$input) {id}}", map[string]any{"id": serverID, "input": serverInput}, nil); err != nil {
		return result, err
	}
	if api.perf != nil {
		result.Perf = api.perf
		if err := api.perf.start(api); err != nil {
			return result, err
		}
	}
	var submitted struct {
		SubmitNzb struct {
			Accepted bool
			Item     *struct{ ID int }
		}
	}
	if err := api.query("mutation($input:SubmitNzbInput!) {submitNzb(input:$input) {accepted item {id}}}", map[string]any{"input": map[string]any{
		"nzbBase64": base64.StdEncoding.EncodeToString(config.NZB), "filename": testCase.Name + ".nzb", "force": true,
	}}, &submitted); err != nil {
		return result, err
	}
	if !submitted.SubmitNzb.Accepted || submitted.SubmitNzb.Item == nil {
		return result, fmt.Errorf("pinned test NZB was rejected")
	}
	result.JobID = submitted.SubmitNzb.Item.ID
	defer func() {
		if err != nil {
			_ = api.query("mutation($id:Int!) {cancelJob(id:$id)}", map[string]any{"id": result.JobID}, nil)
		}
	}()
	output, downloaded, selected, err := waitRealUsenetDownload(api, result.JobID, serverID, egressID, proxyID, profileIDs, testCase.Kind)
	result.DownloadedBytes, result.SelectedProxyID = downloaded, selected
	if api.perf != nil {
		err = errors.Join(err, api.perf.finish())
	}
	if err != nil {
		return result, err
	}
	service := "routed"
	if testCase.Kind == "direct" {
		service = "baseline"
	}
	result.Files, err = realUsenetOutputManifest(filepath.Join(root, service, "data"), output)
	if err != nil {
		return result, err
	}
	// The first successful selected case supplies the reference; a WG-only
	// selection never needs a direct download to establish byte integrity.
	if reference != nil {
		if !reflect.DeepEqual(reference, result.Files) {
			return result, fmt.Errorf("extracted bytes differ from the selected reference download")
		}
	}
	return result, nil
}

func waitRealUsenetDownload(api realUsenetAPI, jobID, serverID, egressID, expectedProxy int, allowedProxies []int, kind string) (string, uint64, int, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var selected int
	for {
		var flow struct {
			NetworkFlow struct {
				Legs []struct {
					Consumer, State, Reason         string
					EgressID, SelectedProxyID, Open int
				}
			}
		}
		if err := api.query("query {networkFlow {legs {consumer egressId selectedProxyId open state reason}}}", nil, &flow); err != nil {
			return "", 0, selected, err
		}
		for _, leg := range flow.NetworkFlow.Legs {
			if leg.Consumer == fmt.Sprintf("server:%d", serverID) && leg.EgressID == egressID && leg.State == "BLOCKED" {
				return "", 0, selected, fmt.Errorf("route blocked: %s", api.redact(leg.Reason))
			}
			if leg.Consumer == fmt.Sprintf("server:%d", serverID) && leg.EgressID == egressID && leg.Open > 0 {
				selected = leg.SelectedProxyID
			}
		}
		if api.perf != nil {
			if err := api.perf.observe(api, jobID); err != nil {
				return "", 0, selected, err
			}
		}
		var status struct {
			HistoryItem *struct {
				State, OutputDir, Error string
				DownloadedBytes         uint64
			}
		}
		// The queue snapshot is purged before asynchronous history archival
		// commits. Completion is observable when this specific history row is
		// readable; queue absence does not establish that a job disappeared.
		if err := api.query("query($id:Int!) {historyItem(id:$id) {state outputDir error downloadedBytes}}", map[string]any{"id": jobID}, &status); err != nil {
			return "", 0, selected, err
		}
		if status.HistoryItem != nil {
			item := status.HistoryItem
			if item.State != "COMPLETED" {
				return "", item.DownloadedBytes, selected, fmt.Errorf("download ended in %s: %s", item.State, api.redact(item.Error))
			}
			if item.DownloadedBytes == 0 {
				return "", 0, selected, fmt.Errorf("completed without downloaded bytes")
			}
			if kind != "direct" {
				if selected == 0 || (expectedProxy != 0 && selected != expectedProxy) || (kind == "pool" && !slices.Contains(allowedProxies, selected)) {
					return "", item.DownloadedBytes, selected, fmt.Errorf("download did not demonstrate the selected route")
				}
			}
			return item.OutputDir, item.DownloadedBytes, selected, nil
		}
		select {
		case <-api.ctx.Done():
			return "", 0, selected, api.ctx.Err()
		case <-ticker.C:
		}
	}
}

func realUsenetOutputManifest(dataDir, output string) ([]realUsenetFile, error) {
	if !strings.HasPrefix(output, "/data/complete/") {
		return nil, fmt.Errorf("completed output is outside the isolated download directory")
	}
	relative := strings.TrimPrefix(output, "/data/")
	if filepath.Clean(relative) != relative {
		return nil, fmt.Errorf("invalid completed output path")
	}
	root := filepath.Join(dataDir, relative)
	var files []realUsenetFile
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("cannot inspect completed output")
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("completed output contains a symlink")
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("completed output contains a non-regular file")
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("cannot identify completed file")
		}
		if name == ".weaver-output-dir" {
			return nil // Weaver ownership metadata varies with the job's path.
		}
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, ".rar") || strings.HasSuffix(lower, ".par2") || strings.HasSuffix(lower, ".nzb") {
			return fmt.Errorf("download left archive, recovery, or NZB files instead of extracted output")
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("cannot read extracted output")
		}
		hash := sha256.New()
		bytes, err := io.Copy(hash, file)
		_ = file.Close()
		if err != nil || bytes == 0 {
			return fmt.Errorf("extracted output is unreadable or empty")
		}
		files = append(files, realUsenetFile{Name: filepath.ToSlash(name), Bytes: bytes, SHA256: hex.EncodeToString(hash.Sum(nil))})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("completed download has no extracted files")
	}
	slices.SortFunc(files, func(a, b realUsenetFile) int { return strings.Compare(a.Name, b.Name) })
	return files, nil
}
