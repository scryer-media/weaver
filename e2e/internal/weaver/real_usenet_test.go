package weaver

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

func realUsenetFixtureProfile(t *testing.T) map[string]any {
	t.Helper()
	key := sha256.Sum256([]byte("synthetic-wireguard-fixture"))
	encoded := base64.StdEncoding.EncodeToString(key[:])
	text := fmt.Sprintf("[Interface]\nPrivateKey = %s\nAddress = 10.0.0.2/32\nDNS = 10.0.0.1\nMTU = 1280\n[Peer]\nPublicKey = %s\nEndpoint = peer.example.com:51820\nAllowedIPs = 0.0.0.0/0, ::/0\nPersistentKeepalive = 25\n", encoded, encoded)
	profile, err := parseRealUsenetWireGuard(text, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestRealUsenetComposeDoesNotInheritUnrelatedHarnessServices(t *testing.T) {
	t.Setenv("COMPOSE_FILE", "")
	for _, kind := range []containerengine.Kind{containerengine.Docker, containerengine.Podman} {
		source := &containerengine.Engine{Kind: kind, Binary: string(kind), ComposeProvider: containerengine.ProviderDockerCompose}
		source.SetComposeFiles("ordinary.yml", "ordinary-overlay.yml")
		isolated := realUsenetComposeEngine(source)
		args := []string{"compose", "-p", "fixture", "-f", "private.json", "up", "--wait", "routed"}
		if !reflect.DeepEqual(isolated.Args(args...), args) {
			t.Fatal("private stack inherited ordinary harness services")
		}
		if kind == containerengine.Podman && source.ComposeOverlay() != "ordinary-overlay.yml" {
			t.Fatal("ordinary harness configuration mutated")
		}
	}
}

func TestRealUsenetChainOverridePreservesPoolAndRejectsInvalidChains(t *testing.T) {
	makeConfig := func() *realUsenetConfig {
		config := &realUsenetConfig{}
		for index := range 8 {
			profile := realUsenetFixtureProfile(t)
			profile["host"] = fmt.Sprintf("peer-%d.example.com", index)
			profile["name"] = realUsenetWGNames[index]
			profile["mtu"] = 1280 + index
			config.Profiles = append(config.Profiles, profile)
		}
		return config
	}
	config := makeConfig()
	original := config.Profiles
	if err := overrideRealUsenetChain(config, "wg-pool-01.conf,wg-pool-02.conf,wg-pool-03.conf"); err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		if !reflect.DeepEqual(config.Profiles[index], original[index+3]) {
			t.Fatal("override changed imported profile capacity or credentials")
		}
	}
	config.Profiles[0]["name"] = "chain-only-name"
	if config.Profiles[3]["name"] != "wg-pool-01.conf" || original[0]["name"] != "wg-chain-01.conf" {
		t.Fatal("chain override mutated source or pool profile")
	}
	for _, invalid := range []string{"wg-pool-01.conf", "../private.conf,wg-pool-02.conf,wg-pool-03.conf", "wg-pool-01.conf,wg-pool-01.conf,wg-pool-03.conf"} {
		config := makeConfig()
		before, _ := json.Marshal(config)
		if err := overrideRealUsenetChain(config, invalid); err == nil {
			t.Fatal("invalid chain accepted")
		}
		after, _ := json.Marshal(config)
		if string(before) != string(after) {
			t.Fatal("failed override mutated configuration")
		}
	}
}

func TestRealUsenetPerfUsesSpecificJobAndCounterDeltas(t *testing.T) {
	reads := 0
	perf := &realUsenetPerf{readCounters: func() (map[string]uint64, error) {
		reads++
		return map[string]uint64{"usage_usec": uint64(reads) * 1000, "throttled_usec": uint64(reads) * 10, "memory_peak": 4096}, nil
	}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Variables map[string]int }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Variables == nil {
			fmt.Fprint(w, `{"data":{"metrics":{"bytesDownloaded":100}}}`)
		} else {
			if request.Variables["id"] != 42 {
				t.Error("sample not tied to submitted job")
			}
			fmt.Fprint(w, `{"data":{"metrics":{"bytesDownloaded":150,"currentDownloadSpeed":10,"activeDownloads":4,"downloadPressureState":"CLEAR","downloadPressureStallsTotal":3},"queueItem":{"id":42,"state":"DOWNLOADING"}}}`)
		}
	}))
	defer server.Close()
	api := realUsenetAPI{ctx: context.Background(), url: server.URL, client: server.Client(), redact: func(s string) string { return s }}
	if err := perf.start(api); err != nil {
		t.Fatal(err)
	}
	if err := perf.observe(api, 42); err != nil {
		t.Fatal(err)
	}
	if err := perf.finish(); err != nil {
		t.Fatal(err)
	}
	if perf.CPUUsec != 1000 || perf.Throttled != 10 || perf.MemoryPeak != 4096 || perf.Progress[0].BytesDownloaded != 50 {
		t.Fatal("performance totals mixed startup or earlier download counters into the measurement")
	}
	if perf.Progress[0].Pipeline.ActiveDownloads != 4 || perf.Progress[0].Pipeline.DownloadPressureState != "CLEAR" || perf.Progress[0].Pipeline.DownloadPressureStallsTotal != 3 {
		t.Fatal("pipeline performance counters were not decoded")
	}
	if _, err := parseRealUsenetCounters("usage_usec 100\nthrottled_usec 0\nmemory_current 32\nmemory_peak 64"); err != nil {
		t.Fatal(err)
	}
	if _, err := parseRealUsenetCounters("usage_usec 100"); err == nil {
		t.Fatal("incomplete cgroup counters accepted")
	}
}

func TestRealUsenetCaseSelectionNeverAddsDirectOrProxyDownloads(t *testing.T) {
	cases, err := selectRealUsenetCases("wireguard-pool-member-05,wireguard-chain-3,wireguard-pool-5")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, testCase := range cases {
		names = append(names, testCase.Name)
	}
	if !reflect.DeepEqual(names, []string{"wireguard-chain-3", "wireguard-pool-5", "wireguard-pool-member-05"}) {
		t.Fatal("selection ran an unrequested route")
	}
	all, err := selectRealUsenetCases("")
	if err != nil || !reflect.DeepEqual(all, realUsenetCases()) {
		t.Fatal("default matrix changed")
	}
	for _, invalid := range []string{"unknown", "wireguard-chain-3,", "wireguard-pool-5,wireguard-pool-5"} {
		if _, err := selectRealUsenetCases(invalid); err == nil {
			t.Fatal("invalid selection accepted")
		}
	}
}

func TestRealUsenetEveryCaseUsesProductionRouteContractAndCleansUp(t *testing.T) {
	for _, testCase := range realUsenetCases() {
		t.Run(testCase.Name, func(t *testing.T) {
			root := t.TempDir()
			service := "routed"
			if testCase.Kind == "direct" {
				service = "baseline"
			}
			dataDir := filepath.Join(root, service, "data")
			output := filepath.Join(dataDir, "complete", "fixture")
			if err := os.MkdirAll(output, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(output, "payload.bin"), []byte("fixture-payload"), 0600); err != nil {
				t.Fatal(err)
			}
			reference, err := realUsenetOutputManifest(dataDir, "/data/complete/fixture")
			if err != nil {
				t.Fatal(err)
			}
			config := &realUsenetConfig{Host: "news.example.com", Port: 563, Connections: 4, Username: "fixture-account", Password: "fixture-password", NZB: []byte("synthetic-nzb")}
			for range 8 {
				config.Profiles = append(config.Profiles, realUsenetFixtureProfile(t))
			}
			var profiles []map[string]any
			var removed []int
			var poolSize, selected int
			var consumerRemoved, poolRemoved bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Query     string
					Variables map[string]any
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					return
				}
				query, vars := payload.Query, payload.Variables
				var data any
				switch {
				case strings.Contains(query, "servers {"):
					data = map[string]any{"servers": []any{}}
				case strings.Contains(query, "saveProxyProfile("):
					input := vars["input"].(map[string]any)
					profiles = append(profiles, input)
					selected = 10 + len(profiles)
					data = map[string]any{"saveProxyProfile": map[string]any{"id": selected, "mtu": 1280}}
				case strings.Contains(query, "createProxyPool("):
					members := vars["input"].(map[string]any)["memberIds"].([]any)
					poolSize = len(members)
					member := max(testCase.Member, 0)
					selected = int(members[member].(float64))
					data = map[string]any{"createProxyPool": map[string]any{"id": 40, "memberIds": members}}
				case strings.Contains(query, "addServer("):
					input := vars["input"].(map[string]any)
					if input["active"] != false || input["tls"] != true || input["connections"] != float64(4) {
						t.Error("server bypassed inactive setup or TLS/cap settings")
					}
					data = map[string]any{"addServer": map[string]int{"id": 1}}
				case strings.Contains(query, "saveNetworkRoute("):
					input := vars["input"].(map[string]any)
					leg := input["legs"].([]any)[0].(map[string]any)
					if input["failover"] != "HOLD" || leg["egressId"] != float64(0) || leg["weight"] != float64(100) {
						t.Error("invalid production route contract")
					}
					path := leg["path"].(map[string]any)
					if testCase.Kind != "direct" && path["ladder"].(map[string]any)["directFallback"] != false {
						t.Error("direct fallback enabled")
					}
					if testCase.Kind == "chain" {
						rung := path["ladder"].(map[string]any)["rungs"].([]any)[0].(map[string]any)
						if testCase.Depth == 1 {
							if rung["proxy"] != float64(selected) || rung["chain"] != nil {
								t.Error("single tunnel submitted as invalid one-member chain")
							}
						} else if len(rung["chain"].([]any)) != testCase.Depth {
							t.Error("incorrect production chain depth")
						}
					}
					data = map[string]any{"saveNetworkRoute": map[string]string{"consumer": "server:1"}}
				case strings.Contains(query, "updateServer("):
					data = map[string]any{"updateServer": map[string]int{"id": 1}}
				case strings.Contains(query, "submitNzb("):
					if vars["input"].(map[string]any)["force"] != true {
						t.Error("repeated benchmark NZB can be rejected as a duplicate")
					}
					data = map[string]any{"submitNzb": map[string]any{"accepted": true, "item": map[string]int{"id": 17}}}
				case strings.Contains(query, "networkFlow"):
					data = map[string]any{"networkFlow": map[string]any{"legs": []map[string]any{{"consumer": "server:1", "egressId": 0, "selectedProxyId": selected, "open": 4}}}}
				case strings.Contains(query, "queueItem("):
					data = map[string]any{"queueItem": nil}
				case strings.Contains(query, "historyItem("):
					if vars["id"] != float64(17) {
						t.Error("wrong job observed")
					}
					data = map[string]any{"historyItem": map[string]any{"state": "COMPLETED", "outputDir": "/data/complete/fixture", "downloadedBytes": 100}, "queueItem": nil}
				case strings.Contains(query, "removeServer("):
					consumerRemoved = true
					data = map[string]any{"removeServer": []any{}}
				case strings.Contains(query, "deleteProxyPool("):
					poolRemoved = true
					data = map[string]any{"deleteProxyPool": true}
				case strings.Contains(query, "deleteProxyProfile("):
					removed = append(removed, int(vars["id"].(float64)))
					data = map[string]any{"deleteProxyProfile": true}
				default:
					t.Error("unexpected flow API operation")
					data = map[string]any{}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
			}))
			defer server.Close()
			api := realUsenetAPI{ctx: context.Background(), url: server.URL, client: server.Client(), redact: realUsenetRedactor(config)}
			if testCase.Name == "wireguard-chain-3" {
				// A selected WG route can establish the reference without a
				// preceding direct connection or download.
				reference = nil
			}
			result, err := runRealUsenetCase(api, config, root, weaverDatastoreSQLite, testCase, "127.0.0.1", reference)
			if err != nil || result.Status != "passed" {
				t.Fatalf("case did not pass: %v", err)
			}
			if !consumerRemoved || len(removed) != len(profiles) {
				t.Fatal("case left a consumer or tunnel session behind")
			}
			if testCase.Kind == "chain" {
				if len(profiles) != testCase.Depth {
					t.Fatal("wrong chain depth")
				}
				for position, profile := range profiles {
					if profile["mtu"] != float64(1280) {
						t.Fatal("carrier capacity increased")
					}
					if position > 0 && profile["host"] != "peer.example.com" {
						t.Fatal("inner endpoint bypasses its carrier")
					}
				}
			}
			if testCase.Kind == "pool" {
				if poolSize != 5 || !poolRemoved {
					t.Fatal("five-member pool was not exercised and removed")
				}
				for member, profile := range profiles {
					if profile["enabled"] != (testCase.Member < 0 || testCase.Member == member) {
						t.Fatal("wrong pool members enabled")
					}
				}
			}
			if config.Profiles[0]["host"] != "peer.example.com" {
				t.Fatal("source profile mutated")
			}
		})
	}
}

func TestRealUsenetEachRouteStartsWithColdStateAndPreservesPreviousOutput(t *testing.T) {
	root := t.TempDir()
	template := map[string]any{"volumes": []string{"original:/config", "original:/data"}, "image": "fixture"}
	var previous []string
	for _, name := range []string{"first", "second"} {
		caseRoot := filepath.Join(root, "cases", name)
		service, err := prepareRealUsenetCase(template, caseRoot)
		if err != nil {
			t.Fatal(err)
		}
		volumes := service["volumes"].([]string)
		if reflect.DeepEqual(volumes, previous) || reflect.DeepEqual(volumes, template["volumes"]) {
			t.Fatal("case reused a retained container's bind paths")
		}
		previous = volumes
		for _, dir := range []string{"config", "data"} {
			entries, err := os.ReadDir(filepath.Join(caseRoot, "routed", dir))
			if err != nil {
				t.Fatal(err)
			}
			if dir == "data" && len(entries) != 0 {
				t.Fatal("next route can reuse downloaded articles")
			}
			if dir == "config" && (len(entries) != 1 || entries[0].Name() != "weaver.toml") {
				t.Fatal("next route can reuse the previous database or profile settings")
			}
		}
		bootstrap, err := os.ReadFile(filepath.Join(caseRoot, "routed", "config", "weaver.toml"))
		if err != nil || string(bootstrap) != realUsenetBootstrapConfig {
			t.Fatal("fresh route configuration missing")
		}
		if err := os.WriteFile(filepath.Join(caseRoot, "routed", "data", "payload.bin"), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"first", "second"} {
		payload, err := os.ReadFile(filepath.Join(root, "cases", name, "routed", "data", "payload.bin"))
		if err != nil || string(payload) != name {
			t.Fatal("later case changed preserved payload")
		}
	}
}

func TestRealUsenetBlockedRouteFailsWithoutPollingAnotherJob(t *testing.T) {
	var queries int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries++
		fmt.Fprint(w, `{"data":{"networkFlow":{"legs":[{"consumer":"server:7","egressId":0,"state":"BLOCKED","reason":"fixture-secret"}]}}}`)
	}))
	defer server.Close()
	api := realUsenetAPI{ctx: context.Background(), url: server.URL, client: server.Client(), redact: strings.NewReplacer("fixture-secret", "[REDACTED]").Replace}
	_, _, _, err := waitRealUsenetDownload(api, 17, 7, 0, 42, []int{42}, "chain")
	if err == nil || !strings.Contains(err.Error(), "route blocked") || strings.Contains(err.Error(), "fixture-secret") || queries != 1 {
		t.Fatal("blocked route was not reported immediately and privately")
	}
}

func TestRealUsenetEnvDoesNotExecuteOrExpandCredentials(t *testing.T) {
	values, err := parseRealUsenetEnv("# fixture\nexport NEWSHOSTING_USER='fixture-account'\nNEWSHOSTING_PASS='literal$VALUE$(false)#value'\nNEWSHOSTING_PORT=563 # TLS\n")
	if err != nil {
		t.Fatal(err)
	}
	if values["NEWSHOSTING_PASS"] != "literal$VALUE$(false)#value" || values["NEWSHOSTING_PORT"] != "563" {
		t.Fatal("credential text was interpreted")
	}
	for _, text := range []string{"NEWSHOSTING_PASS=first\nNEWSHOSTING_PASS=second", "NEWSHOSTING_PASS='unterminated", "not an assignment"} {
		if _, err := parseRealUsenetEnv(text); err == nil {
			t.Fatal("malformed private input accepted")
		}
	}
}

func TestRealUsenetWireGuardPreservesCapacityAndRejectsHooks(t *testing.T) {
	profile := realUsenetFixtureProfile(t)
	if profile["mtu"] != 1280 || profile["keepaliveSeconds"] != 25 {
		t.Fatal("configured tunnel settings changed")
	}
	key := profile["privateKey"].(string)
	base := fmt.Sprintf("[Interface]\nPrivateKey=%s\nAddress=10.0.0.2/32\nDNS=10.0.0.1\n[Peer]\nPublicKey=%s\nEndpoint=[2001:db8::1]:51820\nAllowedIPs=0.0.0.0/0\n", key, key)
	parsed, err := parseRealUsenetWireGuard(base, "fixture")
	if err != nil || parsed["host"] != "2001:db8::1" {
		t.Fatal("IPv6 endpoint not parsed")
	}
	if _, set := parsed["mtu"]; set {
		t.Fatal("missing MTU should use the production default")
	}
	for _, bad := range []string{
		strings.Replace(base, "Address=", "PostUp=fixture-command\nAddress=", 1),
		strings.Replace(base, "0.0.0.0/0", "10.0.0.0/8", 1),
		base + "[Peer]\nPublicKey=" + key,
		strings.Replace(base, "DNS=10.0.0.1", "DNS=fixture-domain", 1),
		strings.Replace(base, "PrivateKey="+key, "PrivateKey=fixture-invalid-private-value", 1),
	} {
		_, err := parseRealUsenetWireGuard(bad, "fixture")
		if err == nil {
			t.Fatal("unsupported configuration accepted")
		}
		if strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "fixture-invalid-private-value") {
			t.Fatal("parser exposed a credential")
		}
	}
}

func TestRealUsenetChainAndPoolRequireDistinctEndpoints(t *testing.T) {
	var profiles []map[string]any
	for index, name := range realUsenetWGNames {
		profile := realUsenetFixtureProfile(t)
		profile["name"] = name
		profile["host"] = fmt.Sprintf("peer-%d.example.com", index)
		profiles = append(profiles, profile)
	}
	if err := validateRealUsenetEndpoints(profiles); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]int{{0, 1}, {3, 4}} {
		original := profiles[pair[1]]["host"]
		profiles[pair[1]]["host"] = strings.ToUpper(profiles[pair[0]]["host"].(string)) + "."
		err := validateRealUsenetEndpoints(profiles)
		if err == nil || strings.Contains(err.Error(), ".example.com") {
			t.Fatal("duplicate endpoint accepted or provider address exposed")
		}
		profiles[pair[1]]["host"] = original
	}
	// A chain server can also be a member of the separate five-server pool.
	profiles[3]["host"] = profiles[0]["host"]
	if err := validateRealUsenetEndpoints(profiles); err != nil {
		t.Fatal("sharing an endpoint between the chain and pool was rejected")
	}
}

func TestRealUsenetPrivateInputsRejectSymlinksAndChangedNZB(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("NEWSHOSTING_USER=fixture\nNEWSHOSTING_PASS=fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, realUsenetNZBName), []byte("fixture-changed-nzb"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRealUsenetConfig(root); err == nil || !strings.Contains(err.Error(), "pinned") {
		t.Fatal("changed NZB accepted")
	}
	if err := os.Symlink(filepath.Join(root, ".env"), filepath.Join(root, "profile.conf")); err != nil {
		t.Fatal(err)
	}
	if _, err := readRealUsenetPrivateFile(root, "profile.conf"); err == nil {
		t.Fatal("private symlink accepted")
	}
}

func TestRealUsenetComposeHasNoDirectExitForRoutedInstance(t *testing.T) {
	t.Setenv("E2E_WEAVER_IMAGE", "fixture-weaver")
	t.Setenv("E2E_WEAVER_PLAYWRIGHT_IMAGE", "fixture-relay")
	compose, err := realUsenetCompose(t.TempDir(), "172.30.90.0/24", "172.30.90.250", weaverDatastorePostgres, runtimePortState{WeaverPort: 19000, LocalWeaverPort: 19001})
	if err != nil {
		t.Fatal(err)
	}
	networks := compose["networks"].(map[string]any)
	if networks["private"].(map[string]any)["internal"] != true {
		t.Fatal("routed network is not internal")
	}
	services := compose["services"].(map[string]any)
	routed := services["routed"].(map[string]any)
	if got := routed["networks"].(map[string]any); len(got) != 1 || got["private"] == nil {
		t.Fatal("routed Weaver has a direct internet path")
	}
	if routed["ports"] != nil || routed["networks"].(map[string]any)["private"].(map[string]any)["ipv4_address"] != "172.30.90.249" {
		t.Fatal("API access bypasses the relay or its fixed private target")
	}
	if got := services["relay"].(map[string]any)["ports"]; !reflect.DeepEqual(got, []string{"127.0.0.1:19001:9090"}) {
		t.Fatal("private API relay is not published only on loopback")
	}
	if len(services["baseline"].(map[string]any)["networks"].(map[string]any)) != 2 {
		t.Fatal("direct baseline has no external network")
	}
	for _, service := range services {
		data, err := json.Marshal(service)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "NEWSHOSTING_") || strings.Contains(string(data), "wg-chain-") || strings.Contains(string(data), realUsenetNZBName) {
			t.Fatal("provider inputs entered compose state")
		}
	}
}

func TestRealUsenetOutputRequiresExtractedFilesAndHashesEveryByte(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "complete", "fixture")
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(output, "payload.bin")
	if err := os.WriteFile(filepath.Join(output, ".weaver-output-dir"), []byte("fixture-ownership"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := realUsenetOutputManifest(root, "/data/complete/fixture"); err == nil {
		t.Fatal("ownership metadata alone counted as downloaded payload")
	}
	if err := os.WriteFile(file, []byte("fixture-payload"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := realUsenetOutputManifest(root, "/data/complete/fixture")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Name != "payload.bin" {
		t.Fatal("per-job ownership metadata counted as payload")
	}
	if err := os.WriteFile(file, []byte("fixture-payloae"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := realUsenetOutputManifest(root, "/data/complete/fixture")
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(first, second) {
		t.Fatal("same-sized byte corruption was missed")
	}
	if _, err := realUsenetOutputManifest(root, "/data/complete/../outside"); err == nil {
		t.Fatal("traversal accepted")
	}
	if err := os.WriteFile(filepath.Join(output, "leftover.rar"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := realUsenetOutputManifest(root, "/data/complete/fixture"); err == nil {
		t.Fatal("unextracted archive accepted")
	}
}

func TestRealUsenetWaitObservesCompletionAcrossQueueHistoryTransition(t *testing.T) {
	var observations []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Query     string
			Variables map[string]any
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(payload.Query, "networkFlow") {
			fmt.Fprint(w, `{"data":{"networkFlow":{"legs":[]}}}`)
			return
		}
		if payload.Variables["id"] != float64(17) {
			t.Error("observed a different job")
		}
		observations = append(observations, "history")
		if len(observations) == 1 {
			// The completed job has left the queue; its deferred history
			// archival has not committed yet. Neither snapshot has the row.
			fmt.Fprint(w, `{"data":{"queueItem":null,"historyItem":null}}`)
		} else {
			fmt.Fprint(w, `{"data":{"historyItem":{"state":"COMPLETED","outputDir":"/data/complete/fixture","downloadedBytes":100}}}`)
		}
	}))
	defer server.Close()
	api := realUsenetAPI{ctx: context.Background(), url: server.URL, client: server.Client(), redact: func(s string) string { return s }}
	_, downloaded, _, err := waitRealUsenetDownload(api, 17, 7, 0, 0, nil, "direct")
	if err != nil || downloaded != 100 || !reflect.DeepEqual(observations, []string{"history", "history"}) {
		t.Fatal("completion was lost between queue and history observations")
	}
}

func TestRealUsenetWaitUsesSpecificJobAndSelectedRoute(t *testing.T) {
	for _, selected := range []int{42, 99} {
		t.Run(fmt.Sprint(selected), func(t *testing.T) {
			var jobs []int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Query     string
					Variables map[string]any
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(payload.Query, "networkFlow") {
					fmt.Fprintf(w, `{"data":{"networkFlow":{"legs":[{"consumer":"server:7","egressId":8,"selectedProxyId":%d,"open":4},{"consumer":"server:6","egressId":8,"selectedProxyId":42,"open":4}]}}}`, selected)
				} else {
					jobs = append(jobs, int(payload.Variables["id"].(float64)))
					fmt.Fprint(w, `{"data":{"historyItem":{"state":"COMPLETED","outputDir":"/data/complete/fixture","error":null,"downloadedBytes":100},"queueItem":null}}`)
				}
			}))
			defer server.Close()
			api := realUsenetAPI{ctx: context.Background(), url: server.URL, client: server.Client(), redact: func(s string) string { return s }}
			_, _, _, err := waitRealUsenetDownload(api, 17, 7, 8, 42, []int{42}, "chain")
			if (err == nil) != (selected == 42) {
				t.Fatal("incorrect route verdict")
			}
			if !reflect.DeepEqual(jobs, []int{17}) {
				t.Fatal("wait observed a different job")
			}
		})
	}
}

func TestRealUsenetErrorsNeverExposeProviderCredentials(t *testing.T) {
	profile := realUsenetFixtureProfile(t)
	config := &realUsenetConfig{Username: "fixture-account", Password: "fixture-password", Host: "news.example.com", Profiles: []map[string]any{profile}}
	redact := realUsenetRedactor(config)
	message := strings.Join([]string{config.Username, config.Password, config.Host, profile["privateKey"].(string)}, " ")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"message": message}}})
	}))
	defer server.Close()
	api := realUsenetAPI{ctx: context.Background(), url: server.URL, client: server.Client(), redact: redact}
	err := api.query("mutation { fixture }", map[string]any{"password": config.Password}, nil)
	if err == nil {
		t.Fatal("GraphQL error ignored")
	}
	for _, value := range []string{config.Username, config.Password, config.Host, profile["privateKey"].(string)} {
		if strings.Contains(err.Error(), value) {
			t.Fatal("provider credential exposed")
		}
	}
}

func TestRealUsenetCommitGuardRejectsForceAddedInputsBeforePrintingContent(t *testing.T) {
	hook := filepath.Join(weaverRepoPath(), ".githooks", "pre-commit")
	for _, name := range []string{"wg-chain-01.conf", ".env", realUsenetNZBName, ".gitkeep"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			run := func(args ...string) {
				t.Helper()
				cmd := exec.Command("git", args...)
				cmd.Dir = root
				if err := cmd.Run(); err != nil {
					t.Fatal("test git operation failed")
				}
			}
			run("init", "--quiet")
			dir := filepath.Join(root, "e2e", "private", "real-network")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), []byte("fixture-secret-marker"), 0600); err != nil {
				t.Fatal(err)
			}
			run("add", "-f", "--", filepath.Join("e2e", "private", "real-network", name))
			cmd := exec.Command("sh", hook)
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "pre-commit blocked:") {
				t.Fatal("private commit was not blocked")
			}
			if strings.Contains(string(output), "fixture-secret-marker") {
				t.Fatal("commit guard printed private content")
			}
		})
	}
}
