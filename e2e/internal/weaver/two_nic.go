package weaver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

// The two-NIC lanes run Weaver on a machine with two physical interfaces and
// the destination stack on another machine, so every leg leaves through a
// real NIC.
//
//	Lane M: a native macOS Weaver (WEAVER_BIN) here, bound to Wi-Fi and LAN;
//	        the stack runs in Docker on E2E_TWO_NIC_REMOTE.
//	Lane L: Weaver's own image with host networking on E2E_TWO_NIC_REMOTE;
//	        the stack runs here.
//
// Packets are counted with tcpdump on Weaver's host, behind the same HTTP
// control the compose lane's capture sidecar serves, so two-nic.spec.ts uses
// the same capture helper. Both lanes need sudo for tcpdump and for link
// changes, and both act on real machines, so the command refuses to start
// unless E2E_TWO_NIC_CONFIRM=1.

const twoNICProject = "weaver-two-nic"

type twoNICLane string

const (
	twoNICLaneM twoNICLane = "M"
	twoNICLaneL twoNICLane = "L"
)

type twoNICEgress struct {
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type twoNICConfig struct {
	Lane          twoNICLane
	Remote        string
	RemoteE2EDir  string
	StackHost     string
	WeaverHost    string
	WeaverPort    int
	Egresses      []twoNICEgress
	CaptureIfaces []string
	Expect        map[string]string
	WifiDevice    string
	WeaverImage   string
	TrustedCIDR   string
	ControlPort   int
	RunDir        string
}

var twoNICNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func twoNICUsage() string {
	return `usage: two-nic <M|L>

Required environment:
  E2E_TWO_NIC_CONFIRM=1        Acknowledge that this command drives real hosts and NICs
  E2E_TWO_NIC_REMOTE           SSH destination of the other machine (user@host)
  E2E_TWO_NIC_REMOTE_E2E_DIR   Path of this e2e directory on the remote machine
  E2E_TWO_NIC_STACK_HOST       Address Weaver dials for the destination stack
  E2E_TWO_NIC_EGRESSES         name=interface:<if>|source:<ip>, comma separated
  E2E_TWO_NIC_CAPTURE_IFACES   Interfaces to capture on Weaver's host, comma separated
  E2E_TWO_NIC_EXPECT           JSON object: egress name -> expected source address
Lane M:
  WEAVER_BIN                   Native Weaver binary
  E2E_TWO_NIC_WIFI_DEVICE      Wi-Fi device toggled with networksetup (default en0)
Lane L:
  E2E_TWO_NIC_WEAVER_HOST      Address of the remote Weaver as seen from here
  E2E_TWO_NIC_WEAVER_IMAGE     Weaver image present on the remote machine
  E2E_TWO_NIC_TRUSTED_CIDR     This machine's address as a CIDR, trusted by the remote Weaver
Optional:
  E2E_TWO_NIC_WEAVER_PORT      Weaver HTTP port (default 19190)
  E2E_TWO_NIC_CONTROL_PORT     Local capture-control port (default 18099)
  E2E_RUN_DIR                  Where pcaps, logs and the Playwright report go`
}

func parseTwoNICEgresses(text string) ([]twoNICEgress, error) {
	var out []twoNICEgress
	for _, entry := range strings.Split(text, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, target, ok := strings.Cut(entry, "=")
		kind, value, kindOK := strings.Cut(target, ":")
		if !ok || !kindOK || !twoNICNamePattern.MatchString(name) {
			return nil, fmt.Errorf("egress %q is not name=interface:<if> or name=source:<ip>", entry)
		}
		switch kind {
		case "interface":
			if !twoNICNamePattern.MatchString(value) {
				return nil, fmt.Errorf("egress %s: invalid interface %q", name, value)
			}
			kind = "INTERFACE"
		case "source":
			if net.ParseIP(value) == nil {
				return nil, fmt.Errorf("egress %s: invalid source address %q", name, value)
			}
			kind = "SOURCE_ADDRESS"
		default:
			return nil, fmt.Errorf("egress %s: unknown binding %q", name, kind)
		}
		out = append(out, twoNICEgress{Name: name, Kind: kind, Value: value})
	}
	if len(out) == 0 {
		return nil, errors.New("no egresses configured")
	}
	return out, nil
}

func loadTwoNICConfig(lane twoNICLane, getenv func(string) string) (twoNICConfig, error) {
	required := func(key string) (string, error) {
		value := strings.TrimSpace(getenv(key))
		if value == "" {
			return "", fmt.Errorf("%s is required", key)
		}
		return value, nil
	}
	var errs []error
	cfg := twoNICConfig{Lane: lane, WeaverPort: 19190, ControlPort: 18099, WifiDevice: "en0"}
	var err error
	if getenv("E2E_TWO_NIC_CONFIRM") != "1" {
		errs = append(errs, errors.New("E2E_TWO_NIC_CONFIRM=1 is required"))
	}
	if cfg.Remote, err = required("E2E_TWO_NIC_REMOTE"); err != nil {
		errs = append(errs, err)
	}
	if cfg.RemoteE2EDir, err = required("E2E_TWO_NIC_REMOTE_E2E_DIR"); err != nil {
		errs = append(errs, err)
	}
	if cfg.StackHost, err = required("E2E_TWO_NIC_STACK_HOST"); err != nil {
		errs = append(errs, err)
	} else if net.ParseIP(cfg.StackHost) == nil {
		errs = append(errs, fmt.Errorf("E2E_TWO_NIC_STACK_HOST %q is not an IP address", cfg.StackHost))
	}
	if text, err := required("E2E_TWO_NIC_EGRESSES"); err != nil {
		errs = append(errs, err)
	} else if cfg.Egresses, err = parseTwoNICEgresses(text); err != nil {
		errs = append(errs, err)
	}
	if text, err := required("E2E_TWO_NIC_CAPTURE_IFACES"); err != nil {
		errs = append(errs, err)
	} else {
		for _, iface := range strings.Split(text, ",") {
			if iface = strings.TrimSpace(iface); iface != "" {
				if !twoNICNamePattern.MatchString(iface) {
					errs = append(errs, fmt.Errorf("invalid capture interface %q", iface))
				}
				cfg.CaptureIfaces = append(cfg.CaptureIfaces, iface)
			}
		}
	}
	if text, err := required("E2E_TWO_NIC_EXPECT"); err != nil {
		errs = append(errs, err)
	} else if err := json.Unmarshal([]byte(text), &cfg.Expect); err != nil {
		errs = append(errs, fmt.Errorf("E2E_TWO_NIC_EXPECT: %w", err))
	}
	if value := strings.TrimSpace(getenv("E2E_TWO_NIC_WEAVER_PORT")); value != "" {
		if cfg.WeaverPort, err = strconv.Atoi(value); err != nil {
			errs = append(errs, fmt.Errorf("E2E_TWO_NIC_WEAVER_PORT: %w", err))
		}
	}
	if value := strings.TrimSpace(getenv("E2E_TWO_NIC_CONTROL_PORT")); value != "" {
		if cfg.ControlPort, err = strconv.Atoi(value); err != nil {
			errs = append(errs, fmt.Errorf("E2E_TWO_NIC_CONTROL_PORT: %w", err))
		}
	}
	switch lane {
	case twoNICLaneM:
		cfg.WeaverHost = "127.0.0.1"
		if value := strings.TrimSpace(getenv("E2E_TWO_NIC_WIFI_DEVICE")); value != "" {
			cfg.WifiDevice = value
		}
		if !twoNICNamePattern.MatchString(cfg.WifiDevice) {
			errs = append(errs, fmt.Errorf("invalid Wi-Fi device %q", cfg.WifiDevice))
		}
		if _, err := required("WEAVER_BIN"); err != nil {
			errs = append(errs, err)
		}
	case twoNICLaneL:
		if cfg.WeaverHost, err = required("E2E_TWO_NIC_WEAVER_HOST"); err != nil {
			errs = append(errs, err)
		}
		if cfg.WeaverImage, err = required("E2E_TWO_NIC_WEAVER_IMAGE"); err != nil {
			errs = append(errs, err)
		}
		if cfg.TrustedCIDR, err = required("E2E_TWO_NIC_TRUSTED_CIDR"); err != nil {
			errs = append(errs, err)
		}
	default:
		errs = append(errs, fmt.Errorf("unknown lane %q", lane))
	}
	cfg.RunDir = absolutePath(env("E2E_RUN_DIR", filepath.Join(e2eDir(), "artifacts", "two-nic", time.Now().UTC().Format("20060102-150405"))))
	return cfg, errors.Join(errs...)
}

func cmdTwoNIC(args []string) {
	if len(args) != 1 {
		log.Fatal(twoNICUsage())
	}
	cfg, err := loadTwoNICConfig(twoNICLane(strings.ToUpper(strings.TrimSpace(args[0]))), os.Getenv)
	if err != nil {
		log.Fatalf("%v\n\n%s", err, twoNICUsage())
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runTwoNIC(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func runTwoNIC(ctx context.Context, cfg twoNICConfig) (err error) {
	for _, dir := range []string{cfg.RunDir, filepath.Join(cfg.RunDir, "captures"), filepath.Join(cfg.RunDir, "artifacts")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	stack := twoNICStackCommands(cfg)
	if err := runTwoNICStep(ctx, "start destination stack", stack.up); err != nil {
		return err
	}
	defer func() {
		if downErr := runTwoNICStep(context.Background(), "stop destination stack", stack.down); downErr != nil {
			err = errors.Join(err, downErr)
		}
	}()

	control := newTwoNICControl(cfg)
	server := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", cfg.ControlPort), Handler: control}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen for capture control: %w", err)
	}
	go func() { _ = server.Serve(listener) }()
	defer func() {
		control.stopAll()
		_ = server.Close()
	}()

	stages := []string{"initial"}
	if cfg.Lane == twoNICLaneL {
		// L02 needs the same Weaver without WEAVER_RETAIN_NET_RAW.
		stages = append(stages, "no-net-raw")
	}
	for _, stage := range stages {
		stopWeaver, err := startTwoNICWeaver(ctx, cfg, stage)
		if err != nil {
			return fmt.Errorf("start Weaver for stage %s: %w", stage, err)
		}
		runErr := runTwoNICPlaywright(ctx, cfg, stage)
		stopErr := stopWeaver()
		if runErr != nil || stopErr != nil {
			return errors.Join(runErr, stopErr)
		}
	}
	return nil
}

type twoNICStack struct {
	up   []string
	down []string
}

// twoNICComposeArgs are the Compose arguments for the destination stack.
func twoNICComposeArgs(args ...string) []string {
	return append([]string{
		"compose", "-p", twoNICProject,
		"-f", "docker-compose.yml",
		"-f", advancedNetworkingComposeFile,
		"-f", "docker-compose.two-nic.yml",
	}, args...)
}

var twoNICStackServices = []string{"nntp", "nntp2", "toxiproxy", "proxy-fixture", "tunnel-fixture", "rss-fixture"}

// twoNICStackCommands returns the argv that brings the stack up and down:
// over SSH for Lane M, through the local container engine for Lane L.
func twoNICStackCommands(cfg twoNICConfig) twoNICStack {
	up := twoNICComposeArgs(append([]string{"up", "-d", "--wait"}, twoNICStackServices...)...)
	down := twoNICComposeArgs("down", "-v", "--remove-orphans")
	if cfg.Lane == twoNICLaneL {
		engine := containerengine.Command().Path
		return twoNICStack{up: append([]string{engine}, up...), down: append([]string{engine}, down...)}
	}
	remote := func(args []string) []string {
		script := fmt.Sprintf("cd %s && E2E_TWO_NIC_STACK_HOST=%s E2E_NNTP_PASSWORD=e2e-pass docker %s",
			shellQuote(cfg.RemoteE2EDir), shellQuote(cfg.StackHost), shellJoin(args))
		return []string{"ssh", cfg.Remote, script}
	}
	return twoNICStack{up: remote(up), down: remote(down)}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func shellJoin(args []string) string {
	quoted := make([]string, len(args))
	for index, arg := range args {
		quoted[index] = shellQuote(arg)
	}
	return strings.Join(quoted, " ")
}

func runTwoNICStep(ctx context.Context, summary string, argv []string) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = e2eDir()
	cmd.Env = append(os.Environ(), "E2E_NNTP_PASSWORD=e2e-pass")
	return runExternalCommand(cmd, summary)
}

// twoNICWeaverConfig points both servers at the stack's published NNTP ports.
func twoNICWeaverConfig(dataDir, stackHost string) string {
	return fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true

[[servers]]
id = 1
host = %q
port = 119
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = 8
active = true
priority = 0

[[servers]]
id = 2
host = %q
port = 2119
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = 4
active = true
priority = 1
`, dataDir, filepath.Join(dataDir, "intermediate"), filepath.Join(dataDir, "complete"), stackHost, stackHost)
}

func startTwoNICWeaver(ctx context.Context, cfg twoNICConfig, stage string) (func() error, error) {
	url := fmt.Sprintf("http://%s:%d", cfg.WeaverHost, cfg.WeaverPort)
	if cfg.Lane == twoNICLaneM {
		dataDir := filepath.Join(cfg.RunDir, "weaver")
		configPath := filepath.Join(cfg.RunDir, "weaver.toml")
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(configPath, []byte(twoNICWeaverConfig(dataDir, cfg.StackHost)), 0o644); err != nil {
			return nil, err
		}
		logPath := filepath.Join(cfg.RunDir, "weaver-"+stage+".log")
		logFile, err := os.Create(logPath)
		if err != nil {
			return nil, err
		}
		cmd := exec.CommandContext(ctx, findWeaverBin(), "--config", configPath, "serve", "--port", strconv.Itoa(cfg.WeaverPort))
		cmd.Env = managedWeaverEnv(os.Environ(), cfg.RunDir, "info")
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			_ = logFile.Close()
			return nil, err
		}
		watch := watchChildExit(cmd, logPath)
		waitForGraphQL(graphqlURL(url), watch.Probe)
		return func() error {
			stopWatchedChild(cmd, watch, 30*time.Second)
			return logFile.Close()
		}, nil
	}
	retain := "true"
	if stage == "no-net-raw" {
		retain = "false"
	}
	run := twoNICRemoteWeaverRun(cfg, retain)
	if err := runTwoNICStep(ctx, "start remote Weaver", []string{"ssh", cfg.Remote, run}); err != nil {
		return nil, err
	}
	waitForGraphQL(graphqlURL(url), func() (bool, string) { return false, "" })
	return func() error {
		return runTwoNICStep(context.Background(), "stop remote Weaver",
			[]string{"ssh", cfg.Remote, "docker rm -f " + twoNICProject + "-weaver"})
	}, nil
}

// twoNICRemoteWeaverRun is the remote shell command that starts Weaver's
// image with host networking, so its interfaces are the host's own NICs.
func twoNICRemoteWeaverRun(cfg twoNICConfig, retainNetRaw string) string {
	config := twoNICWeaverConfig("/data", cfg.StackHost)
	script := fmt.Sprintf("mkdir -p /config /data/intermediate /data/complete && printf '%%s' %s > /config/weaver.toml && exec /entrypoint.sh -c /config/weaver.toml serve --port %d",
		shellQuote(config), cfg.WeaverPort)
	args := []string{
		"docker", "run", "-d", "--rm", "--name", twoNICProject + "-weaver",
		"--network", "host", "--cap-add", "NET_RAW",
		"-e", "WEAVER_RETAIN_NET_RAW=" + retainNetRaw,
		"-e", "WEAVER_HTTP_BIND_ADDRESS=0.0.0.0",
		"-e", "WEAVER_TRUSTED_CIDRS=" + cfg.TrustedCIDR,
		"-e", "WEAVER_HTTP_ALLOWED_HOSTS=" + cfg.WeaverHost,
		"--entrypoint", "/bin/sh",
		cfg.WeaverImage, "-lc", script,
	}
	return shellJoin(args)
}

func runTwoNICPlaywright(ctx context.Context, cfg twoNICConfig, stage string) error {
	egresses, _ := json.Marshal(cfg.Egresses)
	expect, _ := json.Marshal(cfg.Expect)
	stack := cfg.StackHost
	cmd := exec.CommandContext(ctx, "npm", "run", "test:two-nic")
	cmd.Dir = filepath.Join(e2eDir(), "playwright-weaver")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("PLAYWRIGHT_BASE_URL=http://%s:%d", cfg.WeaverHost, cfg.WeaverPort),
		fmt.Sprintf("WEAVER_URL=http://%s:%d", cfg.WeaverHost, cfg.WeaverPort),
		"PLAYWRIGHT_ARTIFACTS_DIR="+filepath.Join(cfg.RunDir, "artifacts"),
		"E2E_WEAVER_ARTIFACT_STAGE="+stage,
		"E2E_WEAVER_STAGE="+stage,
		"E2E_TWO_NIC_LANE="+string(cfg.Lane),
		"E2E_TWO_NIC_STACK_HOST="+stack,
		"E2E_TWO_NIC_EGRESSES="+string(egresses),
		"E2E_TWO_NIC_EXPECT="+string(expect),
		"E2E_TWO_NIC_CAPTURE_IFACES="+strings.Join(cfg.CaptureIfaces, ","),
		"E2E_TWO_NIC_WIFI_DEVICE="+cfg.WifiDevice,
		fmt.Sprintf("CAPTURE_URL=http://127.0.0.1:%d", cfg.ControlPort),
		"PROXY_FIXTURE_URL=http://"+net.JoinHostPort(stack, "8090"),
		"TUNNEL_FIXTURE_URL=http://"+net.JoinHostPort(stack, "8095"),
		"TOXIPROXY_URL=http://"+net.JoinHostPort(stack, "8474"),
		"E2E_NNTP_HOST="+stack,
	)
	return runExternalCommand(cmd, "playwright two-nic "+stage)
}

// twoNICControl serves the capture sidecar's API on Weaver's host:
// POST /start {name}, POST /stop, GET /count?name&iface[&filter],
// GET /interfaces, POST /link {iface, up}. Lane M captures and toggles Wi-Fi
// locally; Lane L does both over SSH, and a link change there is one remote
// command that brings the link back once Weaver reports the egress DOWN.
type twoNICControl struct {
	cfg      twoNICConfig
	mu       sync.Mutex
	captures map[string]*exec.Cmd
	name     string
}

func newTwoNICControl(cfg twoNICConfig) *twoNICControl {
	return &twoNICControl{cfg: cfg, captures: map[string]*exec.Cmd{}}
}

func (c *twoNICControl) pcapPath(name, iface string) string {
	if c.cfg.Lane == twoNICLaneL {
		return "/tmp/" + twoNICProject + "-" + name + "-" + iface + ".pcap"
	}
	return filepath.Join(c.cfg.RunDir, "captures", name+"-"+iface+".pcap")
}

func (c *twoNICControl) hostCommand(argv ...string) *exec.Cmd {
	if c.cfg.Lane == twoNICLaneL {
		return exec.Command("ssh", c.cfg.Remote, shellJoin(argv))
	}
	return exec.Command(argv[0], argv[1:]...)
}

func (c *twoNICControl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	reply := func(status int, body any) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/":
		reply(http.StatusOK, map[string]any{"lane": c.cfg.Lane, "interfaces": c.cfg.CaptureIfaces})
	case r.Method == http.MethodPost && r.URL.Path == "/start":
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !twoNICNamePattern.MatchString(body.Name) {
			reply(http.StatusBadRequest, map[string]string{"error": "name must match [A-Za-z0-9._-]+"})
			return
		}
		if err := c.start(body.Name); err != nil {
			reply(http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		reply(http.StatusOK, map[string]any{"name": body.Name, "interfaces": c.cfg.CaptureIfaces})
	case r.Method == http.MethodPost && r.URL.Path == "/stop":
		c.stopAll()
		reply(http.StatusOK, map[string]any{"stopped": true})
	case r.Method == http.MethodGet && r.URL.Path == "/count":
		name, iface, filter := r.URL.Query().Get("name"), r.URL.Query().Get("iface"), r.URL.Query().Get("filter")
		if !twoNICNamePattern.MatchString(name) || !twoNICNamePattern.MatchString(iface) {
			reply(http.StatusBadRequest, map[string]string{"error": "name and iface are required"})
			return
		}
		count, err := c.count(name, iface, filter)
		if err != nil {
			reply(http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		reply(http.StatusOK, map[string]any{"name": name, "iface": iface, "packets": count})
	case r.Method == http.MethodPost && r.URL.Path == "/link":
		var body struct {
			Iface string `json:"iface"`
			Up    bool   `json:"up"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !twoNICNamePattern.MatchString(body.Iface) {
			reply(http.StatusBadRequest, map[string]string{"error": "iface is required"})
			return
		}
		if err := c.link(body.Iface, body.Up); err != nil {
			reply(http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		reply(http.StatusOK, map[string]any{"iface": body.Iface, "up": body.Up})
	default:
		reply(http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

func (c *twoNICControl) start(name string) error {
	c.stopAll()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, iface := range c.cfg.CaptureIfaces {
		cmd := c.hostCommand("sudo", "-n", "tcpdump", "-i", iface, "-n", "-U", "-w", c.pcapPath(name, iface), "host", c.cfg.StackHost)
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start tcpdump on %s: %w", iface, err)
		}
		// tcpdump announces "listening on" once the capture is armed.
		scanner := bufio.NewScanner(stderr)
		armed := false
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "listening on") {
				armed = true
				break
			}
		}
		if !armed {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return fmt.Errorf("tcpdump on %s exited before listening", iface)
		}
		go func() { _, _ = bufio.NewReader(stderr).WriteTo(discard{}) }()
		c.captures[iface] = cmd
	}
	c.name = name
	return nil
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func (c *twoNICControl) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for iface, cmd := range c.captures {
		if c.cfg.Lane == twoNICLaneL {
			_ = c.hostCommand("sudo", "-n", "pkill", "-INT", "-f", c.pcapPath(c.name, iface)).Run()
		} else {
			_ = cmd.Process.Signal(os.Interrupt)
		}
		_ = cmd.Wait()
		if c.cfg.Lane == twoNICLaneL {
			_ = exec.Command("scp", c.cfg.Remote+":"+c.pcapPath(c.name, iface),
				filepath.Join(c.cfg.RunDir, "captures", c.name+"-"+iface+".pcap")).Run()
		}
		delete(c.captures, iface)
	}
}

func (c *twoNICControl) count(name, iface, filter string) (int, error) {
	argv := []string{"tcpdump", "-n", "-r", c.pcapPath(name, iface)}
	if strings.TrimSpace(filter) != "" {
		argv = append(argv, strings.Fields(filter)...)
	}
	output, err := c.hostCommand(argv...).Output()
	if err != nil {
		return 0, fmt.Errorf("read %s capture on %s: %w", name, iface, err)
	}
	lines := 0
	for _, line := range strings.Split(string(output), "\n") {
		if strings.TrimSpace(line) != "" {
			lines++
		}
	}
	return lines, nil
}

func (c *twoNICControl) link(iface string, up bool) error {
	if c.cfg.Lane == twoNICLaneM {
		if iface != c.cfg.WifiDevice {
			return fmt.Errorf("lane M toggles only the Wi-Fi device %s", c.cfg.WifiDevice)
		}
		state := "off"
		if up {
			state = "on"
		}
		return exec.Command("sudo", "-n", "networksetup", "-setairportpower", iface, state).Run()
	}
	if up {
		return errors.New("lane L brings a link back inside the same remote command; post {up:false} only")
	}
	return c.hostCommand("sudo", "-n", "sh", "-c", twoNICLinkCycleScript(iface, c.cfg.WeaverPort)).Run()
}

// twoNICLinkCycleScript takes a link down and brings it back once the local
// Weaver reports an egress on it DOWN. The SSH session may ride the same link,
// so the whole cycle is one remote command.
func twoNICLinkCycleScript(iface string, weaverPort int) string {
	query := `{"query":"{ egressInterfaces { interfaceName health } }"}`
	return fmt.Sprintf(`ip link set dev %[1]s down
until curl -s -H 'content-type: application/json' -d %[2]s http://127.0.0.1:%[3]d/graphql | grep -q '"interfaceName":"%[1]s","health":"DOWN"'; do sleep 0.2; done
ip link set dev %[1]s up`, iface, shellQuote(query), weaverPort)
}
