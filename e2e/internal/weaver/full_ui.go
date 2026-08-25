package weaver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type fullPhaseContext struct {
	Name             string
	Command          string
	SeedProfile      string
	SkipSeed         bool
	Datastore        string
	Slug             string
	ExtraEnv         map[string]string
	WeaverBin        string
	Project          string
	RootDir          string
	FixturesDir      string
	RunDir           string
	RuntimePortsFile string
	RuntimePorts     runtimePortState
	LogTail          *lineTail
	Cleaned          bool
	cleanMu          sync.Mutex
}

type fullPhaseDefinition struct {
	name        string
	command     string
	slug        string
	seedProfile string
	skipSeed    bool
	datastore   weaverDatastore
	// Extra environment for this phase only. It reaches the Weaver process
	// because the phase subprocess launches it with its own environment, so a
	// WEAVER_* key set here is a product-level toggle for the whole phase.
	extraEnv map[string]string
}

type fullSuiteOptions struct {
	logLabel       string
	summaryLabel   string
	dashboardTitle string
	includePhase   func(fullPhaseDefinition) bool
}

var fullPhaseDefinitions = []fullPhaseDefinition{
	// Direct-store routing is on for the functional corpus rather than in a
	// phase of its own. It began as a duplicate phase re-running all 92 fixtures
	// with the gate flipped, which cost a second full run to learn very little:
	// most of the corpus is archives direct-store is *right* to refuse, so the
	// phase mostly measured refusals. The corpus now carries `direct-store-*`
	// fixtures built to route direct, the gate is on for every functional run,
	// and `assertDirectStoreEngagement` asserts from weaver's own counters that
	// those sets really did route — the only external evidence available, since
	// direct output is byte-identical to the conventional path.
	{name: "Functional SQLite", command: "test-all", slug: "functional-sqlite", seedProfile: "functional", datastore: weaverDatastoreSQLite, extraEnv: map[string]string{"WEAVER_RAR_DIRECT_STORE": "1"}},
	{name: "Functional Postgres", command: "test-all", slug: "functional-postgres", seedProfile: "functional", datastore: weaverDatastorePostgres, extraEnv: map[string]string{"WEAVER_RAR_DIRECT_STORE": "1"}},
	{name: "NNTP Chaos", command: "chaos-test", slug: "nntp-chaos", seedProfile: "chaos", datastore: weaverDatastoreSQLite},
	{name: "TCP Chaos", command: "tcp-chaos", slug: "tcp-chaos", seedProfile: "tcp-chaos", datastore: weaverDatastoreSQLite},
	{name: "Container Restart", command: "container-restart", slug: "container-restart", skipSeed: true, datastore: weaverDatastoreSQLite},
	{name: "Restart SQLite", command: "restart-all", slug: "restart-sqlite", seedProfile: "restart", datastore: weaverDatastoreSQLite},
	{name: "Restart Postgres", command: "restart-all", slug: "restart-postgres", seedProfile: "restart", datastore: weaverDatastorePostgres},
	{name: "Product Behavior Gate", command: "release-gate", slug: "product-behavior", skipSeed: true, datastore: weaverDatastoreSQLite},
}

func functionalFullPhase(def fullPhaseDefinition) bool {
	return def.seedProfile == "functional"
}

// fullPhaseFixtureProfiles is what the selected phases will seed: each seeding
// phase's profile, plus the release gate's own corpus profile when that phase
// runs, since its flows seed probe fixtures of their own.
func fullPhaseFixtureProfiles(phases []*fullPhaseContext) []string {
	var profiles []string
	for _, phase := range phases {
		if phase == nil {
			continue
		}
		if !phase.SkipSeed && strings.TrimSpace(phase.SeedProfile) != "" {
			profiles = append(profiles, phase.SeedProfile)
		}
		if phase.Command == "release-gate" {
			profiles = append(profiles, "release-gate")
		}
	}
	return uniqueSorted(profiles)
}

func fullPhaseNeedsLocalWeaverImage(phase *fullPhaseContext) bool {
	return phase != nil && (phase.Command == "container-restart" || phase.Command == "release-gate")
}

// Start the one Linux image preparation in the parent process. Container
// phases are separate child processes and would otherwise race to rebuild the
// same fixed tag when a source fingerprint changes. Other phases keep seeding
// and waiting on the native release build while Docker/BuildKit works.
func prepareFullSuiteWeaverImage(phases []*fullPhaseContext) func() error {
	return prepareFullSuiteWeaverImageWith(phases, ensureLocalWeaverImage)
}

func prepareFullSuiteWeaverImageWith(phases []*fullPhaseContext, prepare func() error) func() error {
	needed := false
	for _, phase := range phases {
		if fullPhaseNeedsLocalWeaverImage(phase) {
			needed = true
			break
		}
	}
	if !needed {
		return nil
	}

	ready := make(chan struct{})
	var imageErr error
	go func() {
		imageErr = prepare()
		close(ready)
	}()
	return func() error {
		<-ready
		return imageErr
	}
}

type lineTail struct {
	limit int
	mu    sync.Mutex
	lines []string
}

func (t *lineTail) Add(line string) {
	line = strings.TrimSpace(line)
	if line == "" || t.limit <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.lines) < t.limit {
		t.lines = append(t.lines, line)
		return
	}
	copy(t.lines, t.lines[1:])
	t.lines[len(t.lines)-1] = line
}

func (t *lineTail) Lines() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.lines...)
}

type childRunResult struct {
	Phase    string
	Command  string
	Duration time.Duration
	Err      error
}

type phaseRunStatus struct {
	Phase            string         `json:"phase"`
	Command          string         `json:"command"`
	Datastore        string         `json:"datastore"`
	Project          string         `json:"project"`
	RootDir          string         `json:"root_dir"`
	RunDir           string         `json:"run_dir"`
	RuntimePortsFile string         `json:"runtime_ports_file"`
	LogPath          string         `json:"log_path"`
	PID              int            `json:"pid"`
	StartedAt        time.Time      `json:"started_at"`
	FinishedAt       time.Time      `json:"finished_at,omitempty"`
	Duration         time.Duration  `json:"duration,omitempty"`
	Status           string         `json:"status"`
	LastEventAt      time.Time      `json:"last_event_at,omitempty"`
	LastEvent        *progressEvent `json:"last_event,omitempty"`
	LastLogLine      string         `json:"last_log_line,omitempty"`
	Error            string         `json:"error,omitempty"`
}

type phaseRunRecorder struct {
	path  string
	mu    sync.Mutex
	state phaseRunStatus
}

type fullRunManifest struct {
	OwnerPID  int                 `json:"owner_pid"`
	StartedAt time.Time           `json:"started_at"`
	TempRoot  string              `json:"temp_root"`
	Phases    []fullRunPhaseEntry `json:"phases"`
}

type fullRunPhaseEntry struct {
	Name             string `json:"name"`
	Datastore        string `json:"datastore"`
	Project          string `json:"project"`
	RootDir          string `json:"root_dir"`
	RunDir           string `json:"run_dir"`
	RuntimePortsFile string `json:"runtime_ports_file"`
}

type dashboardBar struct {
	Label   string
	Current int
	Total   int
	Status  string
	Detail  string
}

type fullDashboard struct {
	interactive bool
	title       string
	start       time.Time
	seed        dashboardBar
	cache       dashboardBar
	phases      map[string]*dashboardBar
	order       []string
	// Sub-bars for phases that fan out into named flows — the release gate runs
	// one flow per datastore — keyed by phase name, then flow key.
	flows       map[string]map[string]*dashboardBar
	flowOrder   map[string][]string
	seedByPhase map[string]int
	cacheByKey  map[string]int
	cacheTotals map[string]int
	cacheWarn   map[string]bool
	dirty       bool
	lastFrame   string
	stopCh      chan struct{}
	doneCh      chan struct{}
	mu          sync.Mutex
}

type dashboardLayout struct {
	labelWidth    int
	progressWidth int
}

const dashboardRenderInterval = 100 * time.Millisecond

const (
	dashboardMinLabelWidth = 11
	dashboardProgressWidth = 26
)

const (
	ansiReset     = "\x1b[0m"
	ansiBold      = "\x1b[1m"
	ansiDim       = "\x1b[2m"
	ansiCyan      = "\x1b[36m"
	ansiBlue      = "\x1b[34m"
	ansiGreen     = "\x1b[32m"
	ansiYellow    = "\x1b[33m"
	ansiRed       = "\x1b[31m"
	ansiMagenta   = "\x1b[35m"
	ansiBrightBlk = "\x1b[90m"
)

func newFullDashboard(title string, phaseNames []string, seedTotal int) *fullDashboard {
	d := &fullDashboard{
		interactive: isInteractiveTerminal(),
		title:       title,
		start:       time.Now(),
		seed: dashboardBar{
			Label:  "Seeding",
			Total:  seedTotal,
			Status: "running",
			Detail: "isolated stacks",
		},
		cache: dashboardBar{
			Label:  "NNTP Cache",
			Status: "waiting",
			Detail: "checking fingerprints",
		},
		phases:      make(map[string]*dashboardBar, len(phaseNames)),
		order:       append([]string(nil), phaseNames...),
		flows:       make(map[string]map[string]*dashboardBar, len(phaseNames)),
		flowOrder:   make(map[string][]string, len(phaseNames)),
		seedByPhase: make(map[string]int, len(phaseNames)),
		cacheByKey:  make(map[string]int),
		cacheTotals: make(map[string]int),
		cacheWarn:   make(map[string]bool),
		dirty:       true,
	}
	for _, name := range phaseNames {
		d.phases[name] = &dashboardBar{Label: name, Status: "waiting", Detail: "queued"}
	}
	if d.interactive {
		d.stopCh = make(chan struct{})
		d.doneCh = make(chan struct{})
		fmt.Fprint(os.Stdout, "\x1b[?25l")
		d.renderLocked()
		go d.renderLoop()
	}
	return d
}

func (d *fullDashboard) Close() {
	if !d.interactive {
		return
	}
	close(d.stopCh)
	<-d.doneCh
	d.mu.Lock()
	defer d.mu.Unlock()
	fmt.Fprint(os.Stdout, "\x1b[?25h\n")
}

func (d *fullDashboard) renderLoop() {
	ticker := time.NewTicker(dashboardRenderInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.mu.Lock()
			d.renderFrameLocked()
			d.mu.Unlock()
		case <-d.stopCh:
			d.mu.Lock()
			d.renderFrameLocked()
			d.mu.Unlock()
			close(d.doneCh)
			return
		}
	}
}

func (d *fullDashboard) updateSeed(phase string, event progressEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if event.Total > 0 && d.seed.Total == 0 {
		d.seed.Total = event.Total
	}
	switch event.Kind {
	case "seed_total":
		// The full suite precomputes the aggregate seed total across phases.
		// Keep that stable here instead of re-inflating it from per-phase totals.
	case "seed_progress":
		d.seedByPhase[phase] = event.Current
		total := 0
		for _, current := range d.seedByPhase {
			total += current
		}
		d.seed.Current = total
		d.seed.Status = "running"
		d.seed.Detail = fmt.Sprintf("%s: %s", phase, event.Detail)
		if phaseBar, ok := d.phases[phase]; ok && (phaseBar.Status == "waiting" || phaseBar.Status == "seeding") {
			phaseBar.Status = "seeding"
			phaseBar.Detail = event.Detail
		}
	case "seed_done":
		d.seedByPhase[phase] = event.Current
		total := 0
		for _, current := range d.seedByPhase {
			total += current
		}
		d.seed.Current = total
		if event.Status == "fail" {
			d.seed.Status = "fail"
		} else if total >= d.seed.Total {
			d.seed.Status = "pass"
		}
	}
	d.scheduleRenderLocked()
}

func (d *fullDashboard) setPhaseRunning(name string, detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if phase, ok := d.phases[name]; ok {
		phase.Status = "running"
		phase.Detail = detail
	}
	d.scheduleRenderLocked()
}

func (d *fullDashboard) setSeedDetail(status, detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if strings.TrimSpace(status) != "" {
		d.seed.Status = status
	}
	if strings.TrimSpace(detail) != "" {
		d.seed.Detail = detail
	}
	d.scheduleRenderLocked()
}

func (d *fullDashboard) noteNntpCacheHit(profile string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cache.Total == 0 && d.cache.Status != "warning" {
		d.cache.Status = "pass"
		d.cache.Detail = profile + ": current"
		d.scheduleRenderLocked()
	}
}

func (d *fullDashboard) updateNntpCache(key, profile string, current, total int, status, detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if total > 0 {
		d.cacheTotals[key] = total
		d.cacheByKey[key] = current
	}
	if status == "warning" {
		d.cacheWarn[key] = true
	}

	d.cache.Current = 0
	d.cache.Total = 0
	for cacheKey, cacheTotal := range d.cacheTotals {
		d.cache.Total += cacheTotal
		d.cache.Current += d.cacheByKey[cacheKey]
	}
	switch {
	case len(d.cacheWarn) > 0:
		d.cache.Status = "warning"
	case d.cache.Total > 0 && d.cache.Current >= d.cache.Total:
		d.cache.Status = "pass"
	case d.cache.Total > 0:
		d.cache.Status = "running"
	case strings.TrimSpace(status) != "":
		d.cache.Status = status
	}
	if strings.TrimSpace(detail) != "" {
		d.cache.Detail = profile + ": " + detail
	}
	d.scheduleRenderLocked()
}

func (d *fullDashboard) updatePhase(name string, event progressEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()

	phase, ok := d.phases[name]
	if !ok {
		return
	}

	switch event.Kind {
	case "phase_total":
		if event.Total > 0 {
			phase.Total = event.Total
		}
		if event.Detail != "" {
			phase.Detail = event.Detail
		}
		phase.Status = "running"
	case "phase_note":
		if event.Detail != "" {
			phase.Detail = event.Detail
		}
	case "phase_progress":
		if event.Total > 0 {
			phase.Total = event.Total
		}
		if event.Current > phase.Current {
			phase.Current = event.Current
		}
		if event.Detail != "" {
			phase.Detail = event.Detail
		}
		phase.Status = "running"
	case "phase_done":
		if event.Total > 0 {
			phase.Total = event.Total
		}
		if event.Current > phase.Current {
			phase.Current = event.Current
		}
		if event.Status != "" {
			phase.Status = event.Status
		}
	case "flow_pending":
		if bar := d.flowBarLocked(name, event.Name); bar != nil && event.Detail != "" {
			bar.Detail = event.Detail
		}
	case "flow_start":
		if bar := d.flowBarLocked(name, event.Name); bar != nil {
			bar.Status = "running"
			bar.Current = 0
			// Clear "queued"; the status column already says RUNNING, and the
			// duration replaces this when the flow lands.
			bar.Detail = event.Detail
		}
	case "flow_done":
		if bar := d.flowBarLocked(name, event.Name); bar != nil {
			bar.Status = event.Status
			if bar.Status == "" {
				bar.Status = "pass"
			}
			bar.Current = bar.Total
			bar.Detail = event.Detail
		}
	}
	d.scheduleRenderLocked()
}

// flowBarLocked returns the sub-bar for a flow under `phase`, creating it on
// first sight so a phase that discovers its flows at runtime still lists them
// in arrival order.
func (d *fullDashboard) flowBarLocked(phase, flow string) *dashboardBar {
	flow = strings.TrimSpace(flow)
	if flow == "" {
		return nil
	}
	bars, ok := d.flows[phase]
	if !ok {
		bars = make(map[string]*dashboardBar)
		d.flows[phase] = bars
	}
	if bar, ok := bars[flow]; ok {
		return bar
	}
	// Each flow is a single unit of work: it is queued, then it runs, then it
	// lands. A one-step total gives an empty/full bar rather than a fake
	// mid-progress fill the child cannot actually report.
	bar := &dashboardBar{Label: "  " + flow, Total: 1, Status: "waiting", Detail: "queued"}
	bars[flow] = bar
	d.flowOrder[phase] = append(d.flowOrder[phase], flow)
	return bar
}

func (d *fullDashboard) markPhaseResult(name, status string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if phase, ok := d.phases[name]; ok {
		phase.Status = status
		if phase.Total > 0 && phase.Current < phase.Total {
			phase.Current = phase.Total
		}
	}
	d.scheduleRenderLocked()
}

func (d *fullDashboard) renderLocked() {
	if !d.interactive {
		return
	}

	d.dirty = true
	d.renderFrameLocked()
}

func (d *fullDashboard) renderFrameLocked() {
	frame := d.buildFrameLocked()
	if frame == d.lastFrame {
		d.dirty = false
		return
	}
	fmt.Fprint(os.Stdout, frame)
	d.lastFrame = frame
	d.dirty = false
}

func (d *fullDashboard) scheduleRenderLocked() {
	if !d.interactive {
		return
	}
	d.dirty = true
}

func (d *fullDashboard) buildFrameLocked() string {
	var b strings.Builder
	b.WriteString("\x1b[H\x1b[J")
	layout := d.dashboardLayoutLocked()
	elapsed := time.Since(d.start).Truncate(time.Second)
	title := d.title
	if title == "" {
		title = "weaver e2e full"
	}
	if d.interactive {
		title = ansiWrap(title, ansiBold, ansiCyan)
	}
	elapsedText := fmt.Sprintf("elapsed %s", elapsed)
	if d.interactive {
		elapsedText = ansiWrap(elapsedText, ansiDim, ansiBrightBlk)
	}
	b.WriteString(fmt.Sprintf("%s   %s\n\n", title, elapsedText))
	b.WriteString(renderDashboardBar(d.seed, d.interactive, layout))
	b.WriteString("\n")
	b.WriteString(renderDashboardBar(d.cache, d.interactive, layout))
	b.WriteString("\n")
	for _, name := range d.order {
		b.WriteString(renderDashboardBar(*d.phases[name], d.interactive, layout))
		b.WriteString("\n")
		for _, flow := range d.flowOrder[name] {
			b.WriteString(renderDashboardBar(*d.flows[name][flow], d.interactive, layout))
			b.WriteString("\n")
		}
	}
	return b.String()
}

func (d *fullDashboard) dashboardLayoutLocked() dashboardLayout {
	labelWidth := dashboardMinLabelWidth
	if width := len(d.seed.Label); width > labelWidth {
		labelWidth = width
	}
	if width := len(d.cache.Label); width > labelWidth {
		labelWidth = width
	}
	for _, name := range d.order {
		phase := d.phases[name]
		if phase == nil {
			continue
		}
		if width := len(phase.Label); width > labelWidth {
			labelWidth = width
		}
		// Flow sub-bars are indented, so their labels are the widest entries
		// once a fanned-out phase starts; without them every bar shifts right
		// the moment the first flow appears.
		for _, flow := range d.flowOrder[name] {
			if bar := d.flows[name][flow]; bar != nil {
				if width := len(bar.Label); width > labelWidth {
					labelWidth = width
				}
			}
		}
	}
	return dashboardLayout{
		labelWidth:    labelWidth,
		progressWidth: dashboardProgressWidth,
	}
}

func renderDashboardBar(bar dashboardBar, interactive bool, layout dashboardLayout) string {
	width := layout.progressWidth
	if width <= 0 {
		width = dashboardProgressWidth
	}
	labelWidth := layout.labelWidth
	if labelWidth < dashboardMinLabelWidth {
		labelWidth = dashboardMinLabelWidth
	}

	current := bar.Current
	total := bar.Total
	if total > 0 && current > total {
		current = total
	}

	status := bar.Status
	if status == "" {
		status = "waiting"
	}
	displayStatus := statusDisplay(status)

	var progress string
	switch {
	case total <= 0:
		progress = "[" + strings.Repeat(".", width) + "]"
	default:
		filled := width * current / total
		if filled < 0 {
			filled = 0
		}
		if filled > width {
			filled = width
		}
		switch {
		case filled <= 0:
			progress = "[" + strings.Repeat(".", width) + "]"
		case filled >= width:
			progress = "[" + strings.Repeat("=", width) + "]"
		default:
			progress = "[" + strings.Repeat("=", filled-1) + ">" + strings.Repeat(".", width-filled) + "]"
		}
	}

	countText := "--/--"
	if total > 0 {
		countText = fmt.Sprintf("%d/%d", current, total)
	}

	detail := strings.TrimSpace(bar.Detail)
	if detail == "" {
		detail = "-"
	}

	labelText := fmt.Sprintf("%-*s", labelWidth, bar.Label)
	countBlock := fmt.Sprintf("%-10s", countText)
	statusBlock := fmt.Sprintf("%-8s", displayStatus)

	if interactive {
		labelText = ansiWrap(labelText, ansiBold, labelColor(bar.Label))
		progress = ansiWrap(progress, progressColor(current, total, status))
		countBlock = ansiWrap(countBlock, ansiDim, ansiBrightBlk)
		statusBlock = ansiWrap(statusBlock, ansiBold, statusColor(status))
		detail = ansiWrap(detail, ansiDim)
	}

	return fmt.Sprintf("%s %s %s %s %s", labelText, progress, countBlock, statusBlock, detail)
}

func ansiWrap(text string, codes ...string) string {
	if text == "" || len(codes) == 0 {
		return text
	}
	var b strings.Builder
	for _, code := range codes {
		b.WriteString(code)
	}
	b.WriteString(text)
	b.WriteString(ansiReset)
	return b.String()
}

func statusColor(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pass", "complete":
		return ansiGreen
	case "fail", "failed", "error", "timeout":
		return ansiRed
	case "warning":
		return ansiYellow
	case "running", "seeding":
		return ansiCyan
	case "waiting":
		return ansiBrightBlk
	default:
		return ansiMagenta
	}
}

func progressColor(current, total int, status string) string {
	if total > 0 && current >= total {
		return ansiGreen
	}
	if strings.EqualFold(strings.TrimSpace(status), "pass") || strings.EqualFold(strings.TrimSpace(status), "complete") {
		return ansiGreen
	}
	return ansiBrightBlk
}

func statusDisplay(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pass", "complete":
		return "PASS"
	case "fail", "failed", "error", "timeout":
		return "FAIL"
	case "warning":
		return "WARN"
	case "running":
		return "RUNNING"
	case "seeding":
		return "SEEDING"
	case "waiting":
		return "WAIT"
	default:
		return strings.ToUpper(strings.TrimSpace(status))
	}
}

func labelColor(label string) string {
	switch strings.ToLower(strings.TrimSpace(label)) {
	case "seeding", "nntp cache":
		return ansiBlue
	case "functional", "functional sqlite", "functional postgres":
		return ansiCyan
	case "nntp chaos":
		return ansiYellow
	case "tcp chaos":
		return ansiMagenta
	case "container restart", "restart", "restart sqlite", "restart postgres":
		return ansiGreen
	default:
		return ansiBold
	}
}

func isInteractiveTerminal() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	return strings.TrimSpace(strings.ToLower(os.Getenv("TERM"))) != "dumb"
}

func runParallelFullSuite() {
	runParallelFullSuiteWithOptions(fullSuiteOptions{
		logLabel:       "full e2e suite",
		summaryLabel:   "FULL SUITE",
		dashboardTitle: "weaver e2e full",
	})
}

func runFunctionalFullSuite() {
	runParallelFullSuiteWithOptions(fullSuiteOptions{
		logLabel:       "functional e2e suite",
		summaryLabel:   "FUNCTIONAL SUITE",
		dashboardTitle: "weaver e2e functional",
		includePhase:   functionalFullPhase,
	})
}

func runParallelFullSuiteWithOptions(options fullSuiteOptions) {
	if options.logLabel == "" {
		options.logLabel = "full e2e suite"
	}
	if options.summaryLabel == "" {
		options.summaryLabel = "FULL SUITE"
	}
	if options.dashboardTitle == "" {
		options.dashboardTitle = "weaver e2e full"
	}
	log.Printf("starting %s", options.logLabel)

	if err := cleanupAbandonedFullRuns(); err != nil {
		log.Printf("warning: cleanup abandoned full runs: %v", err)
	}

	tempRoot, err := os.MkdirTemp("", "weaver-e2e-full-")
	if err != nil {
		log.Fatalf("create full-suite temp root: %v", err)
	}

	phases, err := newFullPhaseContextsFor(tempRoot, options.includePhase)
	if err != nil {
		log.Fatalf("prepare full-suite contexts: %v", err)
	}
	if len(phases) == 0 {
		log.Fatalf("prepare full-suite contexts: no phases selected")
	}

	manifestPath := filepath.Join(tempRoot, "full-run.json")
	if err := writeFullRunManifest(manifestPath, tempRoot, phases); err != nil {
		log.Fatalf("write full-run manifest: %v", err)
	}

	// Fixtures first, before the dashboard takes the terminal: a fetch or a
	// generation is minutes to an hour of plain log lines, and every phase
	// below seeds from the tree it produces.
	ensureFixtureProfiles(fullPhaseFixtureProfiles(phases)...)

	seedableCount, err := countSeedableFixtures(phases)
	if err != nil {
		log.Fatalf("count seedable fixtures: %v", err)
	}

	phaseNames := make([]string, 0, len(phases))
	for _, phase := range phases {
		phaseNames = append(phaseNames, phase.Name)
	}

	dashboard := newFullDashboard(options.dashboardTitle, phaseNames, seedableCount)
	defer dashboard.Close()
	dashboard.setSeedDetail("running", "preparing images")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := ensureLocalWeaverNNTPImage(); err != nil {
		log.Fatalf("prepare NNTP fixture image for full suite: %v", err)
	}
	if err := ensureNyuuImageBuilt(); err != nil {
		log.Fatalf("prepare nyuu image for full suite: %v", err)
	}
	dashboard.setSeedDetail("running", "seeding while weaver e2e build runs")
	go func() {
		if _, err := ensureE2EWeaverBinary(); err != nil {
			log.Printf("warning: background weaver e2e build failed: %v", err)
		}
	}()
	awaitWeaverImage := prepareFullSuiteWeaverImage(phases)

	keepStacks := envBool("E2E_KEEP_STACKS", false)
	var cleanupErrors []error
	cacheWarmer := newNntpSeedCacheWarmer(ctx, tempRoot, dashboard)

	seedResults, phaseResults := runFullPipeline(ctx, phases, dashboard, keepStacks, awaitWeaverImage, cacheWarmer)
	seedFailed := false
	for _, result := range seedResults {
		if result.Err != nil {
			seedFailed = true
		}
	}
	if ctx.Err() != nil {
		seedFailed = true
	}
	if seedFailed {
		dashboard.mu.Lock()
		dashboard.seed.Status = "fail"
		dashboard.renderLocked()
		dashboard.mu.Unlock()
		cleanupErrors = cleanupFullPhaseContexts(phases, keepStacks)
		printFullSummary(options.summaryLabel, phases, nil, cleanupErrors, tempRoot, seedResults, true, true)
		os.Exit(1)
	}

	failed := false
	for _, result := range phaseResults {
		if result.Err != nil {
			failed = true
		}
	}
	if ctx.Err() != nil {
		failed = true
	}

	cleanupErrors = cleanupFullPhaseContexts(phases, keepStacks)
	printFullSummary(options.summaryLabel, phases, phaseResults, cleanupErrors, tempRoot, seedResults, false, keepStacks || failed || len(cleanupErrors) > 0)
	if !keepStacks && !failed && len(cleanupErrors) == 0 {
		_ = os.Remove(manifestPath)
		_ = os.RemoveAll(tempRoot)
	}
	if failed {
		os.Exit(1)
	}
}

func newFullPhaseContexts(tempRoot string) ([]*fullPhaseContext, error) {
	return newFullPhaseContextsFor(tempRoot, nil)
}

// phaseSlugFilter narrows a run to named phase slugs, comma-separated, via
// E2E_FULL_PHASE_SLUGS. Iterating on one phase otherwise means running every
// phase of its profile in parallel and reading the one you care about out of
// the noise.
func phaseSlugFilter() map[string]bool {
	raw := strings.TrimSpace(os.Getenv("E2E_FULL_PHASE_SLUGS"))
	if raw == "" {
		return nil
	}
	wanted := map[string]bool{}
	for _, slug := range strings.Split(raw, ",") {
		if slug = strings.TrimSpace(slug); slug != "" {
			wanted[slug] = true
		}
	}
	if len(wanted) == 0 {
		return nil
	}
	return wanted
}

func newFullPhaseContextsFor(tempRoot string, includePhase func(fullPhaseDefinition) bool) ([]*fullPhaseContext, error) {
	runID := time.Now().Format("20060102-150405")

	contexts := make([]*fullPhaseContext, 0, len(fullPhaseDefinitions))
	for _, def := range fullPhaseDefinitions {
		if wanted := phaseSlugFilter(); wanted != nil && !wanted[def.slug] {
			continue
		}
		if includePhase != nil && !includePhase(def) {
			continue
		}
		rootDir := filepath.Join(tempRoot, def.slug)
		fixturesDir := filepath.Join(rootDir, "fixtures")
		runDir := filepath.Join(rootDir, "run")
		if err := os.MkdirAll(fixturesDir, 0o755); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			return nil, err
		}

		contexts = append(contexts, &fullPhaseContext{
			Name:             def.name,
			Command:          def.command,
			SeedProfile:      def.seedProfile,
			Slug:             def.slug,
			SkipSeed:         def.skipSeed,
			Datastore:        string(def.datastore),
			Project:          sanitizeProjectName(fmt.Sprintf("e2e-%s-%s", runID, def.slug)),
			RootDir:          rootDir,
			FixturesDir:      fixturesDir,
			RunDir:           runDir,
			RuntimePortsFile: filepath.Join(rootDir, "runtime-ports.json"),
			LogTail:          &lineTail{limit: 120},
			ExtraEnv:         def.extraEnv,
		})
	}
	states, err := allocateRuntimePortStates(len(contexts))
	if err != nil {
		return nil, err
	}
	for i, phase := range contexts {
		phase.RuntimePorts = states[i]
		if err := saveRuntimePortState(phase.RuntimePortsFile, states[i]); err != nil {
			return nil, fmt.Errorf("write %s runtime ports: %w", phase.Name, err)
		}
	}

	return contexts, nil
}

func countSeedableFixtures(phases []*fullPhaseContext) (int, error) {
	total := 0
	for _, phase := range phases {
		if phase.SkipSeed {
			continue
		}
		total += len(fixtureSlugsForSeedProfile(phase.SeedProfile))
	}
	return total, nil
}

func runFullPipeline(
	ctx context.Context,
	phases []*fullPhaseContext,
	dashboard *fullDashboard,
	keepStacks bool,
	awaitWeaverImage func() error,
	cacheWarmer *nntpSeedCacheWarmer,
) ([]childRunResult, []childRunResult) {
	seedResults := make(chan childRunResult, len(phases))
	phaseResults := make(chan childRunResult, len(phases))

	var wg sync.WaitGroup
	for _, phase := range phases {
		wg.Add(1)
		go func(phase *fullPhaseContext) {
			defer wg.Done()
			var cacheJob *nntpSeedCacheWarmJob
			ownsCacheWarm := false
			defer func() {
				if ownsCacheWarm {
					if err := cacheJob.wait(); err != nil {
						phase.LogTail.Add("NNTP cache warning: " + err.Error())
					}
				}
				if !keepStacks {
					if err := cleanupFullPhaseContext(phase); err != nil {
						phase.LogTail.Add("cleanup error: " + err.Error())
					}
				}
			}()

			if !phase.SkipSeed {
				seedResult := runSelfWithEnv(ctx, phase, "seed-all", func(event progressEvent) {
					dashboard.updateSeed(phase.Name, event)
				})
				seedResults <- seedResult
				if seedResult.Err != nil {
					dashboard.markPhaseResult(phase.Name, "fail")
					return
				}
				cacheJob, ownsCacheWarm = cacheWarmer.start(phase)
			}

			setupStarted := time.Now()
			weaverBin, err := ensureE2EWeaverBinary()
			if err != nil {
				setupErr := fmt.Errorf("prepare e2e weaver binary: %w", err)
				phaseResults <- recordPhaseSetupFailure(phase, phase.Command, setupStarted, setupErr)
				dashboard.markPhaseResult(phase.Name, "fail")
				return
			}
			phase.WeaverBin = weaverBin
			if fullPhaseNeedsLocalWeaverImage(phase) && awaitWeaverImage != nil {
				setupStarted = time.Now()
				if err := awaitWeaverImage(); err != nil {
					setupErr := fmt.Errorf("prepare local Weaver image: %w", err)
					phaseResults <- recordPhaseSetupFailure(phase, phase.Command, setupStarted, setupErr)
					dashboard.markPhaseResult(phase.Name, "fail")
					return
				}
			}

			dashboard.setPhaseRunning(phase.Name, "booting")
			phaseResult := runSelfWithEnv(ctx, phase, phase.Command, func(event progressEvent) {
				dashboard.updatePhase(phase.Name, event)
			})
			phaseResults <- phaseResult
			if phaseResult.Err != nil {
				dashboard.markPhaseResult(phase.Name, "fail")
			} else {
				dashboard.markPhaseResult(phase.Name, "pass")
			}
		}(phase)
	}

	wg.Wait()
	close(seedResults)
	close(phaseResults)

	var seedOut []childRunResult
	for result := range seedResults {
		seedOut = append(seedOut, result)
	}

	var phaseOut []childRunResult
	for result := range phaseResults {
		phaseOut = append(phaseOut, result)
	}
	return seedOut, phaseOut
}

func recordPhaseSetupFailure(
	phase *fullPhaseContext,
	command string,
	startedAt time.Time,
	setupErr error,
) childRunResult {
	duration := time.Since(startedAt).Round(time.Second)
	logLine := setupErr.Error()
	phase.LogTail.Add(logLine)

	logPath := filepath.Join(phase.RootDir, command+".log")
	statusPath := filepath.Join(phase.RootDir, command+".status.json")
	recorder, err := newPhaseRunRecorderAt(statusPath, phase, command, logPath, startedAt)
	if err != nil {
		return childRunResult{
			Phase:    phase.Name,
			Command:  command,
			Duration: duration,
			Err:      errors.Join(setupErr, fmt.Errorf("record phase setup failure: %w", err)),
		}
	}

	recordedErr := setupErr
	if err := os.WriteFile(logPath, []byte(logLine+"\n"), 0o644); err != nil {
		recordedErr = errors.Join(setupErr, fmt.Errorf("write phase setup failure log: %w", err))
	}
	recorder.UpdateLogLine(logLine)
	recorder.Finish("fail", duration, recordedErr)

	return childRunResult{
		Phase:    phase.Name,
		Command:  command,
		Duration: duration,
		Err:      recordedErr,
	}
}

func runSelfWithEnv(ctx context.Context, phase *fullPhaseContext, command string, onEvent func(progressEvent)) childRunResult {
	start := time.Now()

	exe, err := os.Executable()
	if err != nil {
		return childRunResult{Phase: phase.Name, Command: command, Err: fmt.Errorf("resolve current executable: %w", err)}
	}

	logPath := filepath.Join(phase.RootDir, command+".log")
	statusPath := filepath.Join(phase.RootDir, command+".status.json")
	recorder, err := newPhaseRunRecorder(statusPath, phase, command, logPath)
	if err != nil {
		return childRunResult{Phase: phase.Name, Command: command, Err: fmt.Errorf("create %s recorder: %w", command, err)}
	}

	logFile, err := os.Create(logPath)
	if err != nil {
		return childRunResult{Phase: phase.Name, Command: command, Err: fmt.Errorf("create %s log: %w", command, err)}
	}
	defer logFile.Close()

	cmd := exec.CommandContext(ctx, exe, command)
	cmd.Dir = e2eDir()
	cmd.Env = mergeChildEnv(os.Environ(), phase.env())

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return childRunResult{Phase: phase.Name, Command: command, Err: fmt.Errorf("stdout pipe: %w", err)}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return childRunResult{Phase: phase.Name, Command: command, Err: fmt.Errorf("stderr pipe: %w", err)}
	}
	if err := cmd.Start(); err != nil {
		return childRunResult{Phase: phase.Name, Command: command, Err: fmt.Errorf("start %s: %w", command, err)}
	}
	recorder.SetPID(cmd.Process.Pid)

	var wg sync.WaitGroup
	var logMu sync.Mutex
	wg.Add(2)
	go streamChildOutput(stdout, logFile, &logMu, phase.LogTail, recorder, onEvent, &wg)
	go streamChildOutput(stderr, logFile, &logMu, phase.LogTail, recorder, onEvent, &wg)

	// Drain before reaping, not after. `cmd.Wait` closes the pipes returned by
	// StdoutPipe/StderrPipe as soon as it sees the child exit, so waiting first
	// races the readers and silently drops whatever the child wrote last — the
	// exact opposite of what a diagnostic log is for. That window swallowed a
	// phase's closing `phase_done` event and its direct-store counter line,
	// which read as "the code never ran" rather than "the output was lost".
	// The scanners end on EOF once the child exits, so this cannot hang on a
	// process that has already terminated.
	wg.Wait()
	waitErr := cmd.Wait()
	status := "pass"
	if waitErr != nil {
		status = "fail"
		if errors.Is(ctx.Err(), context.Canceled) {
			status = "canceled"
		}
	}
	recorder.Finish(status, time.Since(start).Round(time.Second), waitErr)

	return childRunResult{
		Phase:    phase.Name,
		Command:  command,
		Duration: time.Since(start).Round(time.Second),
		Err:      waitErr,
	}
}

func streamChildOutput(reader io.Reader, logFile *os.File, logMu *sync.Mutex, tail *lineTail, recorder *phaseRunRecorder, onEvent func(progressEvent), wg *sync.WaitGroup) {
	defer wg.Done()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if logFile != nil {
			logMu.Lock()
			_, _ = fmt.Fprintln(logFile, line)
			logMu.Unlock()
		}
		if event, ok := parseProgressEventLine(line); ok {
			if recorder != nil {
				recorder.UpdateEvent(event)
			}
			if onEvent != nil {
				onEvent(event)
			}
			continue
		}
		tail.Add(line)
		if recorder != nil {
			recorder.UpdateLogLine(line)
		}
	}
}

const defaultFullSeedJobs = "2"

func fullSeedJobsOverride() (string, bool) {
	if strings.TrimSpace(os.Getenv("E2E_SEED_JOBS")) != "" {
		return "", false
	}
	return defaultFullSeedJobs, true
}

func (p *fullPhaseContext) env() map[string]string {
	datastore := normalizedWeaverDatastoreForPhase(p.Datastore)
	env := map[string]string{
		"E2E_DIR":                e2eDir(),
		"E2E_PROJECT":            p.Project,
		"E2E_SEED_PROFILE":       p.SeedProfile,
		"E2E_WEAVER_DATASTORE":   datastore,
		"FIXTURES_DIR":           p.FixturesDir,
		"E2E_RUN_DIR":            p.RunDir,
		"E2E_RUNTIME_PORTS_FILE": p.RuntimePortsFile,
		"E2E_EVENT_STREAM":       "1",
		nntpSeedImageCaptureEnv:  "0",
	}
	if datastore == string(weaverDatastorePostgres) {
		applyWeaverPostgresPhaseEnv(env)
	}
	if validateRuntimePortState(p.RuntimePorts) == nil {
		for key, value := range runtimePortEnvValues(p.RuntimePorts) {
			env[key] = value
		}
	}
	if strings.TrimSpace(p.WeaverBin) != "" {
		env["WEAVER_BIN"] = p.WeaverBin
	}
	if seedJobs, ok := fullSeedJobsOverride(); ok {
		env["E2E_SEED_JOBS"] = seedJobs
	}
	if nntpSeedImageCacheEnabled() && strings.TrimSpace(p.SeedProfile) != "" {
		if set, err := nntpSeedImageSetForProfile(p.SeedProfile, fixtureSlugsForSeedProfile(p.SeedProfile)); err == nil {
			set.applyToPhaseEnv(env, set.ready())
		}
	}
	if p.Command == "container-restart" {
		env["E2E_WEAVER_ENCRYPTION_KEY"] = ""
	}
	if p.Command == "release-gate" {
		env["E2E_WEAVER_RELEASE_GATE_ROOT"] = filepath.Join(p.RunDir, "release-gate")
	}
	if p.Command == "restart-all" {
		env["E2E_SEED_RETRIES"] = "5"
	}
	if p.Command == "restart-all" && strings.TrimSpace(os.Getenv("E2E_RESTART_PROFILE")) == "" {
		env["E2E_RESTART_PROFILE"] = "hardened"
	}
	// Last, so a phase's own env wins over the defaults above.
	for key, value := range p.ExtraEnv {
		env[key] = value
	}
	return env
}

func mergeChildEnv(base []string, overrides map[string]string) []string {
	envMap := make(map[string]string, len(base)+len(overrides))
	for _, entry := range base {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			continue
		}
		envMap[parts[0]] = parts[1]
	}
	for key, value := range overrides {
		envMap[key] = value
	}
	merged := make([]string, 0, len(envMap))
	for key, value := range envMap {
		merged = append(merged, key+"="+value)
	}
	return merged
}

func cleanupFullPhaseContexts(phases []*fullPhaseContext, keepStacks bool) []error {
	if keepStacks {
		return nil
	}
	var errs []error
	for _, phase := range phases {
		if err := cleanupFullPhaseContext(phase); err != nil {
			errs = append(errs, fmt.Errorf("%s cleanup: %w", phase.Name, err))
			phase.LogTail.Add("cleanup error: " + err.Error())
		}
	}
	return errs
}

func cleanupFullPhaseContext(phase *fullPhaseContext) error {
	phase.cleanMu.Lock()
	defer phase.cleanMu.Unlock()
	if phase.Cleaned {
		return nil
	}
	var errs []error
	if err := capturePhaseDiagnostics(phase); err != nil {
		errs = append(errs, fmt.Errorf("capture diagnostics: %w", err))
	}
	if err := stopManagedLocalWeaverForPhase(phase); err != nil {
		errs = append(errs, fmt.Errorf("stop managed local weaver: %w", err))
	}
	cmd := exec.Command("docker", "compose", "-p", phase.Project, "down", "-v", "--remove-orphans")
	cmd.Dir = e2eDir()
	if err := runExternalCommand(cmd, "docker compose down"); err != nil {
		errs = append(errs, err)
	}
	phase.Cleaned = true
	return errors.Join(errs...)
}

func printFullSummary(
	summaryLabel string,
	phases []*fullPhaseContext,
	phaseResults []childRunResult,
	cleanupErrors []error,
	tempRoot string,
	seedResults []childRunResult,
	seedOnlyFailure bool,
	keepArtifacts bool,
) {
	if summaryLabel == "" {
		summaryLabel = "FULL SUITE"
	}
	fmt.Println()
	fmt.Println(strings.Repeat("=", 70))
	if seedOnlyFailure {
		fmt.Printf("%-14s %-10s %s\n", "STAGE", "RESULT", "DURATION")
		fmt.Println(strings.Repeat("-", 70))
		failed := 0
		for _, result := range seedResults {
			status := "PASS"
			if result.Err != nil {
				status = "FAIL"
				failed++
			}
			fmt.Printf("%-14s %-10s %s\n", result.Phase+" seed", status, result.Duration)
		}
		fmt.Println(strings.Repeat("-", 70))
		fmt.Printf("%s: 0 passed, %d failed during setup\n", summaryLabel, failed)
	} else {
		fmt.Printf("%-14s %-10s %s\n", "PHASE", "RESULT", "DURATION")
		fmt.Println(strings.Repeat("-", 70))
		resultsByName := make(map[string]childRunResult, len(phaseResults))
		for _, result := range phaseResults {
			resultsByName[result.Phase] = result
		}
		failed := 0
		for _, phase := range phases {
			result := resultsByName[phase.Name]
			status := "PASS"
			if result.Err != nil {
				status = "FAIL"
				failed++
			}
			fmt.Printf("%-14s %-10s %s\n", phase.Name, status, result.Duration)
		}
		fmt.Println(strings.Repeat("-", 70))
		fmt.Printf("%s: %d passed, %d failed\n", summaryLabel, len(phases)-failed, failed)
	}

	if len(cleanupErrors) > 0 {
		fmt.Println()
		fmt.Println("Cleanup warnings:")
		for _, err := range cleanupErrors {
			fmt.Printf("  - %v\n", err)
		}
	}

	for _, phase := range phases {
		lines := phase.LogTail.Lines()
		if len(lines) == 0 {
			continue
		}
		if !seedOnlyFailure {
			failed := false
			for _, result := range phaseResults {
				if result.Phase == phase.Name && result.Err != nil {
					failed = true
					break
				}
			}
			if !failed {
				continue
			}
		}
		fmt.Println()
		fmt.Printf("%s log tail:\n", phase.Name)
		start := len(lines) - 12
		if start < 0 {
			start = 0
		}
		for _, line := range lines[start:] {
			fmt.Printf("  %s\n", line)
		}
	}

	if keepArtifacts {
		fmt.Println()
		fmt.Printf("Artifacts kept at %s\n", tempRoot)
	}
}

func newPhaseRunRecorder(path string, phase *fullPhaseContext, command, logPath string) (*phaseRunRecorder, error) {
	return newPhaseRunRecorderAt(path, phase, command, logPath, time.Now())
}

func newPhaseRunRecorderAt(
	path string,
	phase *fullPhaseContext,
	command string,
	logPath string,
	startedAt time.Time,
) (*phaseRunRecorder, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	recorder := &phaseRunRecorder{
		path: path,
		state: phaseRunStatus{
			Phase:            phase.Name,
			Command:          command,
			Datastore:        normalizedWeaverDatastoreForPhase(phase.Datastore),
			Project:          phase.Project,
			RootDir:          phase.RootDir,
			RunDir:           phase.RunDir,
			RuntimePortsFile: phase.RuntimePortsFile,
			LogPath:          logPath,
			StartedAt:        startedAt,
			Status:           "running",
		},
	}
	if err := recorder.flushLocked(); err != nil {
		return nil, err
	}
	return recorder, nil
}

func (r *phaseRunRecorder) SetPID(pid int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.PID = pid
	_ = r.flushLocked()
}

func (r *phaseRunRecorder) UpdateEvent(event progressEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	eventCopy := event
	r.state.LastEvent = &eventCopy
	r.state.LastEventAt = time.Now()
	_ = r.flushLocked()
}

func (r *phaseRunRecorder) UpdateLogLine(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.LastLogLine = trimmed
	_ = r.flushLocked()
}

func (r *phaseRunRecorder) Finish(status string, duration time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Status = status
	r.state.Duration = duration
	r.state.FinishedAt = time.Now()
	if err != nil {
		r.state.Error = err.Error()
	}
	_ = r.flushLocked()
}

func (r *phaseRunRecorder) flushLocked() error {
	body, err := json.MarshalIndent(r.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.path, body, 0o644)
}

func capturePhaseDiagnostics(phase *fullPhaseContext) error {
	var errs []error

	logsCmd := exec.Command("docker", "compose", "-p", phase.Project, "logs", "--no-color", "--timestamps")
	logsCmd.Dir = e2eDir()
	if output, err := logsCmd.CombinedOutput(); err == nil {
		if writeErr := os.WriteFile(filepath.Join(phase.RootDir, "docker-compose.log"), output, 0o644); writeErr != nil {
			errs = append(errs, writeErr)
		}
	} else {
		errs = append(errs, fmt.Errorf("docker compose logs: %w", err))
	}

	psCmd := exec.Command("docker", "compose", "-p", phase.Project, "ps", "-a")
	psCmd.Dir = e2eDir()
	if output, err := psCmd.CombinedOutput(); err == nil {
		if writeErr := os.WriteFile(filepath.Join(phase.RootDir, "docker-compose.ps.txt"), output, 0o644); writeErr != nil {
			errs = append(errs, writeErr)
		}
	} else {
		errs = append(errs, fmt.Errorf("docker compose ps: %w", err))
	}

	return errors.Join(errs...)
}

func stopManagedLocalWeaverForPhase(phase *fullPhaseContext) error {
	pidPath := filepath.Join(phase.RunDir, "weaver", "weaver.pid")
	pidData, err := os.ReadFile(pidPath)
	if err == nil {
		if pid, parseErr := strconv.Atoi(strings.TrimSpace(string(pidData))); parseErr == nil && pid > 0 {
			if process, findErr := os.FindProcess(pid); findErr == nil {
				_ = process.Kill()
			}
		}
	}
	_ = os.Remove(pidPath)

	state, err := loadRuntimePortStateFromFile(phase.RuntimePortsFile)
	if err == nil && state.LocalWeaverPort > 0 {
		killWeaverListenersOnPort(strconv.Itoa(state.LocalWeaverPort))
	}
	return nil
}

func loadRuntimePortStateFromFile(path string) (runtimePortState, error) {
	var state runtimePortState
	body, err := os.ReadFile(path)
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(body, &state); err != nil {
		return state, err
	}
	return state, nil
}

func writeFullRunManifest(path, tempRoot string, phases []*fullPhaseContext) error {
	manifest := fullRunManifest{
		OwnerPID:  os.Getpid(),
		StartedAt: time.Now(),
		TempRoot:  tempRoot,
		Phases:    make([]fullRunPhaseEntry, 0, len(phases)),
	}
	for _, phase := range phases {
		manifest.Phases = append(manifest.Phases, fullRunPhaseEntry{
			Name:             phase.Name,
			Datastore:        normalizedWeaverDatastoreForPhase(phase.Datastore),
			Project:          phase.Project,
			RootDir:          phase.RootDir,
			RunDir:           phase.RunDir,
			RuntimePortsFile: phase.RuntimePortsFile,
		})
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func cleanupAbandonedFullRuns() error {
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "weaver-e2e-full-*", "full-run.json"))
	if err != nil {
		return err
	}
	var errs []error
	for _, manifestPath := range matches {
		body, err := os.ReadFile(manifestPath)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var manifest fullRunManifest
		if err := json.Unmarshal(body, &manifest); err != nil {
			errs = append(errs, err)
			continue
		}
		if manifest.OwnerPID > 0 && processAlive(manifest.OwnerPID) {
			continue
		}
		for _, entry := range manifest.Phases {
			phase := &fullPhaseContext{
				Name:             entry.Name,
				Project:          entry.Project,
				RootDir:          entry.RootDir,
				RunDir:           entry.RunDir,
				RuntimePortsFile: entry.RuntimePortsFile,
			}
			if err := cleanupFullPhaseContext(phase); err != nil {
				errs = append(errs, fmt.Errorf("%s cleanup: %w", entry.Name, err))
			}
		}
		_ = os.Remove(manifestPath)
		_ = os.RemoveAll(manifest.TempRoot)
	}
	return errors.Join(errs...)
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return process.Signal(syscall.Signal(0)) == nil
}
