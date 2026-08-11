// Package benchmark defines the product-neutral fairness contract for a run.
package benchmark

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
)

type Client string

const (
	Weaver  Client = "weaver"
	SABnzbd Client = "sabnzbd"
	NZBGet  Client = "nzbget"
)

const PrimaryMetric = "submission_to_terminal_verified_output"

type Transport string

const (
	Plaintext Transport = "plaintext"
	TLS       Transport = "tls"
)

// TLSValidation is captured separately from transport. It prevents an
// encrypted-but-unverified connection from being reported as authenticated
// TLS just because it used port 563.
type TLSValidation string

const (
	TLSNotApplicable TLSValidation = "not_applicable"
	TLSCAVerified    TLSValidation = "ca_verified"
	TLSDisabled      TLSValidation = "disabled"
)

const (
	ProfileStock                = "stock"
	ProfileEquivalentThroughput = "equivalent-throughput"
)

type ClientProfile struct {
	Client         Client        `json:"client"`
	TLSValidation  TLSValidation `json:"tls_validation"`
	TLSResultLabel string        `json:"tls_result_label"`
}

// ArchiveToolchain identifies the external RAR/PAR2 implementation used by a
// product lane. It is separate from Client so vanilla and Rarpar-backed runs
// can never be grouped as though they were the same configuration.
type ArchiveToolchain string

const (
	VanillaArchiveToolchain ArchiveToolchain = "vanilla"
	RarparArchiveToolchain  ArchiveToolchain = "rarpar"
)

type PlanOptions struct {
	FixtureIDs        []string
	Clients           []Client
	ClientProfiles    []ClientProfile
	ArchiveToolchains []ArchiveToolchain
	Transports        []Transport
	Targets           []ExecutionTarget
	Profile           string
	ServerLink        ServerLinkProfile
	Repetitions       int
	Seed              int64
}

// Plan is deliberately timestamp-free. A saved plan is the authoritative
// order of every run, so a later reviewer never has to re-create randomness.
type Plan struct {
	SchemaVersion     int                `json:"schema_version"`
	Seed              int64              `json:"seed"`
	FixtureIDs        []string           `json:"fixture_ids"`
	Clients           []Client           `json:"clients"`
	ClientProfiles    []ClientProfile    `json:"client_profiles"`
	ArchiveToolchains []ArchiveToolchain `json:"archive_toolchains"`
	Transports        []Transport        `json:"transports"`
	ExecutionTargets  []ExecutionTarget  `json:"execution_targets"`
	Profile           string             `json:"profile"`
	ServerLink        ServerLinkProfile  `json:"server_link"`
	Repetitions       int                `json:"repetitions"`
	Runs              []Run              `json:"runs"`
}

// Run is one planned fixture submission. Primary execution gives every run a
// fresh client process and writable state/configuration/output directory.
type Run struct {
	ID               string            `json:"id"`
	Order            int               `json:"order"`
	FixtureID        string            `json:"fixture_id"`
	Client           Client            `json:"client"`
	ArchiveToolchain ArchiveToolchain  `json:"archive_toolchain"`
	Transport        Transport         `json:"transport"`
	ExecutionTarget  ExecutionTarget   `json:"execution_target"`
	TLSValidation    TLSValidation     `json:"tls_validation"`
	TransportLabel   string            `json:"transport_label"`
	Profile          string            `json:"profile"`
	ServerLink       ServerLinkProfile `json:"server_link"`
	Repetition       int               `json:"repetition"`
	FreshClientState bool              `json:"fresh_client_state"`
	Metric           string            `json:"metric"`
}

func BuildPlan(options PlanOptions) (Plan, error) {
	if options.Profile == "" {
		options.Profile = ProfileEquivalentThroughput
	}
	if options.ServerLink.ID == "" {
		options.ServerLink = DefaultServerLinkProfile()
	}
	if len(options.ClientProfiles) == 0 {
		options.ClientProfiles = DefaultClientProfiles(options.Clients)
	}
	if len(options.ArchiveToolchains) == 0 {
		options.ArchiveToolchains = DefaultArchiveToolchains()
	}
	if len(options.Targets) == 0 {
		options.Targets = DefaultExecutionTargets()
	}
	if err := validateOptions(options); err != nil {
		return Plan{}, err
	}
	plan := Plan{
		SchemaVersion:     5,
		Seed:              options.Seed,
		FixtureIDs:        append([]string(nil), options.FixtureIDs...),
		Clients:           append([]Client(nil), options.Clients...),
		ClientProfiles:    append([]ClientProfile(nil), options.ClientProfiles...),
		ArchiveToolchains: append([]ArchiveToolchain(nil), options.ArchiveToolchains...),
		Transports:        append([]Transport(nil), options.Transports...),
		ExecutionTargets:  append([]ExecutionTarget(nil), options.Targets...),
		Profile:           options.Profile,
		ServerLink:        options.ServerLink,
		Repetitions:       options.Repetitions,
	}
	type round struct {
		fixtureID  string
		transport  Transport
		target     ExecutionTarget
		repetition int
	}
	rounds := make([]round, 0, len(options.FixtureIDs)*len(options.Transports)*len(options.Targets)*options.Repetitions)
	for _, fixtureID := range options.FixtureIDs {
		for _, transport := range options.Transports {
			for _, target := range options.Targets {
				for repetition := 1; repetition <= options.Repetitions; repetition++ {
					rounds = append(rounds, round{fixtureID: fixtureID, transport: transport, target: target, repetition: repetition})
				}
			}
		}
	}
	random := rand.New(rand.NewSource(options.Seed)) // #nosec G404 -- deterministic schedule, not security.
	random.Shuffle(len(rounds), func(i, j int) { rounds[i], rounds[j] = rounds[j], rounds[i] })
	for _, benchmarkRound := range rounds {
		lanes := benchmarkLanes(options.Clients, options.ArchiveToolchains, benchmarkRound.target)
		random.Shuffle(len(lanes), func(i, j int) { lanes[i], lanes[j] = lanes[j], lanes[i] })
		for _, lane := range lanes {
			profile := clientProfileFor(options.ClientProfiles, lane.client)
			validation := TLSNotApplicable
			transportLabel := string(Plaintext)
			if benchmarkRound.transport == TLS {
				validation = profile.TLSValidation
				transportLabel = profile.TLSResultLabel
			}
			order := len(plan.Runs) + 1
			plan.Runs = append(plan.Runs, Run{
				ID:               fmt.Sprintf("run-%04d", order),
				Order:            order,
				FixtureID:        benchmarkRound.fixtureID,
				Client:           lane.client,
				ArchiveToolchain: lane.archiveToolchain,
				Transport:        benchmarkRound.transport,
				ExecutionTarget:  benchmarkRound.target,
				TLSValidation:    validation,
				TransportLabel:   transportLabel,
				Profile:          plan.Profile,
				ServerLink:       plan.ServerLink,
				Repetition:       benchmarkRound.repetition,
				FreshClientState: true,
				Metric:           PrimaryMetric,
			})
		}
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (p Plan) Validate() error {
	if p.SchemaVersion != 5 {
		return fmt.Errorf("unsupported benchmark plan schema version %d", p.SchemaVersion)
	}
	if err := validateOptions(PlanOptions{
		FixtureIDs:        p.FixtureIDs,
		Clients:           p.Clients,
		ClientProfiles:    p.ClientProfiles,
		ArchiveToolchains: p.ArchiveToolchains,
		Transports:        p.Transports,
		Targets:           p.ExecutionTargets,
		Profile:           p.Profile,
		ServerLink:        p.ServerLink,
		Repetitions:       p.Repetitions,
		Seed:              p.Seed,
	}); err != nil {
		return err
	}
	expected := 0
	for _, target := range p.ExecutionTargets {
		expected += len(benchmarkLanes(p.Clients, p.ArchiveToolchains, target))
	}
	expected *= len(p.FixtureIDs) * len(p.Transports) * p.Repetitions
	if len(p.Runs) != expected {
		return fmt.Errorf("benchmark plan contains %d runs, expected %d", len(p.Runs), expected)
	}
	seen := map[string]bool{}
	targets := map[ExecutionTarget]bool{}
	fixtures := map[string]bool{}
	clients := map[Client]bool{}
	transports := map[Transport]bool{}
	for _, target := range p.ExecutionTargets {
		targets[target] = true
	}
	for _, fixtureID := range p.FixtureIDs {
		fixtures[fixtureID] = true
	}
	for _, client := range p.Clients {
		clients[client] = true
	}
	for _, transport := range p.Transports {
		transports[transport] = true
	}
	for index, run := range p.Runs {
		if run.Order != index+1 || run.ID != fmt.Sprintf("run-%04d", index+1) {
			return fmt.Errorf("benchmark plan has non-canonical run ordering at position %d", index+1)
		}
		if !run.FreshClientState || run.Metric != PrimaryMetric || run.Profile != p.Profile || run.ServerLink != p.ServerLink {
			return fmt.Errorf("benchmark plan run %s does not use the required verified submission-to-terminal metric", run.ID)
		}
		if !targets[run.ExecutionTarget] {
			return fmt.Errorf("benchmark plan run %s has an unplanned execution target %q", run.ID, run.ExecutionTarget)
		}
		if !fixtures[run.FixtureID] || !clients[run.Client] || !transports[run.Transport] {
			return fmt.Errorf("benchmark plan run %s has a fixture, client, or transport outside the declared plan", run.ID)
		}
		if !archiveToolchainAllowed(run.Client, run.ArchiveToolchain, run.ExecutionTarget) {
			return fmt.Errorf("benchmark plan run %s has unsupported %s toolchain for %s on %s", run.ID, run.ArchiveToolchain, run.Client, run.ExecutionTarget)
		}
		if !containsArchiveToolchain(p.ArchiveToolchains, run.ArchiveToolchain) {
			return fmt.Errorf("benchmark plan run %s uses archive toolchain %q outside the declared plan", run.ID, run.ArchiveToolchain)
		}
		profile := clientProfileFor(p.ClientProfiles, run.Client)
		if run.Transport == Plaintext {
			if run.TLSValidation != TLSNotApplicable || run.TransportLabel != string(Plaintext) {
				return fmt.Errorf("benchmark plan run %s has invalid plaintext TLS metadata", run.ID)
			}
		} else if run.TLSValidation != profile.TLSValidation || run.TransportLabel != profile.TLSResultLabel {
			return fmt.Errorf("benchmark plan run %s does not report %s TLS policy for %s", run.ID, profile.TLSValidation, run.Client)
		}
		key := strings.Join([]string{run.FixtureID, string(run.Transport), string(run.ExecutionTarget), fmt.Sprint(run.Repetition), string(run.Client), string(run.ArchiveToolchain)}, "\x00")
		if seen[key] {
			return fmt.Errorf("benchmark plan repeats run tuple %q", key)
		}
		seen[key] = true
	}
	return nil
}

func WritePlan(path string, plan Plan) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	contents, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create benchmark plan %s: %w", path, err)
	}
	if _, err := file.Write(contents); err != nil {
		return fmt.Errorf("write benchmark plan %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close benchmark plan %s: %w", path, err)
	}
	return nil
}

func LoadPlan(path string) (Plan, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Plan{}, fmt.Errorf("read benchmark plan %s: %w", path, err)
	}
	var plan Plan
	if err := json.Unmarshal(contents, &plan); err != nil {
		return Plan{}, fmt.Errorf("decode benchmark plan %s: %w", path, err)
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func validateOptions(options PlanOptions) error {
	if len(options.FixtureIDs) == 0 || len(options.Clients) == 0 || len(options.ArchiveToolchains) == 0 || len(options.Transports) == 0 || len(options.Targets) == 0 || options.Repetitions < 1 {
		return fmt.Errorf("fixture ids, clients, archive toolchains, transports, execution targets, and a positive repetition count are required")
	}
	if options.Profile != ProfileStock && options.Profile != ProfileEquivalentThroughput {
		return fmt.Errorf("unsupported benchmark profile %q", options.Profile)
	}
	if err := options.ServerLink.Validate(); err != nil {
		return err
	}
	if err := validateExecutionTargets(options.Targets); err != nil {
		return err
	}
	if hasDuplicateStrings(options.FixtureIDs) {
		return fmt.Errorf("fixture ids must be unique")
	}
	if hasDuplicateClients(options.Clients) {
		return fmt.Errorf("clients must be unique")
	}
	if hasDuplicateArchiveToolchains(options.ArchiveToolchains) {
		return fmt.Errorf("archive toolchains must be unique")
	}
	if hasDuplicateTransports(options.Transports) {
		return fmt.Errorf("transports must be unique")
	}
	if err := validateClientProfiles(options.Clients, options.ClientProfiles); err != nil {
		return err
	}
	for _, client := range options.Clients {
		if client != Weaver && client != SABnzbd && client != NZBGet {
			return fmt.Errorf("unsupported benchmark client %q", client)
		}
	}
	for _, toolchain := range options.ArchiveToolchains {
		if toolchain != VanillaArchiveToolchain && toolchain != RarparArchiveToolchain {
			return fmt.Errorf("unsupported archive toolchain %q", toolchain)
		}
	}
	if !containsArchiveToolchain(options.ArchiveToolchains, VanillaArchiveToolchain) {
		return fmt.Errorf("the vanilla archive toolchain is required")
	}
	for _, transport := range options.Transports {
		if transport != Plaintext && transport != TLS {
			return fmt.Errorf("unsupported benchmark transport %q", transport)
		}
	}
	return nil
}

// DefaultArchiveToolchains preserves stock clients as the baseline and adds
// Rarpar-backed Docker lanes for the two clients that can be configured
// without repackaging their native distributions.
func DefaultArchiveToolchains() []ArchiveToolchain {
	return []ArchiveToolchain{VanillaArchiveToolchain, RarparArchiveToolchain}
}

type benchmarkLane struct {
	client           Client
	archiveToolchain ArchiveToolchain
}

func benchmarkLanes(clients []Client, toolchains []ArchiveToolchain, target ExecutionTarget) []benchmarkLane {
	lanes := make([]benchmarkLane, 0, len(clients)*len(toolchains))
	for _, client := range clients {
		for _, toolchain := range toolchains {
			if archiveToolchainAllowed(client, toolchain, target) {
				lanes = append(lanes, benchmarkLane{client: client, archiveToolchain: toolchain})
			}
		}
	}
	return lanes
}

func archiveToolchainAllowed(client Client, toolchain ArchiveToolchain, target ExecutionTarget) bool {
	if toolchain == VanillaArchiveToolchain {
		return true
	}
	// Rarpar integration needs a replaceable tool invocation. The stock native
	// SABnzbd and NZBGet distributions embed or bundle parts of that toolchain,
	// so scheduling them as Rarpar-backed would misrepresent what ran.
	return toolchain == RarparArchiveToolchain && target == DockerLinux && (client == SABnzbd || client == NZBGet)
}

// DefaultClientProfiles encodes the current isolated-lab policy. SABnzbd's
// local generated CA cannot yet be trusted reliably, so it is intentionally
// marked as encryption without certificate validation rather than silently
// equated to the verified client TLS runs.
func DefaultClientProfiles(clients []Client) []ClientProfile {
	profiles := make([]ClientProfile, 0, len(clients))
	for _, client := range clients {
		profile := ClientProfile{
			Client:         client,
			TLSValidation:  TLSCAVerified,
			TLSResultLabel: "tls-ca-verified",
		}
		if client == SABnzbd {
			profile.TLSValidation = TLSDisabled
			profile.TLSResultLabel = "tls-unverified"
		}
		profiles = append(profiles, profile)
	}
	return profiles
}

func validateClientProfiles(clients []Client, profiles []ClientProfile) error {
	if len(profiles) != len(clients) {
		return fmt.Errorf("client profiles must contain exactly one entry per client")
	}
	seen := map[Client]bool{}
	for _, profile := range profiles {
		if seen[profile.Client] {
			return fmt.Errorf("client profiles repeat %q", profile.Client)
		}
		seen[profile.Client] = true
		if profile.TLSValidation != TLSCAVerified && profile.TLSValidation != TLSDisabled {
			return fmt.Errorf("client profile %q has unsupported TLS validation %q", profile.Client, profile.TLSValidation)
		}
		if strings.TrimSpace(profile.TLSResultLabel) == "" {
			return fmt.Errorf("client profile %q has an empty TLS result label", profile.Client)
		}
	}
	for _, client := range clients {
		if !seen[client] {
			return fmt.Errorf("client profile missing %q", client)
		}
	}
	return nil
}

func clientProfileFor(profiles []ClientProfile, client Client) ClientProfile {
	for _, profile := range profiles {
		if profile.Client == client {
			return profile
		}
	}
	return ClientProfile{}
}

func hasDuplicateStrings(values []string) bool {
	values = append([]string(nil), values...)
	sort.Strings(values)
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			return true
		}
	}
	return false
}

func hasDuplicateClients(values []Client) bool {
	seen := map[Client]bool{}
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

func hasDuplicateTransports(values []Transport) bool {
	seen := map[Transport]bool{}
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

func hasDuplicateArchiveToolchains(values []ArchiveToolchain) bool {
	seen := map[ArchiveToolchain]bool{}
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

func containsArchiveToolchain(values []ArchiveToolchain, target ArchiveToolchain) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
