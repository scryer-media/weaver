package weaver

import (
	"strings"
	"testing"
)

func TestDirectDemotionReasonStripsAnsi(t *testing.T) {
	raw := "WARN direct-store set demoted \x1b[3mjob_id\x1b[0m\x1b[2m=\x1b[0m10008 " +
		"\x1b[3mreason\x1b[0m\x1b[2m=\x1b[0m\"part_checksum_mismatch\""
	if got := directDemotionReason(raw); got != "part_checksum_mismatch" {
		t.Fatalf("coloured line: got %q", got)
	}
	plain := `direct-store set demoted job_id=1 reason="member_compressed"`
	if got := directDemotionReason(plain); got != "member_compressed" {
		t.Fatalf("plain line: got %q", got)
	}
	if got := directDemotionReason("no reason here"); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

// The attribution is what lets one reason mean two different things: a checksum
// demotion from a corrupt-on-purpose fixture is the product working, the same
// demotion from a healthy set is the failure this check exists to catch.
func TestDirectDemotionAttribution(t *testing.T) {
	corrupt := &Scenario{Slug: "rar4-corrupted", ExpectedOutcome: "extraction_failure"}
	if !scenarioAllowsDamageDemotion(corrupt, "member_checksum_mismatch") {
		t.Fatal("a deliberately corrupt fixture must be allowed to demote on damage")
	}
	healthy := &Scenario{Slug: "rar5-direct-clean", ExpectedOutcome: "success"}
	if scenarioAllowsDamageDemotion(healthy, "member_checksum_mismatch") {
		t.Fatal("a healthy direct fixture must not be exempt")
	}
	if scenarioAllowsDamageDemotion(nil, "member_checksum_mismatch") {
		t.Fatal("a job no scenario was submitted as must not be exempt")
	}
	if scenarioAllowsDamageDemotion(corrupt, "member_compressed") {
		t.Fatal("only damage reasons are exempt")
	}
	if !isDamageDemotion("member_checksum_mismatch") || isDamageDemotion("member_compressed") {
		t.Fatal("damage classification is wrong")
	}
	if !byDesignDirectRefusals["member_directory"] {
		t.Fatal("a directory member is a by-design refusal, not a failure")
	}
	// A refused destination over a truthful image: the conventional extractor
	// applies the same path validator and the same collision rule and fails
	// the archive the same way, so the demotion is the product working.
	for _, reason := range []string{"colliding_destinations", "unsafe_destination"} {
		if !byDesignDirectRefusals[reason] {
			t.Fatalf("%s is a refused destination, not a carry failure", reason)
		}
	}
}

// A scenario that pins direct-store behaviour owns its demotions: a damaged
// post it expects to be repaired in place is not excused by its outcome, and
// only the demotion it names is allowed.
func TestDirectStoreScenarioOwnsItsDemotions(t *testing.T) {
	inPlace := &Scenario{
		Slug:              "direct-store-sealed-repair",
		ExpectedOutcome:   "repair_then_success",
		RuntimeAssertions: &ScenarioRuntimeAssertions{DirectStore: &ScenarioDirectStoreAssertion{}},
	}
	if scenarioAllowsDamageDemotion(inPlace, "par2_damaged") {
		t.Fatal("an in-place repair scenario must not be excused a demotion it does not declare")
	}
	declared := &Scenario{
		Slug:            "direct-store-dead-volume",
		ExpectedOutcome: "repair_failure",
		RuntimeAssertions: &ScenarioRuntimeAssertions{DirectStore: &ScenarioDirectStoreAssertion{
			ExpectedDemotionReason: "par2_damaged",
		}},
	}
	if !scenarioAllowsDamageDemotion(declared, "par2_damaged") {
		t.Fatal("the declared demotion must be allowed")
	}
	if scenarioAllowsDamageDemotion(declared, "member_checksum_mismatch") {
		t.Fatal("only the declared demotion is allowed")
	}
}

// The collision is discovered at header time, so it arrives on the same log
// line shape every other reason does and must be filtered out before the
// unexpected-demotion tally sees it.
func TestCollidingDestinationsIsNotAnUnexpectedDemotion(t *testing.T) {
	line := `WARN direct-store set demoted job_id=10103 set_name=archive ` +
		`reason="colliding_destinations" volumes=virtual`
	reason := directDemotionReason(line)
	if reason != "colliding_destinations" {
		t.Fatalf("reason = %q", reason)
	}
	if !byDesignDirectRefusals[reason] {
		t.Fatalf("%s must be allowlisted", reason)
	}
}

// Demotions are attributed by job id to the scenario submitted as that job, and
// reported by slug, whatever the job came to be called in weaver's own log.
func TestUnexpectedDirectDemotionsAttributesBySlug(t *testing.T) {
	jobs := []testJob{
		{slug: "rar4-corrupted", jobID: 10047, scenario: &Scenario{Slug: "rar4-corrupted", ExpectedOutcome: "extraction_failure"}},
		{slug: "rar5-direct-clean", jobID: 10048, scenario: &Scenario{Slug: "rar5-direct-clean", ExpectedOutcome: "success"}},
		{slug: "never-submitted", scenario: &Scenario{Slug: "never-submitted", ExpectedOutcome: "success"}},
	}
	log := "WARN direct-store set demoted \x1b[3mjob_id\x1b[0m\x1b[2m=\x1b[0m10047 " +
		"\x1b[3mreason\x1b[0m\x1b[2m=\x1b[0m\"member_checksum_mismatch\"\n" +
		`WARN direct-store set demoted job_id=10048 reason="member_checksum_mismatch"` + "\n" +
		`WARN direct-store set demoted job_id=10048 reason="member_compressed"` + "\n" +
		`WARN direct-store set demoted job_id=10099 reason="par2_damaged"` + "\n"
	got := unexpectedDirectDemotions(log, submittedScenariosByJobID(jobs))
	want := []string{
		"member_checksum_mismatch (job rar5-direct-clean) x1",
		"par2_damaged (job id 10099) x1",
	}
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

// A nested fixture built to route direct end to end proves it only through the
// posted set's own lines: the finalize line must be there, and no demotion of
// any reason may be, because a fallback delivers byte-identical output.
func TestDirectStoreLinesFinalizedDirectAndForbiddenDemotion(t *testing.T) {
	finalized := "INFO direct-store set finalized without materializing a volume \x1b[3mjob_id\x1b[0m\x1b[2m=\x1b[0m42 set_name=archive members=2\n"
	demoted := `WARN direct-store set demoted job_id=42 set_name=archive reason="member_solid" volumes=virtual` + "\n"
	other := `WARN direct-store set demoted job_id=43 set_name=archive reason="member_solid" volumes=virtual` + "\n"
	strict := &ScenarioDirectStoreAssertion{RequireFinalizedDirect: true, ForbidDemotion: true}

	if err := assertDirectStoreLines(finalized+other, 42, strict); err != nil {
		t.Fatalf("a direct finalize with another job's demotion must pass: %v", err)
	}
	if err := assertDirectStoreLines(other, 42, strict); err == nil {
		t.Fatal("a job with no direct finalize must fail RequireFinalizedDirect")
	}
	if err := assertDirectStoreLines(finalized+demoted, 42, strict); err == nil {
		t.Fatal("a by-design demotion must still fail ForbidDemotion")
	}
	owned := &ScenarioDirectStoreAssertion{ExpectedDemotionReason: "member_solid"}
	if err := assertDirectStoreLines(demoted, 42, owned); err != nil {
		t.Fatalf("the named demotion must satisfy ExpectedDemotionReason: %v", err)
	}
	if err := assertDirectStoreLines(other, 42, owned); err == nil {
		t.Fatal("another job's demotion must not satisfy ExpectedDemotionReason")
	}
}

// A streamed child and one written out as a file deliver the same bytes, so the
// child lines are the whole of the evidence. They are counted per (set, node),
// because a restarted job announces the same child again.
func TestDirectStoreLinesCountStreamedChildrenOncePerNode(t *testing.T) {
	streamed := func(jobID, node string) string {
		return "INFO direct-store child archive streamed into a live engine \x1b[3mjob_id\x1b[0m\x1b[2m=\x1b[0m" + jobID +
			" \x1b[3mset_name\x1b[0m\x1b[2m=\x1b[0marchive \x1b[3mchild\x1b[0m\x1b[2m=\x1b[0minner \x1b[3mnode\x1b[0m\x1b[2m=\x1b[0m" + node + "\n"
	}
	two := &ScenarioDirectStoreAssertion{RequireStreamedChildren: 2}

	if err := assertDirectStoreLines(streamed("42", "1")+streamed("42", "2"), 42, two); err != nil {
		t.Fatalf("two distinct streamed children must satisfy a count of two: %v", err)
	}
	if err := assertDirectStoreLines(streamed("42", "1")+streamed("42", "1"), 42, two); err == nil {
		t.Fatal("one child announced twice across a restart must count once")
	}
	if err := assertDirectStoreLines(streamed("42", "1")+streamed("43", "2"), 42, two); err == nil {
		t.Fatal("another job's streamed child must not count")
	}
	plain := `INFO direct-store child archive streamed into a live engine job_id=42 set_name=archive child=inner node=1` + "\n" +
		`INFO direct-store child archive streamed into a live engine job_id=42 set_name=other child=inner node=1` + "\n"
	if err := assertDirectStoreLines(plain, 42, two); err != nil {
		t.Fatalf("the same node of two posted sets is two children: %v", err)
	}
}

// A stored child in a stored parent is routed by the graph and never reaches an
// engine, so its line counts toward the same requirement as a streamed child's.
func TestDirectStoreLinesCountGraphRoutedChildren(t *testing.T) {
	routed := `INFO direct-store child archive routed by the graph job_id=42 set_name=archive child=inner.7z node=1` + "\n"
	streamed := `INFO direct-store child archive streamed into a live engine job_id=42 set_name=archive child=deeper node=2` + "\n"

	if err := assertDirectStoreLines(routed, 42, &ScenarioDirectStoreAssertion{RequireStreamedChildren: 1}); err != nil {
		t.Fatalf("a graph-routed child must satisfy a count of one: %v", err)
	}
	two := &ScenarioDirectStoreAssertion{RequireStreamedChildren: 2}
	if err := assertDirectStoreLines(routed+streamed, 42, two); err != nil {
		t.Fatalf("a routed and a streamed child are two children: %v", err)
	}
	err := assertDirectStoreLines(routed+routed, 42, two)
	if err == nil {
		t.Fatal("one routed child announced twice across a restart must count once")
	}
	if !strings.Contains(err.Error(), "streamed into a live engine or routed by the graph") {
		t.Fatalf("the failure must name both ways a child is counted, got %v", err)
	}
}

func TestDirectStoreLinesChildMaterialization(t *testing.T) {
	materialized := "INFO direct-store child archive materialized as a file \x1b[3mjob_id\x1b[0m\x1b[2m=\x1b[0m42 " +
		"\x1b[3mset_name\x1b[0m\x1b[2m=\x1b[0marchive \x1b[3mnode\x1b[0m\x1b[2m=\x1b[0m1 " +
		"\x1b[3mreason\x1b[0m\x1b[2m=\x1b[0m\"not_an_archive\"\n"
	other := `INFO direct-store child archive materialized as a file job_id=43 set_name=archive node=1 reason="not_an_archive"` + "\n"

	forbid := &ScenarioDirectStoreAssertion{ForbidChildMaterialization: true}
	if err := assertDirectStoreLines(other, 42, forbid); err != nil {
		t.Fatalf("another job's fallback must not fail this one: %v", err)
	}
	err := assertDirectStoreLines(materialized, 42, forbid)
	if err == nil {
		t.Fatal("a child written out as a file must fail ForbidChildMaterialization")
	}
	if !strings.Contains(err.Error(), `"not_an_archive"`) || !strings.Contains(err.Error(), "archive/1") {
		t.Fatalf("the failure must name the child and its reason, got %v", err)
	}

	require := &ScenarioDirectStoreAssertion{RequireMaterializedChildren: 1}
	if err := assertDirectStoreLines(materialized, 42, require); err != nil {
		t.Fatalf("the fallback line must satisfy RequireMaterializedChildren: %v", err)
	}
	if err := assertDirectStoreLines(other, 42, require); err == nil {
		t.Fatal("another job's fallback must not satisfy RequireMaterializedChildren")
	}
}

func TestDirectLogFieldMatchesWholeFieldNames(t *testing.T) {
	line := `INFO direct-store child archive streamed into a live engine job_id=7 child_node=9 set_name="the set" node=3`
	if got := directLogField(line, "node"); got != "3" {
		t.Fatalf("node = %q, want the node field rather than child_node", got)
	}
	if got := directLogField(line, "set_name"); got != "the set" {
		t.Fatalf("set_name = %q, want the unquoted value", got)
	}
	if got := directLogField(line, "missing"); got != "" {
		t.Fatalf("missing = %q, want empty", got)
	}
}

// A contradictory assertion is refused when the scenario loads, before any run
// can fail on it.
func TestDirectStoreAssertionRejectsContradictions(t *testing.T) {
	valid := []*ScenarioDirectStoreAssertion{
		nil,
		{RequireFinalizedDirect: true, ForbidDemotion: true, RequireStreamedChildren: 2, ForbidChildMaterialization: true},
		{RequireFinalizedDirect: true, ForbidDemotion: true, RequireMaterializedChildren: 1},
		{ExpectedDemotionReason: "member_solid"},
	}
	for _, assertion := range valid {
		if err := assertion.validate(); err != nil {
			t.Fatalf("%+v must be accepted: %v", assertion, err)
		}
	}
	invalid := []*ScenarioDirectStoreAssertion{
		{ExpectedDemotionReason: "member_solid", ForbidDemotion: true},
		{ExpectedDemotionReason: "member_solid", RequireStreamedChildren: 1},
		{ExpectedDemotionReason: "member_solid", RequireMaterializedChildren: 1},
		{ExpectedDemotionReason: "member_solid", ForbidChildMaterialization: true},
		{RequireMaterializedChildren: 1, ForbidChildMaterialization: true},
		{RequireStreamedChildren: -1},
		{RequireMaterializedChildren: -1},
	}
	for _, assertion := range invalid {
		if err := assertion.validate(); err == nil {
			t.Fatalf("%+v must be refused", assertion)
		}
	}
}
