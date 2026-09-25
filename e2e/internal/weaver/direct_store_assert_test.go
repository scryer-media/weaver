package weaver

import "testing"

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
