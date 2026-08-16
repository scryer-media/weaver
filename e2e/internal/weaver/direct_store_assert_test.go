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
	if !jobIsAllowedToDemoteOnDamage("E2E Test RAR4 Corrupted") {
		t.Fatal("a deliberately corrupt fixture must be allowed to demote on damage")
	}
	if jobIsAllowedToDemoteOnDamage("Silver Horizon — S01E01") {
		t.Fatal("a healthy direct fixture must not be exempt")
	}
	if !isDamageDemotion("member_checksum_mismatch") || isDamageDemotion("member_compressed") {
		t.Fatal("damage classification is wrong")
	}
	if !byDesignDirectRefusals["member_directory"] {
		t.Fatal("a directory member is a by-design refusal, not a failure")
	}
}

func TestDirectStoreJobNamesParsesColouredLog(t *testing.T) {
	line := "INFO \x1b[2mweaver_server_core::ingest\x1b[0m: submitted NZB job " +
		"\x1b[3mjob_id\x1b[0m\x1b[2m=\x1b[0m10047 \x1b[3mname\x1b[0m\x1b[2m=\x1b[0mE2E Test RAR4 Corrupted category=\"2000\""
	names := directStoreJobNames(line)
	if names["10047"] != "E2E Test RAR4 Corrupted" {
		t.Fatalf("job name = %q", names["10047"])
	}
}
