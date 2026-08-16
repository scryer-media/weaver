package weaver

import "testing"

func TestExtractFirstMessageIDsReturnsLeadingIDsInNZBOrder(t *testing.T) {
	input := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="&quot;one.bin&quot; yEnc (1/2)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">one@example.com</segment>
      <segment bytes="64" number="2">two@example.com</segment>
    </segments>
  </file>
  <file subject="&quot;two.bin&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">three@example.com</segment>
    </segments>
  </file>
</nzb>`)

	ids, err := extractFirstMessageIDs(input, 2)
	if err != nil {
		t.Fatalf("extract first message ids: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}
	if ids[0] != "one@example.com" || ids[1] != "two@example.com" {
		t.Fatalf("unexpected ids: %#v", ids)
	}
}

func TestScenarioNeedsBackupServerState(t *testing.T) {
	if !scenarioNeedsBackupServerState(&Scenario{PrimaryDeleteFirstMessageIDs: 1}) {
		t.Fatal("primary-only article deletion should require backup state")
	}
	if !scenarioNeedsBackupServerState(&Scenario{PrimaryDeleteSubjectContains: []string{".vol"}}) {
		t.Fatal("primary-only subject deletion should require backup state")
	}
	if !scenarioNeedsBackupServerState(&Scenario{PrimaryChaosConfig: "corrupt_body=100"}) {
		t.Fatal("primary chaos should require backup state")
	}
	if !scenarioNeedsBackupServerState(&Scenario{BackupFixtureAssets: []string{"single-mkv/test-media.mkv"}}) {
		t.Fatal("backup fixture override should require backup state")
	}
	if scenarioNeedsBackupServerState(&Scenario{}) {
		t.Fatal("plain scenario should not require backup state")
	}
}

func TestScenarioUsesExclusiveNntpState(t *testing.T) {
	if !scenarioUsesExclusiveNntpState(&Scenario{PrimaryChaosConfig: "corrupt_body=100"}) {
		t.Fatal("primary chaos should force exclusive NNTP state")
	}
	if scenarioUsesExclusiveNntpState(&Scenario{}) {
		t.Fatal("plain scenario should not force exclusive NNTP state")
	}
}
