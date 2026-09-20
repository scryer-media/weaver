package weaver

import (
	"reflect"
	"testing"
)

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
	if !scenarioNeedsBackupServerState(&Scenario{BackupUnavailableUntilFileComplete: "payload.mkv"}) {
		t.Fatal("backup availability gate should require backup state")
	}
	if !scenarioNeedsBackupServerState(&Scenario{BackupUnavailableUntilJobTerminal: true}) {
		t.Fatal("whole-job backup gate should require backup state")
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
	if !scenarioUsesExclusiveNntpState(&Scenario{BackupUnavailableUntilFileComplete: "payload.mkv"}) {
		t.Fatal("backup availability gate should force exclusive NNTP state")
	}
	if !scenarioUsesExclusiveNntpState(&Scenario{BackupUnavailableUntilJobTerminal: true}) {
		t.Fatal("whole-job backup gate should force exclusive NNTP state")
	}
	if !scenarioUsesExclusiveNntpState(&Scenario{
		RuntimeAssertions: &ScenarioRuntimeAssertions{
			QueueLiveness: &ScenarioQueueLivenessAssertion{ProbeSlug: "deflate-single"},
		},
	}) {
		t.Fatal("queue-liveness assertion should force exclusive execution")
	}
	if scenarioUsesExclusiveNntpState(&Scenario{}) {
		t.Fatal("plain scenario should not force exclusive NNTP state")
	}
}

func TestArticleSyncCommandsUseDockerArchiveStreaming(t *testing.T) {
	source, destination := articleSyncCommands("primary", "backup")
	if want := []string{"docker", "cp", "primary:/data/articles/.", "-"}; !reflect.DeepEqual(source.Args, want) {
		t.Fatalf("source command = %q, want %q", source.Args, want)
	}
	if want := []string{"docker", "cp", "-", "backup:/data/articles"}; !reflect.DeepEqual(destination.Args, want) {
		t.Fatalf("destination command = %q, want %q", destination.Args, want)
	}
}

func TestExtractFirstMessageIDsOrdersFilesBySubjectIndex(t *testing.T) {
	// The poster writes each <file> as it finishes uploading, so a small repair
	// file can be written ahead of the first archive volume. The subject's
	// [n/m] counter is the only record of the order the set was posted in.
	input := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="[3/3] - &quot;set.vol00+01.par2&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">par2-vol@example.com</segment>
    </segments>
  </file>
  <file subject="[1/3] - &quot;set.part1.rar&quot; yEnc (1/2)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">&lt;rar1-a@example.com&gt;</segment>
      <segment bytes="64" number="2">rar1-b@example.com</segment>
    </segments>
  </file>
  <file subject="[2/3] - &quot;set.part2.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">rar2-a@example.com</segment>
    </segments>
  </file>
</nzb>`)

	ids, err := extractFirstMessageIDs(input, 2)
	if err != nil {
		t.Fatalf("extract first message ids: %v", err)
	}
	want := []string{"rar1-a@example.com", "rar1-b@example.com"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("expected the first volume's articles %v, got %v", want, ids)
	}

	all, err := extractFirstMessageIDs(input, 10)
	if err != nil {
		t.Fatalf("extract every message id: %v", err)
	}
	wantAll := []string{
		"rar1-a@example.com",
		"rar1-b@example.com",
		"rar2-a@example.com",
		"par2-vol@example.com",
	}
	if !reflect.DeepEqual(all, wantAll) {
		t.Fatalf("expected posting order %v, got %v", wantAll, all)
	}
}

func TestExtractFirstMessageIDsKeepsUnindexedFilesLast(t *testing.T) {
	input := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="&quot;loose.bin&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">loose@example.com</segment>
    </segments>
  </file>
  <file subject="[2/2] - &quot;set.part2.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">rar2@example.com</segment>
    </segments>
  </file>
  <file subject="[1/2] - &quot;set.part1.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">rar1@example.com</segment>
    </segments>
  </file>
</nzb>`)

	ids, err := extractFirstMessageIDs(input, 3)
	if err != nil {
		t.Fatalf("extract first message ids: %v", err)
	}
	want := []string{"rar1@example.com", "rar2@example.com", "loose@example.com"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("expected indexed files first, got %v", ids)
	}
}
