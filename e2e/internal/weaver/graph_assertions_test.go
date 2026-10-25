// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// graphLine renders one contract line the way the graph's tracing writer does.
func graphLine(message, fields string) string {
	return "2026-10-18T10:00:00.000001Z  INFO weaver_server_core::pipeline::graph::status: " + message + " " + fields
}

func graphLog(lines ...string) string {
	return strings.Join(lines, "\n")
}

func wantVerdict(t *testing.T, err error, wantErr string) {
	t.Helper()
	switch {
	case wantErr == "" && err != nil:
		t.Fatalf("unexpected error: %v", err)
	case wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)):
		t.Fatalf("error = %v, want one containing %q", err, wantErr)
	}
}

func TestEngineUnderTestReadsWeaverEngine(t *testing.T) {
	for _, test := range []struct {
		value string
		want  weaverEngine
	}{
		{value: "", want: engineGraph},
		{value: "classic", want: engineClassic},
		{value: " ClAsSiC ", want: engineClassic},
		{value: "pipeline", want: engineGraph},
		{value: "graph", want: engineGraph},
		{value: " GRAPH ", want: engineGraph},
	} {
		t.Setenv(weaverEngineEnv, test.value)
		if got := engineUnderTest(); got != test.want {
			t.Fatalf("WEAVER_ENGINE=%q: engine = %q, want %q", test.value, got, test.want)
		}
	}
	os.Unsetenv(weaverEngineEnv)
	if got := engineUnderTest(); got != engineGraph {
		t.Fatalf("unset WEAVER_ENGINE: engine = %q, want graph", got)
	}
}

func TestGraphLogEventsReadsContractFields(t *testing.T) {
	raw := graphLog(
		graphLine("member delivered", `job_id=7 name=CERTIFICATE/backup/id (1).bdmv from=CERTIFICATE/backup/id.bdmv depth=1 whole=true`),
		graphLine("member delivered", `job_id=7 name="quoted \"name\".bin" from="quoted \"name\".bin" depth=0 whole=false`),
		"\x1b[2m2026-10-18T10:00:00Z\x1b[0m \x1b[32m INFO\x1b[0m \x1b[2mweaver_server_core::pipeline::graph::namespace\x1b[0m\x1b[2m:\x1b[0m member delivered \x1b[3mjob_id\x1b[0m\x1b[2m=\x1b[0m7 \x1b[3mname\x1b[0m\x1b[2m=\x1b[0mcoloured.bin \x1b[3mfrom\x1b[0m\x1b[2m=\x1b[0mcoloured.bin \x1b[3mdepth\x1b[0m\x1b[2m=\x1b[0m0 \x1b[3mwhole\x1b[0m\x1b[2m=\x1b[0mtrue",
		graphLine("member delivered", `job_id=70 name=other-job.bin from=other-job.bin depth=0 whole=true`),
		`2026-10-18T10:00:00Z  INFO weaver_server_core::pipeline::completion: member delivered job_id=7 name=classic.bin`,
		`2026-10-18T10:00:01Z  WARN weaver_server_core::runtime::job_debug_ring: stall diagnostics line job_id=7 line=INFO weaver_server_core::pipeline::graph::status: member delivered job_id=7 name=replayed.bin`,
	)
	refusals := graphLogEvents(graphLine("object refused", `job_id=7 node=0 kind= member=notes.bin why=holds_breached`), 7, graphObjectRefusedMessage)
	if len(refusals) != 1 || refusals[0]["kind"] != "" || refusals[0]["member"] != "notes.bin" || refusals[0]["why"] != "holds_breached" {
		t.Fatalf("refusal with an empty kind = %v", refusals)
	}
	if _, ok := refusals[0]["kind"]; !ok {
		t.Fatal("an empty kind must still be read as a field")
	}
	ruled := graphLogEvents(graphLine("recovery set ruled", `job_id=7 node=1 kind=par2 set=a.par2 recovery_set_id=00ff slice_size=65536 blocks_reachable=1 damaged_slices=2 outcome=repaired slices_repaired=2 verification=`), 7, graphRecoveryRuledMessage)
	if len(ruled) != 1 || ruled[0]["verification"] != "" || ruled[0]["slices_repaired"] != "2" {
		t.Fatalf("ruling with an empty trailing verification = %v", ruled)
	}
	if _, ok := ruled[0]["verification"]; !ok {
		t.Fatal("an empty trailing verification must still be read as a field")
	}
	events := graphLogEvents(raw, 7, graphMemberDeliveredMessage)
	if len(events) != 3 {
		t.Fatalf("events = %v, want the three graph lines of job 7", events)
	}
	if got := events[0]["name"]; got != "CERTIFICATE/backup/id (1).bdmv" {
		t.Fatalf("bare name with a space = %q", got)
	}
	if got := events[0]["whole"]; got != "true" {
		t.Fatalf("whole = %q", got)
	}
	if got := events[1]["name"]; got != `quoted "name".bin` {
		t.Fatalf("quoted name = %q", got)
	}
	if got := events[2]["name"]; got != "coloured.bin" || events[2]["depth"] != "0" {
		t.Fatalf("coloured line fields = %v", events[2])
	}
}

func TestGraphDirectUnpackVerdict(t *testing.T) {
	bound := func(kind, over, depth string) string {
		return graphLine("object bound", `job_id=30 node=2 kind=`+kind+` over=`+over+` depth=`+depth)
	}
	delivered := func(name, depth, whole string) string {
		return graphLine("member delivered", `job_id=30 name=`+name+` from=`+name+` depth=`+depth+` whole=`+whole)
	}
	repaired := graphLine("recovery set ruled", `job_id=30 node=1 kind=par2 set=archive.par2 recovery_set_id=00ff slice_size=65536 blocks_reachable=10 damaged_slices=2 outcome=repaired slices_repaired=2 verification=`)
	impossible := graphLine("recovery set ruled", `job_id=30 node=1 kind=par2 set=archive.par2 recovery_set_id=00ff slice_size=65536 blocks_reachable=1 damaged_slices=2 outcome=impossible slices_repaired=0 verification=`)
	otherJobBound := graphLine("object bound", `job_id=31 node=2 kind=sevenz_engine over=archive.7z.001 depth=0`)

	armedConsumed := &ScenarioDirectUnpackAssertion{RequireArmed: true, RequireConsumed: true}
	restarted := &ScenarioDirectUnpackAssertion{RequireArmed: true, RequireRearmAfterRestart: true}
	for _, test := range []struct {
		name      string
		assertion *ScenarioDirectUnpackAssertion
		lines     []string
		output    []string
		// relaunchAfter is how many of the lines the first weaver wrote before
		// the harness relaunched it, or graphNoRelaunch; rows that do not judge
		// a restart leave it at 0.
		relaunchAfter int
		wantErr       string
	}{
		{
			name:      "armed and consumed",
			assertion: armedConsumed,
			lines:     []string{bound("sevenz_engine", "archive.7z.001", "0"), delivered("payload.mkv", "1", "true")},
			output:    []string{"payload.mkv"},
		},
		{
			name:      "never armed: only another job's archive",
			assertion: armedConsumed,
			lines:     []string{otherJobBound, delivered("payload.mkv", "1", "true")},
			output:    []string{"payload.mkv"},
			wantErr:   "bound no archive object",
		},
		{
			name:      "never armed: a recovery object is not an archive",
			assertion: armedConsumed,
			lines:     []string{bound("par2", "archive.par2", "0"), delivered("payload.mkv", "1", "true")},
			output:    []string{"payload.mkv"},
			wantErr:   "bound no archive object",
		},
		{
			name:      "never armed: a nested archive is not a root binding",
			assertion: armedConsumed,
			lines:     []string{bound("rar_store", "inner.rar", "1"), delivered("payload.mkv", "2", "true")},
			output:    []string{"payload.mkv"},
			wantErr:   "bound no archive object",
		},
		{
			name:      "the archive itself delivered",
			assertion: armedConsumed,
			lines:     []string{bound("sevenz_engine", "archive.7z.001", "0"), delivered("archive.7z.001", "0", "true")},
			output:    []string{"archive.7z.001"},
			wantErr:   "delivered the archive",
		},
		{
			name:      "an archive left in the output directory",
			assertion: armedConsumed,
			lines:     []string{bound("sevenz_engine", "archive.7z", "0"), delivered("payload.mkv", "1", "true")},
			output:    []string{"payload.mkv", "sub/archive.part01.rar"},
			wantErr:   "output directory holds archive",
		},
		{
			name:      "nothing delivered",
			assertion: armedConsumed,
			lines:     []string{bound("sevenz_engine", "archive.7z", "0")},
			wantErr:   "delivered no member",
		},
		{
			name:      "a gate-off darkness assertion has no gate to judge under graph",
			assertion: &ScenarioDirectUnpackAssertion{RequireArmed: true, RequireConsumed: true, ForbidAnyActivity: true},
			lines:     []string{bound("sevenz_engine", "archive.7z", "0"), delivered("payload.mkv", "1", "true")},
			output:    []string{"payload.mkv"},
		},
		{
			name:          "bound before the restart, every delivery after it",
			assertion:     restarted,
			lines:         []string{bound("sevenz_engine", "archive.7z.001", "0"), delivered("payload.mkv", "1", "true")},
			relaunchAfter: 1,
		},
		{
			name:          "bound and delivered after the restart",
			assertion:     restarted,
			lines:         []string{bound("sevenz_engine", "archive.7z.001", "0"), delivered("payload.mkv", "1", "true")},
			relaunchAfter: 0,
		},
		{
			name:          "a member delivered before the restart",
			assertion:     restarted,
			lines:         []string{bound("sevenz_engine", "archive.7z.001", "0"), delivered("early.mkv", "1", "true"), delivered("payload.mkv", "1", "true")},
			relaunchAfter: 2,
			wantErr:       `delivered "early.mkv" before the restart`,
		},
		{
			name:          "nothing delivered after the restart",
			assertion:     restarted,
			lines:         []string{bound("sevenz_engine", "archive.7z.001", "0")},
			relaunchAfter: 1,
			wantErr:       "no member of this job after the restart",
		},
		{
			name:          "no archive bound across the restart",
			assertion:     &ScenarioDirectUnpackAssertion{RequireRearmAfterRestart: true},
			lines:         []string{bound("par2", "archive.par2", "0"), delivered("payload.mkv", "0", "true")},
			relaunchAfter: 1,
			wantErr:       "bound no archive object",
		},
		{
			name:          "the harness never relaunched weaver",
			assertion:     restarted,
			lines:         []string{bound("sevenz_engine", "archive.7z.001", "0"), delivered("payload.mkv", "1", "true")},
			relaunchAfter: graphNoRelaunch,
			wantErr:       "no restart to judge",
		},
		{
			name:      "repaired in the stream and delivered whole",
			assertion: &ScenarioDirectUnpackAssertion{RequireArmed: true, ExpectedDemotionReason: "repair_rewrote"},
			lines:     []string{bound("sevenz_engine", "archive.7z", "0"), repaired, delivered("payload.mkv", "1", "true")},
		},
		{
			name:      "no repair ruled",
			assertion: &ScenarioDirectUnpackAssertion{RequireArmed: true, ExpectedDemotionReason: "repair_rewrote"},
			lines:     []string{bound("sevenz_engine", "archive.7z", "0"), impossible, delivered("payload.mkv", "1", "true")},
			wantErr:   "ruled repaired",
		},
		{
			name:      "repaired but delivered as it stands",
			assertion: &ScenarioDirectUnpackAssertion{RequireArmed: true, ExpectedDemotionReason: "repair_rewrote"},
			lines:     []string{bound("sevenz_engine", "archive.7z", "0"), repaired, delivered("payload.mkv", "1", "false")},
			wantErr:   "not whole",
		},
		{
			name:      "a demotion the graph has no equivalent for",
			assertion: &ScenarioDirectUnpackAssertion{ExpectedDemotionReason: "decode_failed"},
			lines:     []string{bound("sevenz_engine", "archive.7z", "0")},
			wantErr:   "no graph equivalent",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := graphLog(test.lines...)
			relaunchAt := test.relaunchAfter
			if relaunchAt > 0 {
				// The offset just past the first process's last line.
				relaunchAt = min(len(graphLog(test.lines[:test.relaunchAfter]...))+1, len(raw))
			}
			wantVerdict(t, graphDirectUnpackVerdict(raw, 30, test.assertion, test.output, relaunchAt), test.wantErr)
		})
	}
}

func TestScenarioArchiveVolumesNamesEachPostedVolume(t *testing.T) {
	scenario := &Scenario{
		FixtureAssets: []string{
			"direct-store-par2-repair/archive.part1.rar",
			"direct-store-par2-repair/archive.part4.rar::opaque.part3.rar",
			"direct-store-par2-repair/archive.par2",
		},
		SharedAssets:                []string{"sample.7z"},
		NZBSubjectFilenameOverrides: map[string]string{"opaque.part3.rar": "archive.part3.rar"},
	}
	volumes, err := scenarioArchiveVolumes(scenario, []string{"local.r00", "notes.nfo"})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(volumes))
	for _, volume := range volumes {
		got = append(got, strings.Join(volume, "|"))
	}
	want := "archive.part1.rar,local.r00,opaque.part3.rar|archive.part3.rar,sample.7z"
	if strings.Join(got, ",") != want {
		t.Fatalf("volumes = %q, want %q", strings.Join(got, ","), want)
	}
}

func TestGraphDirectStoreVerdict(t *testing.T) {
	volumes := []graphVolume{{"archive.part1.rar"}, {"opaque.part2.rar", "archive.part2.rar"}}
	storeBound := func(over string) string {
		return graphLine("object bound", `job_id=40 node=3 kind=rar_store over=`+over+` depth=0`)
	}
	refused := func(why string) string {
		return graphLine("object refused", `job_id=40 node=3 kind=rar_store member=archive.part3.rar why=`+why)
	}
	ruled := func(outcome string) string {
		return graphLine("recovery set ruled", `job_id=40 node=1 kind=par2 set=archive.par2 recovery_set_id=00ff slice_size=65536 blocks_reachable=4 damaged_slices=9 outcome=`+outcome+` slices_repaired=0 verification=`)
	}
	reboundBy := func(member, as, by string) string {
		return graphLine("recovery bound", `job_id=40 node=1 kind=par2 set=archive.par2 member=`+member+` as=`+as+` by=`+by)
	}
	routed := []string{storeBound("archive.part1.rar"), storeBound("archive.part2.rar")}

	for _, test := range []struct {
		name      string
		assertion ScenarioDirectStoreAssertion
		volumes   []graphVolume
		lines     []string
		wantErr   string
	}{
		{name: "routed direct over every volume", lines: routed},
		{
			name:    "routed under the posted name",
			lines:   []string{storeBound("archive.part1.rar"), storeBound("opaque.part2.rar")},
			volumes: volumes,
		},
		{
			name:    "one volume never routed",
			lines:   []string{storeBound("archive.part1.rar"), graphLine("object bound", `job_id=40 node=4 kind=rar_engine over=archive.part2.rar depth=0`)},
			wantErr: "no store object was bound over volume(s): opaque.part2.rar",
		},
		{
			name:    "another job's routing does not count",
			lines:   []string{storeBound("archive.part1.rar"), graphLine("object bound", `job_id=41 node=3 kind=rar_store over=archive.part2.rar depth=0`)},
			wantErr: "opaque.part2.rar",
		},
		{name: "no volumes to route", volumes: []graphVolume{}, lines: routed, wantErr: "posts no archive volume"},
		{
			name:      "damage the recovery cannot repair",
			assertion: ScenarioDirectStoreAssertion{ExpectedDemotionReason: "par2_damaged", RequireRoutedByteMaterialization: true},
			lines:     append([]string{ruled("impossible")}, routed...),
		},
		{
			name:      "damage the recovery repaired is not par2_damaged",
			assertion: ScenarioDirectStoreAssertion{ExpectedDemotionReason: "par2_damaged", RequireRoutedByteMaterialization: true},
			lines:     append([]string{ruled("repaired")}, routed...),
			wantErr:   "ruled impossible",
		},
		{
			name:      "a chain posted out of order routes and rebinds by content",
			assertion: ScenarioDirectStoreAssertion{ExpectedDemotionReason: "member_malformed_chain", RequireRoutedByteMaterialization: true, ForbidVolumeRefetch: true},
			lines:     append([]string{reboundBy("archive.part3.rar", "archive.part4.rar", "content")}, routed...),
		},
		{
			name:      "a chain reason alone still needs every volume routed",
			assertion: ScenarioDirectStoreAssertion{ExpectedDemotionReason: "member_malformed_chain"},
			lines:     []string{reboundBy("archive.part3.rar", "archive.part4.rar", "content"), storeBound("archive.part1.rar")},
			wantErr:   "no store object was bound over volume(s): opaque.part2.rar",
		},
		{
			name:      "binding by name is no rebind",
			assertion: ScenarioDirectStoreAssertion{ExpectedDemotionReason: "member_malformed_chain"},
			lines:     append([]string{reboundBy("archive.part3.rar", "archive.part3.rar", "name")}, routed...),
			wantErr:   "bound a posted volume to another name by content",
		},
		{
			name:      "content binding a volume to its own posted name is no rebind",
			assertion: ScenarioDirectStoreAssertion{ExpectedDemotionReason: "member_malformed_chain"},
			lines:     append([]string{reboundBy("archive.part3.rar", "archive.part3.rar", "content")}, routed...),
			wantErr:   "bound a posted volume to another name by content",
		},
		{
			name:      "a refusal is no rebind",
			assertion: ScenarioDirectStoreAssertion{ExpectedDemotionReason: "member_malformed_chain"},
			lines:     append([]string{refused("unsupported")}, routed...),
			wantErr:   "bound a posted volume to another name by content",
		},
		{
			name:      "a demotion the graph has no equivalent for",
			assertion: ScenarioDirectStoreAssertion{ExpectedDemotionReason: "volume_crc_mismatch"},
			wantErr:   "no graph equivalent",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testVolumes := test.volumes
			if testVolumes == nil {
				testVolumes = volumes
			}
			assertion := test.assertion
			wantVerdict(t, graphDirectStoreVerdict(graphLog(test.lines...), 40, &assertion, testVolumes), test.wantErr)
		})
	}
}

func TestGraphPar2CleanSettlementVerdict(t *testing.T) {
	ruled := func(setID, sliceSize, outcome, verification string) string {
		return graphLine("recovery set ruled", `job_id=50 node=1 kind=par2 set=`+setID+`.par2 recovery_set_id=`+setID+
			` slice_size=`+sliceSize+` blocks_reachable=8 damaged_slices=0 outcome=`+outcome+` slices_repaired=0 verification=`+verification)
	}
	assertion := func() *ScenarioPar2CleanSettlementAssertion {
		return &ScenarioPar2CleanSettlementAssertion{
			ExpectedSetSliceSizes: map[string]uint64{"aa11": 65536, "bb22": 98304},
			ExpectedSetVerificationModes: map[string][]string{
				"aa11": {"grid"},
				"bb22": {"quick_digest", "file_crc"},
			},
		}
	}
	for _, test := range []struct {
		name      string
		assertion *ScenarioPar2CleanSettlementAssertion
		lines     []string
		wantErr   string
	}{
		{
			name:      "grid set by grid, read-back set by read",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "65536", "clean", "grid"), ruled("bb22", "98304", "clean", "read")},
		},
		{
			name: "a set whose modes allow either",
			assertion: &ScenarioPar2CleanSettlementAssertion{
				ExpectedSetSliceSizes:        map[string]uint64{"aa11": 65536},
				ExpectedSetVerificationModes: map[string][]string{"aa11": {"grid", "file_crc"}},
			},
			lines: []string{ruled("aa11", "65536", "clean", "grid")},
		},
		{
			name:      "an SFV ruling is no PAR2 set",
			assertion: assertion(),
			lines: []string{
				ruled("aa11", "65536", "clean", "grid"),
				ruled("bb22", "98304", "clean", "read"),
				graphLine("recovery set ruled", `job_id=50 node=4 kind=sfv set=check.sfv recovery_set_id=cc33 slice_size=0 blocks_reachable=0 damaged_slices=0 outcome=clean slices_repaired=0 verification=read`),
			},
		},
		{
			name:      "a PAR3 set counts",
			assertion: &ScenarioPar2CleanSettlementAssertion{ExpectedSetSliceSizes: map[string]uint64{"dd44": 65536}, ExpectedSetVerificationModes: map[string][]string{"dd44": {"grid"}}},
			lines:     []string{graphLine("recovery set ruled", `job_id=50 node=1 kind=par3 set=a.par3 recovery_set_id=dd44 slice_size=65536 blocks_reachable=8 damaged_slices=0 outcome=clean slices_repaired=0 verification=grid`)},
		},
		{
			name:      "a grid-only set the object had to read",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "65536", "clean", "read"), ruled("bb22", "98304", "clean", "read")},
			wantErr:   `verification "read"`,
		},
		{
			name:      "a read-back set the grid vouched for",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "65536", "clean", "grid"), ruled("bb22", "98304", "clean", "grid")},
		},
		{
			name:      "a grid-only set the recovery object read",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "65536", "clean", "read"), ruled("bb22", "98304", "clean", "read")},
			wantErr:   `verification "read"`,
		},
		{
			name:      "a set that needed repair",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "65536", "repaired", ""), ruled("bb22", "98304", "clean", "read")},
			wantErr:   "want clean",
		},
		{
			name:      "the wrong slice size",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "98304", "clean", "grid"), ruled("bb22", "98304", "clean", "read")},
			wantErr:   "slice size 98304, want 65536",
		},
		{
			name:      "a set never ruled",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "65536", "clean", "grid")},
			wantErr:   "no recovery set ruled line for set(s): bb22",
		},
		{
			name:      "an unexpected set",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "65536", "clean", "grid"), ruled("bb22", "98304", "clean", "read"), ruled("cc33", "65536", "clean", "grid")},
			wantErr:   "unexpected set cc33",
		},
		{
			name:      "a set ruled twice",
			assertion: assertion(),
			lines:     []string{ruled("aa11", "65536", "clean", "grid"), ruled("aa11", "65536", "clean", "grid"), ruled("bb22", "98304", "clean", "read")},
			wantErr:   "more than once",
		},
		{
			name: "read-back bytes a grid ruling cannot have",
			assertion: &ScenarioPar2CleanSettlementAssertion{
				ExpectedSetSliceSizes:        map[string]uint64{"aa11": 65536},
				ExpectedSetVerificationModes: map[string][]string{"aa11": {"grid"}},
				VerificationReadBytes:        4096,
			},
			lines:   []string{ruled("aa11", "65536", "clean", "grid")},
			wantErr: "reads none",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			wantVerdict(t, graphPar2CleanSettlementVerdict(graphLog(test.lines...), 50, test.assertion), test.wantErr)
		})
	}
}

func TestGraphFileIdentityRewrite(t *testing.T) {
	recoveryBoundOf := func(kind, member, as, by string) string {
		return graphLine("recovery bound", `job_id=60 node=1 kind=`+kind+` set=archive.`+kind+` member=`+member+` as=`+as+` by=`+by)
	}
	recoveryBound := func(member, as, by string) string { return recoveryBoundOf("par2", member, as, by) }
	delivered := func(name, from string) string {
		return graphLine("member delivered", `job_id=60 name=`+name+` from=`+from+` depth=0 whole=true`)
	}
	objectBound := func(over string) string {
		return graphLine("object bound", `job_id=60 node=2 kind=rar_store over=`+over+` depth=0`)
	}
	par2 := &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar", "archive.part2.rar"},
		ForbiddenCurrentFilenames:    []string{"0a1b.101", "0a1b.102"},
		RequiredClassificationSource: "par2",
	}
	declared := &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"normalize_encrypted.rar"},
		ForbiddenCurrentFilenames:    []string{"normalize?encrypted.rar"},
		RequiredClassificationSource: "declared",
	}
	for _, test := range []struct {
		name      string
		assertion *ScenarioFileIdentityRewriteAssertion
		lines     []string
		want      func(fileIdentityRewriteObservation) bool
	}{
		{
			name:      "bound by content from the obfuscated names",
			assertion: par2,
			lines:     []string{recoveryBound("0a1b.101", "archive.part1.rar", "content"), recoveryBound("0a1b.102", "archive.part2.rar", "content")},
			want:      func(o fileIdentityRewriteObservation) bool { return o.Observed },
		},
		{
			name:      "bound by content by a PAR3 set",
			assertion: par2,
			lines:     []string{recoveryBoundOf("par3", "0a1b.101", "archive.part1.rar", "content"), recoveryBoundOf("par3", "0a1b.102", "archive.part2.rar", "content")},
			want:      func(o fileIdentityRewriteObservation) bool { return o.Observed },
		},
		{
			name:      "bound only by an SFV",
			assertion: par2,
			lines:     []string{recoveryBoundOf("sfv", "archive.part1.rar", "archive.part1.rar", "name"), recoveryBoundOf("sfv", "archive.part2.rar", "archive.part2.rar", "name")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && len(o.WrongClassificationSources) == 2 &&
					strings.Contains(o.WrongClassificationSources[0], "archive.part1.rar (found: sfv)")
			},
		},
		{
			name:      "one name by PAR2, the other only by an SFV",
			assertion: par2,
			lines:     []string{recoveryBound("0a1b.101", "archive.part1.rar", "content"), recoveryBoundOf("sfv", "0a1b.102", "archive.part2.rar", "name")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && len(o.WrongClassificationSources) == 1 &&
					strings.Contains(o.WrongClassificationSources[0], "archive.part2.rar (found: sfv)")
			},
		},
		{
			name:      "an obfuscated name bound by name",
			assertion: par2,
			lines:     []string{recoveryBound("0a1b.101", "archive.part1.rar", "name"), recoveryBound("0a1b.102", "archive.part2.rar", "content")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && len(o.WrongClassificationSources) == 1 && strings.Contains(o.WrongClassificationSources[0], "by=name")
			},
		},
		{
			name:      "a name only delivered, never described by the set",
			assertion: par2,
			lines:     []string{recoveryBound("0a1b.101", "archive.part1.rar", "content"), delivered("archive.part2.rar", "archive.part2.rar")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && len(o.WrongClassificationSources) == 1 && strings.Contains(o.WrongClassificationSources[0], "archive.part2.rar (found: declared)")
			},
		},
		{
			name:      "a required name never seen",
			assertion: par2,
			lines:     []string{recoveryBound("0a1b.101", "archive.part1.rar", "content")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && strings.Join(o.MissingCurrentFilenames, ",") == "archive.part2.rar"
			},
		},
		{
			name:      "a forbidden name described by the set",
			assertion: par2,
			lines:     []string{recoveryBound("0a1b.101", "archive.part1.rar", "content"), recoveryBound("0a1b.102", "archive.part2.rar", "content"), recoveryBound("x.bin", "0a1b.102", "name")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && strings.Join(o.ForbiddenCurrentFilenames, ",") == "0a1b.102"
			},
		},
		{
			name:      "a forbidden name delivered",
			assertion: par2,
			lines:     []string{recoveryBound("0a1b.101", "archive.part1.rar", "content"), recoveryBound("0a1b.102", "archive.part2.rar", "content"), delivered("0a1b.101", "0a1b.101")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && strings.Join(o.ForbiddenCurrentFilenames, ",") == "0a1b.101"
			},
		},
		{
			name:      "declared name an object was bound over",
			assertion: declared,
			lines:     []string{objectBound("normalize_encrypted.rar")},
			want:      func(o fileIdentityRewriteObservation) bool { return o.Observed },
		},
		{
			name:      "declared name a member was delivered from",
			assertion: declared,
			lines:     []string{delivered("normalized-output.mkv", "normalize_encrypted.rar")},
			want:      func(o fileIdentityRewriteObservation) bool { return o.Observed },
		},
		{
			name:      "declared name only described by a set",
			assertion: declared,
			lines:     []string{recoveryBound("normalize?encrypted.rar", "normalize_encrypted.rar", "content")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && len(o.WrongClassificationSources) == 1
			},
		},
		{
			name:      "the unnormalized name delivered",
			assertion: declared,
			lines:     []string{objectBound("normalize_encrypted.rar"), delivered("normalize?encrypted.rar", "normalize?encrypted.rar")},
			want: func(o fileIdentityRewriteObservation) bool {
				return !o.Observed && strings.Join(o.ForbiddenCurrentFilenames, ",") == "normalize?encrypted.rar"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation := graphFileIdentityRewrite(graphLog(test.lines...), 60, test.assertion)
			if !test.want(observation) {
				t.Fatalf("observation = %+v", observation)
			}
		})
	}
}

// Under graph the observer reads the log, not active_file_identities: there is
// no database to open and no trigger to install.
func TestFileIdentityRewriteObserverReadsTheGraphLog(t *testing.T) {
	runDir := t.TempDir()
	t.Setenv("E2E_RUN_DIR", runDir)
	t.Setenv(weaverEngineEnv, "graph")
	if err := os.MkdirAll(localWeaverDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	log := graphLine("recovery bound", `job_id=61 node=1 kind=par2 set=archive.par2 member=0a1b.101 as=archive.part1.rar by=content`)
	if err := os.WriteFile(localWeaverLogPath(), []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}
	missingDB := filepath.Join(runDir, "no-such-dir", "weaver.db")
	if err := installFileIdentityRewriteObserver(missingDB); err != nil {
		t.Fatalf("install under graph touched the database: %v", err)
	}
	observer, err := openFileIdentityRewriteObserver(missingDB)
	if err != nil {
		t.Fatal(err)
	}
	assertion := &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar"},
		ForbiddenCurrentFilenames:    []string{"0a1b.101"},
		RequiredClassificationSource: "par2",
	}
	observation, err := observer.Observe(61, assertion)
	if err != nil || !observation.Observed {
		t.Fatalf("observation = %+v, err = %v", observation, err)
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := observer.Observe(61, assertion); err == nil {
		t.Fatal("a closed observer must refuse to observe")
	}
}

// The engine-keyed fields take over only for the engine they name; under
// classic every scenario reads exactly as before.
func TestScenarioExpectationsAreEngineKeyed(t *testing.T) {
	scenario := &Scenario{
		ExpectedOutcome:               "extraction_failure",
		ExpectedErrorContains:         "classic wording",
		ExpectedOutcomeByEngine:       map[string]string{"graph": "success"},
		ExpectedErrorContainsByEngine: map[string]string{"graph": "graph wording"},
	}
	t.Setenv(weaverEngineEnv, "classic")
	if scenario.expectedOutcome() != "extraction_failure" || scenario.expectedErrorContains() != "classic wording" {
		t.Fatalf("classic expectations = %q / %q", scenario.expectedOutcome(), scenario.expectedErrorContains())
	}
	os.Unsetenv(weaverEngineEnv)
	if scenario.expectedOutcome() != "success" || scenario.expectedErrorContains() != "graph wording" {
		t.Fatalf("graph expectations = %q / %q", scenario.expectedOutcome(), scenario.expectedErrorContains())
	}
	plain := &Scenario{ExpectedOutcome: "health_failure", ExpectedErrorContains: "only wording"}
	if plain.expectedOutcome() != "health_failure" || plain.expectedErrorContains() != "only wording" {
		t.Fatal("an engine the maps do not name falls back to the unkeyed fields")
	}
}

func TestEngineKeyedScenarioFiles(t *testing.T) {
	for _, test := range []struct {
		slug         string
		classic      string
		classicError string
		graph        string
		graphError   string
	}{
		{slug: "dead-post-first-articles-missing", classic: "health_failure", classicError: "first articles are missing", graph: "health_failure", graphError: "first articles are missing"},
		{slug: "dead-post-recovery-unobtainable", classic: "health_failure", classicError: "PAR2 metadata discovery exhausted", graph: "health_failure", graphError: "below critical"},
		{slug: "rar5-colliding-member-paths", classic: "extraction_failure", graph: "success"},
	} {
		scenario, err := loadScenario(filepath.Join(testdataDir(), test.slug))
		if err != nil {
			t.Fatalf("%s: %v", test.slug, err)
		}
		if got := scenario.expectedOutcomeFor(engineClassic); got != test.classic {
			t.Fatalf("%s classic outcome = %q, want %q", test.slug, got, test.classic)
		}
		if got := scenario.expectedErrorContainsFor(engineClassic); got != test.classicError {
			t.Fatalf("%s classic error = %q, want %q", test.slug, got, test.classicError)
		}
		if got := scenario.expectedOutcomeFor(engineGraph); got != test.graph {
			t.Fatalf("%s graph outcome = %q, want %q", test.slug, got, test.graph)
		}
		if got := scenario.expectedErrorContainsFor(engineGraph); got != test.graphError {
			t.Fatalf("%s graph error = %q, want %q", test.slug, got, test.graphError)
		}
	}
}

// Under classic the assertions read the classic lines and nothing else: a log
// holding only graph lines fails them, the classic lines pass. With
// WEAVER_ENGINE unset the same assertions read the graph lines, and the
// direct-store one is judged without the classic gate.
func TestRuntimeAssertionsFollowTheEngineUnderTest(t *testing.T) {
	runDir := t.TempDir()
	t.Setenv("E2E_RUN_DIR", runDir)
	t.Setenv("WEAVER_RAR_DIRECT_STORE", "")
	if err := os.MkdirAll(localWeaverDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	writeLog := func(lines ...string) {
		t.Helper()
		if err := os.WriteFile(localWeaverLogPath(), []byte(graphLog(lines...)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	settlement := &ScenarioPar2CleanSettlementAssertion{
		ExpectedSetSliceSizes:        map[string]uint64{"aa11": 65536},
		ExpectedSetVerificationModes: map[string][]string{"aa11": {"grid"}},
	}
	classicLines := []string{
		`job_id=70 recovery_set_id=aa11 slice_size=65536 verification_mode=grid PAR2 clean set verification source`,
		`job_id=70 recovery_set_id=aa11 slice_size=65536 verdict=clean verification_read_bytes=0 PAR2 set settled clean from in-stream grid evidence`,
	}
	graphRuled := graphLine("recovery set ruled", `job_id=70 node=1 kind=par2 set=a.par2 recovery_set_id=aa11 slice_size=65536 blocks_reachable=4 damaged_slices=0 outcome=clean slices_repaired=0 verification=grid`)
	directStore := &Scenario{
		Slug:              "graph-twin-direct-store",
		FixtureAssets:     []string{"direct-store-par2-repair/archive.part1.rar"},
		RuntimeAssertions: &ScenarioRuntimeAssertions{DirectStore: &ScenarioDirectStoreAssertion{ExpectedDemotionReason: "par2_damaged"}},
	}
	graphImpossible := graphLine("recovery set ruled", `job_id=70 node=1 kind=par2 set=a.par2 recovery_set_id=aa11 slice_size=65536 blocks_reachable=1 damaged_slices=9 outcome=impossible slices_repaired=0 verification=`)

	t.Setenv(weaverEngineEnv, "classic")
	writeLog(graphRuled)
	wantVerdict(t, assertPar2CleanSettlement(70, settlement), "did not observe set(s): aa11")
	writeLog(classicLines...)
	wantVerdict(t, assertPar2CleanSettlement(70, settlement), "")
	// Classic judges direct-store only with its gate on.
	writeLog(graphImpossible)
	wantVerdict(t, assertDirectStoreScenario(70, directStore), "")
	t.Setenv("WEAVER_RAR_DIRECT_STORE", "1")
	wantVerdict(t, assertDirectStoreScenario(70, directStore), `did not demote with "par2_damaged"`)
	t.Setenv("WEAVER_RAR_DIRECT_STORE", "")

	os.Unsetenv(weaverEngineEnv)
	writeLog(classicLines...)
	wantVerdict(t, assertPar2CleanSettlement(70, settlement), "no recovery set ruled line for set(s): aa11")
	writeLog(graphRuled)
	wantVerdict(t, assertPar2CleanSettlement(70, settlement), "")
	writeLog(`WARN direct-store set demoted job_id=70 reason="par2_damaged"`)
	wantVerdict(t, assertDirectStoreScenario(70, directStore), "ruled impossible")
	writeLog(graphImpossible)
	wantVerdict(t, assertDirectStoreScenario(70, directStore), "")
}

func TestGraphDirectStoreEngagementVerdict(t *testing.T) {
	bound := func(jobID, kind string) string {
		return graphLine("object bound", `job_id=`+jobID+` node=2 kind=`+kind+` over=archive.part1.rar depth=0`)
	}
	for _, test := range []struct {
		name    string
		lines   []string
		jobIDs  []int
		wantErr string
	}{
		{name: "a store object in one job of several", lines: []string{bound("81", "rar_engine"), bound("82", "rar_store")}, jobIDs: []int{81, 82, 83}},
		{name: "a 7z store object counts", lines: []string{bound("83", "sevenz_store")}, jobIDs: []int{81, 82, 83}},
		{name: "a zip store object counts", lines: []string{bound("81", "zip")}, jobIDs: []int{81}},
		{name: "only engine objects bound", lines: []string{bound("81", "rar_engine"), bound("82", "sevenz_engine")}, jobIDs: []int{81, 82}, wantErr: "no job of the phase bound a store object"},
		{name: "a store object of a job outside the phase", lines: []string{bound("90", "rar_store")}, jobIDs: []int{81, 82}, wantErr: "no job of the phase bound a store object"},
		{
			name:    "a replayed store line",
			lines:   []string{`2026-10-18T10:00:01Z  WARN weaver_server_core::runtime::job_debug_ring: stall diagnostics line job_id=81 line=INFO weaver_server_core::pipeline::graph::status: object bound job_id=81 node=2 kind=rar_store over=a.rar depth=0`},
			jobIDs:  []int{81},
			wantErr: "no job of the phase bound a store object",
		},
		{name: "no jobs submitted", lines: []string{bound("81", "rar_store")}, wantErr: "submitted no job"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wantVerdict(t, graphDirectStoreEngagementVerdict(graphLog(test.lines...), test.jobIDs), test.wantErr)
		})
	}
}

// Under graph the phase-level engagement check reads every job's lines from
// the weaver log, with no direct-store gate and no metrics query.
func TestAssertDirectStoreEngagementReadsTheGraphLog(t *testing.T) {
	runDir := t.TempDir()
	t.Setenv("E2E_RUN_DIR", runDir)
	t.Setenv("WEAVER_RAR_DIRECT_STORE", "")
	t.Setenv(weaverEngineEnv, "graph")
	if err := os.MkdirAll(localWeaverDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	jobs := []testJob{{slug: "a", jobID: 91}, {slug: "b", jobID: 92}, {slug: "submit-failed", jobID: 0}}
	writeLog := func(lines ...string) {
		t.Helper()
		if err := os.WriteFile(localWeaverLogPath(), []byte(graphLog(lines...)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// An unreachable URL: under graph the counters are never queried.
	const noWeaver = "http://127.0.0.1:1/graphql"
	writeLog(graphLine("object bound", `job_id=92 node=2 kind=rar_store over=archive.part1.rar depth=0`))
	wantVerdict(t, assertDirectStoreEngagement(noWeaver, jobs), "")
	writeLog(graphLine("object bound", `job_id=91 node=2 kind=rar_engine over=archive.part1.rar depth=0`))
	wantVerdict(t, assertDirectStoreEngagement(noWeaver, jobs), "no job of the phase bound a store object")

	// Classic with its gate off stays a no-op, whatever the log holds.
	t.Setenv(weaverEngineEnv, string(engineClassic))
	wantVerdict(t, assertDirectStoreEngagement(noWeaver, jobs), "")
}

func TestFileIdentityRewriteFailureNamesItsEvidence(t *testing.T) {
	empty := fileIdentityRewriteObservation{}
	seen := fileIdentityRewriteObservation{
		MissingCurrentFilenames:  []string{"archive.part2.rar"},
		ObservedCurrentFilenames: []string{"archive.part1.rar (par2)"},
	}

	t.Setenv(weaverEngineEnv, string(engineClassic))
	if got := empty.terminalFailureMessage("failed", ""); !strings.Contains(got, "no matching active_file_identities rows were observed") {
		t.Fatalf("classic message = %q", got)
	}
	if got := seen.terminalFailureMessage("failed", ""); !strings.Contains(got, "effective file identity rows: archive.part1.rar (par2)") || strings.Contains(got, "graph") {
		t.Fatalf("classic message = %q", got)
	}

	t.Setenv(weaverEngineEnv, "graph")
	if got := empty.terminalFailureMessage("failed", ""); !strings.Contains(got, "no matching graph log lines") || strings.Contains(got, "active_file_identities") {
		t.Fatalf("graph message = %q", got)
	}
	if got := seen.terminalFailureMessage("failed", ""); !strings.Contains(got, "file identities in the graph log: archive.part1.rar (par2)") || strings.Contains(got, "rows") {
		t.Fatalf("graph message = %q", got)
	}
}
