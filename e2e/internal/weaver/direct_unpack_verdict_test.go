package weaver

import (
	"strings"
	"testing"
)

func TestDirectUnpackVerdictRequiresConsumptionUnlessTheChaseYieldedItsMemory(t *testing.T) {
	armed := `INFO direct unpack armed job_id=10030 set_name="solid"`
	consumed := `INFO installed direct-unpack members instead of re-extracting job_id=10030 set_name="solid"`
	demoted := func(reason string) string {
		return `WARN direct unpack demoted job_id=10030 set_name="solid" reason="` + reason + `" error="stopped"`
	}
	otherJob := `INFO installed direct-unpack members instead of re-extracting job_id=10031 set_name="solid"`
	assertion := &ScenarioDirectUnpackAssertion{RequireArmed: true, RequireConsumed: true}

	cases := []struct {
		name    string
		lines   []string
		wantErr string
	}{
		{name: "consumed", lines: []string{armed, consumed}},
		{name: "yielded its memory", lines: []string{armed, demoted("memory_yielded"), otherJob}},
		{name: "decode failed", lines: []string{armed, demoted("decode_failed"), otherJob}, wantErr: "did not install its members"},
		{name: "failed after an earlier yield", lines: []string{armed, demoted("memory_yielded"), armed, demoted("decode_failed")}, wantErr: "did not install its members"},
		{name: "armed again after a yield and never finished", lines: []string{armed, demoted("memory_yielded"), armed}, wantErr: "did not install its members"},
		{name: "yielded after an earlier failure", lines: []string{armed, demoted("decode_failed"), armed, demoted("memory_yielded")}},
		{name: "never finished", lines: []string{armed}, wantErr: "did not install its members"},
		{name: "never armed", lines: []string{demoted("memory_yielded")}, wantErr: "never armed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := directUnpackVerdict(strings.Join(tc.lines, "\n"), 10030, assertion, true)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}
