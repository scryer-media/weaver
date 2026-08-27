package weaver

import (
	"bytes"
	"testing"
)

func TestRewriteNZBSegmentNumbers(t *testing.T) {
	input := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="poster" date="1700000000" subject="&quot;sparse.mkv&quot; yEnc (1/3)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">one@example.com</segment>
      <segment bytes="64" number="2">two@example.com</segment>
      <segment bytes="32" number="3">three@example.com</segment>
    </segments>
  </file>
</nzb>`)

	output, err := rewriteNZBSegmentNumbers(input, []int{1, 4, 6})
	if err != nil {
		t.Fatalf("rewriteNZBSegmentNumbers() error = %v", err)
	}

	for _, want := range [][]byte{
		[]byte(`number="1"`),
		[]byte(`number="4"`),
		[]byte(`number="6"`),
	} {
		if !bytes.Contains(output, want) {
			t.Fatalf("rewritten NZB missing %s in %q", want, output)
		}
	}
	if bytes.Contains(output, []byte(`number="2"`)) || bytes.Contains(output, []byte(`number="3"`)) {
		t.Fatalf("rewritten NZB kept dense numbering: %q", output)
	}

	ids, err := extractAllMessageIDsFromNZB(output)
	if err != nil {
		t.Fatalf("extractAllMessageIDsFromNZB() error = %v", err)
	}
	wantIDs := []string{"one@example.com", "two@example.com", "three@example.com"}
	if len(ids) != len(wantIDs) {
		t.Fatalf("message id count = %d, want %d", len(ids), len(wantIDs))
	}
	for index, want := range wantIDs {
		if ids[index] != want {
			t.Fatalf("message id %d = %q, want %q", index, ids[index], want)
		}
	}
}

func TestRewriteNZBSegmentNumbersRejectsMismatchedCounts(t *testing.T) {
	input := []byte(`<nzb><file><segments><segment bytes="1" number="1">one</segment></segments></file></nzb>`)
	if _, err := rewriteNZBSegmentNumbers(input, []int{1, 4}); err == nil {
		t.Fatal("expected mismatch error")
	}
}

func TestRewriteNZBSubjectFilenames(t *testing.T) {
	input := []byte(`<nzb><file subject="&quot;posted.part1.rar&quot; yEnc (1/1)"><segments><segment bytes="1" number="1">one</segment></segments></file></nzb>`)
	output, err := rewriteNZBSubjectFilenames(input, map[string]string{
		"posted.part1.rar": "declared.part1.rar",
	})
	if err != nil {
		t.Fatalf("rewrite NZB subjects: %v", err)
	}
	if !bytes.Contains(output, []byte(`&quot;declared.part1.rar&quot;`)) || bytes.Contains(output, []byte(`&quot;posted.part1.rar&quot;`)) {
		t.Fatalf("subject rewrite = %q", output)
	}
	if _, err := rewriteNZBSubjectFilenames(input, map[string]string{"missing.rar": "declared.rar"}); err == nil {
		t.Fatal("expected an unmatched source filename error")
	}
}

func TestScenarioNZBSegmentNumbersBuildsGeneratedSequence(t *testing.T) {
	input := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="poster" date="1700000000" subject="&quot;sparse.mkv&quot; yEnc (1/3)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="64" number="1">one@example.com</segment>
      <segment bytes="64" number="2">two@example.com</segment>
      <segment bytes="32" number="3">three@example.com</segment>
    </segments>
  </file>
</nzb>`)

	numbers, err := scenarioNZBSegmentNumbers(input, &Scenario{
		NZBSegmentNumberStart: 1001,
		NZBSegmentNumberStep:  2,
	})
	if err != nil {
		t.Fatalf("scenarioNZBSegmentNumbers() error = %v", err)
	}
	want := []int{1001, 1003, 1005}
	if len(numbers) != len(want) {
		t.Fatalf("generated number count = %d, want %d", len(numbers), len(want))
	}
	for index, expected := range want {
		if numbers[index] != expected {
			t.Fatalf("generated number %d = %d, want %d", index, numbers[index], expected)
		}
	}
}
