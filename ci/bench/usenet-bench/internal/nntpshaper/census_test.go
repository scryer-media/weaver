package nntpshaper

import (
	"strings"
	"testing"
)

func TestCommandCensusCountsArticleRequestsAndRepeats(t *testing.T) {
	attestation := NewAttestation(AttestationConfig{})
	if err := attestation.AcquireExecutionLease(strings.Repeat("d", 64)); err != nil {
		t.Fatal(err)
	}
	// Two connections, pipelined commands, one line split across writes, and
	// the same article requested three times: once bracketed on the first
	// connection, once bare on the second, once bracketed again.
	first := NewCommandCensus(attestation)
	first.Observe([]byte("AUTHINFO USER bench\r\nAUTHINFO PASS secret\r\nBODY <a@example>\r\nBO"))
	first.Observe([]byte("DY <b@example>\r\nbody <a@example>\r\n"))
	second := NewCommandCensus(attestation)
	second.Observe([]byte("STAT a@example\r\nARTICLE <c@example>\r\nHEAD 12345\r\nQUIT\r\n"))

	snapshot := attestation.Snapshot()
	if snapshot.ArticleRequests != 6 {
		t.Fatalf("article requests=%d, want 6", snapshot.ArticleRequests)
	}
	if snapshot.DistinctArticleRequests != 3 {
		t.Fatalf("distinct articles=%d, want 3 (a, b, c)", snapshot.DistinctArticleRequests)
	}
	if snapshot.RepeatedArticleRequests != 2 {
		t.Fatalf("repeated requests=%d, want 2 (a twice more)", snapshot.RepeatedArticleRequests)
	}
	for verb, want := range map[string]uint64{"AUTHINFO": 2, "BODY": 3, "STAT": 1, "ARTICLE": 1, "HEAD": 1, "QUIT": 1} {
		if got := snapshot.DownstreamCommands[verb]; got != want {
			t.Errorf("%s=%d, want %d", verb, got, want)
		}
	}
}

func TestCommandCensusResetsDistinctSetPerLease(t *testing.T) {
	attestation := NewAttestation(AttestationConfig{})
	if err := attestation.AcquireExecutionLease(strings.Repeat("e", 64)); err != nil {
		t.Fatal(err)
	}
	NewCommandCensus(attestation).Observe([]byte("BODY <a@example>\r\nBODY <a@example>\r\n"))
	if err := attestation.ReleaseExecutionLease(strings.Repeat("e", 64)); err != nil {
		t.Fatal(err)
	}
	if err := attestation.AcquireExecutionLease(strings.Repeat("f", 64)); err != nil {
		t.Fatal(err)
	}
	before := attestation.Snapshot()
	if before.DistinctArticleRequests != 0 {
		t.Fatalf("distinct set survived the lease boundary: %d", before.DistinctArticleRequests)
	}
	if before.ArticleRequests != 2 || before.RepeatedArticleRequests != 1 {
		t.Fatalf("cumulative counters must not reset: %+v", before)
	}
	// The same article in a new lease is a fresh request, not a repeat.
	NewCommandCensus(attestation).Observe([]byte("BODY <a@example>\r\n"))
	after := attestation.Snapshot()
	if after.ArticleRequests-before.ArticleRequests != 1 || after.RepeatedArticleRequests != before.RepeatedArticleRequests || after.DistinctArticleRequests != 1 {
		t.Fatalf("unexpected census across leases: before=%+v after=%+v", before, after)
	}
}

func TestCommandCensusToleratesGarbage(t *testing.T) {
	attestation := NewAttestation(AttestationConfig{})
	census := NewCommandCensus(attestation)
	census.Observe([]byte("\r\n"))
	census.Observe([]byte(strings.Repeat("x", maxCommandLine+10)))
	census.Observe([]byte("still oversized\r\nBODY <ok@example>\r\n"))
	snapshot := attestation.Snapshot()
	if snapshot.DownstreamCommands["EMPTY"] != 1 || snapshot.DownstreamCommands["OVERSIZED"] != 1 || snapshot.DownstreamCommands["BODY"] != 1 {
		t.Fatalf("unexpected tallies: %+v", snapshot.DownstreamCommands)
	}
	if snapshot.ArticleRequests != 1 || snapshot.DistinctArticleRequests != 1 {
		t.Fatalf("the request after the oversized line was lost: %+v", snapshot)
	}
}

func TestNormalizeMessageID(t *testing.T) {
	for input, want := range map[string]string{
		"<a@example>": "<a@example>",
		"a@example":   "<a@example>",
		"12345":       "",
		"":            "",
		"<>":          "",
	} {
		if got := normalizeMessageID(input); got != want {
			t.Errorf("normalizeMessageID(%q)=%q, want %q", input, got, want)
		}
	}
}

func TestCommandCensusBoundsVerbKeysOnNonNNTPStreams(t *testing.T) {
	attestation := NewAttestation(AttestationConfig{})
	census := NewCommandCensus(attestation)
	// Ciphertext-like bytes: newlines land at random offsets, so every
	// pseudo-line starts with a different random token.
	payload := make([]byte, 0, 64*1024)
	seed := uint32(0x9e3779b9)
	for len(payload) < cap(payload) {
		seed = seed*1664525 + 1013904223
		payload = append(payload, byte(seed>>24))
	}
	census.Observe(payload)
	census.Observe([]byte("\r\nBODY <ok@example>\r\nxfeature compress gzip\r\n"))
	snapshot := attestation.Snapshot()
	// A random pseudo-line occasionally starts with a short all-alphanumeric
	// token that passes for a verb; that is fine as long as the key space
	// stays small and every key is verb-shaped.
	if len(snapshot.DownstreamCommands) > 12 {
		t.Fatalf("verb key space not bounded: %d keys in %+v", len(snapshot.DownstreamCommands), snapshot.DownstreamCommands)
	}
	for verb := range snapshot.DownstreamCommands {
		if !isCommandVerb(verb) {
			t.Fatalf("non-verb key %q in %+v", verb, snapshot.DownstreamCommands)
		}
	}
	if snapshot.DownstreamCommands["NONVERB"] == 0 {
		t.Fatalf("random tokens were not tallied as NONVERB: %+v", snapshot.DownstreamCommands)
	}
	if snapshot.DownstreamCommands["BODY"] != 1 || snapshot.DownstreamCommands["XFEATURE"] != 1 || snapshot.ArticleRequests != 1 {
		t.Fatalf("real commands after the garbage were lost: %+v", snapshot.DownstreamCommands)
	}
}

func TestIsCommandVerb(t *testing.T) {
	for verb, want := range map[string]bool{
		"BODY": true, "AUTHINFO": true, "XZVER": true, "CAPABILITIES": true, "MODE": true,
		"": false, "BO-DY": false, "\x16\x03": false, "TOOLONGFORANYNNTPVERB": false, "body": false,
	} {
		if got := isCommandVerb(verb); got != want {
			t.Errorf("isCommandVerb(%q) = %v, want %v", verb, got, want)
		}
	}
}
