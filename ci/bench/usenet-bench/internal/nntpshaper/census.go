package nntpshaper

import (
	"bytes"
	"strings"
)

// The command census answers one question the byte counters cannot: when a
// client pulls more bytes than the NZB carries, did it ask for articles twice?
// Downstream bytes are what the shaper wrote into client sockets, so kernel
// retransmits never inflate them — but a client that re-requests an article
// after abandoning a connection, or hedges the same article on two
// connections, does. Counting the article requests it sent, and how many of
// them named a message-id already requested during the same execution lease,
// separates a redundant fetch from a client that simply keeps reading.

// The article verbs whose argument names one article. Everything else the
// client sends is tallied under its own verb without an argument census.
var articleVerbs = map[string]bool{"ARTICLE": true, "BODY": true, "HEAD": true, "STAT": true}

// maxCommandLine bounds how much of a client line the census keeps while it
// waits for the line terminator. NNTP lines are short; anything past this is a
// client speaking something other than NNTP and is tallied as oversized.
const maxCommandLine = 4096

// CommandCensus splits one client's upstream byte stream back into command
// lines and reports each to the attestation. One census per downstream
// connection: a line may straddle two writes, so the split state is per
// stream.
type CommandCensus struct {
	attestation *Attestation
	pending     []byte
	oversized   bool
}

func NewCommandCensus(attestation *Attestation) *CommandCensus {
	return &CommandCensus{attestation: attestation}
}

// Observe consumes bytes the client sent upstream. It never fails: the census
// is evidence, not a gate, and a client speaking garbage still gets proxied.
func (census *CommandCensus) Observe(payload []byte) {
	for len(payload) > 0 {
		newline := bytes.IndexByte(payload, '\n')
		if newline < 0 {
			census.keep(payload)
			return
		}
		census.keep(payload[:newline])
		census.flush()
		payload = payload[newline+1:]
	}
}

func (census *CommandCensus) keep(fragment []byte) {
	if census.oversized {
		return
	}
	if len(census.pending)+len(fragment) > maxCommandLine {
		census.oversized = true
		census.pending = census.pending[:0]
		return
	}
	census.pending = append(census.pending, fragment...)
}

func (census *CommandCensus) flush() {
	if census.oversized {
		census.attestation.ObserveCommand("OVERSIZED", "")
		census.oversized = false
		return
	}
	line := strings.TrimRight(string(census.pending), "\r")
	census.pending = census.pending[:0]
	fields := strings.Fields(line)
	if len(fields) == 0 {
		census.attestation.ObserveCommand("EMPTY", "")
		return
	}
	verb := strings.ToUpper(fields[0])
	argument := ""
	if len(fields) > 1 {
		argument = fields[1]
	}
	census.attestation.ObserveCommand(verb, argument)
}

// normalizeMessageID gives bracketed and bare forms of the same message-id one
// key, so a client that switches forms between attempts still registers a
// repeat. A numeric argument is an article number in the selected group, not
// a message-id, and returns "" so it is counted as a request without an
// identity.
func normalizeMessageID(argument string) string {
	argument = strings.TrimSpace(argument)
	if argument == "" {
		return ""
	}
	if strings.Trim(argument, "0123456789") == "" {
		return ""
	}
	argument = strings.TrimPrefix(argument, "<")
	argument = strings.TrimSuffix(argument, ">")
	if argument == "" {
		return ""
	}
	return "<" + argument + ">"
}
