// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"strconv"
	"strings"
)

// weaverEngine names the pipeline the managed weaver ran a phase with. The
// harness judges every runtime assertion that reads engine internals by the
// engine's own evidence: the classic engine's log lines and state rows, or the
// graph engine's observability lines.
type weaverEngine string

const (
	engineClassic weaverEngine = "classic"
	engineGraph   weaverEngine = "graph"
)

// weaverEngineEnv is the switch weaver reads to pick its pipeline. It reaches
// the managed weaver through the inherited environment (managedWeaverEnv), so
// the harness reads the same value the product read.
const weaverEngineEnv = "WEAVER_ENGINE"

// engineUnderTest is the engine this harness process tests: `classic` when
// WEAVER_ENGINE says so, the graph engine for anything else, unset included,
// as weaver itself reads the switch.
func engineUnderTest() weaverEngine {
	return parseWeaverEngine(os.Getenv(weaverEngineEnv))
}

func parseWeaverEngine(value string) weaverEngine {
	if strings.EqualFold(strings.TrimSpace(value), string(engineClassic)) {
		return engineClassic
	}
	return engineGraph
}

// The graph engine's observability lines, one per fact, each carrying
// `job_id` first and its fields as `key=value`.
const (
	graphLogTarget              = "weaver_server_core::pipeline::graph"
	graphObjectBoundMessage     = "object bound"
	graphObjectRefusedMessage   = "object refused"
	graphRecoveryRuledMessage   = "recovery set ruled"
	graphRecoveryBoundMessage   = "recovery bound"
	graphMemberDeliveredMessage = "member delivered"
)

// graphLogEvents returns the fields of every line in a weaver log that is the
// graph's `message` line for the job.
func graphLogEvents(raw string, jobID int, message string) []map[string]string {
	return graphLogEventsOfJobs(raw, message, []int{jobID})
}

// graphLogEventsOfJobs returns the fields of every line in a weaver log that is
// the graph's `message` line for any of the jobs. A line counts when it is
// under the graph's target, is not a debug-ring replay, and carries the message
// followed by `job_id=<one of jobIDs>`, which the contract puts first.
func graphLogEventsOfJobs(raw string, message string, jobIDs []int) []map[string]string {
	needle := message + " job_id="
	wanted := make(map[string]bool, len(jobIDs))
	for _, jobID := range jobIDs {
		wanted[strconv.Itoa(jobID)] = true
	}
	var events []map[string]string
	for _, rawLine := range strings.Split(raw, "\n") {
		line := ansiEscape.ReplaceAllString(rawLine, "")
		if !strings.Contains(line, graphLogTarget) || isWeaverDebugRingReplay(line) {
			continue
		}
		start := strings.Index(line, needle)
		if start < 0 {
			continue
		}
		fields := parseGraphLogFields(line[start+len(message):])
		if !wanted[fields["job_id"]] {
			continue
		}
		events = append(events, fields)
	}
	return events
}

// parseGraphLogFields reads `key=value` pairs. A quoted value runs to its
// closing quote, with `\"` and `\\` unescaped; a bare value runs to the next
// ` key=` or the end of the line, so a member name with a space in it, which
// tracing writes bare under `%`, is read whole.
func parseGraphLogFields(text string) map[string]string {
	fields := make(map[string]string)
	i := 0
	for i < len(text) {
		for i < len(text) && (text[i] == ' ' || text[i] == '\t') {
			i++
		}
		keyEnd := graphFieldKeyEnd(text, i)
		if keyEnd < 0 {
			// Not a field: skip the word.
			for i < len(text) && text[i] != ' ' && text[i] != '\t' {
				i++
			}
			continue
		}
		key := text[i:keyEnd]
		i = keyEnd + 1
		if i < len(text) && text[i] == '"' {
			var value strings.Builder
			i++
			for i < len(text) && text[i] != '"' {
				if text[i] == '\\' && i+1 < len(text) {
					i++
				}
				value.WriteByte(text[i])
				i++
			}
			i++
			fields[key] = value.String()
			continue
		}
		start := i
		for i < len(text) {
			if (text[i] == ' ' || text[i] == '\t') && graphFieldKeyEnd(text, i+1) >= 0 {
				break
			}
			i++
		}
		fields[key] = strings.TrimRight(text[start:i], " \t")
	}
	return fields
}

// graphFieldKeyEnd is the index of the `=` ending a field key that starts at
// `start`, or -1 when no key starts there.
func graphFieldKeyEnd(text string, start int) int {
	for i := start; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '=':
			if i == start {
				return -1
			}
			return i
		case c == '_' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
		case c >= '0' && c <= '9':
			if i == start {
				return -1
			}
		default:
			return -1
		}
	}
	return -1
}
