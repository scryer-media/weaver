package fixturegen

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// ScenarioDigestKey is the scenario.json field that pins the BLAKE3 of an
// extracted member. Regenerating a payload changes it, so the generator owns
// it — and nothing else in the file.
const ScenarioDigestKey = "expectedOutputBLAKE3"

// RewriteScenarioDigests replaces the expectedOutputBLAKE3 block in place.
// The rest of the file is left byte for byte alone: the block is located in
// the raw text and only its braces are replaced, so field order, spacing and
// the trailing newline all survive untouched.
func RewriteScenarioDigests(path string, digests map[string]string) error {
	original, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	start, end, indent, err := locateScenarioBlock(string(original), ScenarioDigestKey)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	replacement := renderDigestBlock(digests, indent)
	updated := string(original[:start]) + replacement + string(original[end:])
	if updated == string(original) {
		return nil
	}
	var check map[string]json.RawMessage
	if err := json.Unmarshal([]byte(updated), &check); err != nil {
		return fmt.Errorf("%s: rewriting %s produced invalid JSON: %w", path, ScenarioDigestKey, err)
	}
	return os.WriteFile(path, []byte(updated), 0o644)
}

// locateScenarioBlock finds `"key": { ... }` and returns the byte range of the
// whole `"key": {...}` run plus the indentation the key sits at.
func locateScenarioBlock(document, key string) (int, int, string, error) {
	needle := `"` + key + `"`
	index := strings.Index(document, needle)
	if index < 0 {
		return 0, 0, "", fmt.Errorf("no %s field to rewrite", key)
	}
	lineStart := strings.LastIndexByte(document[:index], '\n') + 1
	indent := document[lineStart:index]
	if strings.TrimSpace(indent) != "" {
		return 0, 0, "", fmt.Errorf("%s is not at the start of its line", key)
	}
	open := strings.IndexByte(document[index:], '{')
	if open < 0 {
		return 0, 0, "", fmt.Errorf("%s is not an object", key)
	}
	depth := 0
	for cursor := index + open; cursor < len(document); cursor++ {
		switch document[cursor] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return index, cursor + 1, indent, nil
			}
		case '"':
			for cursor++; cursor < len(document); cursor++ {
				if document[cursor] == '\\' {
					cursor++
					continue
				}
				if document[cursor] == '"' {
					break
				}
			}
		}
	}
	return 0, 0, "", fmt.Errorf("%s object is not closed", key)
}

func renderDigestBlock(digests map[string]string, indent string) string {
	members := make([]string, 0, len(digests))
	for member := range digests {
		members = append(members, member)
	}
	sort.Strings(members)
	var builder strings.Builder
	builder.WriteString(`"` + ScenarioDigestKey + `": {`)
	for position, member := range members {
		if position > 0 {
			builder.WriteByte(',')
		}
		name, _ := json.Marshal(member)
		digest, _ := json.Marshal(digests[member])
		builder.WriteString("\n" + indent + "  " + string(name) + ": " + string(digest))
	}
	builder.WriteString("\n" + indent + "}")
	return builder.String()
}

// ScenarioHasDigests reports whether a scenario pins member digests.
func ScenarioHasDigests(path string) (bool, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(contents, &document); err != nil {
		return false, fmt.Errorf("%s: %w", path, err)
	}
	_, ok := document[ScenarioDigestKey]
	return ok, nil
}
