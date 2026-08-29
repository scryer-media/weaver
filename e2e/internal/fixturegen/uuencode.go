package fixturegen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// UUCodecToolchain is the pinned id of the uuencode/uudecode oracle pair.
const UUCodecToolchain = "uudeview-0.5.20"

// UUPlanFile is the manifest the seeder reads: testdata/<slug>/uu/plan.json.
// It exists because uuencoded releases cannot be posted the way every other
// fixture is. Nyuu is a yEnc poster and has no encoding selector, so a uu
// scenario ships its article bodies pre-encoded as corpus objects and the
// harness posts those bytes verbatim. The plan is what tells it which bytes
// belong to which file, in which order, and how large each article is on the
// wire.
const UUPlanFile = "uu/plan.json"

// UUPlanSchemaVersion is bumped whenever the plan's shape changes.
const UUPlanSchemaVersion = 1

// uuMinimumLinesPerPart is UUDeview's own floor: uuenview warns and ignores
// any smaller chunk size, so asking for one would silently produce a
// single-part encoding where the fixture wanted several.
const uuMinimumLinesPerPart = 200

// uuBytesPerLine is how many payload bytes one full uuencoded line carries.
// It is the format's constant, not a tunable: the line's length character can
// express at most 45.
const uuBytesPerLine = 45

// UUPlan is one scenario's uuencoded posting set.
type UUPlan struct {
	SchemaVersion int               `json:"schema_version"`
	Files         []UUPlanFileEntry `json:"files"`
}

// UUPlanFileEntry is one encoded file and the articles it was split across.
type UUPlanFileEntry struct {
	// Name is the name on the `begin` line, which is also the name a decoder
	// writes and therefore the name the scenario's output digest is keyed by.
	Name string `json:"name"`
	// Size is the decoded payload size in bytes.
	Size int64 `json:"size"`
	// Parts are the articles, in posting order.
	Parts []UUPlanPart `json:"parts"`
}

// UUPlanPart is one article body.
type UUPlanPart struct {
	// Number is the article's 1-based position, the `(i/N)` of its subject.
	Number int `json:"number"`
	// Body is the article body's path relative to the scenario directory.
	Body string `json:"body"`
	// Bytes is the body's size as it goes on the wire: every line CRLF
	// terminated. It is what the NZB's `bytes=` attribute must carry, and it
	// is not the size of the file on disk, which is LF terminated.
	Bytes int64 `json:"bytes"`
}

// UUSpec is one file's uuencoding, expressed as posting shape rather than
// command-line flags.
type UUSpec struct {
	// Source is the host path of the payload to encode.
	Source string
	// Name is the name the `begin` line carries. A decoder writes exactly
	// this, so it is also the fixture's expected output name.
	Name string
	// LinesPerPart splits the encoding across articles every this many
	// uuencoded lines. Zero posts the whole file as one article. UUDeview
	// will not chunk below 200 lines and neither will this.
	LinesPerPart int
	// KeepEncoderPreamble leaves uuenview's own `_=_ Part n of m` correlation
	// block in place at the head of every article. That block is prose as far
	// as the uu format is concerned, so it is exactly the preamble a decoder
	// has to skip over — in continuation articles as well as the first.
	KeepEncoderPreamble bool
	// UnpadFinalGroup rewrites the last data line so its final group carries
	// only the characters the remaining bytes need, instead of the four a
	// canonical encoder always emits. This is the one deliberate deviation
	// from what the oracle produced: it reproduces a class of broken encoder
	// that really did post to Usenet, and it is what the tail-tolerance probe
	// is for. The payload size must not be a multiple of three, or there is
	// no partial group to strip.
	UnpadFinalGroup bool
}

// EncodeUU encodes every spec with the pinned oracle, writes the article
// bodies and the plan into the scenario's output directory, and then proves
// the result by decoding it with a real decoder: the bytes uudeview recovers
// from the articles must equal the payload that went in. A fixture that does
// not survive that round trip never reaches the corpus.
func EncodeUU(ctx context.Context, env *Env, specs []UUSpec) error {
	if len(specs) == 0 {
		return fmt.Errorf("uuencode: no files to encode")
	}
	toolchain, err := env.Lock.Find(UUCodecToolchain)
	if err != nil {
		return err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return err
	}
	env.usedToolchain(toolchain.ID)

	plan := UUPlan{SchemaVersion: UUPlanSchemaVersion}
	for index, spec := range specs {
		entry, err := encodeUUFile(ctx, env, toolchain, index, spec)
		if err != nil {
			return fmt.Errorf("uuencode %s: %w", spec.Name, err)
		}
		plan.Files = append(plan.Files, entry)
	}

	rendered, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(env.OutputPath(UUPlanFile), func(writer io.Writer) error {
		_, err := writer.Write(append(rendered, '\n'))
		return err
	})
}

func encodeUUFile(
	ctx context.Context,
	env *Env,
	toolchain Toolchain,
	index int,
	spec UUSpec,
) (UUPlanFileEntry, error) {
	if strings.TrimSpace(spec.Name) == "" {
		return UUPlanFileEntry{}, fmt.Errorf("a spec needs the name its begin line will carry")
	}
	if spec.LinesPerPart != 0 && spec.LinesPerPart < uuMinimumLinesPerPart {
		return UUPlanFileEntry{}, fmt.Errorf(
			"LinesPerPart %d is below UUDeview's floor of %d; it would be ignored and the file posted whole",
			spec.LinesPerPart, uuMinimumLinesPerPart)
	}

	// The encoder runs in a directory of its own: it names its output after
	// the input's base name, so two files whose names differ only by
	// extension would otherwise collide.
	relative := filepath.Join("uu-encode", strconv.Itoa(index))
	work := filepath.Join(env.Work, relative)
	if err := os.MkdirAll(work, 0o755); err != nil {
		return UUPlanFileEntry{}, err
	}
	if err := CopyFile(spec.Source, filepath.Join(work, spec.Name)); err != nil {
		return UUPlanFileEntry{}, err
	}
	size, err := FileSize(spec.Source)
	if err != nil {
		return UUPlanFileEntry{}, err
	}

	arguments := []string{"uuenview", "-u"}
	if spec.LinesPerPart > 0 {
		arguments = append(arguments, "-"+strconv.Itoa(spec.LinesPerPart))
	}
	arguments = append(arguments, "-od", ".", spec.Name)
	if err := env.Docker.Run(ctx, toolchain, env.Work, relative, arguments...); err != nil {
		return UUPlanFileEntry{}, err
	}

	produced, err := uuEncoderParts(work, spec.Name)
	if err != nil {
		return UUPlanFileEntry{}, err
	}

	entry := UUPlanFileEntry{Name: spec.Name, Size: size}
	bodies := make([][]string, 0, len(produced))
	for _, path := range produced {
		contents, err := os.ReadFile(path)
		if err != nil {
			return UUPlanFileEntry{}, err
		}
		lines := strings.Split(strings.ReplaceAll(string(contents), "\r\n", "\n"), "\n")
		lines = trimTrailingBlankLines(lines)
		if !spec.KeepEncoderPreamble {
			lines = stripEncoderPreamble(lines)
		}
		bodies = append(bodies, lines)
	}
	if len(bodies) == 0 {
		return UUPlanFileEntry{}, fmt.Errorf("the encoder produced no parts")
	}
	if spec.UnpadFinalGroup {
		last := len(bodies) - 1
		unpadded, err := unpadFinalGroup(bodies[last])
		if err != nil {
			return UUPlanFileEntry{}, err
		}
		bodies[last] = unpadded
	}

	for number, lines := range bodies {
		body := fmt.Sprintf("uu/%s.%03d", spec.Name, number+1)
		if err := writeUUBody(env.OutputPath(body), lines); err != nil {
			return UUPlanFileEntry{}, err
		}
		entry.Parts = append(entry.Parts, UUPlanPart{
			Number: number + 1,
			Body:   body,
			Bytes:  wireSize(lines),
		})
	}

	if err := verifyUURoundTrip(ctx, env, toolchain, index, spec, bodies); err != nil {
		return UUPlanFileEntry{}, err
	}
	return entry, nil
}

// uuEncoderParts lists what uuenview wrote, in part order. It names its output
// after the input's base name with a three-digit suffix, so the source file
// itself is the one entry to leave out.
func uuEncoderParts(dir, source string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var parts []string
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == source {
			continue
		}
		suffix := filepath.Ext(entry.Name())
		if len(suffix) != 4 {
			continue
		}
		if _, err := strconv.Atoi(suffix[1:]); err != nil {
			continue
		}
		parts = append(parts, filepath.Join(dir, entry.Name()))
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("uuenview wrote no numbered parts into %s", dir)
	}
	sort.Strings(parts)
	return parts, nil
}

// stripEncoderPreamble drops uuenview's `_=_` correlation block, and the blank
// lines around it, from the head of an article, leaving the bare canonical
// shape: `begin` in the first article, nothing but data lines in the rest.
// Only a leading run is removed — a blank line inside the encoding would be
// corruption, and this must not paper over one.
func stripEncoderPreamble(lines []string) []string {
	start := 0
	for start < len(lines) {
		trimmed := strings.TrimSpace(lines[start])
		if trimmed != "" && !strings.HasPrefix(trimmed, "_=_") {
			break
		}
		start++
	}
	return lines[start:]
}

func trimTrailingBlankLines(lines []string) []string {
	end := len(lines)
	for end > 0 && strings.TrimSpace(lines[end-1]) == "" {
		end--
	}
	return lines[:end]
}

// unpadFinalGroup strips the padding characters a canonical encoder emits for
// the last, partial group of the last data line. The line's length character
// is left alone: it still declares how many bytes the line carries, which is
// the whole reason a tolerant decoder can recover the bytes anyway.
func unpadFinalGroup(lines []string) ([]string, error) {
	if len(lines) < 3 {
		return nil, fmt.Errorf("an article with %d lines cannot be the tail of an encoding", len(lines))
	}
	if strings.TrimSpace(lines[len(lines)-1]) != "end" {
		return nil, fmt.Errorf("the last line is %q, not the end trailer", lines[len(lines)-1])
	}
	if terminator := lines[len(lines)-2]; strings.TrimSpace(terminator) != "" && terminator != "`" {
		return nil, fmt.Errorf("the line before end is %q, not the terminator", terminator)
	}
	index := len(lines) - 3
	line := lines[index]
	if line == "" {
		return nil, fmt.Errorf("the final data line is empty")
	}

	declared := int(line[0]) - 0x20
	payload := line[1:]
	if declared <= 0 || declared > uuBytesPerLine {
		return nil, fmt.Errorf("the final data line declares %d bytes", declared)
	}
	if want := 4 * ((declared + 2) / 3); len(payload) != want {
		return nil, fmt.Errorf(
			"the final data line carries %d characters for %d bytes, want the canonical %d",
			len(payload), declared, want)
	}
	remainder := declared % 3
	if remainder == 0 {
		return nil, fmt.Errorf(
			"the payload is a multiple of three bytes, so its final group is full and there is no padding to strip")
	}

	// Only the characters that encode nothing but the zero-fill come off. For
	// one trailing byte that is the last two characters, for two it is the
	// last one; either way each must be the encoding of zero, or the line is
	// not shaped the way this thinks it is.
	strip := 3 - remainder
	for _, character := range payload[len(payload)-strip:] {
		if character != '`' && character != ' ' {
			return nil, fmt.Errorf(
				"the final group's character %q is not zero-fill; refusing to strip data", character)
		}
	}
	out := append([]string(nil), lines...)
	out[index] = string(line[0]) + payload[:len(payload)-strip]
	return out, nil
}

// wireSize is what the NZB's `bytes=` attribute has to say: the article body
// as it is served, every line CRLF terminated.
func wireSize(lines []string) int64 {
	var total int64
	for _, line := range lines {
		total += int64(len(line)) + 2
	}
	return total
}

func writeUUBody(path string, lines []string) error {
	return writeFile(path, func(writer io.Writer) error {
		for _, line := range lines {
			if _, err := io.WriteString(writer, line+"\n"); err != nil {
				return err
			}
		}
		return nil
	})
}

// verifyUURoundTrip decodes the article bodies back with the pinned decoder
// and fails unless it recovers the payload byte for byte. Concatenating the
// bodies in posting order is exactly the byte stream a reader assembles from
// the articles, so this is the same input weaver will see, minus the
// transport.
func verifyUURoundTrip(
	ctx context.Context,
	env *Env,
	toolchain Toolchain,
	index int,
	spec UUSpec,
	bodies [][]string,
) error {
	relative := filepath.Join("uu-verify", strconv.Itoa(index))
	work := filepath.Join(env.Work, relative)
	if err := os.MkdirAll(work, 0o755); err != nil {
		return err
	}
	stream := filepath.Join(work, "stream.uu")
	var joined []string
	for _, lines := range bodies {
		joined = append(joined, lines...)
	}
	if err := writeUUBody(stream, joined); err != nil {
		return err
	}

	if err := env.Docker.Run(ctx, toolchain, env.Work, relative,
		"uudeview", "-i", "-q", "-o", "-p", ".", "stream.uu"); err != nil {
		return fmt.Errorf("decode round trip: %w", err)
	}

	decoded := filepath.Join(work, spec.Name)
	same, err := sameContents(spec.Source, decoded)
	if err != nil {
		return fmt.Errorf("decode round trip: %w", err)
	}
	if !same {
		return fmt.Errorf(
			"the decoder did not recover %s: what it wrote differs from the payload that was encoded", spec.Name)
	}
	return nil
}

func sameContents(left, right string) (bool, error) {
	leftDigest, err := corpus.DigestFile(left)
	if err != nil {
		return false, err
	}
	rightDigest, err := corpus.DigestFile(right)
	if err != nil {
		return false, err
	}
	return leftDigest.BLAKE3 == rightDigest.BLAKE3 && leftDigest.Size == rightDigest.Size, nil
}
