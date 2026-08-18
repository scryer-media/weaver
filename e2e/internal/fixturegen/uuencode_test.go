package fixturegen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The uu fixtures are written by a pinned oracle and proved by a second one:
// EncodeUU will not publish an encoding that uudeview cannot decode back to
// the payload. These tests are the third opinion, and a deliberately
// independent one — the decoder below is a few lines of Go that implement the
// format from its definition, so a shared misunderstanding between the two
// halves of UUDeview cannot hide here.

// uuFixture describes one generated file: where its articles are, and how to
// reproduce the payload they were encoded from.
type uuFixture struct {
	slug     string
	name     string
	parts    int
	payload  func(path string) error
	preamble bool
	unpadded bool
}

func uuFixtures() []uuFixture {
	prng := func(seed string, size int64) func(string) error {
		return func(path string) error { return WritePRNG(path, seed, size) }
	}
	text := func(body string, size int) func(string) error {
		return func(path string) error { return WriteText(path, body, size) }
	}
	return []uuFixture{
		{
			slug: "uu-release", name: uuReleaseMedia, parts: 8,
			payload: prng("silver-horizon", uuMediaBytes),
		},
		{
			slug: "uu-release", name: uuReleaseNFO, parts: 1,
			payload: text(uuReleaseNotesText, uuShortSidecarBytes),
		},
		{
			slug: "uu-mixed-yenc", name: uuMixedNFO, parts: 3,
			payload: text(uuMixedNotesText, uuLongSidecarBytes),
		},
		{
			slug: "uu-preamble-tail", name: uuPreambleMedia, parts: 8,
			payload: prng("violet-cascade", uuMediaBytes), preamble: true,
		},
		{
			slug: "uu-preamble-tail", name: uuPreambleNFO, parts: 3,
			payload: text(uuPreambleNotesText, uuLongSidecarBytes), unpadded: true,
		},
		{
			slug: "uu-missing-middle", name: uuMissingMedia, parts: 8,
			payload: prng("crimson-vale", uuMediaBytes),
		},
	}
}

// TestUUFixturesRoundTripThroughAReferenceDecoder is the byte-equality claim:
// concatenate a file's articles in posting order, decode them with the
// reference decoder below, and the result must be the payload the recipe
// declares. It covers the canonical shape, the prose-preamble shape and the
// unpadded tail alike.
func TestUUFixturesRoundTripThroughAReferenceDecoder(t *testing.T) {
	for _, fixture := range uuFixtures() {
		t.Run(fixture.slug+"/"+fixture.name, func(t *testing.T) {
			lines := readUUArticles(t, fixture)
			decoded, name := referenceDecodeUU(t, lines)
			if name != fixture.name {
				t.Errorf("the begin line names %q, want %q", name, fixture.name)
			}

			want := filepath.Join(t.TempDir(), "payload")
			if err := fixture.payload(want); err != nil {
				t.Fatal(err)
			}
			expected, err := os.ReadFile(want)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, expected) {
				t.Fatalf("decoded %d bytes, want %d; the articles do not carry the payload the recipe declares",
					len(decoded), len(expected))
			}
		})
	}
}

// TestUUArticleShapes pins the posting shape the format and the field expect:
// `begin` only in the first article, `end` only in the last, continuations
// opening straight into data, and every split falling on a line boundary.
func TestUUArticleShapes(t *testing.T) {
	for _, fixture := range uuFixtures() {
		t.Run(fixture.slug+"/"+fixture.name, func(t *testing.T) {
			for index := 1; index <= fixture.parts; index++ {
				lines := readUUArticle(t, fixture, index)
				if len(lines) == 0 {
					t.Fatalf("article %d is empty", index)
				}
				data := lines
				if fixture.preamble {
					if strings.TrimSpace(lines[0]) != "" {
						t.Errorf("article %d should open with the encoder's prose block", index)
					}
					data = stripEncoderPreamble(lines)
				}

				switch index {
				case 1:
					if !strings.HasPrefix(data[0], "begin ") {
						t.Fatalf("the first article opens with %q, want a begin line", data[0])
					}
					var mode int
					var name string
					if _, err := fmt.Sscanf(data[0], "begin %o %s", &mode, &name); err != nil {
						t.Fatalf("begin line %q does not parse: %v", data[0], err)
					}
					if mode != 0o644 {
						t.Errorf("begin line declares mode %o, want 644", mode)
					}
					data = data[1:]
				default:
					// A continuation carries no header of any kind: the first
					// thing in it is a data line.
					if strings.HasPrefix(data[0], "begin ") {
						t.Fatalf("article %d repeats the begin line", index)
					}
					if err := checkUUDataLine(data[0], false); err != nil {
						t.Fatalf("article %d does not open on a data line: %v", index, err)
					}
				}

				if index == fixture.parts {
					if got := data[len(data)-1]; got != "end" {
						t.Errorf("the last article ends with %q, want the end trailer", got)
					}
					if got := data[len(data)-2]; got != "`" && strings.TrimSpace(got) != "" {
						t.Errorf("the line before end is %q, want the terminator", got)
					}
					data = data[:len(data)-2]
				} else {
					for _, line := range data {
						if line == "end" {
							t.Fatalf("article %d contains the end trailer", index)
						}
					}
				}

				// Every remaining line is a data line, which is what makes the
				// split a line-boundary split: a byte split would leave a
				// truncated one at the seam.
				for offset, line := range data {
					last := index == fixture.parts && offset == len(data)-1
					if err := checkUUDataLine(line, last && fixture.unpadded); err != nil {
						t.Errorf("article %d line %d: %v", index, offset, err)
					}
					if !last && lineBytes(line) != 45 {
						t.Errorf("article %d line %d declares %d bytes; only the final line may be short",
							index, offset, lineBytes(line))
					}
				}
			}
		})
	}
}

// TestUUUnpaddedTailIsTheOnlyDeviation keeps the broken-encoder probe honest:
// exactly one fixture may carry a short final group, it must be short by the
// right number of characters, and its length character must still declare the
// full byte count.
func TestUUUnpaddedTailIsTheOnlyDeviation(t *testing.T) {
	probes := 0
	for _, fixture := range uuFixtures() {
		lines := readUUArticle(t, fixture, fixture.parts)
		if fixture.preamble {
			lines = stripEncoderPreamble(lines)
		}
		final := lines[len(lines)-3]
		declared := lineBytes(final)
		payload := len(final) - 1
		canonical := 4 * ((declared + 2) / 3)

		if !fixture.unpadded {
			if payload != canonical {
				t.Errorf("%s/%s: final group is %d characters, want the canonical %d",
					fixture.slug, fixture.name, payload, canonical)
			}
			continue
		}
		probes++
		remainder := declared % 3
		if remainder == 0 {
			t.Fatalf("%s/%s: the probe payload is a multiple of three; there is nothing to strip",
				fixture.slug, fixture.name)
		}
		if want := canonical - (3 - remainder); payload != want {
			t.Errorf("%s/%s: final group is %d characters, want %d for %d trailing bytes",
				fixture.slug, fixture.name, payload, want, remainder)
		}
	}
	if probes != 1 {
		t.Errorf("the corpus carries %d unpadded-tail probes, want exactly 1", probes)
	}
}

// TestUUPlanDescribesTheArticlesOnDisk checks the manifest the seeder trusts:
// the part list, and the wire size it reports for each article, which is the
// CRLF-terminated size and not the file's size on disk.
func TestUUPlanDescribesTheArticlesOnDisk(t *testing.T) {
	for _, slug := range []string{"uu-release", "uu-mixed-yenc", "uu-preamble-tail", "uu-missing-middle"} {
		t.Run(slug, func(t *testing.T) {
			dir := filepath.Join(repoRoot, "testdata", slug)
			contents, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(UUPlanFile)))
			if err != nil {
				t.Skipf("fixture not hydrated: %v", err)
			}
			var plan UUPlan
			if err := json.Unmarshal(contents, &plan); err != nil {
				t.Fatal(err)
			}
			if plan.SchemaVersion != UUPlanSchemaVersion {
				t.Fatalf("plan schema version %d, want %d", plan.SchemaVersion, UUPlanSchemaVersion)
			}
			for _, file := range plan.Files {
				if len(file.Parts) == 0 {
					t.Fatalf("%s lists no parts", file.Name)
				}
				for index, part := range file.Parts {
					if part.Number != index+1 {
						t.Errorf("%s part %d is numbered %d", file.Name, index+1, part.Number)
					}
					body, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(part.Body)))
					if err != nil {
						t.Fatalf("%s part %d: %v", file.Name, part.Number, err)
					}
					// Every line gains one byte going from LF to CRLF.
					want := int64(len(body) + bytes.Count(body, []byte("\n")))
					if part.Bytes != want {
						t.Errorf("%s part %d reports %d wire bytes, want %d",
							file.Name, part.Number, part.Bytes, want)
					}
				}
			}
		})
	}
}

func TestStripEncoderPreambleLeavesTheEncodingAlone(t *testing.T) {
	lines := []string{"", "_=_ ", "_=_ Part 001 of 002 of file x.bin", "_=_ ", "", "begin 644 x.bin", "M...", "", "M..."}
	got := stripEncoderPreamble(lines)
	want := []string{"begin 644 x.bin", "M...", "", "M..."}
	if !equal(got, want) {
		t.Fatalf("stripEncoderPreamble = %q, want %q (an interior blank line is corruption, not preamble)", got, want)
	}
}

func TestUnpadFinalGroupStripsOnlyZeroFill(t *testing.T) {
	// '$' is 0x20+4: a four-byte line. That is two groups, the second of which
	// carries one real byte and two characters of zero fill.
	canonical := []string{"$0V]R00``", "`", "end"}
	got, err := unpadFinalGroup(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != "$0V]R00" {
		t.Fatalf("unpadFinalGroup left %q, want %q", got[0], "$0V]R00")
	}
	if canonical[0] != "$0V]R00``" {
		t.Fatal("unpadFinalGroup mutated its input")
	}

	for name, lines := range map[string][]string{
		"a full final group has no padding":  {"#04)#", "`", "end"},
		"a non-canonical width is not ours":  {"$0V]R00`", "`", "end"},
		"a missing trailer is not the tail":  {"$0V]R00``", "`"},
		"data must never be mistaken for it": {"$0V]R00XY", "`", "end"},
	} {
		if _, err := unpadFinalGroup(lines); err == nil {
			t.Errorf("%s: expected a refusal", name)
		}
	}
}

// ---------------------------------------------------------------- helpers

// referenceDecodeUU is a uudecode written from the format's definition rather
// than from either half of the pinned oracle. It skips anything ahead of
// `begin`, decodes four characters to three bytes, trusts each line's length
// character over the number of characters present, and stops at `end`.
func referenceDecodeUU(t *testing.T, lines []string) ([]byte, string) {
	t.Helper()
	var (
		out     []byte
		name    string
		started bool
	)
	for _, line := range lines {
		if !started {
			if strings.HasPrefix(line, "begin ") {
				fields := strings.SplitN(line, " ", 3)
				if len(fields) != 3 {
					t.Fatalf("malformed begin line %q", line)
				}
				name = fields[2]
				started = true
			}
			continue
		}
		if line == "end" {
			return out, name
		}
		if line == "" || line == "`" || line == " " {
			continue
		}
		// Anything that cannot be a data line is prose. Skipping it is the
		// tolerance the preamble fixture exists to probe: a decoder that
		// treated every line after `begin` as data would decode the encoder's
		// own `_=_` correlation block into the payload.
		if checkUUDataLine(line, true) != nil {
			continue
		}
		count := int(line[0]-0x20) & 0x3F
		if count == 0 {
			continue
		}
		body := line[1:]
		var decoded []byte
		for offset := 0; offset < len(body); offset += 4 {
			var group [4]byte
			for slot := range group {
				if offset+slot < len(body) {
					// A broken encoder drops the trailing zero fill; the
					// missing characters decode as the zeroes they stood for.
					group[slot] = byte(body[offset+slot]-0x20) & 0x3F
				}
			}
			decoded = append(decoded,
				group[0]<<2|group[1]>>4,
				group[1]<<4|group[2]>>2,
				group[2]<<6|group[3])
		}
		if count > len(decoded) {
			t.Fatalf("line %q declares %d bytes but only %d were decodable", line, count, len(decoded))
		}
		out = append(out, decoded[:count]...)
	}
	t.Fatal("the encoding has no end trailer")
	return nil, ""
}

func readUUArticles(t *testing.T, fixture uuFixture) []string {
	t.Helper()
	var lines []string
	for index := 1; index <= fixture.parts; index++ {
		lines = append(lines, readUUArticle(t, fixture, index)...)
	}
	return lines
}

func readUUArticle(t *testing.T, fixture uuFixture, part int) []string {
	t.Helper()
	path := filepath.Join(repoRoot, "testdata", fixture.slug,
		"uu", fmt.Sprintf("%s.%03d", fixture.name, part))
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("fixture not hydrated: %v", err)
	}
	return trimTrailingBlankLines(strings.Split(string(contents), "\n"))
}

// lineBytes is what a data line's length character declares.
func lineBytes(line string) int {
	if line == "" {
		return 0
	}
	return int(line[0]-0x20) & 0x3F
}

func checkUUDataLine(line string, allowShortFinalGroup bool) error {
	if line == "" {
		return fmt.Errorf("empty line")
	}
	count := lineBytes(line)
	if count < 1 || count > 45 {
		return fmt.Errorf("length character %q declares %d bytes", line[0], count)
	}
	for index := 1; index < len(line); index++ {
		if line[index] < 0x20 || line[index] > 0x60 {
			return fmt.Errorf("character %q at %d is outside the uu alphabet", line[index], index)
		}
	}
	payload := len(line) - 1
	canonical := 4 * ((count + 2) / 3)
	if payload == canonical {
		return nil
	}
	if allowShortFinalGroup && payload == canonical-(3-count%3) && count%3 != 0 {
		return nil
	}
	return fmt.Errorf("carries %d characters for %d bytes, want %d", payload, count, canonical)
}
