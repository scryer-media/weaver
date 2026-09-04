package nntp

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

// postingPlan is one fixture's intended NZB file order, split into the files
// Nyuu actually posts and the files the NZB lists but that were deliberately
// never posted. A withheld file is what a real short post looks like from the
// client's side: every article it asks for is refused.
type postingPlan struct {
	// Order is every file the NZB must contain, in the order the manifest
	// declared. It includes withheld files.
	Order []string
	// Posted is the subset handed to Nyuu, in the same relative order.
	Posted []string
	// Withheld maps a path in Order to its recorded digest entry.
	Withheld map[string]fixture.FileDigest
}

func newPostingPlan(manifest fixture.GeneratedManifest) (postingPlan, error) {
	plan := postingPlan{
		Order:    append([]string(nil), manifest.NZBFileOrder...),
		Withheld: make(map[string]fixture.FileDigest, len(manifest.WithheldFiles)),
	}
	posted := make(map[string]bool, len(manifest.ArchiveFiles))
	for _, file := range manifest.ArchiveFiles {
		posted[file.Path] = true
	}
	for _, file := range manifest.WithheldFiles {
		if posted[file.Path] {
			return postingPlan{}, fmt.Errorf("fixture %q lists %s as both posted and withheld", manifest.Case.ID, file.Path)
		}
		plan.Withheld[file.Path] = file
	}
	if len(plan.Order) != len(posted)+len(plan.Withheld) {
		return postingPlan{}, fmt.Errorf("fixture %q declares %d NZB files but has %d posted and %d withheld", manifest.Case.ID, len(plan.Order), len(posted), len(plan.Withheld))
	}
	seen := make(map[string]bool, len(plan.Order))
	for _, entry := range plan.Order {
		if seen[entry] {
			return postingPlan{}, fmt.Errorf("fixture %q repeats %s in its NZB order", manifest.Case.ID, entry)
		}
		seen[entry] = true
		switch {
		case posted[entry]:
			plan.Posted = append(plan.Posted, entry)
		case plan.Withheld[entry].Path != "":
		default:
			return postingPlan{}, fmt.Errorf("fixture %q orders unknown file %s", manifest.Case.ID, entry)
		}
	}
	if len(plan.Posted) == 0 {
		return postingPlan{}, fmt.Errorf("fixture %q has no files to post", manifest.Case.ID)
	}
	return plan, nil
}

// quotedNameExpression matches the conventional quoted file name in a Usenet
// binary subject. Every poster this harness supports emits one.
var quotedNameExpression = regexp.MustCompile(`"([^"]+)"`)

var (
	fileCounterExpression  = regexp.MustCompile(`\[(\d+)/(\d+)\]`)
	partCounterExpression  = regexp.MustCompile(`\((\d+)/(\d+)\)`)
	trailingSizeExpression = regexp.MustCompile(`(\d+)(\s*)$`)
)

// nzbFileName extracts the posted file name from a subject line. It is the
// same field every client keys on, so an order assertion built on it checks
// what the client will actually see rather than an internal detail.
func nzbFileName(subject string) (string, error) {
	match := quotedNameExpression.FindStringSubmatch(subject)
	if match == nil {
		return "", fmt.Errorf("subject %q has no quoted file name", subject)
	}
	return match[1], nil
}

// assertNZBFileOrder fails loudly when the emitted NZB does not list files in
// the order the fixture declared. Posting order is a measured axis, so a
// silent reordering by the poster would invalidate every run over the fixture.
func assertNZBFileOrder(document NZBDocument, expected []string) error {
	if len(document.Files) != len(expected) {
		return fmt.Errorf("NZB has %d files, expected %d", len(document.Files), len(expected))
	}
	for index, file := range document.Files {
		name, err := nzbFileName(file.Subject)
		if err != nil {
			return fmt.Errorf("NZB file %d: %w", index, err)
		}
		want := path.Base(expected[index])
		if name != want {
			return fmt.Errorf("NZB file %d is %q, expected %q (declared posting order was not preserved)", index, name, want)
		}
	}
	return nil
}

// withheldMessageID names an article that is deliberately absent from the
// server. The literal "withheld" segment cannot collide with a posted
// identifier, whose file field is always the poster's decimal file number.
func withheldMessageID(runID, fixtureID string, fileIndex, part int) string {
	return fmt.Sprintf("bench-%s-%s-withheld%03d-%05d@nntp-bench", safeID(runID), safeID(fixtureID), fileIndex, part)
}

// modalFullSegmentBytes reports the encoded article size the poster used for a
// full segment, measured from the articles it just wrote rather than assumed
// from a yEnc expansion constant. Each file's final segment is excluded
// because it is short by construction.
func modalFullSegmentBytes(document NZBDocument) int64 {
	counts := make(map[int64]int)
	for _, file := range document.Files {
		for index, segment := range file.Segments {
			if index == len(file.Segments)-1 {
				continue
			}
			counts[segment.Bytes]++
		}
	}
	var best int64
	bestCount := 0
	for size, count := range counts {
		if count > bestCount || (count == bestCount && size > best) {
			best, bestCount = size, count
		}
	}
	return best
}

// spliceWithheldFiles inserts the never-posted volumes into the NZB at their
// declared positions and renumbers every subject's file counter, so the
// document reads exactly like a post whose articles went missing rather than
// like a post that was one volume shorter.
func spliceWithheldFiles(document NZBDocument, plan postingPlan, runID, fixtureID string, segmentBytes int) (NZBDocument, error) {
	if len(plan.Withheld) == 0 {
		return document, nil
	}
	if len(document.Files) == 0 {
		return NZBDocument{}, fmt.Errorf("cannot splice withheld files into an empty NZB")
	}
	if segmentBytes <= 0 {
		return NZBDocument{}, fmt.Errorf("segment size must be positive to describe withheld articles")
	}
	fullSegment := modalFullSegmentBytes(document)
	if fullSegment <= 0 {
		// Single-segment posts carry no full segment to measure. The encoded
		// size then falls back to the raw segment size, which understates the
		// yEnc expansion but is never used to fetch anything.
		fullSegment = int64(segmentBytes)
	}
	template := document.Files[0]
	templateName, err := nzbFileName(template.Subject)
	if err != nil {
		return NZBDocument{}, err
	}
	files := make([]NZBFile, 0, len(plan.Order))
	postedIndex := 0
	for _, entry := range plan.Order {
		withheld, isWithheld := plan.Withheld[entry]
		if !isWithheld {
			files = append(files, document.Files[postedIndex])
			postedIndex++
			continue
		}
		parts := int((withheld.Size + int64(segmentBytes) - 1) / int64(segmentBytes))
		if parts < 1 {
			parts = 1
		}
		file := NZBFile{
			Poster:   template.Poster,
			Date:     template.Date,
			Groups:   append([]NZBGroup(nil), template.Groups...),
			Segments: make([]NZBSegment, 0, parts),
		}
		remaining := withheld.Size
		for part := 1; part <= parts; part++ {
			raw := int64(segmentBytes)
			if remaining < raw {
				raw = remaining
			}
			remaining -= raw
			encoded := fullSegment
			if raw < int64(segmentBytes) {
				encoded = raw * fullSegment / int64(segmentBytes)
			}
			if encoded < 1 {
				encoded = 1
			}
			file.Segments = append(file.Segments, NZBSegment{
				Bytes:     encoded,
				Number:    part,
				MessageID: withheldMessageID(runID, fixtureID, len(files)+1, part),
			})
		}
		file.Subject = rewriteSubject(template.Subject, templateName, path.Base(entry), 0, 0, 1, parts, withheld.Size)
		files = append(files, file)
	}
	for index := range files {
		files[index].Subject = renumberSubjectFileCounter(files[index].Subject, index+1, len(files))
	}
	document.Files = files
	return document, nil
}

// rewriteSubject reproduces the poster's own subject shape for a file it never
// posted. Only the name, the counters and the byte count change, so a client
// parses the withheld file exactly as it parses its neighbours.
func rewriteSubject(template, templateName, name string, fileNum, totalFiles, part, parts int, size int64) string {
	subject := strings.Replace(template, `"`+templateName+`"`, `"`+name+`"`, 1)
	if fileNum > 0 && totalFiles > 0 {
		subject = renumberSubjectFileCounter(subject, fileNum, totalFiles)
	}
	subject = partCounterExpression.ReplaceAllString(subject, fmt.Sprintf("(%d/%d)", part, parts))
	subject = trailingSizeExpression.ReplaceAllString(subject, strconv.FormatInt(size, 10)+"$2")
	return subject
}

// renumberSubjectFileCounter rewrites the leading [n/N] counter. The file
// number is zero-padded to the width of the new total, which is what the
// poster itself does, so adding a withheld volume that crosses a power of ten
// widens every counter consistently.
func renumberSubjectFileCounter(subject string, fileNum, totalFiles int) string {
	replaced := false
	width := len(strconv.Itoa(totalFiles))
	return fileCounterExpression.ReplaceAllStringFunc(subject, func(match string) string {
		if replaced {
			return match
		}
		replaced = true
		return fmt.Sprintf("[%0*d/%d]", width, fileNum, totalFiles)
	})
}

// sortedPaths is used wherever a stable, order-independent view of a file set
// is needed for reporting.
func sortedPaths(files []fixture.FileDigest) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	sort.Strings(paths)
	return paths
}
