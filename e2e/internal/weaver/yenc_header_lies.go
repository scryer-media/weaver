// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

// Lying yEnc headers.
//
// Nyuu writes headers that agree with each other and with the body. A
// scenario that needs a post whose headers lie names the lie here, and the
// seeder files the affected articles again with only their `=ybegin` or
// `=ypart` line changed: the body, its checksum and the NZB stay what nyuu
// posted, so the lie is in the articles a reader fetches and nowhere else.

// ScenarioYencHeaderLie is one rewrite of the headers of a set of articles.
type ScenarioYencHeaderLie struct {
	// SubjectContains narrows the lie to the files whose NZB subject holds
	// one of these; with none, every file.
	SubjectContains []string `json:"subjectContains,omitempty"`
	// Segments are the NZB segment numbers within each file the lie applies
	// to; with none, every segment of the file.
	Segments []int `json:"segments,omitempty"`
	// Size replaces the `size=` of `=ybegin`, which a multi-part article
	// uses for the whole file's length.
	Size int64 `json:"size,omitempty"`
	// EndBelowBegin sets `=ypart end=` one below its `begin=`.
	EndBelowBegin bool `json:"endBelowBegin,omitempty"`
	// Shift moves `=ypart begin=` and `end=` by this many bytes, down when
	// negative.
	Shift int64 `json:"shift,omitempty"`
}

var (
	ybeginSizeField = regexp.MustCompile(`(^=ybegin .*?\bsize=)(\d+)`)
	ypartFields     = regexp.MustCompile(`^=ypart begin=(\d+) end=(\d+)`)
)

// tellYencHeaderLie rewrites one article's wire form, as readStoredArticle
// gives it back, with the lie applied. Only the first `=ybegin` and `=ypart`
// lines are touched; everything after them is the body nyuu posted.
func tellYencHeaderLie(wire []byte, lie ScenarioYencHeaderLie) ([]byte, error) {
	lines := bytes.Split(wire, []byte("\r\n"))
	toldSize, toldPart := lie.Size == 0, !lie.EndBelowBegin && lie.Shift == 0
	for index, line := range lines {
		if toldSize && toldPart {
			break
		}
		if !toldSize && bytes.HasPrefix(line, []byte("=ybegin ")) {
			// The name runs to the end of the line and may itself hold
			// "size=", so only the fields before it are searched.
			head, name, named := bytes.Cut(line, []byte(" name="))
			match := ybeginSizeField.FindSubmatchIndex(head)
			if match == nil {
				return nil, fmt.Errorf("=ybegin line has no size field: %q", line)
			}
			told := append([]byte{}, head[:match[3]]...)
			told = strconv.AppendInt(told, lie.Size, 10)
			told = append(told, head[match[1]:]...)
			if named {
				told = append(append(told, []byte(" name=")...), name...)
			}
			lines[index] = told
			toldSize = true
			continue
		}
		if !toldPart && bytes.HasPrefix(line, []byte("=ypart ")) {
			match := ypartFields.FindSubmatch(line)
			if match == nil {
				return nil, fmt.Errorf("=ypart line is not begin= end=: %q", line)
			}
			begin, _ := strconv.ParseInt(string(match[1]), 10, 64)
			end, _ := strconv.ParseInt(string(match[2]), 10, 64)
			if lie.EndBelowBegin {
				end = begin - 1
			}
			begin, end = begin+lie.Shift, end+lie.Shift
			if begin < 1 || end < 0 {
				return nil, fmt.Errorf("=ypart shifted by %d leaves the file: begin=%d end=%d", lie.Shift, begin, end)
			}
			lines[index] = []byte(fmt.Sprintf("=ypart begin=%d end=%d", begin, end))
			toldPart = true
		}
	}
	if !toldSize || !toldPart {
		return nil, fmt.Errorf("article has no header line for the lie %+v", lie)
	}
	return bytes.Join(lines, []byte("\r\n")), nil
}

// yencHeaderLieMessageIDs is every article the lie reaches in the NZB.
func yencHeaderLieMessageIDs(nzbData []byte, lie ScenarioYencHeaderLie) ([]string, error) {
	numbers := lie.Segments
	if len(numbers) == 0 {
		files, err := parseNZBFileSegments(nzbData)
		if err != nil {
			return nil, err
		}
		seen := map[int]struct{}{}
		for _, segments := range files {
			for _, segment := range segments {
				if _, dup := seen[segment.Number]; !dup {
					seen[segment.Number] = struct{}{}
					numbers = append(numbers, segment.Number)
				}
			}
		}
		sort.Ints(numbers)
	}
	return extractMessageIDsBySegmentNumbers(nzbData, lie.SubjectContains, numbers)
}

// tellYencHeaderLies files every article a scenario's lies reach again with
// its headers rewritten. An article two lies reach carries both.
func tellYencHeaderLies(host, port string, nzbData []byte, lies []ScenarioYencHeaderLie) (int, error) {
	if len(lies) == 0 {
		return 0, nil
	}
	session, err := openNntpCommandSession(host, port, true)
	if err != nil {
		return 0, err
	}
	told := map[string][]byte{}
	var order []string
	for _, lie := range lies {
		messageIDs, err := yencHeaderLieMessageIDs(nzbData, lie)
		if err != nil {
			session.close()
			return 0, err
		}
		if len(messageIDs) == 0 {
			session.close()
			return 0, fmt.Errorf("no NZB segment matches the yEnc header lie %+v", lie)
		}
		for _, messageID := range messageIDs {
			wire, seen := told[messageID]
			if !seen {
				wire, err = readStoredArticle(session, messageID)
				if err != nil {
					session.close()
					return 0, err
				}
				order = append(order, messageID)
			}
			if told[messageID], err = tellYencHeaderLie(wire, lie); err != nil {
				session.close()
				return 0, fmt.Errorf("<%s>: %w", messageID, err)
			}
		}
	}
	session.close()
	if err := deleteArticleIDsAt(host, port, order); err != nil {
		return 0, err
	}
	articles := make([]withheldArticle, 0, len(order))
	for _, messageID := range order {
		articles = append(articles, withheldArticle{messageID: messageID, wire: told[messageID]})
	}
	if err := restoreWithheldArticles(host, port, articles); err != nil {
		return 0, err
	}
	return len(order), nil
}
