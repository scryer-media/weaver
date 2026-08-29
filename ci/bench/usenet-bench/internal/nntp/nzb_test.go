package nntp

import "testing"

func TestMarshalNZBRoundTripsMessageIDs(t *testing.T) {
	contents, err := MarshalNZB([]NZBFile{{
		Poster:  "nntp-bench",
		Date:    1704067200,
		Subject: "fixture.part01.rar yEnc",
		Groups:  []NZBGroup{"alt.binaries.test"},
		Segments: []NZBSegment{{
			Bytes:     123,
			Number:    1,
			MessageID: "<example@nntp-bench.invalid>",
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	document, err := UnmarshalNZB(contents)
	if err != nil {
		t.Fatal(err)
	}
	if document.XMLNS != NZBXMLNamespace || len(document.Files) != 1 {
		t.Fatalf("unexpected NZB document: %#v", document)
	}
	if got := document.Files[0].Segments[0].MessageID; got != "<example@nntp-bench.invalid>" {
		t.Fatalf("message ID = %q", got)
	}
}
