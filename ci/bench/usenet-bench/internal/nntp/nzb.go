package nntp

import (
	"bytes"
	"encoding/xml"
	"fmt"
)

const NZBXMLNamespace = "http://www.newzbin.com/DTD/2003/nzb"

type NZBDocument struct {
	XMLName xml.Name  `xml:"nzb"`
	XMLNS   string    `xml:"xmlns,attr"`
	Files   []NZBFile `xml:"file"`
}

type NZBFile struct {
	Poster   string       `xml:"poster,attr"`
	Date     int64        `xml:"date,attr"`
	Subject  string       `xml:"subject,attr"`
	Groups   []NZBGroup   `xml:"groups>group"`
	Segments []NZBSegment `xml:"segments>segment"`
}

type NZBGroup string

type NZBSegment struct {
	Bytes     int64  `xml:"bytes,attr"`
	Number    int    `xml:"number,attr"`
	MessageID string `xml:",chardata"`
}

func MarshalNZB(files []NZBFile) ([]byte, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("cannot create an NZB with no files")
	}
	document := NZBDocument{XMLNS: NZBXMLNamespace, Files: files}
	contents, err := xml.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(append([]byte(xml.Header), contents...), '\n'), nil
}

func UnmarshalNZB(contents []byte) (NZBDocument, error) {
	var document NZBDocument
	if err := xml.NewDecoder(bytes.NewReader(contents)).Decode(&document); err != nil {
		return NZBDocument{}, err
	}
	return document, nil
}
