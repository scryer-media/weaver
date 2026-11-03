// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"io"
	"testing"

	"github.com/andybalholm/brotli"
)

func TestTrailingNamesCarryRawStreamsAndSafePayloadOracles(t *testing.T) {
	env := &Env{Work: t.TempDir()}
	recipe := trailingNameStreamRecipe()
	if err := recipe.Build(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	outputs, err := recipe.ExpectedOutputs(context.Background(), env)
	if err != nil || len(outputs) != len(trailingNameStreams) {
		t.Fatalf("output digest sources: %v, %v", outputs, err)
	}
	archive, err := zip.OpenReader(env.OutputPath("streams.zip"))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if len(archive.File) != len(trailingNameStreams) {
		t.Fatalf("ZIP entries: %d", len(archive.File))
	}
	for index, entry := range archive.File {
		stream := trailingNameStreams[index]
		if entry.Name != stream.member || entry.Method != zip.Store {
			t.Fatalf("entry metadata changed: %q method %d", entry.Name, entry.Method)
		}
		input, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		compressed, err := io.ReadAll(input)
		input.Close()
		if err != nil {
			t.Fatal(err)
		}
		var decoded io.Reader
		if stream.codec == "deflate" {
			raw := flate.NewReader(bytes.NewReader(compressed))
			defer raw.Close()
			decoded = raw
		} else {
			decoded = brotli.NewReader(bytes.NewReader(compressed))
		}
		contents := tar.NewReader(decoded)
		header, err := contents.Next()
		if err != nil || header.Name != stream.payload || header.Format != tar.FormatUSTAR {
			t.Fatalf("raw %s stream %q does not hold the signed TAR: %v, %v", stream.codec, entry.Name, header, err)
		}
		payload, err := io.ReadAll(contents)
		if err != nil || !bytes.Equal(payload, PatternBytes("sniffer-trim/"+stream.payload, 4096)) {
			t.Fatalf("decoded %q changed its payload: %v", entry.Name, err)
		}
		if !bytes.Equal(payload, read(t, outputs[stream.payload])) {
			t.Fatalf("digest oracle for %q does not describe its decoded bytes", stream.payload)
		}
		if _, err := contents.Next(); err != io.EOF {
			t.Fatalf("unexpected trailing TAR member: %v", err)
		}
	}
}
