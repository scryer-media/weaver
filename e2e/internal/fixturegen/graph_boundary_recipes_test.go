// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func boundaryDecode(codec string, compressed []byte) ([]byte, error) {
	switch codec {
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	case "bzip2":
		return io.ReadAll(bzip2.NewReader(bytes.NewReader(compressed)))
	default:
		reader, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return reader.DecodeAll(compressed, nil)
	}
}

func TestBoundaryNumberedCodecsRoundTripAndRejectDamage(t *testing.T) {
	for _, codec := range []string{"gzip", "bzip2", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			healthy := boundaryStreamRecipe("test", codec, true, "", false)
			env := &Env{Work: t.TempDir()}
			if err := healthy.Build(context.Background(), env); err != nil {
				t.Fatal(err)
			}
			parts, err := env.Outputs()
			if err != nil || len(parts) != 3 {
				t.Fatalf("numbered inputs: %v, %v", parts, err)
			}
			var compressed []byte
			for _, part := range parts {
				compressed = append(compressed, read(t, env.OutputPath(part))...)
			}
			decoded, err := boundaryDecode(codec, compressed)
			if err != nil {
				t.Fatalf("healthy numbered stream does not decode: %v", err)
			}
			if !bytes.Equal(decoded, PatternBytes("graph-boundary/stream", boundaryPayloadSize)) {
				t.Fatal("joined numbered stream changed its payload")
			}
			compressed[len(compressed)/3+1024] ^= 0x80
			if _, err := boundaryDecode(codec, compressed); err == nil {
				t.Fatal("corruption must fail codec integrity, not silently produce altered usable bytes")
			}
		})
	}
}

func TestBoundaryRetainedDigestRewritePreservesFailedContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.json")
	original := "{\n  \"expected_outcome\": \"extraction_failure\",\n  \"expectedRetainedOutputBLAKE3\": {}\n}\n"
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RewriteScenarioDigests(path, map[string]string{"joined.xz": strings.Repeat("a", 64)}); err != nil {
		t.Fatal(err)
	}
	updated := string(read(t, path))
	if strings.Contains(updated, `"expectedOutputBLAKE3"`) || !strings.Contains(updated, `"expected_outcome": "extraction_failure"`) {
		t.Fatalf("retained bytes became usable-output expectations: %s", updated)
	}
	if yes, err := ScenarioHasDigests(path); err != nil || !yes {
		t.Fatalf("retained digest declaration was not recognized: %v, %v", yes, err)
	}
}

func TestUnknownMissingPieceRetainsOnlyOriginalPieces(t *testing.T) {
	recipe := boundaryStreamRecipe("test", "gzip", true, "missing", false)
	env := &Env{Work: t.TempDir()}
	if err := recipe.Build(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	expected, err := recipe.ExpectedOutputs(context.Background(), env)
	if err != nil || len(expected) != 2 {
		t.Fatalf("remaining-piece oracle: %v, %v", expected, err)
	}
	for _, name := range []string{"lantern.bin.gz.001", "lantern.bin.gz.003"} {
		path, exists := expected[name]
		if !exists || !bytes.Equal(read(t, path), read(t, env.OutputPath(name))) {
			t.Fatalf("retained %q must be the original posted piece", name)
		}
	}
	if _, err := os.Stat(env.OutputPath("lantern.bin.gz.002")); !os.IsNotExist(err) {
		t.Fatalf("middle piece unexpectedly present: %v", err)
	}
}
