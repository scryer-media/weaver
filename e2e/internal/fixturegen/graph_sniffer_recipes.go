// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import "context"

var trailingNameStreams = []struct {
	member, codec, payload string
}{
	{`quote.tar.deflate"`, "deflate", "quote.bin"},
	{"underscore.tar.br_", "brotli", "underscore.bin"},
	{"dots.tar.deflate..", "deflate", "dots.bin"},
}

func trailingNameStreamRecipe() Recipe {
	return Recipe{
		Slug: "sniff-trailing-names", Family: "graph stream boundary", ByteReproducible: true,
		Notes: "Signatureless Deflate/Brotli streams have quoted, underscored, or dotted names in ZIP metadata. Each decodes a signed TAR holding a safe final filename; no physical fixture path contains Windows-invalid punctuation.",
		Build: func(_ context.Context, env *Env) error {
			var entries []zipEntry
			for _, stream := range trailingNameStreams {
				payload := env.StagePath(stream.payload)
				if err := WritePRNG(payload, "sniffer-trim/"+stream.payload, 4096); err != nil {
					return err
				}
				tar := env.StagePath(stream.payload + ".tar")
				if err := WriteTar(tar, nil, []Member{{Name: stream.payload, Source: payload}}); err != nil {
					return err
				}
				compressed := env.StagePath(stream.payload + ".compressed")
				if err := CompressFile(stream.codec, compressed, tar); err != nil {
					return err
				}
				entries = append(entries, zipEntry{RawName: stream.member, Source: compressed, UTF8Flag: true})
			}
			// Inner codec suffixes exist only in the ZIP's metadata, so the
			// output-path provenance inference cannot discover these writers.
			for _, id := range goWriterIDs(env.Lock, []string{"payload.tar", "payload.deflate", "payload.br"}) {
				env.usedToolchain(id)
			}
			return writeZipEntries(env.OutputPath("streams.zip"), entries)
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			outputs := make(map[string]string, len(trailingNameStreams))
			for _, stream := range trailingNameStreams {
				outputs[stream.payload] = env.StagePath(stream.payload)
			}
			return outputs, nil
		},
	}
}
