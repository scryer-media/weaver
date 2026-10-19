// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/zeebo/blake3"
)

// The torrent lane's fixtures. Every torrent is generated here from corpus
// payloads or deterministic synthetic bytes; nothing generated is committed.
// The seeder serves the staged payloads, the tracker hands out peers, and the
// scenarios, the facade checks and the browser flows read the manifest.

// The tracker as the compose network names it. HTTP announces go to the
// first, UDP announces to the second.
const (
	torrentTrackerHTTP = "http://torrent-tracker:8080/announce"
	torrentTrackerUDP  = "udp://torrent-tracker:8081"
)

// torrentFileSpec is one file of a torrent: copied from a corpus fixture, or
// synthetic bytes of a given size.
type torrentFileSpec struct {
	Path      string
	Corpus    string
	Synthetic int64
}

// torrentFixtureSpec describes one generated torrent.
type torrentFixtureSpec struct {
	Slug    string
	Name    string
	Private bool
	UDP     bool
	// Single is a single-file torrent; its one file's path is the name.
	Single      bool
	PieceLength int64
	CorpusSlug  string
	Files       []torrentFileSpec
	// ExpectedOutput maps output-relative paths to the BLAKE3 Weaver's
	// output must have. Payload files with no entry are expected to be
	// delivered as they are.
	ExpectedOutput map[string]string
	// AbsentOutput are output-relative paths that must not be on disk once
	// the torrent completes.
	AbsentOutput []string
	// RetainedOutput are payload paths whose bytes must stay on disk under
	// the output directory while the torrent seeds, under whatever name
	// Weaver keeps them.
	RetainedOutput []string
	// Seeded is false for a torrent no peer serves (refused before any
	// announce, so nobody needs to).
	Unseeded bool
}

// torrentFixture is a generated torrent and what the harness knows of it.
type torrentFixture struct {
	Slug        string            `json:"slug"`
	Name        string            `json:"name"`
	Infohash    string            `json:"infohash"`
	Private     bool              `json:"private"`
	Tracker     string            `json:"tracker"`
	TorrentFile string            `json:"torrent_file"`
	Magnet      string            `json:"magnet"`
	TotalBytes  int64             `json:"total_bytes"`
	PieceLength int64             `json:"piece_length"`
	Files       []torrentFileInfo `json:"files"`
	// Output is the BLAKE3 of every file Weaver's output must hold.
	Output map[string]string `json:"output"`
	Absent []string          `json:"absent,omitempty"`
	// Retained is the BLAKE3 of every payload file whose bytes must stay
	// under the output directory, by payload path, under any name.
	Retained map[string]string `json:"retained,omitempty"`
	Seeded   bool              `json:"seeded"`
}

type torrentFileInfo struct {
	Path   string `json:"path"`
	Length int64  `json:"length"`
	BLAKE3 string `json:"blake3"`
}

type torrentManifest struct {
	Version  int                        `json:"version"`
	Torrents map[string]*torrentFixture `json:"torrents"`
}

const kib = 1024
const mib = 1024 * kib

// synthetic returns a payload file of deterministic incompressible bytes.
func synthetic(path string, size int64) torrentFileSpec {
	return torrentFileSpec{Path: path, Synthetic: size}
}

func corpusFile(path, source string) torrentFileSpec {
	return torrentFileSpec{Path: path, Corpus: source}
}

// torrentFixtureSpecs is every torrent the lane uses. Each scenario owns its
// torrents, so no scenario's state leaks into another's waits.
func torrentFixtureSpecs() []torrentFixtureSpec {
	return []torrentFixtureSpec{
		{
			Slug: "plain-multi", Name: "Copper.Meadow.Plain.Bundle", CorpusSlug: "par3-packed-tails",
			PieceLength: 64 * kib,
			Files: []torrentFileSpec{
				corpusFile("copper.meadow.s01e01.mkv", "copper.meadow.s01e01.mkv"),
				corpusFile("subs/copper.meadow.s01e01.en.srt", "copper.meadow.s01e01.en.srt"),
				corpusFile("subs/copper.meadow.s01e01.forced.srt", "copper.meadow.s01e01.forced.srt"),
			},
		},
		{
			Slug: "rar-store", Name: "Amber.Trail.Store.Set", CorpusSlug: "direct-store-multivolume", Private: true,
			PieceLength: 256 * kib,
			Files: []torrentFileSpec{
				corpusFile("archive.part1.rar", "archive.part1.rar"),
				corpusFile("archive.part2.rar", "archive.part2.rar"),
				corpusFile("archive.part3.rar", "archive.part3.rar"),
				corpusFile("archive.part4.rar", "archive.part4.rar"),
			},
			ExpectedOutput: map[string]string{
				"amber.trail.s01e02.mkv": "0f9527235f4aeccd73d0ef80e25572b51b96d4a9dfe96d24a73911590efed9ab",
			},
			AbsentOutput: []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar", "archive.part4.rar"},
		},
		{
			Slug: "rar-compressed", Name: "Nested.Compressed.Set", CorpusSlug: "nested-xz-rar",
			PieceLength:    64 * kib,
			Files:          []torrentFileSpec{corpusFile("archive.rar", "archive.rar")},
			RetainedOutput: []string{"archive.rar"},
			ExpectedOutput: map[string]string{
				"release.nfo":    "c2d20ba1a4a41e1ae7f6567528402d01ee9b571bb1f24466cd89a48c4f5897b2",
				"test-media.mkv": "bd8b3991787e2bc03ae0f47bda585f1b9dbd831448493fe3c4a9118f4f947cf2",
			},
		},
		{
			Slug: "par2-set", Name: "Lattice.Harbor.Parity.Set", CorpusSlug: "par2-multi-grid-overlap-clean",
			PieceLength: 64 * kib,
			Files: []torrentFileSpec{
				corpusFile("payload.mkv", "payload.mkv"),
				corpusFile("00-grid64.par2", "00-grid64.par2"),
				corpusFile("00-grid64.vol0+1.par2", "00-grid64.vol0+1.par2"),
				corpusFile("01-grid96.par2", "01-grid96.par2"),
				corpusFile("01-grid96.vol0+1.par2", "01-grid96.vol0+1.par2"),
			},
			ExpectedOutput: map[string]string{
				"payload.mkv": "bd8b3991787e2bc03ae0f47bda585f1b9dbd831448493fe3c4a9118f4f947cf2",
			},
		},
		{
			Slug: "single-file", Name: "lattice.single.payload.mkv", CorpusSlug: "par2-multi-grid-overlap-clean",
			Single: true, UDP: true, PieceLength: 64 * kib,
			Files: []torrentFileSpec{corpusFile("lattice.single.payload.mkv", "payload.mkv")},
		},
		{
			Slug: "unwanted", Name: "Granite.Unwanted.Bundle", PieceLength: 32 * kib, Unseeded: true,
			Files: []torrentFileSpec{synthetic("readme.txt", 40*kib), synthetic("extras/run.cmd", 8*kib)},
		},
		{Slug: "magnet", Name: "Slate.Magnet.Bundle", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("slate.part.a.bin", 3*mib), synthetic("slate.part.b.bin", 1*mib+517)}},
		{Slug: "duplicate", Name: "Basalt.Duplicate.Bundle", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("basalt.bin", 2*mib)}},
		{Slug: "controls", Name: "Quartz.Controls.Bundle", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("quartz.bin", 6*mib)}},
		{Slug: "ratio", Name: "Ember.Ratio.Bundle", Private: true, PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("ember.bin", 4*mib)}},
		{Slug: "remove-keep", Name: "Flint.Keep.Bundle", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("flint.bin", 1*mib)}},
		{Slug: "remove-data", Name: "Shale.Data.Bundle", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("shale.bin", 1*mib)}},
		{Slug: "private-goal", Name: "Onyx.Private.Bundle", Private: true, PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("onyx.bin", 1*mib)}},
		{Slug: "disabled", Name: "Mica.Disabled.Bundle", PieceLength: 32 * kib, Unseeded: true,
			Files: []torrentFileSpec{synthetic("mica.bin", 256*kib)}},
		{Slug: "category", Name: "Jasper.Category.Bundle", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("jasper.bin", 2*mib)}},
		{Slug: "restart", Name: "Garnet.Restart.Bundle", Private: true, PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("garnet.bin", 3*mib)}},
		{Slug: "socks", Name: "Cobalt.Proxied.Bundle", Private: true, PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("cobalt.bin", 2*mib)}},
		{Slug: "capped", Name: "Iron.Capped.Bundle", PieceLength: 256 * kib,
			Files: []torrentFileSpec{synthetic("iron.bin", 12*mib)}},
		{Slug: "split", Name: "Nickel.Split.Bundle", PieceLength: 256 * kib,
			Files: []torrentFileSpec{synthetic("nickel.bin", 12*mib)}},
		{Slug: "facade-file", Name: "Pewter.Facade.File", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("pewter.bin", 1*mib)}},
		{Slug: "facade-magnet", Name: "Tin.Facade.Magnet", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("tin.bin", 1*mib)}},
		{Slug: "facade-refused", Name: "Lead.Facade.Refused", PieceLength: 32 * kib, Unseeded: true,
			Files: []torrentFileSpec{synthetic("lead.txt", 16*kib), synthetic("setup.exe", 8*kib)}},
		{Slug: "ui-magnet", Name: "Topaz.Browser.Magnet", PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("topaz.bin", 2*mib)}},
		{Slug: "ui-file", Name: "Beryl.Browser.File", Private: true, PieceLength: 64 * kib,
			Files: []torrentFileSpec{synthetic("beryl.bin", 2*mib), synthetic("notes/beryl.txt", 12*kib)}},
	}
}

func torrentFixturesRoot() string { return filepath.Join(fixturesDir(), "torrents") }

func torrentPayloadRoot() string { return filepath.Join(torrentFixturesRoot(), "payload") }

func torrentManifestPath() string { return filepath.Join(torrentFixturesRoot(), "manifest.json") }

// cmdSeedTorrents generates every torrent and its payload under the fixtures
// directory and writes the manifest. It needs no running service.
func cmdSeedTorrents() (*torrentManifest, error) {
	manifest := &torrentManifest{Version: 1, Torrents: map[string]*torrentFixture{}}
	if err := os.RemoveAll(torrentFixturesRoot()); err != nil {
		return nil, err
	}
	for _, spec := range torrentFixtureSpecs() {
		fixture, err := buildTorrentFixture(spec)
		if err != nil {
			return nil, fmt.Errorf("torrent fixture %s: %w", spec.Slug, err)
		}
		manifest.Torrents[spec.Slug] = fixture
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(torrentManifestPath(), data, 0o644); err != nil {
		return nil, err
	}
	return manifest, nil
}

func loadTorrentManifest() (*torrentManifest, error) {
	data, err := os.ReadFile(torrentManifestPath())
	if err != nil {
		return nil, fmt.Errorf("read torrent manifest (run seed-torrents first): %w", err)
	}
	var manifest torrentManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func buildTorrentFixture(spec torrentFixtureSpec) (*torrentFixture, error) {
	payloadDir := filepath.Join(torrentPayloadRoot(), spec.Slug)
	if err := os.MkdirAll(payloadDir, 0o777); err != nil {
		return nil, err
	}
	// The seeder runs as root but the payload directory is bind-mounted, so
	// keep it writable for whichever user the engine maps it to.
	_ = os.Chmod(payloadDir, 0o777)
	fixture := &torrentFixture{
		Slug: spec.Slug, Name: spec.Name, Private: spec.Private, PieceLength: spec.PieceLength,
		Output: map[string]string{}, Absent: spec.AbsentOutput, Seeded: !spec.Unseeded,
	}
	fixture.Tracker = torrentTrackerHTTP
	if spec.UDP {
		fixture.Tracker = torrentTrackerUDP
	}
	var pending, pieces []byte
	addPieces := func(data []byte) {
		pending = append(pending, data...)
		offset := 0
		for int64(len(pending)-offset) >= spec.PieceLength {
			sum := sha1.Sum(pending[offset : offset+int(spec.PieceLength)])
			pieces = append(pieces, sum[:]...)
			offset += int(spec.PieceLength)
		}
		pending = append([]byte(nil), pending[offset:]...)
	}
	var files []any
	for _, file := range spec.Files {
		data, err := torrentFileBytes(spec, file)
		if err != nil {
			return nil, err
		}
		target := filepath.Join(payloadDir, filepath.FromSlash(file.Path))
		if err := os.MkdirAll(filepath.Dir(target), 0o777); err != nil {
			return nil, err
		}
		if err := os.WriteFile(target, data, 0o666); err != nil {
			return nil, err
		}
		digest := blake3.Sum256(data)
		info := torrentFileInfo{Path: file.Path, Length: int64(len(data)), BLAKE3: hex.EncodeToString(digest[:])}
		fixture.Files = append(fixture.Files, info)
		fixture.TotalBytes += info.Length
		addPieces(data)
		files = append(files, map[string]any{
			"length": int64(len(data)),
			"path":   pathComponents(file.Path),
		})
	}
	if len(pending) > 0 {
		sum := sha1.Sum(pending)
		pieces = append(pieces, sum[:]...)
	}
	info := map[string]any{
		"name":         spec.Name,
		"piece length": spec.PieceLength,
		"pieces":       pieces,
	}
	if spec.Single {
		info["length"] = fixture.Files[0].Length
	} else {
		info["files"] = files
	}
	if spec.Private {
		info["private"] = int64(1)
	}
	infoBytes := bencodeValue(info)
	infohash := sha1.Sum(infoBytes)
	fixture.Infohash = hex.EncodeToString(infohash[:])
	metainfo := bencodeDictRaw([]bencodePair{
		{"announce", bencodeValue(fixture.Tracker)},
		{"created by", bencodeValue("weaver-e2e")},
		{"info", infoBytes},
	})
	fixture.TorrentFile = filepath.Join(torrentFixturesRoot(), spec.Slug+".torrent")
	if err := os.WriteFile(fixture.TorrentFile, metainfo, 0o644); err != nil {
		return nil, err
	}
	fixture.Magnet = "magnet:?xt=urn:btih:" + fixture.Infohash +
		"&dn=" + url.QueryEscape(spec.Name) + "&tr=" + url.QueryEscape(fixture.Tracker)
	if err := os.WriteFile(filepath.Join(torrentFixturesRoot(), spec.Slug+".magnet"), []byte(fixture.Magnet+"\n"), 0o644); err != nil {
		return nil, err
	}

	// What the output must hold: the payload files as they are, unless the
	// fixture names what unpacking leaves instead.
	for _, file := range fixture.Files {
		fixture.Output[file.Path] = file.BLAKE3
	}
	for _, absent := range spec.AbsentOutput {
		delete(fixture.Output, absent)
	}
	for _, retained := range spec.RetainedOutput {
		if fixture.Retained == nil {
			fixture.Retained = map[string]string{}
		}
		fixture.Retained[retained] = fixture.Output[retained]
		delete(fixture.Output, retained)
	}
	for rel, digest := range spec.ExpectedOutput {
		fixture.Output[rel] = digest
	}
	return fixture, nil
}

func pathComponents(p string) []any {
	var out []any
	for _, part := range strings.Split(path.Clean(p), "/") {
		out = append(out, part)
	}
	return out
}

func torrentFileBytes(spec torrentFixtureSpec, file torrentFileSpec) ([]byte, error) {
	if file.Corpus != "" {
		source := filepath.Join(testdataDir(), spec.CorpusSlug, file.Corpus)
		data, err := os.ReadFile(source)
		if err != nil {
			return nil, fmt.Errorf("read corpus payload %s (hydrate %s first): %w", source, spec.CorpusSlug, err)
		}
		return data, nil
	}
	// Seeded by the torrent and path so every run builds the same bytes.
	seed := sha1.Sum([]byte(spec.Slug + "/" + file.Path))
	var key [32]byte
	copy(key[:], seed[:])
	stream := rand.NewChaCha8(key)
	data := make([]byte, file.Synthetic)
	if _, err := io.ReadFull(stream, data); err != nil {
		return nil, err
	}
	return data, nil
}

// bencode, for metainfo. Dictionaries are written with sorted keys, as the
// format requires.
type bencodePair struct {
	key   string
	value []byte
}

func bencodeDictRaw(pairs []bencodePair) []byte {
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
	var out bytes.Buffer
	out.WriteByte('d')
	for _, pair := range pairs {
		out.Write(bencodeValue(pair.key))
		out.Write(pair.value)
	}
	out.WriteByte('e')
	return out.Bytes()
}

func bencodeValue(value any) []byte {
	switch typed := value.(type) {
	case string:
		return []byte(strconv.Itoa(len(typed)) + ":" + typed)
	case []byte:
		return append([]byte(strconv.Itoa(len(typed))+":"), typed...)
	case int64:
		return []byte("i" + strconv.FormatInt(typed, 10) + "e")
	case int:
		return []byte("i" + strconv.Itoa(typed) + "e")
	case []any:
		var out bytes.Buffer
		out.WriteByte('l')
		for _, item := range typed {
			out.Write(bencodeValue(item))
		}
		out.WriteByte('e')
		return out.Bytes()
	case map[string]any:
		pairs := make([]bencodePair, 0, len(typed))
		for key, item := range typed {
			pairs = append(pairs, bencodePair{key, bencodeValue(item)})
		}
		return bencodeDictRaw(pairs)
	default:
		panic(fmt.Sprintf("bencode: unsupported %T", value))
	}
}
