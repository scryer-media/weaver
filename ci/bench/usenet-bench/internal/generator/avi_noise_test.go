package generator

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// buildTestAVI assembles a minimal RIFF/AVI: a `hdrl` list with one header
// chunk, a `movi` list holding one video chunk, one audio chunk and a second
// video chunk of odd length (exercising the RIFF pad byte), and an `idx1`.
func buildTestAVI(video1, audio, video2 []byte) []byte {
	chunk := func(fourcc string, body []byte) []byte {
		out := []byte(fourcc)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(body)))
		out = append(out, body...)
		if len(body)%2 == 1 {
			out = append(out, 0)
		}
		return out
	}
	list := func(kind string, body []byte) []byte {
		return chunk("LIST", append([]byte(kind), body...))
	}
	hdrl := list("hdrl", chunk("avih", make([]byte, 56)))
	movi := list("movi", bytes.Join([][]byte{chunk("00dc", video1), chunk("01wb", audio), chunk("00dc", video2)}, nil))
	idx := chunk("idx1", make([]byte, 16))
	body := bytes.Join([][]byte{[]byte("AVI "), hdrl, movi, idx}, nil)
	return chunk("RIFF", body)
}

func TestSampleNoiseTouchesOnlyVideoChunkBodies(t *testing.T) {
	video1 := bytes.Repeat([]byte{0x80}, 300)
	audio := bytes.Repeat([]byte{0x11}, 64)
	video2 := bytes.Repeat([]byte{0x40}, 301)
	original := buildTestAVI(video1, audio, video2)
	noised := append([]byte(nil), original...)

	changed, err := addSampleNoiseToRIFF(noised, compressibleNoiseBits, 7)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(len(video1) + len(video2)); changed != want {
		t.Fatalf("noised %d bytes, want %d", changed, want)
	}
	if len(noised) != len(original) {
		t.Fatalf("noise changed the file length: %d -> %d", len(original), len(noised))
	}
	// Everything but the two video bodies is byte-identical.
	v1 := bytes.Index(original, video1)
	v2 := bytes.Index(original, video2)
	for index := range original {
		inVideo := (index >= v1 && index < v1+len(video1)) || (index >= v2 && index < v2+len(video2))
		if inVideo {
			if delta := noised[index] - original[index]; delta > 1<<compressibleNoiseBits-1 {
				t.Fatalf("byte %d moved by %d, more than %d bits of noise allow", index, delta, compressibleNoiseBits)
			}
			continue
		}
		if noised[index] != original[index] {
			t.Fatalf("byte %d outside the video chunks changed", index)
		}
	}
	// The noise is real: not every sample is unchanged, and it is uniform
	// enough that both video chunks differ from their flat originals.
	if bytes.Equal(noised[v1:v1+len(video1)], video1) || bytes.Equal(noised[v2:v2+len(video2)], video2) {
		t.Fatal("video samples were left unchanged")
	}
}

func TestSampleNoiseIsDeterministicPerStreamAndChunk(t *testing.T) {
	video := bytes.Repeat([]byte{0x80}, 4096)
	first := buildTestAVI(video, nil, video)
	second := append([]byte(nil), first...)
	if _, err := addSampleNoiseToRIFF(first, compressibleNoiseBits, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := addSampleNoiseToRIFF(second, compressibleNoiseBits, 3); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("the same stream must produce the same noise")
	}
	// Different chunks of one stream do not share a noise sequence, and a
	// different stream differs everywhere — the point of keying the XOF.
	clean := buildTestAVI(video, nil, video)
	v1, v2 := bytes.Index(clean, video), bytes.LastIndex(clean, video)
	if bytes.Equal(first[v1:v1+len(video)], first[v2:v2+len(video)]) {
		t.Fatal("two video chunks of one stream repeated the same noise")
	}
	other := buildTestAVI(video, nil, video)
	if _, err := addSampleNoiseToRIFF(other, compressibleNoiseBits, 4); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, other) {
		t.Fatal("different streams must produce different noise")
	}
}

func TestSampleNoiseRejectsNonAVIInput(t *testing.T) {
	if _, err := addSampleNoiseToRIFF([]byte("RIFF\x04\x00\x00\x00WAVE"), compressibleNoiseBits, 1); err == nil {
		t.Fatal("a non-AVI RIFF must be rejected")
	}
	if err := addSampleNoiseToAVI("/nonexistent/file.avi", 0, 1); err == nil {
		t.Fatal("a zero-bit noise width must be rejected")
	}
}
