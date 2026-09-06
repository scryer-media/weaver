package generator

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/zeebo/blake3"
)

// compressibleNoiseBits is the entropy added to every video sample of a
// compressible payload: each 8-bit luma or chroma sample carries this many
// bits of BLAKE3-derived uniform noise on top of the rendered test pattern.
//
// The rendered pattern alone is almost entirely redundant (RAR shrinks it to
// about 4% of its size), which would make a "compressible" fixture a few
// megabytes on the wire however large its payload. FFmpeg's own noise filters
// cannot fix that: `noise` draws from a fixed table and `geq=random()` repeats
// its sequence across slice threads, so a large-dictionary compressor finds
// the repeats and the archive stays tiny. Adding independent noise per sample
// puts the ratio under the generator's control instead of the encoder's.
//
// Four bits was calibrated on the pinned writers against a raw-video AVI of
// the fixture pattern: RAR 7.23 at -m5 -md256m keeps 70% of the payload and
// 7-Zip 26.02 LZMA2 at -mx5 keeps 62%, so both lanes still compress
// substantially while a compressible fixture's archive clears the posted-size
// floor from a payload well under a gigabyte.
const compressibleNoiseBits = 4

// addSampleNoiseToAVI adds deterministic uniform noise of the given width, in
// bits, to every sample of every video chunk in a raw-video AVI, in place.
// Only `00dc` chunk bodies change: the RIFF structure, stream headers, index
// and audio chunks are untouched, so the file stays a valid AVI that FFmpeg
// decodes exactly as before, frame for frame, and the same stream number
// always produces the same bytes.
func addSampleNoiseToAVI(path string, bits uint, stream uint64) error {
	if bits == 0 || bits > 8 {
		return fmt.Errorf("sample noise width must be 1-8 bits, got %d", bits)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	noised, err := addSampleNoiseToRIFF(data, bits, stream)
	if err != nil {
		return fmt.Errorf("add sample noise to %s: %w", path, err)
	}
	if noised == 0 {
		return fmt.Errorf("add sample noise to %s: no video chunks found", path)
	}
	return os.WriteFile(path, data, 0o644)
}

// addSampleNoiseToRIFF walks a RIFF container and noises every `00dc` chunk
// body in place, returning the number of video bytes changed.
func addSampleNoiseToRIFF(data []byte, bits uint, stream uint64) (int64, error) {
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "AVI " {
		return 0, fmt.Errorf("not an AVI RIFF file")
	}
	mask := byte(1<<bits - 1)
	var chunkIndex uint64
	var noised int64
	var walk func(start, end int) error
	walk = func(start, end int) error {
		offset := start
		for offset+8 <= end {
			fourcc := string(data[offset : offset+4])
			size := int(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
			body := offset + 8
			if body+size > end {
				return fmt.Errorf("chunk %q at offset %d runs past its parent", fourcc, offset)
			}
			switch fourcc {
			case "RIFF", "LIST":
				if size < 4 {
					return fmt.Errorf("list %q at offset %d is too short", fourcc, offset)
				}
				if err := walk(body+4, body+size); err != nil {
					return err
				}
			case "00dc":
				noiseVideoChunk(data[body:body+size], mask, stream, chunkIndex)
				chunkIndex++
				noised += int64(size)
			}
			offset = body + size + size&1
		}
		return nil
	}
	if err := walk(0, len(data)); err != nil {
		return 0, err
	}
	return noised, nil
}

// noiseVideoChunk adds `noise & mask` to every byte of one video chunk. The
// noise is the BLAKE3 extendable output keyed by the payload stream and the
// chunk's ordinal, so it is independent per sample, reproducible, and never
// repeats across frames or slices.
func noiseVideoChunk(chunk []byte, mask byte, stream, chunkIndex uint64) {
	var seed [16]byte
	binary.BigEndian.PutUint64(seed[:8], stream)
	binary.BigEndian.PutUint64(seed[8:], chunkIndex)
	hasher := blake3.New()
	_, _ = hasher.Write(seed[:])
	digest := hasher.Digest()
	noise := make([]byte, 64<<10)
	for offset := 0; offset < len(chunk); offset += len(noise) {
		window := chunk[offset:]
		if len(window) > len(noise) {
			window = window[:len(noise)]
		}
		_, _ = digest.Read(noise[:len(window)])
		for index := range window {
			window[index] += noise[index] & mask
		}
	}
}
