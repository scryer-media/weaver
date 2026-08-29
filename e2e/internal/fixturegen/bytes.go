package fixturegen

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// CanonicalTime is stamped on every payload file the generator writes, so the
// only clock a recipe depends on is this constant. RAR and 7z still record
// their own creation time inside the archives they write, which is why the
// archive families are shape-reproducible rather than byte-reproducible.
var CanonicalTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// WritePRNG writes size bytes of a deterministic, incompressible stream: the
// SHA-256 of seed concatenated with a 64-bit block counter, repeated. The
// stream depends on nothing but the seed, so a payload fed to an archiver is
// reproducible from the recipe alone.
func WritePRNG(path, seed string, size int64) error {
	if size < 0 {
		return fmt.Errorf("payload size must not be negative, got %d", size)
	}
	return writeFile(path, func(writer io.Writer) error {
		return streamPRNG(writer, seed, size)
	})
}

func streamPRNG(writer io.Writer, seed string, size int64) error {
	prefix := []byte(seed)
	var counter uint64
	var written int64
	block := make([]byte, len(prefix)+8)
	copy(block, prefix)
	for written < size {
		binary.BigEndian.PutUint64(block[len(prefix):], counter)
		digest := sha256.Sum256(block)
		chunk := digest[:]
		if remaining := size - written; int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}
		if _, err := writer.Write(chunk); err != nil {
			return err
		}
		written += int64(len(chunk))
		counter++
	}
	return nil
}

// WriteCompressiblePRNG writes size bytes that a compressor can work on: four
// fresh pseudorandom 32 KiB blocks followed by a repeat of the first, so the
// stream carries about 20% redundancy while staying deterministic.
func WriteCompressiblePRNG(path, seed string, size int64) error {
	return writeFile(path, func(writer io.Writer) error {
		const blockSize = 32 << 10
		const uniqueBlocks = 4
		blocks := make([][]byte, uniqueBlocks)
		var written int64
		var generation uint64
		for written < size {
			for index := range blocks {
				buffer := make([]byte, 0, blockSize)
				collector := &sliceWriter{buffer: &buffer}
				if err := streamPRNG(collector, fmt.Sprintf("%s/%d/%d", seed, generation, index), blockSize); err != nil {
					return err
				}
				blocks[index] = buffer
			}
			for index := 0; index <= uniqueBlocks && written < size; index++ {
				chunk := blocks[index%uniqueBlocks]
				if remaining := size - written; int64(len(chunk)) > remaining {
					chunk = chunk[:remaining]
				}
				if _, err := writer.Write(chunk); err != nil {
					return err
				}
				written += int64(len(chunk))
			}
			generation++
		}
		return nil
	})
}

type sliceWriter struct{ buffer *[]byte }

func (writer *sliceWriter) Write(data []byte) (int, error) {
	*writer.buffer = append(*writer.buffer, data...)
	return len(data), nil
}

// WriteText writes an exact-length text file, padding with spaces and always
// ending in a newline, so a fixture member's size is part of the recipe.
func WriteText(path, text string, size int) error {
	contents := []byte(text)
	switch {
	case len(contents) > size:
		contents = contents[:size]
	default:
		for len(contents) < size {
			contents = append(contents, ' ')
		}
	}
	if size > 0 {
		contents[size-1] = '\n'
	}
	return writeFile(path, func(writer io.Writer) error {
		_, err := writer.Write(contents)
		return err
	})
}

// CopyFile copies one file, preserving nothing but its bytes.
func CopyFile(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	return writeFile(destination, func(writer io.Writer) error {
		_, err := io.Copy(writer, input)
		return err
	})
}

// SliceFile writes length bytes of source starting at offset. It is used to
// carve fixed-size payload members out of a rendered clip; the result is a
// media byte range, not a re-encode.
func SliceFile(source, destination string, offset, length int64) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	if offset+length > info.Size() {
		return fmt.Errorf("slice %s[%d:%d] runs past the end of a %d byte file", source, offset, offset+length, info.Size())
	}
	return writeFile(destination, func(writer io.Writer) error {
		_, err := io.Copy(writer, io.NewSectionReader(input, offset, length))
		return err
	})
}

// SplitFile cuts a file into fixed-size parts named by nameFor. It is a plain
// byte split: nothing in it understands the container it is cutting, which is
// what makes it safe to use on a RAR set.
func SplitFile(source string, partSize int64, nameFor func(index int) string) ([]string, error) {
	input, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return nil, err
	}
	if partSize <= 0 {
		return nil, fmt.Errorf("split part size must be positive, got %d", partSize)
	}
	var parts []string
	reader := bufio.NewReaderSize(input, 1<<20)
	for index, remaining := 0, info.Size(); remaining > 0; index++ {
		take := partSize
		if remaining < take {
			take = remaining
		}
		part := nameFor(index)
		if err := writeFile(part, func(writer io.Writer) error {
			_, err := io.CopyN(writer, reader, take)
			return err
		}); err != nil {
			return nil, err
		}
		parts = append(parts, part)
		remaining -= take
	}
	return parts, nil
}

// ConcatFiles joins parts end to end.
func ConcatFiles(destination string, parts ...string) error {
	return writeFile(destination, func(writer io.Writer) error {
		for _, part := range parts {
			input, err := os.Open(part)
			if err != nil {
				return err
			}
			_, err = io.Copy(writer, input)
			closeErr := input.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
		return nil
	})
}

// ZeroRange overwrites length bytes at offset with zeros. This is the damage
// every "corrupted" and PAR2-repair fixture uses: it is deterministic, it is
// format-agnostic, and it destroys payload rather than structure so the failure
// surfaces as a checksum mismatch rather than an unparseable header.
func ZeroRange(path string, offset, length int64) error {
	return OverwriteRange(path, offset, make([]byte, length))
}

// OverwriteRange replaces bytes at offset in place.
func OverwriteRange(path string, offset int64, contents []byte) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if offset+int64(len(contents)) > info.Size() {
		return fmt.Errorf("overwrite %s at %d+%d runs past the end of a %d byte file", path, offset, len(contents), info.Size())
	}
	if _, err := file.WriteAt(contents, offset); err != nil {
		return err
	}
	return file.Close()
}

// TruncateBy shortens a file by count bytes.
func TruncateBy(path string, count int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if count >= info.Size() {
		return fmt.Errorf("truncating %s by %d would empty a %d byte file", path, count, info.Size())
	}
	return os.Truncate(path, info.Size()-count)
}

// PatternBytes is a deterministic non-zero overwrite, used where zeroing would
// be indistinguishable from a legitimately sparse region.
func PatternBytes(seed string, length int) []byte {
	buffer := make([]byte, 0, length)
	collector := &sliceWriter{buffer: &buffer}
	_ = streamPRNG(collector, seed, int64(length))
	return buffer
}

func writeFile(path string, write func(io.Writer) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, 1<<20)
	if err := write(writer); err != nil {
		file.Close()
		return err
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Chtimes(path, CanonicalTime, CanonicalTime)
}

// FileSize is os.Stat reduced to the one field recipes ask for.
func FileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
