package fixturegen

import (
	"archive/tar"
	"archive/zip"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
)

// Member is one file going into a Go-written container.
type Member struct {
	// Name is the path the container records, always `/`-separated.
	Name string
	// Source is the host file whose bytes are stored.
	Source string
}

// tarBlockFactor is GNU tar's default: a tar stream is padded to a whole
// number of 20-record (10 KiB) blocks. Go's archive/tar only pads to the two
// 512-byte end-of-archive records, so the generator adds the rest itself.
const tarBlockFactor = 20 * 512

// WriteTar writes a ustar container with root ownership and the generator's
// canonical timestamp, so the same members always produce the same bytes.
// dirEntries are recorded as directory members ahead of the files.
func WriteTar(destination string, dirEntries []string, members []Member) error {
	return writeFile(destination, func(writer io.Writer) error {
		counter := &countingWriter{inner: writer}
		archive := tar.NewWriter(counter)
		for _, directory := range dirEntries {
			if err := archive.WriteHeader(&tar.Header{
				Name: directory, Typeflag: tar.TypeDir, Mode: 0o755,
				Uname: "root", Gname: "root", ModTime: CanonicalTime, Format: tar.FormatUSTAR,
			}); err != nil {
				return err
			}
		}
		for _, member := range members {
			size, err := FileSize(member.Source)
			if err != nil {
				return err
			}
			if err := archive.WriteHeader(&tar.Header{
				Name: member.Name, Typeflag: tar.TypeReg, Mode: 0o644, Size: size,
				Uname: "root", Gname: "root", ModTime: CanonicalTime, Format: tar.FormatUSTAR,
			}); err != nil {
				return err
			}
			if err := copyInto(archive, member.Source); err != nil {
				return err
			}
		}
		if err := archive.Close(); err != nil {
			return err
		}
		if padding := counter.written % tarBlockFactor; padding != 0 {
			if _, err := counter.Write(make([]byte, tarBlockFactor-padding)); err != nil {
				return err
			}
		}
		return nil
	})
}

// WriteZip writes a zip whose single member is stored uncompressed. When
// password is non-empty the member is encrypted with the legacy PKWARE
// ZipCrypto cipher, which is the shape the corpus has always carried.
func WriteZip(destination string, members []Member, password string) error {
	return writeFile(destination, func(writer io.Writer) error {
		archive := zip.NewWriter(writer)
		for _, member := range members {
			contents, err := os.ReadFile(member.Source)
			if err != nil {
				return err
			}
			header := &zip.FileHeader{
				Name:               member.Name,
				Method:             zip.Store,
				Modified:           CanonicalTime,
				CRC32:              crc32.ChecksumIEEE(contents),
				UncompressedSize64: uint64(len(contents)),
			}
			// CreateRaw writes the legacy MS-DOS date and time fields
			// verbatim rather than deriving them from Modified, and an unset
			// pair reads as 1980-00-00. Fill them from the same constant.
			// The fields are deprecated for callers of Create/CreateHeader,
			// which derive them; CreateRaw does not.
			header.ModifiedDate, header.ModifiedTime = msdosTime(CanonicalTime) //lint:ignore SA1019 CreateRaw copies these fields verbatim
			header.SetMode(0o644)
			payload := contents
			if password != "" {
				payload = zipCryptoEncrypt(contents, password, header.CRC32, member.Name)
				header.Flags |= 0x1
			}
			header.CompressedSize64 = uint64(len(payload))
			entry, err := archive.CreateRaw(header)
			if err != nil {
				return err
			}
			if _, err := entry.Write(payload); err != nil {
				return err
			}
		}
		return archive.Close()
	})
}

// WriteGzip wraps one file in a gzip stream. innerName goes in the header's
// FNAME field when set, which is how the loose `.gz` fixtures name their
// payload.
func WriteGzip(destination, source, innerName string) error {
	return writeFile(destination, func(writer io.Writer) error {
		compressor, err := gzip.NewWriterLevel(writer, gzip.BestSpeed)
		if err != nil {
			return err
		}
		compressor.Name = innerName
		compressor.ModTime = CanonicalTime
		if err := copyInto(compressor, source); err != nil {
			return err
		}
		return compressor.Close()
	})
}

// WriteDeflate writes a raw DEFLATE stream with no container framing.
func WriteDeflate(destination, source string) error {
	return writeFile(destination, func(writer io.Writer) error {
		compressor, err := flate.NewWriter(writer, flate.BestSpeed)
		if err != nil {
			return err
		}
		if err := copyInto(compressor, source); err != nil {
			return err
		}
		return compressor.Close()
	})
}

// WriteBzip2 writes a bzip2 stream.
func WriteBzip2(destination, source string) error {
	return writeFile(destination, func(writer io.Writer) error {
		compressor, err := bzip2.NewWriter(writer, &bzip2.WriterConfig{Level: bzip2.BestSpeed})
		if err != nil {
			return err
		}
		if err := copyInto(compressor, source); err != nil {
			return err
		}
		return compressor.Close()
	})
}

// WriteZstd writes a zstd stream.
func WriteZstd(destination, source string) error {
	return writeFile(destination, func(writer io.Writer) error {
		compressor, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return err
		}
		if err := copyInto(compressor, source); err != nil {
			return err
		}
		return compressor.Close()
	})
}

// WriteBrotli writes a brotli stream.
func WriteBrotli(destination, source string) error {
	return writeFile(destination, func(writer io.Writer) error {
		compressor := brotli.NewWriterLevel(writer, brotli.BestSpeed)
		if err := copyInto(compressor, source); err != nil {
			return err
		}
		return compressor.Close()
	})
}

// CompressFile applies one of the stream codecs by name, so a recipe can name
// its codec instead of branching.
func CompressFile(codec, destination, source string) error {
	switch codec {
	case "gzip":
		return WriteGzip(destination, source, path.Base(source))
	case "gzip-anonymous":
		return WriteGzip(destination, source, "")
	case "deflate":
		return WriteDeflate(destination, source)
	case "bzip2":
		return WriteBzip2(destination, source)
	case "zstd":
		return WriteZstd(destination, source)
	case "brotli":
		return WriteBrotli(destination, source)
	default:
		return fmt.Errorf("unknown stream codec %q", codec)
	}
}

// msdosTime encodes a timestamp in the packed MS-DOS form the zip local and
// central headers carry: date is year-1980, month, day; time is hour, minute,
// two-second units.
func msdosTime(moment time.Time) (date, clock uint16) {
	date = uint16(moment.Day()) | uint16(moment.Month())<<5 | uint16(moment.Year()-1980)<<9
	clock = uint16(moment.Second()/2) | uint16(moment.Minute())<<5 | uint16(moment.Hour())<<11
	return date, clock
}

func copyInto(writer io.Writer, source string) error {
	file, err := os.Open(source)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(writer, file)
	return err
}

type countingWriter struct {
	inner   io.Writer
	written int64
}

func (writer *countingWriter) Write(data []byte) (int, error) {
	count, err := writer.inner.Write(data)
	writer.written += int64(count)
	return count, err
}

// zipCryptoEncrypt applies the traditional PKWARE stream cipher from APPNOTE
// section 6.0. It prepends the mandatory 12-byte encryption header whose last
// byte must equal the high byte of the member's CRC-32; the other eleven bytes
// are drawn from the generator's own deterministic stream rather than a random
// source, which is what keeps the encrypted zip byte-reproducible.
func zipCryptoEncrypt(plaintext []byte, password string, checksum uint32, seed string) []byte {
	header := PatternBytes("zipcrypto/"+seed, 12)
	header[11] = byte(checksum >> 24)

	keys := [3]uint32{0x12345678, 0x23456789, 0x34567890}
	update := func(value byte) {
		keys[0] = crcStep(keys[0], value)
		keys[1] += keys[0] & 0xff
		keys[1] = keys[1]*134775813 + 1
		keys[2] = crcStep(keys[2], byte(keys[1]>>24))
	}
	for index := 0; index < len(password); index++ {
		update(password[index])
	}
	stream := func() byte {
		temp := keys[2] | 2
		return byte((temp * (temp ^ 1)) >> 8)
	}
	output := make([]byte, 0, len(header)+len(plaintext))
	for _, value := range header {
		cipher := value ^ stream()
		update(value)
		output = append(output, cipher)
	}
	for _, value := range plaintext {
		cipher := value ^ stream()
		update(value)
		output = append(output, cipher)
	}
	return output
}

// crcStep is the un-inverted CRC-32 byte step the PKWARE key schedule uses.
// hash/crc32's Update applies the standard pre- and post-inversion, which this
// cipher does not.
func crcStep(value uint32, input byte) uint32 {
	return crc32.IEEETable[byte(value)^input] ^ (value >> 8)
}
