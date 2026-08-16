package fixturegen

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

// par2Magic opens every packet in a PAR2 file.
var par2Magic = []byte("PAR2\x00PKT")

// par2MainType is the packet that carries the recovery set's slice size.
var par2MainType = []byte("PAR 2.0\x00Main\x00\x00\x00\x00")

// PAR2SliceSize reads the block size par2cmdline chose for a recovery set.
// A damage recipe that says "sixteen blocks" has to zero exactly sixteen
// blocks, and only the index file knows how big a block ended up being.
func PAR2SliceSize(index string) (int64, error) {
	contents, err := os.ReadFile(index)
	if err != nil {
		return 0, err
	}
	for offset := 0; ; {
		found := bytes.Index(contents[offset:], par2Magic)
		if found < 0 {
			break
		}
		start := offset + found
		if start+64 > len(contents) {
			break
		}
		length := binary.LittleEndian.Uint64(contents[start+8:])
		if length < 64 || start+int(length) > len(contents) {
			break
		}
		if bytes.Equal(contents[start+48:start+64], par2MainType) {
			return int64(binary.LittleEndian.Uint64(contents[start+64:])), nil
		}
		offset = start + int(length)
	}
	return 0, fmt.Errorf("%s carries no PAR2 main packet", index)
}
