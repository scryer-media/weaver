// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
)

// A file's restart checkpoint as Weaver persists it in active_file_progress:
// `WFCV | u16 LE schema version | MessagePack [[first_ordinal, ordinals,
// start, end], ...]`. The harness reads it to know exactly which ordinals a
// restart may trust, so a case can name the articles a restart has to fetch
// again instead of guessing from byte counts.
const (
	fileCoverageMagic         = "WFCV"
	fileCoverageSchemaVersion = 1
)

type fileCoverageRun struct {
	FirstOrdinal uint32 `json:"first_ordinal"`
	Ordinals     uint32 `json:"ordinals"`
	Start        uint64 `json:"start"`
	End          uint64 `json:"end"`
}

type fileCoverage struct {
	Runs []fileCoverageRun `json:"runs"`
}

func (coverage fileCoverage) CoveredBytes() uint64 {
	var total uint64
	for _, run := range coverage.Runs {
		if run.End > run.Start {
			total += run.End - run.Start
		}
	}
	return total
}

// Contains reports whether the zero-based ordinal is durable.
func (coverage fileCoverage) Contains(ordinal uint32) bool {
	for _, run := range coverage.Runs {
		if ordinal >= run.FirstOrdinal && ordinal < run.FirstOrdinal+run.Ordinals {
			return true
		}
	}
	return false
}

type fileCoverageRow struct {
	JobID     int          `json:"job_id"`
	FileIndex int          `json:"file_index"`
	Coverage  fileCoverage `json:"coverage"`
	Err       error        `json:"-"`
}

func readFileCoverageRows(db *sql.DB, datastore weaverDatastore) ([]fileCoverageRow, error) {
	rows, err := db.Query(rebindWeaverSQL(datastore, `SELECT job_id, file_index, coverage FROM active_file_progress ORDER BY job_id, file_index`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fileCoverageRow
	for rows.Next() {
		var row fileCoverageRow
		var blob []byte
		if err := rows.Scan(&row.JobID, &row.FileIndex, &blob); err != nil {
			return nil, err
		}
		row.Coverage, row.Err = decodeFileCoverage(blob)
		out = append(out, row)
	}
	return out, rows.Err()
}

func readJobFileCoverage(dbPath string, jobID int) ([]fileCoverageRow, error) {
	available, err := weaverStateDBAvailable(dbPath)
	if err != nil || !available {
		return nil, err
	}
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer closeWeaverStateDB(db, datastore)
	all, err := readFileCoverageRows(db, datastore)
	if err != nil {
		return nil, err
	}
	var out []fileCoverageRow
	for _, row := range all {
		if row.JobID == jobID {
			out = append(out, row)
		}
	}
	return out, nil
}

func decodeFileCoverage(blob []byte) (fileCoverage, error) {
	if len(blob) < 6 {
		return fileCoverage{}, fmt.Errorf("file coverage truncated (%d bytes)", len(blob))
	}
	if string(blob[:4]) != fileCoverageMagic {
		return fileCoverage{}, errors.New("file coverage has a bad magic")
	}
	if version := binary.LittleEndian.Uint16(blob[4:6]); version != fileCoverageSchemaVersion {
		return fileCoverage{}, fmt.Errorf("file coverage schema version %d is not %d", version, fileCoverageSchemaVersion)
	}
	reader := &msgpackReader{data: blob[6:]}
	count, err := reader.arrayLen()
	if err != nil {
		return fileCoverage{}, err
	}
	coverage := fileCoverage{Runs: make([]fileCoverageRun, 0, count)}
	for i := 0; i < count; i++ {
		fields, err := reader.arrayLen()
		if err != nil {
			return fileCoverage{}, err
		}
		if fields != 4 {
			return fileCoverage{}, fmt.Errorf("coverage run %d has %d fields", i, fields)
		}
		var values [4]uint64
		for field := range values {
			if values[field], err = reader.uint(); err != nil {
				return fileCoverage{}, err
			}
		}
		coverage.Runs = append(coverage.Runs, fileCoverageRun{
			FirstOrdinal: uint32(values[0]),
			Ordinals:     uint32(values[1]),
			Start:        values[2],
			End:          values[3],
		})
	}
	if reader.pos != len(reader.data) {
		return fileCoverage{}, errors.New("trailing bytes after the coverage body")
	}
	return coverage, nil
}

// encodeFileCoverage is the inverse, for unit tests and for cases that plant a
// checkpoint of their own.
func encodeFileCoverage(coverage fileCoverage, version uint16) []byte {
	out := []byte(fileCoverageMagic)
	out = binary.LittleEndian.AppendUint16(out, version)
	out = appendMsgpackArrayLen(out, len(coverage.Runs))
	for _, run := range coverage.Runs {
		out = appendMsgpackArrayLen(out, 4)
		for _, value := range []uint64{uint64(run.FirstOrdinal), uint64(run.Ordinals), run.Start, run.End} {
			out = appendMsgpackUint(out, value)
		}
	}
	return out
}

// The MessagePack subset rmp_serde writes for a Vec of integer tuples:
// arrays and unsigned integers.
type msgpackReader struct {
	data []byte
	pos  int
}

func (reader *msgpackReader) next(n int) ([]byte, error) {
	if reader.pos+n > len(reader.data) {
		return nil, errors.New("coverage body truncated")
	}
	out := reader.data[reader.pos : reader.pos+n]
	reader.pos += n
	return out, nil
}

func (reader *msgpackReader) arrayLen() (int, error) {
	head, err := reader.next(1)
	if err != nil {
		return 0, err
	}
	switch b := head[0]; {
	case b >= 0x90 && b <= 0x9f:
		return int(b & 0x0f), nil
	case b == 0xdc:
		raw, err := reader.next(2)
		if err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint16(raw)), nil
	case b == 0xdd:
		raw, err := reader.next(4)
		if err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(raw)), nil
	default:
		return 0, fmt.Errorf("expected a MessagePack array, found 0x%02x", b)
	}
}

func (reader *msgpackReader) uint() (uint64, error) {
	head, err := reader.next(1)
	if err != nil {
		return 0, err
	}
	b := head[0]
	if b <= 0x7f {
		return uint64(b), nil
	}
	width := map[byte]int{0xcc: 1, 0xcd: 2, 0xce: 4, 0xcf: 8}[b]
	if width == 0 {
		return 0, fmt.Errorf("expected a MessagePack unsigned integer, found 0x%02x", b)
	}
	raw, err := reader.next(width)
	if err != nil {
		return 0, err
	}
	var value uint64
	for _, octet := range raw {
		value = value<<8 | uint64(octet)
	}
	return value, nil
}

func appendMsgpackArrayLen(out []byte, n int) []byte {
	switch {
	case n <= 15:
		return append(out, 0x90|byte(n))
	case n <= 0xffff:
		return binary.BigEndian.AppendUint16(append(out, 0xdc), uint16(n))
	default:
		return binary.BigEndian.AppendUint32(append(out, 0xdd), uint32(n))
	}
}

func appendMsgpackUint(out []byte, value uint64) []byte {
	switch {
	case value <= 0x7f:
		return append(out, byte(value))
	case value <= 0xff:
		return append(out, 0xcc, byte(value))
	case value <= 0xffff:
		return binary.BigEndian.AppendUint16(append(out, 0xcd), uint16(value))
	case value <= 0xffffffff:
		return binary.BigEndian.AppendUint32(append(out, 0xce), uint32(value))
	default:
		return binary.BigEndian.AppendUint64(append(out, 0xcf), value)
	}
}
