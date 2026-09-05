// Package fixture defines the checked-in, reproducible archive fixture matrix.
package fixture

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FixturePassword is intentionally public: these archives exercise encrypted
// archive handling, rather than password discovery. It must never be reused
// outside this benchmark corpus.
const FixturePassword = "nntp-bench-fixture-password"

// ArchiveFormat is the on-wire container family a fixture is written in. It
// is separate from the writer release that produced it: RAR 6 and 7 are newer
// writers of the same RAR5 container.
type ArchiveFormat string

const (
	// RAR4 is the legacy pre-RAR5 family emitted by the source-locked 3.93
	// and 4.20 writers.
	RAR4 ArchiveFormat = "rar4"
	// RAR5 is the on-wire format introduced by RAR 5.x and used by 5.x-7.x.
	RAR5 ArchiveFormat = "rar5"
	// SevenZip is the 7z container written by the official 7-Zip console
	// build. Its multi-volume members are named fixture.7z.001, .002, ...
	SevenZip ArchiveFormat = "7z"
)

func (f ArchiveFormat) Valid() bool {
	switch f {
	case RAR4, RAR5, SevenZip:
		return true
	default:
		return false
	}
}

// NZBOrder declares the order in which a fixture's posted files appear in its
// NZB. Real posts are not always in volume order, and a client that schedules
// by archive need rather than by NZB order behaves differently on the two.
type NZBOrder string

const (
	// SequentialNZBOrder posts archive files in sorted volume order and
	// leaves repair material trailing. It is the default.
	SequentialNZBOrder NZBOrder = "sequential"
	// ScatteredNZBOrder posts archive files in a deterministic pseudo-random
	// permutation, so no volume is guaranteed to arrive first and repair
	// material is interleaved among the volumes.
	ScatteredNZBOrder NZBOrder = "scattered"
)

func (o NZBOrder) Valid() bool {
	return o == SequentialNZBOrder || o == ScatteredNZBOrder
}

type Compression string

const (
	Store  Compression = "store"
	Normal Compression = "normal"
)

type Encryption string

const (
	NoEncryption     Encryption = "none"
	DataEncryption   Encryption = "data"
	HeaderEncryption Encryption = "headers"
)

type PayloadKind string

const (
	IncompressiblePayload PayloadKind = "incompressible"
	CompressiblePayload   PayloadKind = "compressible"
)

// PayloadLayout controls the topology of the source files inside an archive.
// It is separate from PayloadKind so a workload can describe both its file
// shape and whether its bulk data compresses.
type PayloadLayout string

const (
	UniformPayloadLayout    PayloadLayout = "uniform"
	BluRayDiscPayloadLayout PayloadLayout = "bluray-disc"
)

// RepairProfile declares a deliberately damaged corpus member and the
// independent repair material posted with it. It is a test dimension, not a
// statement about how frequently any profile appears on Usenet.
type RepairProfile string

const (
	CleanRepairProfile            RepairProfile = "clean"
	PAR2LightRepairProfile        RepairProfile = "par2-light"
	PAR2HeavyRepairProfile        RepairProfile = "par2-heavy"
	RARRecoveryVolumeLightProfile RepairProfile = "rar-recovery-volume-light"
	RARRecoveryVolumeHeavyProfile RepairProfile = "rar-recovery-volume-heavy"
	// PAR2HeavyWithheldProfile posts the same PAR2 material as par2-heavy,
	// but the absent volume is still listed in the NZB with article
	// identifiers that were never posted. That is what a real short post
	// looks like to a client: the file is requested and every article for it
	// is refused, rather than the file simply never being mentioned.
	PAR2HeavyWithheldProfile RepairProfile = "par2-heavy-withheld"
)

func (p RepairProfile) Valid() bool {
	switch p {
	case CleanRepairProfile, PAR2LightRepairProfile, PAR2HeavyRepairProfile, PAR2HeavyWithheldProfile, RARRecoveryVolumeLightProfile, RARRecoveryVolumeHeavyProfile:
		return true
	default:
		return false
	}
}

// UsesPAR2 reports whether the profile posts PAR2 recovery material.
func (p RepairProfile) UsesPAR2() bool {
	switch p {
	case PAR2LightRepairProfile, PAR2HeavyRepairProfile, PAR2HeavyWithheldProfile:
		return true
	default:
		return false
	}
}

// UsesRARRecoveryVolumes reports whether the profile posts RAR .rev recovery
// volumes, which only the RAR writers can produce.
func (p RepairProfile) UsesRARRecoveryVolumes() bool {
	return p == RARRecoveryVolumeLightProfile || p == RARRecoveryVolumeHeavyProfile
}

// Matrix is the stable source definition. It expands to one ArchiveCase for
// every useful combination, so reviewers can see which cases exist without
// committing any large archive data.
type Matrix struct {
	SchemaVersion int          `json:"schema_version"`
	Description   string       `json:"description"`
	Sets          []FixtureSet `json:"sets"`
}

type FixtureSet struct {
	ID        string `json:"id"`
	WriterEra string `json:"writer_era"`
	// GeneratorToolchain is the pinned RARLAB image used for this set. It
	// always supplies the FFmpeg payload renderer, and for the RAR lanes it
	// is also the archive writer.
	GeneratorToolchain string `json:"generator_toolchain"`
	// ArchiveWriter names the pinned toolchain that writes the container when
	// it is not the RARLAB image — the official 7-Zip build for the 7z lane.
	// It defaults to GeneratorToolchain.
	ArchiveWriter  string          `json:"archive_writer,omitempty"`
	ArchiveFormat  ArchiveFormat   `json:"archive_format"`
	Compressions   []Compression   `json:"compressions"`
	Solid          []bool          `json:"solid"`
	Encryptions    []Encryption    `json:"encryptions"`
	Payloads       []PayloadKind   `json:"payloads"`
	PayloadLayout  PayloadLayout   `json:"payload_layout,omitempty"`
	RepairProfiles []RepairProfile `json:"repair_profiles,omitempty"`
	NZBOrder       NZBOrder        `json:"nzb_order,omitempty"`
	FileCount      int             `json:"file_count"`
	VolumeSize     string          `json:"volume_size"`
}

// ArchiveCase is one materialized archive fixture.
type ArchiveCase struct {
	ID                 string        `json:"id"`
	SetID              string        `json:"set_id"`
	WriterEra          string        `json:"writer_era"`
	GeneratorToolchain string        `json:"generator_toolchain"`
	ArchiveWriter      string        `json:"archive_writer"`
	ArchiveFormat      ArchiveFormat `json:"archive_format"`
	Compression        Compression   `json:"compression"`
	Solid              bool          `json:"solid"`
	Encryption         Encryption    `json:"encryption"`
	Payload            PayloadKind   `json:"payload"`
	PayloadLayout      PayloadLayout `json:"payload_layout"`
	RepairProfile      RepairProfile `json:"repair_profile"`
	NZBOrder           NZBOrder      `json:"nzb_order"`
	FileCount          int           `json:"file_count"`
	VolumeSize         string        `json:"volume_size"`
}

func LoadMatrix(path string) (Matrix, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Matrix{}, fmt.Errorf("read fixture matrix %s: %w", path, err)
	}

	var matrix Matrix
	if err := json.Unmarshal(contents, &matrix); err != nil {
		return Matrix{}, fmt.Errorf("decode fixture matrix %s: %w", path, err)
	}
	if err := matrix.Validate(); err != nil {
		return Matrix{}, err
	}
	return matrix, nil
}

func (m Matrix) Validate() error {
	if m.SchemaVersion != 2 {
		return fmt.Errorf("unsupported fixture matrix schema version %d", m.SchemaVersion)
	}
	if len(m.Sets) == 0 {
		return fmt.Errorf("fixture matrix has no sets")
	}
	_, err := m.Expand()
	return err
}

// Expand deterministically materializes the cartesian product for every set.
func (m Matrix) Expand() ([]ArchiveCase, error) {
	ids := make(map[string]struct{})
	cases := make([]ArchiveCase, 0)
	for _, set := range m.Sets {
		if err := set.validate(); err != nil {
			return nil, err
		}
		layout := set.PayloadLayout
		if layout == "" {
			layout = UniformPayloadLayout
		}
		profiles := set.RepairProfiles
		if len(profiles) == 0 {
			profiles = []RepairProfile{CleanRepairProfile}
		}
		order := set.NZBOrder
		if order == "" {
			order = SequentialNZBOrder
		}
		writer := set.ArchiveWriter
		if writer == "" {
			writer = set.GeneratorToolchain
		}
		for _, profile := range profiles {
			for _, compression := range set.Compressions {
				for _, solid := range set.Solid {
					for _, encryption := range set.Encryptions {
						for _, payload := range set.Payloads {
							parts := []string{
								set.ID,
							}
							if profile != CleanRepairProfile {
								parts = append(parts, string(profile))
							}
							parts = append(parts,
								string(compression),
								solidID(solid),
								string(encryption),
								string(payload),
							)
							id := strings.Join(parts, "-")
							if _, exists := ids[id]; exists {
								return nil, fmt.Errorf("fixture matrix has duplicate case %q", id)
							}
							ids[id] = struct{}{}
							cases = append(cases, ArchiveCase{
								ID:                 id,
								SetID:              set.ID,
								WriterEra:          set.WriterEra,
								GeneratorToolchain: set.GeneratorToolchain,
								ArchiveWriter:      writer,
								ArchiveFormat:      set.ArchiveFormat,
								Compression:        compression,
								Solid:              solid,
								Encryption:         encryption,
								Payload:            payload,
								PayloadLayout:      layout,
								RepairProfile:      profile,
								NZBOrder:           order,
								FileCount:          set.FileCount,
								VolumeSize:         set.VolumeSize,
							})
						}
					}
				}
			}
		}
	}
	return cases, nil
}

func (s FixtureSet) validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return fmt.Errorf("fixture set has an empty id")
	}
	if strings.TrimSpace(s.WriterEra) == "" {
		return fmt.Errorf("fixture set %q has an empty writer_era", s.ID)
	}
	if strings.TrimSpace(s.GeneratorToolchain) == "" {
		return fmt.Errorf("fixture set %q has an empty generator_toolchain", s.ID)
	}
	if !s.ArchiveFormat.Valid() {
		return fmt.Errorf("fixture set %q has unsupported archive_format %q", s.ID, s.ArchiveFormat)
	}
	if s.NZBOrder != "" && !s.NZBOrder.Valid() {
		return fmt.Errorf("fixture set %q has unsupported nzb_order %q", s.ID, s.NZBOrder)
	}
	if len(s.Compressions) == 0 || len(s.Solid) == 0 || len(s.Encryptions) == 0 || len(s.Payloads) == 0 {
		return fmt.Errorf("fixture set %q must specify every matrix axis", s.ID)
	}
	for _, profile := range s.RepairProfiles {
		if !profile.Valid() {
			return fmt.Errorf("fixture set %q has unsupported repair_profile %q", s.ID, profile)
		}
		// RAR recovery volumes are a RAR container feature. 7-Zip has no
		// equivalent, so pairing them is a matrix authoring mistake rather
		// than a generation-time surprise.
		if s.ArchiveFormat == SevenZip && profile.UsesRARRecoveryVolumes() {
			return fmt.Errorf("fixture set %q cannot use repair_profile %q: RAR recovery volumes are not a 7z feature", s.ID, profile)
		}
	}
	if s.PayloadLayout == "" {
		s.PayloadLayout = UniformPayloadLayout
	}
	if s.PayloadLayout != UniformPayloadLayout && s.PayloadLayout != BluRayDiscPayloadLayout {
		return fmt.Errorf("fixture set %q has unsupported payload_layout %q", s.ID, s.PayloadLayout)
	}
	if s.PayloadLayout == UniformPayloadLayout && s.FileCount < 1 {
		return fmt.Errorf("fixture set %q has invalid file_count %d", s.ID, s.FileCount)
	}
	if strings.TrimSpace(s.VolumeSize) == "" {
		return fmt.Errorf("fixture set %q has an empty volume_size", s.ID)
	}
	for _, compression := range s.Compressions {
		if compression != Store && compression != Normal {
			return fmt.Errorf("fixture set %q has unsupported compression %q", s.ID, compression)
		}
	}
	for _, encryption := range s.Encryptions {
		if encryption != NoEncryption && encryption != DataEncryption && encryption != HeaderEncryption {
			return fmt.Errorf("fixture set %q has unsupported encryption %q", s.ID, encryption)
		}
	}
	for _, payload := range s.Payloads {
		if payload != IncompressiblePayload && payload != CompressiblePayload {
			return fmt.Errorf("fixture set %q has unsupported payload %q", s.ID, payload)
		}
	}
	return nil
}

func solidID(solid bool) string {
	if solid {
		return "solid"
	}
	return "nonsolid"
}

// RARArgs returns the RARLAB invocation suffix for this fixture. archive and
// inputs are paths within the container's mounted working directory.
func (c ArchiveCase) RARArgs(archive string, inputs []string) ([]string, error) {
	if archive == "" {
		return nil, fmt.Errorf("fixture %q has an empty archive path", c.ID)
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("fixture %q has no input files", c.ID)
	}
	if c.ArchiveFormat == SevenZip {
		return nil, fmt.Errorf("fixture %q is a 7z case; use SevenZipArgs", c.ID)
	}
	args := []string{"a", "-idq", "-y", "-ep1"}
	switch c.ArchiveFormat {
	case RAR4:
		// The source-locked 3.x and 4.x writers predate the -ma selector;
		// their default archive format is the explicitly selected legacy lane.
	case RAR5:
		// Quick-open data speeds listing, but adds bytes to every upload and
		// is not needed by the download/extract clients under test.
		args = append(args, "-ma5", "-qo-")
	default:
		return nil, fmt.Errorf("fixture %q has unsupported archive_format %q", c.ID, c.ArchiveFormat)
	}
	switch c.Compression {
	case Store:
		args = append(args, "-m0")
	case Normal:
		// The release-shaped lanes use maximal compression with an explicit
		// dictionary. RAR4's 4 MiB setting is its largest supported dictionary;
		// 256 MiB is a practical large RAR5 dictionary across the locked 5.x-
		// 7.x writers. Store lanes remain intentional controls.
		args = append(args, "-m5")
		switch c.ArchiveFormat {
		case RAR4:
			args = append(args, "-md4096")
		case RAR5:
			args = append(args, "-md256m")
		}
	default:
		return nil, fmt.Errorf("fixture %q has unsupported compression %q", c.ID, c.Compression)
	}
	if c.Solid {
		args = append(args, "-s")
	} else {
		args = append(args, "-s-")
	}
	switch c.Encryption {
	case NoEncryption:
	case DataEncryption:
		args = append(args, "-p"+FixturePassword)
	case HeaderEncryption:
		args = append(args, "-hp"+FixturePassword)
	default:
		return nil, fmt.Errorf("fixture %q has unsupported encryption %q", c.ID, c.Encryption)
	}
	args = append(args, "-v"+c.VolumeSize, filepath.ToSlash(archive))
	for _, input := range inputs {
		args = append(args, filepath.ToSlash(input))
	}
	return args, nil
}

// SevenZipArgs returns the official 7-Zip console invocation suffix for this
// fixture. archive and inputs are paths relative to the working directory the
// generator gives the 7-Zip container, so stored member names carry no
// staging prefix. Multi-volume output is named archive.001, archive.002, ...
func (c ArchiveCase) SevenZipArgs(archive string, inputs []string) ([]string, error) {
	if c.ArchiveFormat != SevenZip {
		return nil, fmt.Errorf("fixture %q is not a 7z case", c.ID)
	}
	if archive == "" {
		return nil, fmt.Errorf("fixture %q has an empty archive path", c.ID)
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("fixture %q has no input files", c.ID)
	}
	if strings.TrimSpace(c.VolumeSize) == "" {
		return nil, fmt.Errorf("fixture %q has an empty volume_size", c.ID)
	}
	// -bso0 and -bsp0 silence the informational and progress streams without
	// hiding errors, which stay on stderr.
	args := []string{"a", "-t7z", "-y", "-bso0", "-bsp0"}
	switch c.Compression {
	case Store:
		// -mx0 selects copy mode and -m0=Copy states the member codec
		// explicitly, so a stored 7z fixture cannot silently pick up a
		// different default filter from a later 7-Zip release.
		args = append(args, "-mx0", "-m0=Copy")
	case Normal:
		// LZMA2 at the writer's default level. The RAR lanes deliberately use
		// maximum compression with an explicit dictionary; the 7z lane keeps
		// the writer default so it stays a normal-release control.
		args = append(args, "-mx5")
	default:
		return nil, fmt.Errorf("fixture %q has unsupported compression %q", c.ID, c.Compression)
	}
	if c.Solid {
		args = append(args, "-ms=on")
	} else {
		args = append(args, "-ms=off")
	}
	switch c.Encryption {
	case NoEncryption:
	case DataEncryption:
		args = append(args, "-p"+FixturePassword)
	case HeaderEncryption:
		args = append(args, "-p"+FixturePassword, "-mhe=on")
	default:
		return nil, fmt.Errorf("fixture %q has unsupported encryption %q", c.ID, c.Encryption)
	}
	args = append(args, "-v"+c.VolumeSize, filepath.ToSlash(archive))
	for _, input := range inputs {
		args = append(args, filepath.ToSlash(input))
	}
	return args, nil
}

// SevenZipPasswordArgs returns the password selector every read-side 7-Zip
// invocation needs for an encrypted fixture. 7-Zip prompts interactively
// without it, which would hang a generation run.
func (c ArchiveCase) SevenZipPasswordArgs() []string {
	if !c.RequiresPassword() {
		return nil
	}
	return []string{"-p" + FixturePassword}
}

func (c ArchiveCase) RequiresPassword() bool {
	return c.Encryption != NoEncryption
}
