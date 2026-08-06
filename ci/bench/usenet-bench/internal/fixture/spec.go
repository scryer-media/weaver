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

type RARFormat string

const (
	// RAR3 is the legacy format emitted by the source-locked 3.x writer.
	// It intentionally has its own corpus lane instead of being represented by
	// a newer writer's RAR4 compatibility switch.
	RAR3 RARFormat = "rar3"
	// RAR4 is the classic pre-RAR5 family emitted by the 4.x writer.
	RAR4 RARFormat = "rar4"
	// RAR5 is the on-wire format introduced by RAR 5.x and used by 5.x-7.x.
	RAR5 RARFormat = "rar5"
)

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
)

func (p RepairProfile) Valid() bool {
	switch p {
	case CleanRepairProfile, PAR2LightRepairProfile, PAR2HeavyRepairProfile, RARRecoveryVolumeLightProfile, RARRecoveryVolumeHeavyProfile:
		return true
	default:
		return false
	}
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
	ID                 string          `json:"id"`
	WriterEra          string          `json:"writer_era"`
	GeneratorToolchain string          `json:"generator_toolchain"`
	RARFormat          RARFormat       `json:"rar_format"`
	Compressions       []Compression   `json:"compressions"`
	Solid              []bool          `json:"solid"`
	Encryptions        []Encryption    `json:"encryptions"`
	Payloads           []PayloadKind   `json:"payloads"`
	PayloadLayout      PayloadLayout   `json:"payload_layout,omitempty"`
	RepairProfiles     []RepairProfile `json:"repair_profiles,omitempty"`
	FileCount          int             `json:"file_count"`
	VolumeSize         string          `json:"volume_size"`
}

// ArchiveCase is one materialized archive fixture.
type ArchiveCase struct {
	ID                 string        `json:"id"`
	SetID              string        `json:"set_id"`
	WriterEra          string        `json:"writer_era"`
	GeneratorToolchain string        `json:"generator_toolchain"`
	RARFormat          RARFormat     `json:"rar_format"`
	Compression        Compression   `json:"compression"`
	Solid              bool          `json:"solid"`
	Encryption         Encryption    `json:"encryption"`
	Payload            PayloadKind   `json:"payload"`
	PayloadLayout      PayloadLayout `json:"payload_layout"`
	RepairProfile      RepairProfile `json:"repair_profile"`
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
	if m.SchemaVersion != 1 {
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
								RARFormat:          set.RARFormat,
								Compression:        compression,
								Solid:              solid,
								Encryption:         encryption,
								Payload:            payload,
								PayloadLayout:      layout,
								RepairProfile:      profile,
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
	if s.RARFormat != RAR3 && s.RARFormat != RAR4 && s.RARFormat != RAR5 {
		return fmt.Errorf("fixture set %q has unsupported rar_format %q", s.ID, s.RARFormat)
	}
	if len(s.Compressions) == 0 || len(s.Solid) == 0 || len(s.Encryptions) == 0 || len(s.Payloads) == 0 {
		return fmt.Errorf("fixture set %q must specify every matrix axis", s.ID)
	}
	for _, profile := range s.RepairProfiles {
		if !profile.Valid() {
			return fmt.Errorf("fixture set %q has unsupported repair_profile %q", s.ID, profile)
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
	args := []string{"a", "-idq", "-y", "-ep1"}
	switch c.RARFormat {
	case RAR3, RAR4:
		// The source-locked 3.x and 4.x writers predate the -ma selector;
		// their default archive format is the explicitly selected legacy lane.
	case RAR5:
		// Quick-open data speeds listing, but adds bytes to every upload and
		// is not needed by the download/extract clients under test.
		args = append(args, "-ma5", "-qo-")
	default:
		return nil, fmt.Errorf("fixture %q has unsupported rar_format %q", c.ID, c.RARFormat)
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
		switch c.RARFormat {
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

func (c ArchiveCase) RequiresPassword() bool {
	return c.Encryption != NoEncryption
}
