package benchmark

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
	"github.com/zeebo/blake3"
)

type OutputVerification struct {
	FixtureID string               `json:"fixture_id"`
	Files     []VerifiedOutputFile `json:"files"`
}

type VerifiedOutputFile struct {
	ExpectedPath string `json:"expected_path"`
	ActualPath   string `json:"actual_path"`
	Size         int64  `json:"size"`
	BLAKE3       string `json:"blake3"`
}

// VerifyOutput accepts client-specific completion nesting but requires the
// exact expected basename, byte count, and SHA-256 for every generated file.
func VerifyOutput(fixtureDir, outputDir string) (OutputVerification, error) {
	manifest, err := fixture.LoadGeneratedManifest(filepath.Join(fixtureDir, "fixture-manifest.json"))
	if err != nil {
		return OutputVerification{}, err
	}
	actual, err := discoverFiles(outputDir)
	if err != nil {
		return OutputVerification{}, err
	}
	result := OutputVerification{FixtureID: manifest.Case.ID, Files: make([]VerifiedOutputFile, 0, len(manifest.ExpectedFiles))}
	for _, expected := range manifest.ExpectedFiles {
		candidates := actual[filepath.Base(expected.Path)]
		if len(candidates) == 0 {
			return OutputVerification{}, fmt.Errorf("missing expected output file %s", expected.Path)
		}
		var verified *VerifiedOutputFile
		for _, candidate := range candidates {
			if candidate.size != expected.Size {
				continue
			}
			digest, err := hashFile(candidate.path)
			if err != nil {
				return OutputVerification{}, err
			}
			if digest == expected.BLAKE3 {
				candidatePath, err := filepath.Rel(outputDir, candidate.path)
				if err != nil {
					return OutputVerification{}, err
				}
				verified = &VerifiedOutputFile{
					ExpectedPath: expected.Path,
					ActualPath:   filepath.ToSlash(candidatePath),
					Size:         expected.Size,
					BLAKE3:       digest,
				}
				break
			}
		}
		if verified == nil {
			return OutputVerification{}, fmt.Errorf("no output file matching %s passed size and SHA-256 verification", expected.Path)
		}
		result.Files = append(result.Files, *verified)
	}
	return result, nil
}

// DeleteOutputFiles removes completed download contents while retaining the
// output root itself. A live Docker bind mount continues to reference that
// root, so removing the root directory would make subsequent fixture cleanup
// depend on container-specific mount behaviour.
func DeleteOutputFiles(outputDir string) error {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return fmt.Errorf("read client output %s: %w", outputDir, err)
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(outputDir, entry.Name())); err != nil {
			return fmt.Errorf("delete client output %s: %w", entry.Name(), err)
		}
	}
	return nil
}

type discoveredFile struct {
	path string
	size int64
}

func discoverFiles(root string) (map[string][]discoveredFile, error) {
	files := map[string][]discoveredFile{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		files[entry.Name()] = append(files[entry.Name()], discoveredFile{path: path, size: info.Size()})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan client output %s: %w", root, err)
	}
	for _, candidates := range files {
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].path < candidates[j].path })
	}
	return files, nil
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := blake3.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
