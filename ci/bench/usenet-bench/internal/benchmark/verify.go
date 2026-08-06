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

// VerifyOutput accepts client-specific completion nesting and filename
// deobfuscation. It prefers an exact expected basename, then falls back to a
// unique byte-count and BLAKE3 match. An output file can satisfy only one
// expected member, so a flattened-name collision remains a verification
// failure rather than being hidden by content matching.
func VerifyOutput(fixtureDir, outputDir string) (OutputVerification, error) {
	manifest, err := fixture.LoadGeneratedManifest(filepath.Join(fixtureDir, "fixture-manifest.json"))
	if err != nil {
		return OutputVerification{}, err
	}
	actual, err := discoverFiles(outputDir)
	if err != nil {
		return OutputVerification{}, err
	}
	allCandidates := make([]discoveredFile, 0)
	for _, byName := range actual {
		allCandidates = append(allCandidates, byName...)
	}
	sort.Slice(allCandidates, func(i, j int) bool { return allCandidates[i].path < allCandidates[j].path })
	result := OutputVerification{FixtureID: manifest.Case.ID, Files: make([]VerifiedOutputFile, 0, len(manifest.ExpectedFiles))}
	used := make(map[string]bool, len(manifest.ExpectedFiles))
	digests := make(map[string]string)
	for _, expected := range manifest.ExpectedFiles {
		verified, err := verifyExpectedFile(expected, actual[filepath.Base(expected.Path)], used, digests, outputDir)
		if err != nil {
			return OutputVerification{}, err
		}
		if verified == nil {
			verified, err = verifyExpectedFile(expected, allCandidates, used, digests, outputDir)
			if err != nil {
				return OutputVerification{}, err
			}
		}
		if verified == nil {
			return OutputVerification{}, fmt.Errorf("no unused output file matching %s passed size and BLAKE3 verification", expected.Path)
		}
		used[filepath.Clean(filepath.Join(outputDir, filepath.FromSlash(verified.ActualPath)))] = true
		result.Files = append(result.Files, *verified)
	}
	return result, nil
}

func verifyExpectedFile(expected fixture.FileDigest, candidates []discoveredFile, used map[string]bool, digests map[string]string, outputDir string) (*VerifiedOutputFile, error) {
	for _, candidate := range candidates {
		if used[candidate.path] || candidate.size != expected.Size {
			continue
		}
		digest, ok := digests[candidate.path]
		if !ok {
			var err error
			digest, err = hashFile(candidate.path)
			if err != nil {
				return nil, err
			}
			digests[candidate.path] = digest
		}
		if digest != expected.BLAKE3 {
			continue
		}
		candidatePath, err := filepath.Rel(outputDir, candidate.path)
		if err != nil {
			return nil, err
		}
		return &VerifiedOutputFile{
			ExpectedPath: expected.Path,
			ActualPath:   filepath.ToSlash(candidatePath),
			Size:         expected.Size,
			BLAKE3:       digest,
		}, nil
	}
	return nil, nil
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
