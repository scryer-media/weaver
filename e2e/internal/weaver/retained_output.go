// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"io/fs"
	"path/filepath"
)

// Retained damaged files are evidence of a failed job, never usable output.
// Check their complete file set as well as their bytes: leaving split pieces
// or graph scratch beside the expected joined stream must fail this oracle.
func assertRetainedOutputBLAKE3(dbPath string, jobID int, expected map[string]string) error {
	return assertExactOutputBLAKE3(dbPath, jobID, expected, true)
}

func assertExactOutputBLAKE3(dbPath string, jobID int, expected map[string]string, allowMarker bool) error {
	root, err := loadJobOutputDir(dbPath, jobID)
	if err != nil {
		return err
	}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			if !entry.IsDir() {
				return fmt.Errorf("job %d output root is not a directory", jobID)
			}
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.Name() == ".weaver-chunks" {
			return fmt.Errorf("job %d retained graph scratch %q", jobID, relative)
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("job %d retained a non-regular file %q", jobID, relative)
		}
		marker := ".weaver-output-dir"
		if allowMarker {
			marker = ".weaver-job-dir"
		}
		if relative == marker {
			return nil
		}
		if _, wanted := expected[relative]; !wanted {
			return fmt.Errorf("job %d retained unexpected file %q", jobID, relative)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return assertOutputBLAKE3(dbPath, jobID, expected)
}
