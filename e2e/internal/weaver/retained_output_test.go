// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/zeebo/blake3"
)

func TestRetainedOutputRequiresExactFailedPayloadAndNoScratch(t *testing.T) {
	for _, extra := range []string{"", "ember.bin.xz.001", ".weaver-chunks"} {
		t.Run(fmt.Sprintf("extra=%s", extra), func(t *testing.T) {
			dbPath := newTestWeaverStateDB(t)
			db := openTestWeaverStateDB(t, dbPath)
			defer db.Close()
			root := t.TempDir()
			payload := []byte("damaged compressed bytes")
			for name, data := range map[string][]byte{"ember.bin.xz": payload, ".weaver-job-dir": []byte("owner")} {
				if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if extra == ".weaver-chunks" {
				if err := os.Mkdir(filepath.Join(root, extra), 0700); err != nil {
					t.Fatal(err)
				}
			} else if extra != "" {
				if err := os.WriteFile(filepath.Join(root, extra), payload, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(`INSERT INTO job_history (job_id, status, output_dir) VALUES (17, 'FAILED', ?)`, root); err != nil {
				t.Fatal(err)
			}
			expected := map[string]string{"ember.bin.xz": fmt.Sprintf("%x", blake3.Sum256(payload))}
			err := assertRetainedOutputBLAKE3(dbPath, 17, expected)
			if (err != nil) != (extra != "") {
				t.Fatalf("extra=%q: %v", extra, err)
			}
			if extra == "" {
				if err := assertExactOutputBLAKE3(dbPath, 17, expected, false); err == nil {
					t.Fatal("delivered output accepted the ownership marker")
				}
				if err := os.Remove(filepath.Join(root, ".weaver-job-dir")); err != nil {
					t.Fatal(err)
				}
				if err := assertExactOutputBLAKE3(dbPath, 17, expected, false); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, ".weaver-output-dir"), []byte("owner"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := assertExactOutputBLAKE3(dbPath, 17, expected, false); err != nil {
					t.Fatalf("published output rejected its ownership marker: %v", err)
				}
				if err := assertRetainedOutputBLAKE3(dbPath, 17, expected); err == nil {
					t.Fatal("failed retained output accepted a publication marker")
				}
				if err := os.Remove(filepath.Join(root, ".weaver-output-dir")); err != nil {
					t.Fatal(err)
				}
				expected["ember.bin.xz"] = fmt.Sprintf("%x", blake3.Sum256([]byte("different bytes")))
				if err := assertRetainedOutputBLAKE3(dbPath, 17, expected); err == nil {
					t.Fatal("wrong retained bytes passed")
				}
			}
		})
	}
}
