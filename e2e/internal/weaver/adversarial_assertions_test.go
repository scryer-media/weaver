// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAdversarialPartialRequiresExactUsableOutput(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
	t.Setenv("E2E_RUN_DIR", t.TempDir())
	if err := os.MkdirAll(localWeaverDir(), 0755); err != nil {
		t.Fatal(err)
	}
	db := openTestWeaverStateDB(t, localWeaverDBPath())
	defer db.Close()
	mustExecWeaverStateSQL(t, db, `CREATE TABLE job_history (job_id INTEGER PRIMARY KEY, output_dir TEXT)`)
	output := t.TempDir()
	if _, err := db.Exec(`INSERT INTO job_history VALUES (7, ?)`, output); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(output, "payload.bin")
	if err := os.WriteFile(path, []byte("intact payload"), 0600); err != nil {
		t.Fatal(err)
	}
	digest, err := fileBLAKE3(path)
	if err != nil {
		t.Fatal(err)
	}
	tc := adversarialCase{Scenario: &Scenario{ExpectedOutputBLAKE3: map[string]string{"payload.bin": digest}}, Spec: adversarialSpec{Outcome: "settles"}}
	settled := adversarialSettled{JobID: 7, Status: "PARTIAL", Outputs: []string{"payload.bin"}, Error: "recovery metadata rejected"}
	if problems := checkAdversarialVerdict(tc, settled); len(problems) != 0 {
		t.Fatalf("intact partial: %v", problems)
	}
	if err := os.WriteFile(path, []byte("corrupt payload"), 0600); err != nil {
		t.Fatal(err)
	}
	if problems := checkAdversarialVerdict(tc, settled); len(problems) == 0 {
		t.Fatal("corrupt partial accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if problems := checkAdversarialVerdict(tc, settled); len(problems) == 0 {
		t.Fatal("missing partial accepted")
	}
	tc.Scenario.ExpectedOutputBLAKE3 = nil
	if problems := checkAdversarialVerdict(tc, settled); len(problems) == 0 {
		t.Fatal("partial without output oracle accepted")
	}
}
