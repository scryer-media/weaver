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

func TestAdversarialPartialAcceptsOnlyPartial(t *testing.T) {
	tc := adversarialCase{Scenario: &Scenario{}, Spec: adversarialSpec{Group: "default", Outcome: "partial", MinOutputFiles: 1}}
	if err := tc.Spec.validate(); err != nil {
		t.Fatalf("partial spec refused: %v", err)
	}
	failed := adversarialSettled{JobID: 9, Status: "FAILED", Error: "no file was delivered whole"}
	if problems := checkAdversarialVerdict(tc, failed); len(problems) == 0 {
		t.Fatal("failed job accepted as partial")
	}
	complete := adversarialSettled{JobID: 9, Status: "COMPLETE", Outputs: []string{"show.mkv"}}
	if problems := checkAdversarialVerdict(tc, complete); len(problems) == 0 {
		t.Fatal("complete job accepted as partial")
	}
	partial := adversarialSettled{JobID: 9, Status: "PARTIAL", Outputs: []string{"show.mkv"}}
	if problems := checkAdversarialVerdict(tc, partial); len(problems) == 0 {
		t.Fatal("partial job without an output oracle accepted")
	}
	tc.Spec.FailureKind = "password"
	if err := tc.Spec.validate(); err == nil {
		t.Fatal("partial spec naming a failure accepted")
	}
}
