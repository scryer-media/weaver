// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNamespaceBootstrapKeepsRecoveryIndexesAvailable(t *testing.T) {
	for _, indexName := range []string{"set.par2", "set.par3"} {
		sentinel, allowed, err := namespaceBootstrapFiles([]string{
			`[1/4] "posted.bin" yEnc`,
			`[2/4] "` + indexName + `" yEnc`,
			`[3/4] "00-keep.bin" yEnc`,
			`[4/4] "set.vol00+04.par2" yEnc`,
		})
		if err != nil || sentinel != 2 || !reflect.DeepEqual(allowed, map[int]bool{1: true, 2: true}) {
			t.Fatalf("%s: sentinel=%d allowed=%v err=%v", indexName, sentinel, allowed, err)
		}
	}
	for _, subjects := range [][]string{
		{`"00-keep.bin"`, `"set.vol00+04.par2"`},
		{`"set.par2"`, `"posted.bin"`},
		{`"00-keep.bin"`, `"00-keep.bin"`, `"set.par2"`},
		{`"00-keep.bin"`, `"set.par2"`, `"set.par3"`},
	} {
		if _, _, err := namespaceBootstrapFiles(subjects); err == nil {
			t.Fatalf("accepted ambiguous bootstrap subjects %v", subjects)
		}
	}
}

func TestNamespaceMoveNeedsRecordedOwnershipAndMovedBytes(t *testing.T) {
	for _, test := range []struct {
		name       string
		hasFile    bool
		onDisk     any
		oldPresent bool
		newPresent bool
		wrongBytes bool
		accept     bool
	}{
		{name: "committed move", hasFile: true, onDisk: "posted.bin", newPresent: true, accept: true},
		{name: "unrecorded file", onDisk: "posted.bin", newPresent: true},
		{name: "no move intent", hasFile: true, newPresent: true},
		{name: "before move", hasFile: true, onDisk: "posted.bin", oldPresent: true, newPresent: true},
		{name: "missing destination", hasFile: true, onDisk: "posted.bin"},
		{name: "wrong destination bytes", hasFile: true, onDisk: "posted.bin", newPresent: true, wrongBytes: true},
		{name: "escaping path", hasFile: true, onDisk: "../posted.bin", newPresent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
			root := t.TempDir()
			dbPath := filepath.Join(root, "state.db")
			db := openTestWeaverStateDB(t, dbPath)
			defer db.Close()
			mustExecWeaverStateSQL(t, db, `CREATE TABLE active_jobs (job_id INTEGER, output_dir TEXT)`)
			mustExecWeaverStateSQL(t, db, `CREATE TABLE graph_members (job_id INTEGER, node_id INTEGER, member_id INTEGER, has_file BOOLEAN, on_disk TEXT, output_name TEXT)`)
			if _, err := db.Exec(`INSERT INTO active_jobs VALUES (7, ?)`, root); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO graph_members VALUES (7, 0, 2, ?, ?, 'a.bin')`, test.hasFile, test.onDisk); err != nil {
				t.Fatal(err)
			}
			// Unrecorded matching bytes elsewhere never substitute for the
			// recorded desired location, even when both recorded paths are lost.
			unrecorded := filepath.Join(root, "unrecorded.bin")
			if err := os.WriteFile(unrecorded, []byte("owned bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			expected, err := fileBLAKE3(unrecorded)
			if err != nil {
				t.Fatal(err)
			}
			if test.oldPresent {
				if err := os.WriteFile(filepath.Join(root, "posted.bin"), []byte("owned bytes"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if test.newPresent {
				bytes := []byte("owned bytes")
				if test.wrongBytes {
					bytes = []byte("other bytes")
				}
				if err := os.WriteFile(filepath.Join(root, "a.bin"), bytes, 0600); err != nil {
					t.Fatal(err)
				}
			}
			move, err := readNamespaceMove(dbPath, 7, "a.bin", expected)
			if (err == nil) != test.accept {
				t.Fatalf("move=%+v error=%v accepted=%t", move, err, test.accept)
			}
		})
	}
}

func TestNamespaceChainFindsEitherCommittedFirstMove(t *testing.T) {
	for _, moved := range []struct{ wanted, identity string }{
		{"lantern.bin", "lantern.bin"},
		{"beacon (1).bin", "beacon.bin"},
	} {
		t.Run(moved.wanted, func(t *testing.T) {
			t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
			root := t.TempDir()
			dbPath := filepath.Join(root, "state.db")
			db := openTestWeaverStateDB(t, dbPath)
			defer db.Close()
			mustExecWeaverStateSQL(t, db, `CREATE TABLE active_jobs (job_id INTEGER, output_dir TEXT)`)
			mustExecWeaverStateSQL(t, db, `CREATE TABLE graph_members (job_id INTEGER, node_id INTEGER, member_id INTEGER, has_file BOOLEAN, on_disk TEXT, output_name TEXT)`)
			if _, err := db.Exec(`INSERT INTO active_jobs VALUES (7, ?)`, root); err != nil {
				t.Fatal(err)
			}
			expected := map[string]string{}
			for _, identity := range []string{"beacon.bin", "lantern.bin"} {
				path := filepath.Join(root, "unrecorded-"+identity)
				if err := os.WriteFile(path, []byte(identity), 0600); err != nil {
					t.Fatal(err)
				}
				digest, err := fileBLAKE3(path)
				if err != nil {
					t.Fatal(err)
				}
				expected[identity] = digest
			}
			if _, err := db.Exec(`INSERT INTO graph_members VALUES (7, 0, 2, TRUE, 'old.bin', ?)`, moved.wanted); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, moved.wanted), []byte(moved.identity), 0600); err != nil {
				t.Fatal(err)
			}
			move, identity, err := readNamespaceChainMove(dbPath, 7, expected)
			if err != nil || identity != moved.identity || move.Wanted != moved.wanted {
				t.Fatalf("move=%+v identity=%q error=%v", move, identity, err)
			}
		})
	}
}
