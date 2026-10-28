// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAdmissionTraceAcceptsACapHeldInOrder(t *testing.T) {
	trace := newAdmissionTrace(2, []int{1, 2, 3, 4})
	trace.observe(0, []int{1, 2}, nil)
	trace.observe(1, []int{2, 3}, []int{1})
	trace.observe(2, []int{4}, []int{1, 2, 3})
	if err := trace.verdict(); err != nil {
		t.Fatalf("verdict = %v, want nil", err)
	}
}

func TestAdmissionTraceCountsAJobFinishedBetweenSamplesAsAdmitted(t *testing.T) {
	trace := newAdmissionTrace(1, []int{1, 2})
	trace.observe(0, []int{1}, nil)
	trace.observe(1, nil, []int{1, 2})
	if err := trace.verdict(); err != nil {
		t.Fatalf("verdict = %v, want nil", err)
	}
}

func TestAdmissionTraceFlagsASampleOverTheCap(t *testing.T) {
	trace := newAdmissionTrace(2, []int{1, 2, 3})
	trace.observe(0, []int{3, 1, 2}, nil)
	err := trace.verdict()
	if err == nil || !strings.Contains(err.Error(), "sample 0 admitted [1 2 3]") {
		t.Fatalf("verdict = %v, want the over-cap sample named", err)
	}
}

func TestAdmissionTraceFlagsAJobAdmittedAheadOfItsTurn(t *testing.T) {
	trace := newAdmissionTrace(1, []int{1, 2})
	trace.observe(0, []int{2}, nil)
	trace.observe(1, []int{1}, []int{2})
	err := trace.verdict()
	if err == nil || !strings.Contains(err.Error(), "before the job ahead of it") {
		t.Fatalf("verdict = %v, want the order broken", err)
	}
}

func TestAdmissionTraceFlagsAJobNeverAdmitted(t *testing.T) {
	trace := newAdmissionTrace(2, []int{1, 2})
	trace.observe(0, []int{1}, nil)
	err := trace.verdict()
	if err == nil || !strings.Contains(err.Error(), "job 2 was never admitted") {
		t.Fatalf("verdict = %v, want job 2 named", err)
	}
}

func TestSchemaRewindPlanTouchesOnlyWhatIsThere(t *testing.T) {
	rewind := schemaRewind{
		DropTables:    []string{"graph_nodes", "graph_gone"},
		DropColumns:   []tableColumn{{Table: "active_jobs", Column: "engine"}, {Table: "active_jobs", Column: "absent"}},
		RestoreTables: []tableDDL{{Name: "active_volume_status", DDL: "CREATE TABLE active_volume_status (job_id INTEGER)"}, {Name: "kept", DDL: "CREATE TABLE kept (x)"}},
		AddColumns:    []columnAdd{{Table: "job_history", Column: "engine", Definition: "TEXT"}, {Table: "job_history", Column: "name", Definition: "TEXT"}},
		LedgerFrom:    66,
	}
	shape := schemaShape{
		Tables: map[string]bool{"graph_nodes": true, "kept": true, "active_jobs": true, "job_history": true},
		Columns: map[string]map[string]bool{
			"active_jobs": {"engine": true, "job_id": true},
			"job_history": {"name": true},
		},
		Indexes: map[string]map[string]string{
			"active_jobs": {
				"idx_b_engine": "CREATE INDEX idx_b_engine ON active_jobs (engine)",
				"idx_a_engine": "CREATE INDEX idx_a_engine ON active_jobs (status, engine)",
				"idx_status":   "CREATE INDEX idx_status ON active_jobs (status)",
			},
		},
	}
	want := []string{
		"DROP INDEX IF EXISTS idx_a_engine",
		"DROP INDEX IF EXISTS idx_b_engine",
		"ALTER TABLE active_jobs DROP COLUMN engine",
		"DROP TABLE graph_nodes",
		"CREATE TABLE active_volume_status (job_id INTEGER)",
		"ALTER TABLE job_history ADD COLUMN engine TEXT",
		"DELETE FROM _sqlx_migrations WHERE version >= 66",
	}
	if got := rewind.plan(shape); !reflect.DeepEqual(got, want) {
		t.Fatalf("plan =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestEngineStampedRewindAllowsLaterGraphMigrationsToReplay(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
	dbPath := filepath.Join(t.TempDir(), "state.db")
	if err := execAll(dbPath, []string{
		`CREATE TABLE _sqlx_migrations (version INTEGER PRIMARY KEY, success BOOLEAN NOT NULL)`,
		`INSERT INTO _sqlx_migrations VALUES (73, 1), (74, 1), (75, 1), (76, 1), (77, 1), (78, 1)`,
		`CREATE TABLE active_jobs (job_id INTEGER PRIMARY KEY)`,
		`CREATE TABLE job_history (job_id INTEGER PRIMARY KEY)`,
		`CREATE TABLE graph_members (job_id INTEGER, member_id INTEGER)`,
		`CREATE TABLE graph_envelopes (job_id INTEGER, node_id INTEGER)`,
	}); err != nil {
		t.Fatal(err)
	}
	var migrations []string
	for _, path := range []string{"0075_graph_member_crc32/sqlite.sql", "0076_graph_held_parts/sqlite.sql", "0077_graph_file_on_disk/sqlite.sql", "0078_feed_verdict/schema.sql"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "server", "crates", "weaver-server-core", "src", "db", "migrations", path))
		if err != nil {
			t.Fatal(err)
		}
		migrations = append(migrations, string(data))
	}
	if err := execAll(dbPath, migrations); err != nil {
		t.Fatal(err)
	}
	if err := applySchemaRewind(dbPath, rewindToPreGraphEngine, nil); err != nil {
		t.Fatal(err)
	}
	if err := execAll(dbPath, migrations); err != nil {
		t.Fatalf("the historical schema must accept the original migrations again: %v", err)
	}
	versions, err := migrationVersions(dbPath)
	if err != nil || !reflect.DeepEqual(versions, map[int]bool{73: true}) {
		t.Fatalf("historical ledger = %v, error = %v", versions, err)
	}
}

// The rewind and the history clone run against a real SQLite file shaped
// like the parts of the state database they touch.
func TestSchemaRewindAndHistoryCloneApplyToSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	setup := []string{
		`CREATE TABLE _sqlx_migrations (version INTEGER PRIMARY KEY, success BOOLEAN NOT NULL)`,
		`INSERT INTO _sqlx_migrations VALUES (65, 1), (66, 1), (73, 1)`,
		`CREATE TABLE active_jobs (job_id INTEGER PRIMARY KEY, status TEXT, engine TEXT NOT NULL DEFAULT 'classic', nzb_admitted INTEGER NOT NULL DEFAULT 0 CHECK (nzb_admitted IN (0, 1)))`,
		`CREATE INDEX idx_active_engine ON active_jobs (engine)`,
		`CREATE TABLE job_history (job_id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, engine TEXT NOT NULL DEFAULT 'classic')`,
		`CREATE TABLE graph_nodes (job_id INTEGER)`,
		`INSERT INTO active_jobs (job_id, status) VALUES (7, 'downloading')`,
		`INSERT INTO job_history (job_id, name) VALUES (3, 'done')`,
	}
	if err := execAll(dbPath, setup); err != nil {
		t.Fatal(err)
	}
	if err := execAll(dbPath, cloneHistoryRow(3, 100003)); err != nil {
		t.Fatal(err)
	}
	rewind := schemaRewind{
		DropTables:    []string{"graph_nodes"},
		DropColumns:   []tableColumn{{Table: "active_jobs", Column: "engine"}, {Table: "active_jobs", Column: "nzb_admitted"}, {Table: "job_history", Column: "engine"}},
		RestoreTables: []tableDDL{{Name: "active_volume_status", DDL: `CREATE TABLE active_volume_status (job_id INTEGER, volume INTEGER, set_name TEXT)`}},
		LedgerFrom:    66,
	}
	plant := []string{`INSERT INTO active_volume_status VALUES (7, 0, 'e2e-planted')`}
	if err := applySchemaRewind(dbPath, rewind, plant); err != nil {
		t.Fatal(err)
	}
	// A second pass finds nothing left to drop or restore.
	if err := applySchemaRewind(dbPath, rewind, nil); err != nil {
		t.Fatalf("second rewind: %v", err)
	}
	versions, err := migrationVersions(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(versions, map[int]bool{65: true}) {
		t.Fatalf("ledger = %v, want only 65", versions)
	}
	err = withStateDB(dbPath, func(db *stateConn) error {
		tables, err := existingTables(db)
		if err != nil {
			return err
		}
		if tables["graph_nodes"] || !tables["active_volume_status"] {
			t.Fatalf("tables = %v", tables)
		}
		var names string
		if err := db.QueryRow(`SELECT group_concat(name, ',') FROM (SELECT name FROM pragma_table_info('active_jobs') ORDER BY cid)`).Scan(&names); err != nil {
			return err
		}
		if names != "job_id,status" {
			t.Fatalf("active_jobs columns = %s", names)
		}
		var histories int
		if err := db.QueryRow(`SELECT COUNT(*) FROM job_history WHERE name = 'done' AND job_id IN (3, 100003)`).Scan(&histories); err != nil {
			return err
		}
		if histories != 2 {
			t.Fatalf("history rows = %d, want the row and its clone", histories)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Every classic-only table the rewinds put back is valid SQLite, and the
// planted rows fit the tables they go into.
func TestClassicOnlyTablesAndPlantsApplyToSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	var statements []string
	for _, table := range classicOnlyTables {
		statements = append(statements, table.DDL)
	}
	statements = append(statements, plantClassicRows(7)...)
	if err := execAll(dbPath, statements); err != nil {
		t.Fatal(err)
	}
	left, err := plantedRowsLeft(dbPath, []int{7})
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range plantedClassicTables {
		if left[table] == 0 {
			t.Fatalf("%s holds no planted row: %v", table, left)
		}
	}
}

// The rewinds put back exactly the tables migration 74 drops.
func TestClassicOnlyTablesAreWhatTheGraphEngineMigrationDrops(t *testing.T) {
	want := []string{
		"active_par2", "active_par2_files", "active_failed_extractions",
		"active_extraction_chunks", "active_archive_headers", "active_rar_volume_facts",
		"active_volume_status", "active_rar_verified_suspect", "active_direct_coverage",
		"active_repair_outputs",
	}
	var got []string
	for _, table := range classicOnlyTables {
		got = append(got, table.Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("classicOnlyTables = %v, want %v", got, want)
	}
}

// After migration 74 the post-case sweeps read the active-state tables the
// database still has, and still find what lingers in them.
func TestActiveStateSweepsSkipTablesTheUpgradeDropped(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	var drops []string
	for _, table := range classicOnlyTables {
		drops = append(drops, `DROP TABLE IF EXISTS `+table.Name)
	}
	if err := execAll(dbPath, drops); err != nil {
		t.Fatal(err)
	}
	if err := assertNoLingeringWeaverActiveState(dbPath); err != nil {
		t.Fatalf("clean database after the drop: %v", err)
	}
	if err := assertNoOrphanActiveStatePath(dbPath); err != nil {
		t.Fatalf("orphan sweep after the drop: %v", err)
	}
	if err := execAll(dbPath, []string{
		`INSERT INTO job_history (job_id, status) VALUES (9, 'complete')`,
		`INSERT INTO active_extracted (job_id, member_name, output_path) VALUES (9, 'a.mkv', '/x/a.mkv')`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := assertNoLingeringWeaverActiveState(dbPath); err == nil || !strings.Contains(err.Error(), "active_extracted=1") {
		t.Fatalf("lingering sweep = %v, want the remaining table named", err)
	}
	if err := assertNoOrphanActiveStatePath(dbPath); err == nil || !strings.Contains(err.Error(), "active_extracted=1 (jobs 9)") {
		t.Fatalf("orphan sweep = %v, want the orphan named", err)
	}
	if err := assertTerminalFixtureStateSweeping(dbPath, 9, "complete", false); err == nil || !strings.Contains(err.Error(), "active_extracted=1") {
		t.Fatalf("job sweep = %v, want the lingering row named", err)
	}
}

// On Postgres the classic-only tables come back in its own dialect, and on
// SQLite exactly as SQLite created them.
func TestClassicOnlyTablesTranslateToPostgres(t *testing.T) {
	for _, table := range classicOnlyTables {
		if got := table.in(weaverDatastoreSQLite); got != table.DDL {
			t.Fatalf("%s on SQLite = %s", table.Name, got)
		}
		ddl := table.in(weaverDatastorePostgres)
		for _, sqliteOnly := range []string{"WITHOUT ROWID", "INTEGER", "BLOB"} {
			if strings.Contains(ddl, sqliteOnly) {
				t.Fatalf("%s on Postgres keeps %s: %s", table.Name, sqliteOnly, ddl)
			}
		}
		if !strings.HasPrefix(ddl, "CREATE TABLE "+table.Name+" (") {
			t.Fatalf("%s on Postgres = %s", table.Name, ddl)
		}
	}
}

// The SQLite refusal makes the database refuse exactly the named job's
// history row, and lifting it lets that row land.
func TestRefuseArchiveRefusesOnlyTheNamedJobOnSQLite(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
	dbPath := filepath.Join(t.TempDir(), "state.db")
	refuse, lift := refuseArchive(7)
	if err := execAll(dbPath, append([]string{`CREATE TABLE job_history (job_id INTEGER PRIMARY KEY)`}, refuse...)); err != nil {
		t.Fatal(err)
	}
	if err := execAll(dbPath, []string{`INSERT INTO job_history VALUES (8)`}); err != nil {
		t.Fatalf("another job's row was refused: %v", err)
	}
	if err := execAll(dbPath, []string{`INSERT INTO job_history VALUES (7)`}); err == nil || !strings.Contains(err.Error(), "e2e injected archive failure") {
		t.Fatalf("the named job's row = %v, want the injected refusal", err)
	}
	if err := execAll(dbPath, append(lift, `INSERT INTO job_history VALUES (7)`)); err != nil {
		t.Fatalf("after the lift: %v", err)
	}
}
