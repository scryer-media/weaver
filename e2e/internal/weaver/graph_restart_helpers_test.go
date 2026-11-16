// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
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

// testMigrationsDir is the migration folder of the weaver this harness
// lives in.
var testMigrationsDir = filepath.Join("..", "..", "..", "server", "crates", "weaver-server-core", "src", "db", "migrations")

// What the rewind drops is read from the migrations after the graph-engine
// one, so a migration that adds a table or a column needs no line here.
func TestLaterMigrationsNameWhatTheGraphEngineRewindDrops(t *testing.T) {
	later, err := readLaterMigrations(testMigrationsDir, graphEngineMigration)
	if err != nil {
		t.Fatal(err)
	}
	wantColumns := []tableColumn{
		{"graph_members", "crc32"},
		{"graph_members", "on_disk"},
		{"graph_envelopes", "file"},
		{"active_jobs", "feed_verdict"},
		{"graph_members", "refuted_kind"},
	}
	for _, column := range wantColumns {
		found := false
		for _, got := range later.Columns {
			found = found || got == column
		}
		if !found {
			t.Fatalf("later columns = %v, missing %v", later.Columns, column)
		}
	}
	heldParts := false
	for _, table := range later.Tables {
		heldParts = heldParts || table == "graph_held_parts"
	}
	if !heldParts {
		t.Fatalf("later tables = %v, want graph_held_parts among them", later.Tables)
	}
}

// A later migration whose step the rewind cannot undo is named, not skipped.
func TestLaterMigrationsRefuseAStepTheRewindCannotUndo(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	if err := os.MkdirAll(filepath.Join(dir, "0002_b"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `format_version = 1

[[migration]]
version = 1
description = "a"

[[migration.steps]]
kind = "rust"
hook_id = "a"

[[migration]]
version = 2
description = "b"

[[migration.steps]]
kind = "sql"
file = "migrations/0002_b/schema.sql"
scope = "all"
`
	if err := os.WriteFile(filepath.Join(dir, "manifest.toml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	sql := "-- a comment; with a semicolon\nALTER TABLE t ADD COLUMN c TEXT;\nUPDATE t SET c = 'x';\n"
	if err := os.WriteFile(filepath.Join(dir, "0002_b", "schema.sql"), []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	if latest, err := latestMigration(dir); err != nil || latest != 2 {
		t.Fatalf("latest = %d, %v; want 2", latest, err)
	}
	if _, err := readLaterMigrations(dir, 0); err == nil || !strings.Contains(err.Error(), `migration 1 has a "rust" step (hook "a")`) {
		t.Fatalf("from 0 = %v, want the rust step and its hook named", err)
	}
	if _, err := readLaterMigrations(dir, 1); err == nil || !strings.Contains(err.Error(), "UPDATE t SET c = 'x'") {
		t.Fatalf("from 1 = %v, want the update named", err)
	}
}

// writeSQLMigrations writes a manifest of one SQL step per migration, in
// version order, and returns its folder.
func writeSQLMigrations(t *testing.T, versions []int, sql map[int]string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	manifest := "format_version = 1\n"
	for _, version := range versions {
		folder := fmt.Sprintf("%04d_m", version)
		if err := os.MkdirAll(filepath.Join(dir, folder), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, folder, "schema.sql"), []byte(sql[version]), 0o644); err != nil {
			t.Fatal(err)
		}
		manifest += fmt.Sprintf("\n[[migration]]\nversion = %d\ndescription = \"m\"\n\n[[migration.steps]]\nkind = \"sql\"\nfile = \"migrations/%s/schema.sql\"\nscope = \"all\"\n", version, folder)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.toml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// A write to a table that was there before is passed only when a reverse
// lists that very statement; another write to the same table is named.
func TestLaterMigrationsPassOnlyTheWritesAReverseLists(t *testing.T) {
	listed := "UPDATE egress_interfaces\n   SET download_bps = NULLIF(max_download_speed, 0),\n       quota_download_bytes = download_quota_limit_bytes,\n       quota_download_enabled = download_quota_enabled;\n"
	dir := writeSQLMigrations(t, []int{93, 94}, map[int]string{94: listed})
	later, err := readLaterMigrations(dir, 93)
	if err != nil || !reflect.DeepEqual(later.Hooks, []string{"egress_bandwidth_columns_v94"}) {
		t.Fatalf("listed write = %+v, %v; want its reverse", later, err)
	}
	dir = writeSQLMigrations(t, []int{93, 94}, map[int]string{94: listed + "UPDATE egress_interfaces SET upload_bps = 1;\n"})
	if _, err := readLaterMigrations(dir, 93); err == nil || !strings.Contains(err.Error(), "UPDATE egress_interfaces SET upload_bps = 1") {
		t.Fatalf("unlisted write = %v, want it named", err)
	}
}

// A later migration that makes, IF NOT EXISTS, a table, index or column an
// earlier one left in place is named: the rewind would drop it from the older
// build. One an earlier migration dropped again is the later one's own.
func TestLaterMigrationsRefuseRemakingWhatAnEarlierOneMade(t *testing.T) {
	earlier := "CREATE TABLE t (id INTEGER PRIMARY KEY);\nCREATE INDEX idx_t ON t (id);\nALTER TABLE t ADD COLUMN c TEXT;\nCREATE TABLE gone (id INTEGER);\nDROP TABLE gone;\nCREATE TABLE old_name (id INTEGER);\nALTER TABLE old_name RENAME TO kept;\n"
	for _, tc := range []struct{ later, refusal string }{
		{"CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY);", "creates table t"},
		{"CREATE INDEX IF NOT EXISTS idx_t ON t (id);", "creates index idx_t"},
		{"ALTER TABLE t ADD COLUMN IF NOT EXISTS c TEXT;", "creates column t.c"},
		{"CREATE TABLE IF NOT EXISTS kept (id INTEGER);", "creates table kept"},
		{"CREATE TABLE IF NOT EXISTS gone (id INTEGER);", ""},
		{"CREATE TABLE IF NOT EXISTS old_name (id INTEGER);", ""},
	} {
		dir := writeSQLMigrations(t, []int{1, 2}, map[int]string{1: earlier, 2: tc.later})
		_, err := readLaterMigrations(dir, 1)
		if tc.refusal == "" {
			if err != nil {
				t.Fatalf("%s: %v, want it read", tc.later, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), "migration 2 "+tc.refusal) {
			t.Fatalf("%s: %v, want %q", tc.later, err, tc.refusal)
		}
	}
}

// A created index and a Rust step with a named reverse are steps the rewind
// can undo, and are reported.
func TestLaterMigrationsReportIndexesAndReversibleRustSteps(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	if err := os.MkdirAll(filepath.Join(dir, "0002_b"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `format_version = 1

[[migration]]
version = 1
description = "a"

[[migration.steps]]
kind = "rust"
hook_id = "move_nzbs_and_outlines_v85"
scope = "all"

[[migration]]
version = 2
description = "b"

[[migration.steps]]
kind = "sql"
file = "migrations/0002_b/schema.sql"
scope = "all"
`
	if err := os.WriteFile(filepath.Join(dir, "manifest.toml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	sql := "CREATE INDEX IF NOT EXISTS idx_a\n    ON t (a DESC);\nCREATE UNIQUE INDEX idx_b ON t (b);\n"
	if err := os.WriteFile(filepath.Join(dir, "0002_b", "schema.sql"), []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	later, err := readLaterMigrations(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := laterMigrations{Indexes: []string{"idx_a", "idx_b"}, Hooks: []string{"move_nzbs_and_outlines_v85"}}
	if !reflect.DeepEqual(later, want) {
		t.Fatalf("later = %+v, want %+v", later, want)
	}
}

// The checked-in manifest reads whole: every step after the graph-engine
// migration is one the rewind can undo.
func TestLaterMigrationsReadTheCheckedInManifest(t *testing.T) {
	later, err := readLaterMigrations(testMigrationsDir, graphEngineMigration)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []string{"idx_job_history_size_job", "idx_job_history_name_job"} {
		if !contains(later.Indexes, index) {
			t.Fatalf("later indexes = %v, missing %s", later.Indexes, index)
		}
	}
	for _, table := range []string{"active_job_nzb_files", "job_nzbs", "active_job_outlines"} {
		if !contains(later.Tables, table) {
			t.Fatalf("later tables = %v, missing %s", later.Tables, table)
		}
	}
	if !contains(later.Columns, tableColumn{"active_jobs", "staging_dir"}) {
		t.Fatalf("later columns = %v, missing active_jobs.staging_dir", later.Columns)
	}
	if !contains(later.Hooks, "move_nzbs_and_outlines_v85") {
		t.Fatalf("later hooks = %v, want the NZB move", later.Hooks)
	}
	// Every Rust step a rewind replays has a reverse, so a new hook fails
	// here and not in the restart stage.
	entries, err := readMigrationManifest(testMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Version <= graphEngineMigration {
			continue
		}
		for _, step := range entry.Steps {
			if step.Kind != "rust" {
				continue
			}
			if _, known := rustStepReverses[step.HookID]; !known {
				t.Fatalf("migration %d: Rust hook %q has no reverse in rustStepReverses", entry.Version, step.HookID)
			}
			if !contains(later.Hooks, step.HookID) {
				t.Fatalf("later hooks = %v, missing %q of migration %d", later.Hooks, step.HookID, entry.Version)
			}
		}
	}
}

// The reverse of migration 92 passes a list of 50 servers and refuses one
// past the limit before the rewind changes anything.
func TestServerTrimReverseRefusesAListPastTheLimit(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
	rewind := schemaRewind{Reverses: []string{"trim_servers_to_limit_v92"}, LedgerFrom: 92}
	for _, count := range []int{serverLimitV92, serverLimitV92 + 1} {
		dbPath := filepath.Join(t.TempDir(), "state.db")
		setup := []string{
			`CREATE TABLE _sqlx_migrations (version INTEGER PRIMARY KEY, success BOOLEAN NOT NULL)`,
			`INSERT INTO _sqlx_migrations VALUES (91, 1), (92, 1)`,
			`CREATE TABLE servers (id INTEGER PRIMARY KEY NOT NULL)`,
			fmt.Sprintf(`WITH RECURSIVE n(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM n WHERE id < %d) INSERT INTO servers (id) SELECT id FROM n`, count),
		}
		if err := execAll(dbPath, setup); err != nil {
			t.Fatal(err)
		}
		err := applySchemaRewind(dbPath, rewind, nil)
		versions, readErr := migrationVersions(dbPath)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if count <= serverLimitV92 {
			if err != nil || !reflect.DeepEqual(versions, map[int]bool{91: true}) {
				t.Fatalf("%d servers: rewind = %v, ledger %v; want it rewound to 91", count, err, versions)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), "stores 51 servers") {
			t.Fatalf("%d servers: rewind = %v, want it refused", count, err)
		}
		if !reflect.DeepEqual(versions, map[int]bool{91: true, 92: true}) {
			t.Fatalf("%d servers: ledger %v, want it untouched", count, versions)
		}
	}
}

// The reverses of migrations 94 and 124 refuse what an older build cannot
// hold before the rewind changes anything, and pass a database without it.
func TestBandwidthAndDNSReversesRefuseWhatAnOlderBuildCannotHold(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
	rewind := schemaRewind{Reverses: []string{"move_bandwidth_to_networking_v94", "egress_bandwidth_columns_v94", "dns_profiles_from_proxies_v124"}, LedgerFrom: 94}
	for _, tc := range []struct {
		name, plant, refusal string
	}{
		{"nothing to refuse", "", ""},
		{"a global limit of 0", `UPDATE network_bandwidth SET download_bps = 0`, "global download limit is 0"},
		{"a global upload limit", `UPDATE network_bandwidth SET upload_bps = 100`, "global upload limit is set"},
		{"a usenet split", `UPDATE network_bandwidth SET usenet_split_pct = 40`, "split between usenet and torrents"},
		{"an egress limit of 0", `UPDATE egress_interfaces SET download_bps = 0`, "1 rows of egress_interfaces: egresses have a download limit of 0"},
		{"an egress upload limit", `UPDATE egress_interfaces SET upload_bps = 100`, "egresses have an upload limit"},
		{"an egress upload quota", `UPDATE egress_interfaces SET quota_upload_soft_bytes = 100`, "egresses have an upload quota"},
		{"an egress hard upload quota", `UPDATE egress_interfaces SET quota_upload_hard_bytes = 100`, "egresses have a hard upload quota"},
		{"an egress upload trickle", `UPDATE egress_interfaces SET quota_upload_trickle_bps = 0`, "upload trickle other than the default"},
		{"counted uploads", `INSERT INTO egress_download_usage (egress_id, upload_lifetime_bytes) VALUES (0, 5)`, "egresses have counted uploads"},
		{"an upload quota baseline", `INSERT INTO egress_download_usage (egress_id, upload_quota_baseline_bytes) VALUES (0, 5)`, "upload quota baseline"},
		{"a set schedule limit", `INSERT INTO settings (key, value) VALUES ('schedules', '[{"action":{"type":"speed_limit","limits":[{"bytes_per_sec":500,"upload_bytes_per_sec":null}]}}]')`, ""},
		{"a lifted schedule limit", `INSERT INTO settings (key, value) VALUES ('schedules', '[{"action":{"type":"speed_limit","limits":[{"bytes_per_sec":null}]}}]')`, ""},
		{"a schedule split", `INSERT INTO settings (key, value) VALUES ('schedules', '[{"action":{"type":"bandwidth_split","usenet_pct":40}}]')`, "shares the download limit between usenet and torrents"},
		{"a schedule split lifted", `INSERT INTO settings (key, value) VALUES ('schedules', '[{"action":{"type":"bandwidth_split","usenet_pct":null}}]')`, "shares the download limit between usenet and torrents"},
		{"a quota rule for one egress", `INSERT INTO settings (key, value) VALUES ('schedules', '[{"action":{"type":"set_quota_metering","enabled":true,"target":{"kind":"egress","id":0}}}]')`, ""},
		{"a schedule limit of 0", `INSERT INTO settings (key, value) VALUES ('schedules', '[{"action":{"type":"speed_limit","limits":[{"bytes_per_sec":0}]}}]')`, "lets nothing through with 0"},
		{"a schedule upload limit", `INSERT INTO settings (key, value) VALUES ('schedules', '[{"action":{"type":"speed_limit","limits":[{"bytes_per_sec":500,"upload_bytes_per_sec":100}]}}]')`, "sets an upload limit"},
		{"a DNS profile", `INSERT INTO dns_profiles (id) VALUES (1)`, "rows in dns_profiles"},
	} {
		dbPath := filepath.Join(t.TempDir(), "state.db")
		setup := []string{
			`CREATE TABLE _sqlx_migrations (version INTEGER PRIMARY KEY, success BOOLEAN NOT NULL)`,
			`INSERT INTO _sqlx_migrations VALUES (93, 1), (94, 1)`,
			`CREATE TABLE settings (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)`,
			`CREATE TABLE network_bandwidth (id INTEGER PRIMARY KEY, download_bps INTEGER, upload_bps INTEGER, usenet_split_pct INTEGER)`,
			`INSERT INTO network_bandwidth VALUES (1, NULL, NULL, NULL)`,
			`CREATE TABLE egress_interfaces (id INTEGER PRIMARY KEY, download_bps INTEGER, upload_bps INTEGER, quota_download_bytes INTEGER, quota_download_enabled BOOLEAN NOT NULL DEFAULT FALSE, quota_upload_soft_bytes INTEGER, quota_upload_trickle_bps INTEGER NOT NULL DEFAULT 2048, quota_upload_hard_bytes INTEGER, quota_period TEXT NOT NULL DEFAULT 'one_time')`,
			`CREATE TABLE egress_download_usage (egress_id INTEGER PRIMARY KEY, upload_lifetime_bytes INTEGER NOT NULL DEFAULT 0, upload_quota_baseline_bytes INTEGER NOT NULL DEFAULT 0)`,
			`INSERT INTO egress_interfaces (id) VALUES (0)`,
			`CREATE TABLE dns_server_lists (id INTEGER PRIMARY KEY)`,
			`CREATE TABLE dns_profiles (id INTEGER PRIMARY KEY)`,
		}
		if tc.plant != "" {
			setup = append(setup, tc.plant)
		}
		if err := execAll(dbPath, setup); err != nil {
			t.Fatal(err)
		}
		err := applySchemaRewind(dbPath, rewind, nil)
		versions, readErr := migrationVersions(dbPath)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if tc.refusal == "" {
			if err != nil || !reflect.DeepEqual(versions, map[int]bool{93: true}) {
				t.Fatalf("%s: rewind = %v, ledger %v; want it rewound to 93", tc.name, err, versions)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.refusal) {
			t.Fatalf("%s: rewind = %v, want it refused with %q", tc.name, err, tc.refusal)
		}
		if !reflect.DeepEqual(versions, map[int]bool{93: true, 94: true}) {
			t.Fatalf("%s: ledger %v, want it untouched", tc.name, versions)
		}
	}
}

// The 0.16.5 rewind undoes what came after the graph-engine migration as the
// engine-stamped one does, on top of its own part, naming each thing once.
func TestRewindTo0165FollowsTheLaterMigrations(t *testing.T) {
	later, err := readLaterMigrations(testMigrationsDir, graphEngineMigration)
	if err != nil {
		t.Fatal(err)
	}
	rewind := rewindTo0165(later)
	stamped := preGraphEngineRewind(later)
	if !reflect.DeepEqual(rewind.Reverses, stamped.Reverses) || !reflect.DeepEqual(rewind.DropIndexes, stamped.DropIndexes) {
		t.Fatalf("0.16.5 reverses %v indexes %v, engine-stamped %v %v", rewind.Reverses, rewind.DropIndexes, stamped.Reverses, stamped.DropIndexes)
	}
	for _, table := range append(append([]string(nil), graphTables...), later.Tables...) {
		if !contains(rewind.DropTables, table) {
			t.Fatalf("0.16.5 drops tables %v, missing %s", rewind.DropTables, table)
		}
	}
	for _, column := range append([]tableColumn{{"active_jobs", "engine"}, {"job_history", "engine"}}, later.Columns...) {
		if !contains(rewind.DropColumns, column) {
			t.Fatalf("0.16.5 drops columns %v, missing %v", rewind.DropColumns, column)
		}
	}
	if len(appendNew(nil, rewind.DropTables...)) != len(rewind.DropTables) || len(appendNew(nil, rewind.DropColumns...)) != len(rewind.DropColumns) {
		t.Fatalf("0.16.5 names a table or column twice: %v %v", rewind.DropTables, rewind.DropColumns)
	}
	if rewind.LedgerFrom != 66 {
		t.Fatalf("0.16.5 ledger from %d, want 66", rewind.LedgerFrom)
	}
}

// The NZB move is undone first, in the datastore's dialect, so the NZBs are
// back on their rows before job_nzbs and the indexes go.
func TestSchemaRewindPlanPutsTheNZBsBackFirst(t *testing.T) {
	rewind := schemaRewind{
		Reverses:    []string{"move_nzbs_and_outlines_v85"},
		DropIndexes: []string{"idx_job_history_size_job"},
		DropTables:  []string{"job_nzbs", "active_job_outlines"},
		DropColumns: []tableColumn{{"active_jobs", "staging_dir"}, {"job_nzbs", "nzb_zstd"}},
		LedgerFrom:  74,
	}
	shape := schemaShape{
		Datastore: weaverDatastorePostgres,
		Tables:    map[string]bool{"active_jobs": true, "job_history": true, "job_nzbs": true},
		Columns: map[string]map[string]bool{
			"active_jobs": {"job_id": true, "staging_dir": true},
			"job_history": {"job_id": true, "nzb_zstd": true},
			"job_nzbs":    {"job_id": true, "nzb_zstd": true},
		},
	}
	want := []string{
		"ALTER TABLE active_jobs ADD COLUMN nzb_zstd BYTEA",
		"UPDATE active_jobs SET nzb_zstd = (SELECT n.nzb_zstd FROM job_nzbs AS n WHERE n.job_id = active_jobs.job_id) WHERE nzb_zstd IS NULL",
		"UPDATE job_history SET nzb_zstd = (SELECT n.nzb_zstd FROM job_nzbs AS n WHERE n.job_id = job_history.job_id) WHERE nzb_zstd IS NULL",
		"DROP INDEX IF EXISTS idx_job_history_size_job",
		"ALTER TABLE active_jobs DROP COLUMN staging_dir",
		"DROP TABLE job_nzbs",
		"DELETE FROM _sqlx_migrations WHERE version >= 74",
	}
	if got := rewind.plan(shape); !reflect.DeepEqual(got, want) {
		t.Fatalf("plan =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	shape.Datastore = weaverDatastoreSQLite
	if got := rewind.plan(shape)[0]; got != "ALTER TABLE active_jobs ADD COLUMN nzb_zstd BLOB" {
		t.Fatalf("SQLite column = %s", got)
	}
}

func contains[T comparable](items []T, want T) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

// testRustSteps does on SQLite what each Rust step does to the rows these
// tests hold, so the checked-in migrations replay whole.
var testRustSteps = map[string][]string{
	"move_nzbs_and_outlines_v85": {
		`INSERT INTO job_nzbs (job_id, nzb_zstd) SELECT job_id, nzb_zstd FROM active_jobs WHERE nzb_zstd IS NOT NULL`,
		`INSERT INTO job_nzbs (job_id, nzb_zstd) SELECT job_id, nzb_zstd FROM job_history WHERE nzb_zstd IS NOT NULL ON CONFLICT (job_id) DO NOTHING`,
		`DROP TABLE active_job_nzb_files`,
		`ALTER TABLE active_jobs DROP COLUMN nzb_zstd`,
		`ALTER TABLE job_history DROP COLUMN nzb_zstd`,
	},
	"fill_history_release_names_v87": {
		`UPDATE job_history SET release_name = name`,
	},
	"fill_history_search_keys_v88": {
		`UPDATE job_history SET search_key = name`,
	},
	"seed_history_retention_v89": {
		`INSERT INTO settings (key, value) SELECT 'history_retention_days', '0' WHERE EXISTS (SELECT 1 FROM job_history) ON CONFLICT(key) DO NOTHING`,
	},
	// The step deletes servers only past 50; these tests hold fewer.
	"trim_servers_to_limit_v92": nil,
	"move_bandwidth_to_networking_v94": {
		`INSERT INTO network_bandwidth (id, download_bps, upload_bps) SELECT 1, CAST(value AS INTEGER), NULL FROM settings WHERE key = 'max_download_speed' AND CAST(value AS INTEGER) > 0 ON CONFLICT(id) DO UPDATE SET download_bps = excluded.download_bps`,
		`DELETE FROM settings WHERE key = 'max_download_speed'`,
		// The step writes each speed rule's limit of 0 as null. Before 94 no
		// rule had an upload limit, so only the download limit matches.
		`UPDATE settings SET value = replace(value, '"bytes_per_sec":0', '"bytes_per_sec":null') WHERE key = 'schedules'`,
	},
	// These tests hold no torrents and no proxies.
	"fill_tracker_hosts_v105":        nil,
	"dns_profiles_from_proxies_v124": nil,
}

func TestEngineStampedRewindAllowsLaterGraphMigrationsToReplay(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
	dbPath := filepath.Join(t.TempDir(), "state.db")
	entries, err := readMigrationManifest(testMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := latestMigration(testMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	ledger := []string{"(73, 1)"}
	var migrations []string
	for _, entry := range entries {
		if entry.Version < graphEngineMigration {
			continue
		}
		ledger = append(ledger, fmt.Sprintf("(%d, 1)", entry.Version))
		if entry.Version == graphEngineMigration {
			continue
		}
		for _, step := range entry.Steps {
			if step.Kind == "rust" {
				statements, known := testRustSteps[step.HookID]
				if !known {
					t.Fatalf("migration %d: no test stand-in for hook %q", entry.Version, step.HookID)
				}
				migrations = append(migrations, statements...)
				continue
			}
			if step.Engine == "postgres" || strings.Contains(step.File, "postgres") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(filepath.Dir(testMigrationsDir), filepath.FromSlash(step.File)))
			if err != nil {
				t.Fatal(err)
			}
			migrations = append(migrations, string(data))
		}
	}
	if len(ledger) != latest-73+1 {
		t.Fatalf("ledger rows = %v, want every version from 73 to %d", ledger, latest)
	}
	// The shape migration 74 left: the stored NZBs still on the job rows.
	if err := execAll(dbPath, []string{
		`CREATE TABLE _sqlx_migrations (version INTEGER PRIMARY KEY, success BOOLEAN NOT NULL)`,
		`INSERT INTO _sqlx_migrations VALUES ` + strings.Join(ledger, ", "),
		`CREATE TABLE active_jobs (job_id INTEGER PRIMARY KEY, output_dir TEXT, metadata TEXT, password TEXT, password_candidates TEXT, nzb_zstd BLOB)`,
		`CREATE TABLE job_history (job_id INTEGER PRIMARY KEY, name TEXT, status TEXT, category TEXT, total_bytes INTEGER, health INTEGER, completed_at INTEGER, output_dir TEXT, metadata TEXT, nzb_zstd BLOB)`,
		`CREATE TABLE async_operation_targets (operation_id INTEGER, job_id INTEGER)`,
		`CREATE TABLE settings (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)`,
		`CREATE TABLE servers (id INTEGER PRIMARY KEY NOT NULL)`,
		`INSERT INTO servers (id) VALUES (1), (2)`,
		`CREATE TABLE categories (id INTEGER PRIMARY KEY NOT NULL)`,
		// Migration 21's seen RSS items, which migration 123 indexes.
		`CREATE TABLE rss_seen_items (feed_id INTEGER NOT NULL, decision TEXT)`,
		`CREATE TABLE proxy_profiles (id INTEGER PRIMARY KEY NOT NULL, config TEXT NOT NULL)`,
		`CREATE TABLE proxy_routes (consumer TEXT PRIMARY KEY NOT NULL, policy TEXT NOT NULL)`,
		// Migration 53's egress tables, with a global limit and an egress
		// limit and quota that migration 94 moves.
		`CREATE TABLE egress_interfaces (id BIGINT PRIMARY KEY CHECK (id >= 0), name TEXT NOT NULL, binding_kind TEXT NOT NULL, binding_value TEXT, enabled BOOLEAN NOT NULL, max_download_speed BIGINT NOT NULL CHECK (max_download_speed >= 0), download_quota_enabled BOOLEAN NOT NULL DEFAULT FALSE, download_quota_limit_bytes BIGINT NOT NULL DEFAULT 0 CHECK (download_quota_limit_bytes >= 0), download_quota_period TEXT NOT NULL DEFAULT 'one_time', download_quota_reset_time_minutes_local BIGINT NOT NULL DEFAULT 0, download_quota_weekly_reset_weekday TEXT NOT NULL DEFAULT 'mon', download_quota_monthly_reset_day BIGINT NOT NULL DEFAULT 1, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL)`,
		`INSERT INTO egress_interfaces (id, name, binding_kind, binding_value, enabled, max_download_speed, download_quota_enabled, download_quota_limit_bytes, download_quota_period, created_at, updated_at) VALUES (0, 'System', 'system', NULL, TRUE, 4096, TRUE, 9000, 'monthly', 0, 0)`,
		`CREATE TABLE egress_download_usage (egress_id BIGINT PRIMARY KEY NOT NULL, lifetime_bytes BIGINT NOT NULL DEFAULT 0, quota_baseline_bytes BIGINT NOT NULL DEFAULT 0)`,
		`INSERT INTO settings (key, value) VALUES ('max_download_speed', '2048')`,
		// A schedule from before 94: one rule lifts the limit with 0, one sets it.
		`INSERT INTO settings (key, value) VALUES ('schedules', '` + strings.ReplaceAll(schedulesBefore94, "'", "''") + `')`,
		`CREATE TABLE graph_members (job_id INTEGER, member_id INTEGER)`,
		`CREATE TABLE graph_envelopes (job_id INTEGER, node_id INTEGER)`,
		`CREATE TABLE graph_holds (job_id INTEGER)`,
		`INSERT INTO active_jobs (job_id, nzb_zstd) VALUES (7, x'0107'), (8, NULL)`,
		`INSERT INTO job_history (job_id, name, status, nzb_zstd) VALUES (3, 'done', 'complete', x'0203')`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := execAll(dbPath, migrations); err != nil {
		t.Fatal(err)
	}
	if got := storedNZBs(t, dbPath, `SELECT job_id, hex(nzb_zstd) AS nzb FROM job_nzbs`); got != "3=0203 7=0107" {
		t.Fatalf("job_nzbs after the upgrade = %s", got)
	}
	if got := savedSchedulesIn(t, dbPath); got != schedulesSince94 {
		t.Fatalf("schedules after the upgrade = %s, want %s", got, schedulesSince94)
	}
	// An egress made since migration 94, with no limit and no quota amount.
	if err := execAll(dbPath, []string{`INSERT INTO egress_interfaces (id, name, binding_kind, enabled, created_at, updated_at) VALUES (1, 'Second', 'interface', TRUE, 0, 0)`}); err != nil {
		t.Fatal(err)
	}
	later, err := readLaterMigrations(testMigrationsDir, graphEngineMigration)
	if err != nil {
		t.Fatal(err)
	}
	if err := applySchemaRewind(dbPath, preGraphEngineRewind(later), nil); err != nil {
		t.Fatal(err)
	}
	if got := storedNZBs(t, dbPath, `SELECT job_id, hex(nzb_zstd) AS nzb FROM active_jobs UNION ALL SELECT job_id, hex(nzb_zstd) AS nzb FROM job_history`); got != "3=0203 7=0107 8=" {
		t.Fatalf("NZBs on the rewound rows = %s", got)
	}
	// A lifted schedule limit is back as 0, the rest of the schedule as it was.
	if got := savedSchedulesIn(t, dbPath); got != schedulesBefore94 {
		t.Fatalf("rewound schedules = %s, want %s", got, schedulesBefore94)
	}
	err = withStateDB(dbPath, func(db *stateConn) error {
		tables, err := existingTables(db)
		if err != nil {
			return err
		}
		for _, table := range []string{"job_nzbs", "active_job_outlines", "active_job_nzb_files"} {
			if tables[table] {
				t.Fatalf("rewound database keeps %s", table)
			}
		}
		var indexes int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_job_history_%'`).Scan(&indexes); err != nil {
			return err
		}
		if indexes != 0 {
			t.Fatalf("rewound database keeps %d history indexes", indexes)
		}
		var seeded int
		if err := db.QueryRow(`SELECT COUNT(*) FROM settings WHERE key = 'history_retention_days'`).Scan(&seeded); err != nil {
			return err
		}
		if seeded != 0 {
			t.Fatalf("rewound database keeps the retention migration 89 seeded")
		}
		// Migration 94's limits are back where an older build kept them.
		var global string
		if err := db.QueryRow(`SELECT value FROM settings WHERE key = 'max_download_speed'`).Scan(&global); err != nil || global != "2048" {
			t.Fatalf("rewound global limit = %q, %v; want 2048", global, err)
		}
		var speed, limit int
		var enabled bool
		var period string
		if err := db.QueryRow(`SELECT max_download_speed, download_quota_enabled, download_quota_limit_bytes, download_quota_period FROM egress_interfaces WHERE id = 0`).Scan(&speed, &enabled, &limit, &period); err != nil {
			return fmt.Errorf("rewound egress: %w", err)
		}
		if speed != 4096 || !enabled || limit != 9000 || period != "monthly" {
			t.Fatalf("rewound egress = %d %v %d %s, want 4096 true 9000 monthly", speed, enabled, limit, period)
		}
		// Migration 53's columns hold no NULL: no limit and no quota amount
		// are 0 there, with the quota off.
		if err := db.QueryRow(`SELECT max_download_speed, download_quota_enabled, download_quota_limit_bytes FROM egress_interfaces WHERE id = 1`).Scan(&speed, &enabled, &limit); err != nil {
			return fmt.Errorf("rewound second egress: %w", err)
		}
		if speed != 0 || enabled || limit != 0 {
			t.Fatalf("rewound second egress = %d %v %d, want 0 false 0", speed, enabled, limit)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := execAll(dbPath, migrations); err != nil {
		t.Fatalf("the historical schema must accept the original migrations again: %v", err)
	}
	if got := storedNZBs(t, dbPath, `SELECT job_id, hex(nzb_zstd) AS nzb FROM job_nzbs`); got != "3=0203 7=0107" {
		t.Fatalf("job_nzbs after the second upgrade = %s", got)
	}
	if got := savedSchedulesIn(t, dbPath); got != schedulesSince94 {
		t.Fatalf("schedules after the second upgrade = %s, want %s", got, schedulesSince94)
	}
	err = withStateDB(dbPath, func(db *stateConn) error {
		var retention string
		if err := db.QueryRow(`SELECT value FROM settings WHERE key = 'history_retention_days'`).Scan(&retention); err != nil {
			return fmt.Errorf("retention after the second upgrade: %w", err)
		}
		if retention != "0" {
			t.Fatalf("retention after the second upgrade = %q, want 0 (history has rows)", retention)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := migrationVersions(dbPath)
	if err != nil || !reflect.DeepEqual(versions, map[int]bool{73: true}) {
		t.Fatalf("historical ledger = %v, error = %v", versions, err)
	}
}

// schedulesBefore94 is a saved schedule from before migration 94, in the
// key order the rewind writes: one rule lifts the limit with 0, one sets it.
// The name carries a quote the rewind must write back whole.
const schedulesBefore94 = `[{"action":{"limits":[{"bytes_per_sec":0,"target":{"kind":"global"}}],"type":"speed_limit"},"name":"Night's lift"},{"action":{"limits":[{"bytes_per_sec":500,"target":{"id":1,"kind":"server"}}],"type":"speed_limit"},"name":"Day"}]`

// schedulesSince94 is schedulesBefore94 as migration 94 leaves it.
var schedulesSince94 = strings.Replace(schedulesBefore94, `"bytes_per_sec":0`, `"bytes_per_sec":null`, 1)

// savedSchedulesIn is the saved schedules setting of the database.
func savedSchedulesIn(t *testing.T, dbPath string) string {
	t.Helper()
	var raw string
	if err := withStateDB(dbPath, func(db *stateConn) error {
		return db.QueryRow(`SELECT value FROM settings WHERE key = 'schedules'`).Scan(&raw)
	}); err != nil {
		t.Fatal(err)
	}
	return raw
}

// storedNZBs is "job=hex" for each (job_id, nzb) row the query gives, in job order.
func storedNZBs(t *testing.T, dbPath, query string) string {
	t.Helper()
	var out []string
	err := withStateDB(dbPath, func(db *stateConn) error {
		rows, err := db.Query(`SELECT job_id, COALESCE(nzb, '') FROM (` + query + `) ORDER BY job_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int
			var nzb string
			if err := rows.Scan(&id, &nzb); err != nil {
				return err
			}
			out = append(out, fmt.Sprintf("%d=%s", id, nzb))
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(out, " ")
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

// A restart wait ends on the state it reads, never on the case timeout: a
// check that holds only on its third read still resolves when the case
// timeout has already run out.
func TestRestartPollForEndsOnStateNotTheCaseTimeout(t *testing.T) {
	ctx := &restartCaseContext{Timeout: 0}
	reads := 0
	err := ctx.pollFor("third read", func() (bool, error) {
		reads++
		return reads == 3, nil
	})
	if err != nil {
		t.Fatalf("pollFor = %v, want nil", err)
	}
	if reads != 3 {
		t.Fatalf("reads = %d, want 3", reads)
	}
}

func TestBodyHoldBudgetPaysForTheLongestBodiesAndOneCutBodyPerConnection(t *testing.T) {
	lines := []int{10, 40, 20, 30, 50}
	budget, most, err := bodyHoldBudget(lines, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	// The two longest (50 + 40) and one longest body per connection (3 * 50).
	if budget != 240 {
		t.Fatalf("budget = %d, want 240", budget)
	}
	// 10 + 20 + 30 + 40 = 100 fits; all five, 150, fits too.
	if most != 5 {
		t.Fatalf("most = %d, want 5", most)
	}
	if _, most, _ = bodyHoldBudget(lines, 1, 1); most != 4 {
		// Budget 100: 10 + 20 + 30 + 40 = 100 fits, adding 50 overshoots.
		t.Fatalf("most = %d, want 4", most)
	}
	for _, bad := range [][2]int{{0, 1}, {6, 1}, {1, 0}} {
		if _, _, err := bodyHoldBudget(lines, bad[0], bad[1]); err == nil {
			t.Fatalf("need %d over %d connections: want an error", bad[0], bad[1])
		}
	}
}

func TestBodyHoldBudgetCompletesTheNeedInAnyServingOrder(t *testing.T) {
	lines := []int{7, 3, 9, 4, 8, 2, 6}
	const need, sessions = 3, 2
	budget, _, err := bodyHoldBudget(lines, need, sessions)
	if err != nil {
		t.Fatal(err)
	}
	// Worst case: every connection is cut short one line before the end of
	// the longest body, and the completed ones are the longest.
	cut := sessions * (9 - 1)
	completed, spent := 0, cut
	for _, count := range []int{9, 8, 7, 6, 4, 3, 2} {
		if spent+count > budget {
			break
		}
		spent += count
		completed++
	}
	if completed < need {
		t.Fatalf("budget %d completes %d bodies in the worst order, want at least %d", budget, completed, need)
	}
}

func TestArticleBodyLinesCountsFromTheYencHeaderToTheTerminator(t *testing.T) {
	wire := []byte("Message-ID: <a@b>\r\n\r\n=ybegin part=1 line=128 size=10 name=x\r\n=ypart begin=1 end=10\r\nabc\r\n=yend size=10 part=1\r\n.\r\n")
	got, err := articleBodyLines(wire)
	if err != nil {
		t.Fatal(err)
	}
	if got != 4 {
		t.Fatalf("lines = %d, want 4", got)
	}
	if _, err := articleBodyLines([]byte("Message-ID: <a@b>\r\n\r\nplain\r\n.\r\n")); err == nil {
		t.Fatal("an article with no yEnc header: want an error")
	}
}
