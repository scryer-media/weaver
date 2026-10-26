// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The helpers the graph-engine restart cases share: the operator's settings
// and servers set through the API, the admission and checkpoint state read
// from the state database, and the schema rewind that turns a database the
// current build wrote into one an older build left behind.

// graphTables are the graph checkpoint's tables, every one keyed by job.
var graphTables = []string{
	"graph_nodes",
	"graph_members",
	"graph_coverage",
	"graph_envelopes",
	"graph_crypt",
	"graph_recovery",
	"graph_holds",
	"graph_facts_parts",
}

// holdEveryBody makes the primary news server serve one line and then block
// every body until the hold is lifted, so a job stays mid-download for as long
// as the case needs it there.
const holdEveryBody = "hold_body=1"

func holdPrimaryBodies() error {
	return setNntpChaosOnServer(nntpHost(), nntpPort(), holdEveryBody)
}

// liftPrimaryHold clears the primary's chaos without reloading its articles,
// so anything a case withheld stays withheld.
func liftPrimaryHold() error {
	return setNntpChaosOnServer(nntpHost(), nntpPort(), "off")
}

// pollFor reads `check` until it holds. Every wait in these cases ends on the
// state it reads; the case timeout only ends a wait that never resolves.
func (ctx *restartCaseContext) pollFor(what string, check func() (bool, error)) error {
	deadline := time.Now().Add(ctx.Timeout)
	for {
		done, err := check()
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if done {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("never saw %s", what)
		}
		time.Sleep(chaosPollInterval)
	}
}

// graphQLMutate posts one operation and fails on a transport error or any
// GraphQL error.
func graphQLMutate(weaverURL, query string, variables map[string]any, out any) error {
	payload, _ := json.Marshal(map[string]any{"query": query, "variables": variables})
	resp, err := postGraphQL(weaverURL, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode GraphQL response (%d): %w: %s", resp.StatusCode, err, strings.TrimSpace(string(body)))
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("gql: %s", envelope.Errors[0].Message)
	}
	if out != nil {
		return json.Unmarshal(envelope.Data, out)
	}
	return nil
}

// addNewsServerGraphQL adds a server on the local fake NNTP fleet with the
// harness's credentials.
func addNewsServerGraphQL(weaverURL, port string, connections, priority int, backfill bool) (int, error) {
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return 0, fmt.Errorf("news server port %q: %w", port, err)
	}
	var out struct {
		AddServer struct {
			ID int `json:"id"`
		} `json:"addServer"`
	}
	err = graphQLMutate(weaverURL, `mutation($input: ServerInput!) { addServer(input: $input) { id } }`, map[string]any{
		"input": map[string]any{
			"host": "localhost", "port": portNumber, "tls": false,
			"username": "e2e-user", "password": "e2e-pass",
			"connections": connections, "active": true,
			"priority": priority, "backfill": backfill,
		},
	}, &out)
	if err != nil {
		return 0, err
	}
	return out.AddServer.ID, nil
}

func setMaxConcurrentNzbsGraphQL(weaverURL string, limit int) error {
	return graphQLMutate(weaverURL, `mutation($input: GeneralSettingsInput!) { updateSettings(input: $input) { __typename } }`,
		map[string]any{"input": map[string]any{"maxConcurrentNzbs": limit}}, nil)
}

func setArchivePasswordsGraphQL(weaverURL string, passwords []string) error {
	return graphQLMutate(weaverURL, `mutation($passwords: [String!]) { updateArchivePasswordSettings(passwords: $passwords) { __typename } }`,
		map[string]any{"passwords": passwords}, nil)
}

// submitScenarioAs submits the scenario's NZB under another title.
func (ctx *restartCaseContext) submitScenarioAs(slug, title string, force bool) (int, error) {
	scenario := *ctx.Scenarios[slug]
	scenario.Title = title
	return submitOneNZBWithOptions(ctx.weaverURL, &scenario, submitNZBOptions{force: force})
}

// submitScenarioWithoutPassword submits the scenario's NZB with no job
// password, so only the operator's archive passwords can open it.
func (ctx *restartCaseContext) submitScenarioWithoutPassword(slug string) (int, error) {
	scenario := *ctx.Scenarios[slug]
	scenario.Password = ""
	return submitOneNZBWithOptions(ctx.weaverURL, &scenario, submitNZBOptions{})
}

// withStateDB opens the case's state database for one read or write.
func withStateDB(dbPath string, use func(*sql.DB) error) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return err
	}
	defer closeWeaverStateDB(db, datastore)
	// One connection, so a temporary table one statement makes is there for
	// the next.
	db.SetMaxOpenConns(1)
	return use(db)
}

// admissionRow is what the active row says of one job's NZB admission.
type admissionRow struct {
	Status   string
	Admitted bool
	Error    string
}

func readAdmissionRows(dbPath string, ids []int) (map[int]admissionRow, error) {
	out := map[int]admissionRow{}
	err := withStateDB(dbPath, func(db *sql.DB) error {
		rows, err := db.Query(`SELECT job_id, status, nzb_admitted, COALESCE(error, '') FROM active_jobs`)
		if err != nil {
			return err
		}
		defer rows.Close()
		wanted := map[int]bool{}
		for _, id := range ids {
			wanted[id] = true
		}
		for rows.Next() {
			var id, admitted int
			var row admissionRow
			if err := rows.Scan(&id, &row.Status, &admitted, &row.Error); err != nil {
				return err
			}
			if wanted[id] {
				row.Admitted = admitted == 1
				out[id] = row
			}
		}
		return rows.Err()
	})
	return out, err
}

// admissionTrace records, sample by sample, which of a case's jobs held an
// NZB admission, and judges the cap and the order they were admitted in.
type admissionTrace struct {
	limit   int
	order   []int
	first   map[int]int
	peak    int
	overCap []string
}

func newAdmissionTrace(limit int, order []int) *admissionTrace {
	return &admissionTrace{limit: limit, order: append([]int(nil), order...), first: map[int]int{}}
}

// observe records one sample: the jobs admitted now, and the jobs that have
// finished, which were admitted at the latest by this sample.
func (trace *admissionTrace) observe(sample int, admitted, finished []int) {
	if len(admitted) > trace.peak {
		trace.peak = len(admitted)
	}
	if len(admitted) > trace.limit {
		sorted := append([]int(nil), admitted...)
		sort.Ints(sorted)
		trace.overCap = append(trace.overCap, fmt.Sprintf("sample %d admitted %v", sample, sorted))
	}
	for _, group := range [][]int{admitted, finished} {
		for _, id := range group {
			if _, seen := trace.first[id]; !seen {
				trace.first[id] = sample
			}
		}
	}
}

// verdict is nil when no sample admitted more than the cap, every job was
// admitted, and no job was admitted before one submitted ahead of it.
func (trace *admissionTrace) verdict() error {
	var problems []string
	if len(trace.overCap) > 0 {
		problems = append(problems, "over the cap: "+strings.Join(trace.overCap, "; "))
	}
	previous := -1
	for index, id := range trace.order {
		sample, seen := trace.first[id]
		if !seen {
			problems = append(problems, fmt.Sprintf("job %d was never admitted", id))
			continue
		}
		if sample < previous {
			problems = append(problems, fmt.Sprintf("job %d (submitted %d) was admitted at sample %d, before the job ahead of it (sample %d)", id, index+1, sample, previous))
		}
		previous = sample
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	return nil
}

func (trace *admissionTrace) summary() string {
	parts := make([]string, 0, len(trace.order))
	for _, id := range trace.order {
		parts = append(parts, fmt.Sprintf("%d@%d", id, trace.first[id]))
	}
	return fmt.Sprintf("cap=%d peak_admitted=%d first_admitted_sample=[%s]", trace.limit, trace.peak, strings.Join(parts, " "))
}

// graphNodeRows is how many checkpoint nodes the job has written.
func graphNodeRows(dbPath string, jobID int) (int, error) {
	count := 0
	err := withStateDB(dbPath, func(db *sql.DB) error {
		return db.QueryRow(`SELECT COUNT(*) FROM graph_nodes WHERE job_id = ?`, jobID).Scan(&count)
	})
	return count, err
}

// existingTables is every table the database has.
func existingTables(db *sql.DB) (map[string]bool, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

// deleteGraphCheckpoint removes every checkpoint row of the job: the job then
// stands as one stopped before its first checkpoint was written.
func deleteGraphCheckpoint(dbPath string, jobID int) error {
	return withStateDB(dbPath, func(db *sql.DB) error {
		tables, err := existingTables(db)
		if err != nil {
			return err
		}
		for _, table := range graphTables {
			if !tables[table] {
				continue
			}
			if _, err := db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE job_id = ?`, table), jobID); err != nil {
				return fmt.Errorf("clear %s for job %d: %w", table, jobID, err)
			}
		}
		return nil
	})
}

// execAll runs the statements in order on one connection.
func execAll(dbPath string, statements []string) error {
	return withStateDB(dbPath, func(db *sql.DB) error {
		for _, statement := range statements {
			if _, err := db.Exec(statement); err != nil {
				return fmt.Errorf("%s: %w", statement, err)
			}
		}
		return nil
	})
}

// historyOutputDir is where a finished job delivered.
func historyOutputDir(dbPath string, jobID int) (string, error) {
	return loadJobOutputDir(dbPath, jobID)
}

// deliveredCopy reports whether a file under dir has the size and BLAKE3.
func deliveredCopy(dir string, size int64, digest string) bool {
	found := false
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || found || !info.Mode().IsRegular() || info.Size() != size {
			return nil
		}
		if got, err := fileBLAKE3(path); err == nil && got == digest {
			found = true
		}
		return nil
	})
	return found
}

// singleMKVSource is the payload single-mkv posts, and its digest.
func singleMKVSource() (int64, string, error) {
	source := filepath.Join(testdataDir(), "single-mkv", "test-media.mkv")
	info, err := os.Stat(source)
	if err != nil {
		return 0, "", err
	}
	digest, err := fileBLAKE3(source)
	return info.Size(), digest, err
}

// --------------------------------------------------------- schema rewind

// tableColumn names one column of one table.
type tableColumn struct {
	Table  string
	Column string
}

// columnAdd is a column an older schema carried, with its definition.
type columnAdd struct {
	Table      string
	Column     string
	Definition string
}

// tableDDL is a table an older schema carried, as that schema created it.
type tableDDL struct {
	Name string
	DDL  string
}

// schemaShape is what a database holds now: its tables, each table's
// columns, and each table's index definitions.
type schemaShape struct {
	Tables  map[string]bool
	Columns map[string]map[string]bool
	Indexes map[string]map[string]string
}

// schemaRewind turns a database into the shape an older build left: the
// tables and columns later migrations added are dropped, the ones later
// migrations dropped are put back, and the migration ledger forgets every
// version from LedgerFrom on, so the current build applies them again.
type schemaRewind struct {
	DropTables    []string
	DropColumns   []tableColumn
	RestoreTables []tableDDL
	AddColumns    []columnAdd
	LedgerFrom    int
}

// plan is the statements that rewind a database of the given shape. Only what
// is there is dropped and only what is missing is put back, so the same rewind
// serves whether or not a later migration has already run.
func (rewind schemaRewind) plan(shape schemaShape) []string {
	var statements []string
	for _, drop := range rewind.DropColumns {
		if !shape.Columns[drop.Table][drop.Column] {
			continue
		}
		names := make([]string, 0, len(shape.Indexes[drop.Table]))
		for name := range shape.Indexes[drop.Table] {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if strings.Contains(shape.Indexes[drop.Table][name], drop.Column) {
				statements = append(statements, fmt.Sprintf(`DROP INDEX IF EXISTS %s`, name))
			}
		}
		statements = append(statements, fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, drop.Table, drop.Column))
	}
	for _, table := range rewind.DropTables {
		if shape.Tables[table] {
			statements = append(statements, fmt.Sprintf(`DROP TABLE %s`, table))
		}
	}
	for _, table := range rewind.RestoreTables {
		if !shape.Tables[table.Name] {
			statements = append(statements, table.DDL)
		}
	}
	for _, add := range rewind.AddColumns {
		if !shape.Columns[add.Table][add.Column] {
			statements = append(statements, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, add.Table, add.Column, add.Definition))
		}
	}
	statements = append(statements, fmt.Sprintf(`DELETE FROM _sqlx_migrations WHERE version >= %d`, rewind.LedgerFrom))
	return statements
}

// readSchemaShape reads the shape of the tables the rewind touches.
func readSchemaShape(db *sql.DB, rewind schemaRewind) (schemaShape, error) {
	tables, err := existingTables(db)
	if err != nil {
		return schemaShape{}, err
	}
	shape := schemaShape{Tables: tables, Columns: map[string]map[string]bool{}, Indexes: map[string]map[string]string{}}
	touched := map[string]bool{}
	for _, drop := range rewind.DropColumns {
		touched[drop.Table] = true
	}
	for _, add := range rewind.AddColumns {
		touched[add.Table] = true
	}
	for table := range touched {
		if !tables[table] {
			continue
		}
		columns := map[string]bool{}
		rows, err := db.Query(fmt.Sprintf(`SELECT name FROM pragma_table_info('%s')`, table))
		if err != nil {
			return schemaShape{}, err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return schemaShape{}, err
			}
			columns[name] = true
		}
		rows.Close()
		shape.Columns[table] = columns
		indexes := map[string]string{}
		rows, err = db.Query(`SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL`, table)
		if err != nil {
			return schemaShape{}, err
		}
		for rows.Next() {
			var name, definition string
			if err := rows.Scan(&name, &definition); err != nil {
				rows.Close()
				return schemaShape{}, err
			}
			indexes[name] = definition
		}
		rows.Close()
		shape.Indexes[table] = indexes
	}
	return shape, nil
}

// applySchemaRewind rewinds the database and then runs the statements that
// plant what the older build would have left in it.
func applySchemaRewind(dbPath string, rewind schemaRewind, plant []string) error {
	return withStateDB(dbPath, func(db *sql.DB) error {
		shape, err := readSchemaShape(db, rewind)
		if err != nil {
			return err
		}
		for _, statement := range append(rewind.plan(shape), plant...) {
			if _, err := db.Exec(statement); err != nil {
				return fmt.Errorf("%s: %w", statement, err)
			}
		}
		return nil
	})
}

// classicOnlyTables are exactly the tables migration 74 drops, as a database
// written before it holds them (migrations 21, 26, 38 and 47), in the order
// the migration drops them.
var classicOnlyTables = []tableDDL{
	{"active_par2", `CREATE TABLE active_par2 (job_id INTEGER PRIMARY KEY NOT NULL, slice_size INTEGER NOT NULL, recovery_block_count INTEGER NOT NULL)`},
	{"active_par2_files", `CREATE TABLE active_par2_files (job_id INTEGER NOT NULL, file_index INTEGER NOT NULL, filename TEXT NOT NULL, recovery_block_count INTEGER NOT NULL DEFAULT 0, promoted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (job_id, file_index)) WITHOUT ROWID`},
	{"active_failed_extractions", `CREATE TABLE active_failed_extractions (job_id INTEGER NOT NULL, member_name TEXT NOT NULL, PRIMARY KEY (job_id, member_name))`},
	{"active_extraction_chunks", `CREATE TABLE active_extraction_chunks (job_id INTEGER NOT NULL, set_name TEXT NOT NULL, member_name TEXT NOT NULL, volume_index INTEGER NOT NULL, bytes_written INTEGER NOT NULL, temp_path TEXT NOT NULL, start_offset INTEGER NOT NULL, end_offset INTEGER NOT NULL, verified INTEGER NOT NULL, appended INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (job_id, set_name, member_name, volume_index)) WITHOUT ROWID`},
	{"active_archive_headers", `CREATE TABLE active_archive_headers (job_id INTEGER NOT NULL, set_name TEXT NOT NULL, headers BLOB NOT NULL, PRIMARY KEY (job_id, set_name)) WITHOUT ROWID`},
	{"active_rar_volume_facts", `CREATE TABLE active_rar_volume_facts (job_id INTEGER NOT NULL, set_name TEXT NOT NULL, volume_index INTEGER NOT NULL, facts_blob BLOB NOT NULL, PRIMARY KEY (job_id, set_name, volume_index)) WITHOUT ROWID`},
	{"active_volume_status", `CREATE TABLE active_volume_status (job_id INTEGER NOT NULL, set_name TEXT NOT NULL, volume_index INTEGER NOT NULL, extracted INTEGER NOT NULL DEFAULT 0, par2_clean INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (job_id, set_name, volume_index)) WITHOUT ROWID`},
	{"active_rar_verified_suspect", `CREATE TABLE active_rar_verified_suspect (job_id INTEGER NOT NULL, set_name TEXT NOT NULL, volume_index INTEGER NOT NULL, PRIMARY KEY (job_id, set_name, volume_index)) WITHOUT ROWID`},
	{"active_direct_coverage", `CREATE TABLE active_direct_coverage (job_id INTEGER NOT NULL, set_name TEXT NOT NULL, snapshot BLOB NOT NULL, PRIMARY KEY (job_id, set_name)) WITHOUT ROWID`},
	{"active_repair_outputs", `CREATE TABLE active_repair_outputs (job_id BIGINT NOT NULL REFERENCES active_jobs(job_id) ON DELETE CASCADE, file_index BIGINT NOT NULL CHECK (file_index >= 0 AND file_index <= 4294967295), filename TEXT NOT NULL, expected_length BIGINT NOT NULL CHECK (expected_length >= 0), PRIMARY KEY (job_id, file_index), UNIQUE (job_id, filename))`},
}

// plantedClassicTables is the dropped table the upgrade cases leave a row in.
var plantedClassicTables = []string{"active_volume_status"}

func plantClassicRows(jobID int) []string {
	return []string{
		fmt.Sprintf(`INSERT INTO active_volume_status (job_id, set_name, volume_index, extracted, par2_clean, deleted) VALUES (%d, 'e2e-planted', 0, 0, 1, 0)`, jobID),
	}
}

// rewindTo0165 is a 0.16.5 database: its last migration is 65, so it has no
// engine stamp, no root phase, no NZB admission columns and no graph
// checkpoint, and it still has every classic-only table.
var rewindTo0165 = schemaRewind{
	DropTables: graphTables,
	DropColumns: []tableColumn{
		{"active_jobs", "engine"},
		{"active_jobs", "root_phase"},
		{"active_jobs", "nzb_admitted"},
		{"active_jobs", "nzb_release_pending"},
		{"job_history", "engine"},
	},
	RestoreTables: classicOnlyTables,
	LedgerFrom:    66,
}

// graphEngineMigration is the migration that leaves the graph engine the only
// engine: it drops active_jobs.engine, job_history.engine and the ten
// classicOnlyTables.
const graphEngineMigration = 74

// rewindToPreGraphEngine is a database written before that migration: the
// engine stamps and every classic-only table are there.
var rewindToPreGraphEngine = schemaRewind{
	RestoreTables: classicOnlyTables,
	AddColumns: []columnAdd{
		{"active_jobs", "engine", "TEXT NOT NULL DEFAULT 'classic'"},
		{"job_history", "engine", "TEXT NOT NULL DEFAULT 'classic'"},
	},
	LedgerFrom: graphEngineMigration,
}

// cloneHistoryRow copies a history row under another job id, every column
// as it stands, so a case has a second history row without a second
// download.
func cloneHistoryRow(from, to int) []string {
	return []string{
		`DROP TABLE IF EXISTS temp.e2e_history_clone`,
		fmt.Sprintf(`CREATE TEMP TABLE e2e_history_clone AS SELECT * FROM job_history WHERE job_id = %d`, from),
		fmt.Sprintf(`UPDATE e2e_history_clone SET job_id = %d`, to),
		`INSERT INTO job_history SELECT * FROM e2e_history_clone`,
		`DROP TABLE temp.e2e_history_clone`,
	}
}

// migrationVersions is every version the ledger records as applied.
func migrationVersions(dbPath string) (map[int]bool, error) {
	out := map[int]bool{}
	err := withStateDB(dbPath, func(db *sql.DB) error {
		rows, err := db.Query(`SELECT version FROM _sqlx_migrations WHERE success`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var version int
			if err := rows.Scan(&version); err != nil {
				return err
			}
			out[version] = true
		}
		return rows.Err()
	})
	return out, err
}

// plantedRowsLeft counts what is left of the planted rows: a table the upgrade
// dropped holds none.
func plantedRowsLeft(dbPath string, jobIDs []int) (map[string]int, error) {
	out := map[string]int{}
	err := withStateDB(dbPath, func(db *sql.DB) error {
		tables, err := existingTables(db)
		if err != nil {
			return err
		}
		for _, table := range plantedClassicTables {
			if !tables[table] {
				continue
			}
			for _, id := range jobIDs {
				var count int
				if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE job_id = ?`, table), id).Scan(&count); err != nil {
					return err
				}
				out[table] += count
			}
		}
		return nil
	})
	return out, err
}
