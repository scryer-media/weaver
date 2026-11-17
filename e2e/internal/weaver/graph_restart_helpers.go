// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
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
	"graph_held_parts",
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
// state it reads, never on elapsed time; the enclosing runner bounds a wait
// that never resolves.
func (ctx *restartCaseContext) pollFor(what string, check func() (bool, error)) error {
	for {
		done, err := check()
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if done {
			return nil
		}
		time.Sleep(chaosPollInterval)
	}
}

// pollJob is pollFor for a wait on one job's state short of its end: it
// also ends, failing with the state the job ended in, when the job ends
// first, as the state it waits for can no longer come.
func (ctx *restartCaseContext) pollJob(what, dbPath string, jobID int, check func() (bool, error)) error {
	return ctx.pollFor(what, func() (bool, error) {
		done, err := check()
		if err == nil && !done {
			err = endedFirst(dbPath, jobID)
		}
		return done, err
	})
}

// loginRefusalWait is what the queue says of a job while a fill server
// refuses the login: its state, what it waits on, and the bytes it has booked
// missing.
type loginRefusalWait struct {
	State       string `json:"state"`
	WaitReason  string `json:"downloadWaitReason"`
	FailedBytes uint64 `json:"failedBytes"`
}

// waitForLoginRefusalWait waits until the queue says the job waits on the
// news server at `port` refusing the login, and returns what it said then. A
// refusal is a wait for the server, not a verdict, until its give-up time.
func (ctx *restartCaseContext) waitForLoginRefusalWait(jobID int, port string) (loginRefusalWait, error) {
	var seen loginRefusalWait
	err := ctx.pollJob("the job reports it waits on the refusing server", caseDBPath(ctx), jobID, func() (bool, error) {
		var out struct {
			QueueItem *loginRefusalWait `json:"queueItem"`
		}
		if err := graphQLMutate(ctx.weaverURL, `query($id: Int!) { queueItem(id: $id) { state downloadWaitReason failedBytes } }`,
			map[string]any{"id": jobID}, &out); err != nil {
			return false, err
		}
		if out.QueueItem == nil {
			return false, nil
		}
		seen = *out.QueueItem
		servers, ok := strings.CutPrefix(seen.WaitReason, "login_refused:")
		if !ok {
			return false, nil
		}
		for _, server := range strings.Split(servers, ", ") {
			if strings.HasSuffix(server, ":"+port) {
				return true, nil
			}
		}
		return false, nil
	})
	return seen, err
}

func endedFirst(dbPath string, jobID int) error {
	status, ok := restartJobStatusFromDB(dbPath, jobID)
	if status = normalizeDBStatus(status); ok && (dbStatusTerminal(status) || status == "cancelled") {
		return fmt.Errorf("job %d ended %s first", jobID, status)
	}
	return nil
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

// stateConn is one connection to the case's state database. Statements are
// written with `?` placeholders and rebound to the datastore's own, and every
// statement runs on the same connection, so a temporary table one statement
// makes is there for the next.
type stateConn struct {
	conn      *sql.Conn
	datastore weaverDatastore
}

func (c *stateConn) Query(query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(context.Background(), rebindWeaverSQL(c.datastore, query), args...)
}

func (c *stateConn) QueryRow(query string, args ...any) *sql.Row {
	return c.conn.QueryRowContext(context.Background(), rebindWeaverSQL(c.datastore, query), args...)
}

func (c *stateConn) Exec(query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(context.Background(), rebindWeaverSQL(c.datastore, query), args...)
}

// withStateDB opens the case's state database for one read or write, on one
// connection of its own. The Postgres pool is shared, so the connection is
// taken from it rather than the pool narrowed.
func withStateDB(dbPath string, use func(*stateConn) error) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return err
	}
	defer closeWeaverStateDB(db, datastore)
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return use(&stateConn{conn: conn, datastore: datastore})
}

// admissionRow is what the active row says of one job's NZB admission.
type admissionRow struct {
	Status   string
	Admitted bool
	Error    string
}

func readAdmissionRows(dbPath string, ids []int) (map[int]admissionRow, error) {
	out := map[int]admissionRow{}
	err := withStateDB(dbPath, func(db *stateConn) error {
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
	err := withStateDB(dbPath, func(db *stateConn) error {
		return db.QueryRow(`SELECT COUNT(*) FROM graph_nodes WHERE job_id = ?`, jobID).Scan(&count)
	})
	return count, err
}

// existingTables is every table the database has.
func existingTables(db *stateConn) (map[string]bool, error) {
	query := `SELECT name FROM sqlite_master WHERE type = 'table'`
	if db.datastore == weaverDatastorePostgres {
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'`
	}
	rows, err := db.Query(query)
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
	return withStateDB(dbPath, func(db *stateConn) error {
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
	return withStateDB(dbPath, func(db *stateConn) error {
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

// tableDDL is a table an older schema carried, as that schema created it on
// SQLite.
type tableDDL struct {
	Name string
	DDL  string
}

var (
	sqliteIntegerType = regexp.MustCompile(`\bINTEGER\b`)
	sqliteBlobType    = regexp.MustCompile(`\bBLOB\b`)
)

// in is the table's DDL in the datastore's dialect. Postgres held the same
// tables with BIGINT for INTEGER, BYTEA for BLOB, and no WITHOUT ROWID.
func (table tableDDL) in(datastore weaverDatastore) string {
	if datastore != weaverDatastorePostgres {
		return table.DDL
	}
	ddl := strings.ReplaceAll(table.DDL, " WITHOUT ROWID", "")
	ddl = sqliteIntegerType.ReplaceAllString(ddl, "BIGINT")
	return sqliteBlobType.ReplaceAllString(ddl, "BYTEA")
}

// schemaShape is what a database holds now: its datastore, its tables, each
// table's columns, and each table's index definitions.
type schemaShape struct {
	Datastore weaverDatastore
	Tables    map[string]bool
	Columns   map[string]map[string]bool
	Indexes   map[string]map[string]string
}

// schemaRewind turns a database into the shape an older build left: the
// reverses of later Rust steps run first, the indexes, tables and columns
// later migrations added are dropped, the ones later migrations dropped are
// put back, and the migration ledger forgets every version from LedgerFrom
// on, so the current build applies them again.
type schemaRewind struct {
	Reverses      []string
	DropIndexes   []string
	DropTables    []string
	DropColumns   []tableColumn
	RestoreTables []tableDDL
	AddColumns    []columnAdd
	LedgerFrom    int
}

// rustStepReverse undoes one Rust migration step: the tables whose columns it
// reads from the shape, and the statements that undo the step on a database
// of that shape. Refuse, when set, reads the database before anything is
// planned and names a state the reverse cannot undo, so the rewind stops
// there instead of leaving the step's work in place. Rewrite, when set, reads
// the values the step changed and gives the statements that write them back;
// they run after every refusal and before the plan.
type rustStepReverse struct {
	Tables  []string
	Plan    func(shape schemaShape) []string
	Refuse  func(db *stateConn) error
	Rewrite func(db *stateConn) ([]string, error)
}

// rustStepReverses is every Rust migration step a rewind can undo, by hook.
var rustStepReverses = map[string]rustStepReverse{
	"move_nzbs_and_outlines_v85": {Tables: []string{"active_jobs", "job_history"}, Plan: reverseNzbMoveV85},
	// Migration 87 fills the release_name column its own SQL step adds. The
	// rewind drops that column, which undoes the fill with it.
	"fill_history_release_names_v87": {Plan: func(schemaShape) []string { return nil }},
	// Migration 88 fills the search_key column its own SQL step adds, and
	// the rewind drops that column. On Postgres the step also makes a
	// function and, where pg_trgm installs, an index: both go first, so the
	// step makes them again when it is replayed.
	"fill_history_search_keys_v88": {Plan: reverseHistorySearchV88},
	// Migration 89 writes history_retention_days = 0 when history has rows
	// and the key is not saved yet. No schema before 89 knew the key, so an
	// older build never left it: the rewind deletes it, and the replayed
	// step writes it again from the history the case left.
	"seed_history_retention_v89": {Plan: func(schemaShape) []string {
		return []string{`DELETE FROM settings WHERE key = 'history_retention_days'`}
	}},
	// Migration 92 deletes servers only past the limit of 50, and a list of
	// 50 or fewer it leaves as it is, so on such a list there is nothing to
	// undo. A list past the limit is one the rewind cannot put back: it is
	// refused, never passed through.
	"trim_servers_to_limit_v92": {Plan: func(schemaShape) []string { return nil }, Refuse: refuseServersPastLimitV92},
	// Migration 94 moves the global download limit from its setting into
	// network_bandwidth, and its SQL moves each egress's limit and quota into
	// new columns. Both go back before the rewind drops the new table and
	// columns. A database holding a value an older build cannot keep or
	// reads otherwise is refused: see bandwidthStatesV94 and the schedule
	// rules. A quota with no amount goes back as 0, which migration 53's
	// column holds in its place. A schedule rule that lifts a limit with null
	// goes back to 0, the only value the step turns into null.
	"move_bandwidth_to_networking_v94": {Plan: reverseGlobalBandwidthV94, Refuse: refuseBandwidthV94, Rewrite: rewriteScheduleLimitsV94},
	"egress_bandwidth_columns_v94":     {Tables: []string{"egress_interfaces"}, Plan: reverseEgressBandwidthV94},
	// Migration 105 fills tracker_hosts from torrent_trackers, two tables
	// made after the graph-engine migration that the rewind drops.
	"fill_tracker_hosts_v105": {Plan: func(schemaShape) []string { return nil }},
	// Migration 112 deletes a torrent setting. Torrents came with migration
	// 93, so a database from before it never held the key.
	"forget_saved_mapped_ports_v112": {Plan: func(schemaShape) []string { return nil }},
	// Migration 125 turns each proxy's DNS setting into server lists and
	// profiles, and takes it off the proxy. A proxy that kept the default
	// gets no row, and only keys meaning that default leave its config. A
	// list or profile is a setting the rewind cannot write back onto the
	// proxies, so a database holding one is refused.
	"dns_profiles_from_proxies_v125": {Plan: func(schemaShape) []string { return nil }, Refuse: refuseDNSProfilesV125},
}

// migrationStatement is one SQL statement of one migration, its whitespace
// collapsed.
type migrationStatement struct {
	Version   int
	Statement string
}

// sqlStepReverses names the reverse in rustStepReverses that undoes each SQL
// statement on a table that was there before its migration, other than a
// created table, index or added column. Each statement is listed whole, so
// one the reverse was not written for is an error, not passed through.
var sqlStepReverses = map[migrationStatement]string{
	{94, "UPDATE egress_interfaces SET download_bps = NULLIF(max_download_speed, 0), quota_download_bytes = download_quota_limit_bytes, quota_download_enabled = download_quota_enabled"}: "egress_bandwidth_columns_v94",
	{94, "ALTER TABLE egress_interfaces RENAME COLUMN download_quota_period TO quota_period"}:                                                                                             "egress_bandwidth_columns_v94",
	{94, "ALTER TABLE egress_interfaces RENAME COLUMN download_quota_reset_time_minutes_local TO quota_reset_time_minutes_local"}:                                                         "egress_bandwidth_columns_v94",
	{94, "ALTER TABLE egress_interfaces RENAME COLUMN download_quota_weekly_reset_weekday TO quota_weekly_reset_weekday"}:                                                                 "egress_bandwidth_columns_v94",
	{94, "ALTER TABLE egress_interfaces RENAME COLUMN download_quota_monthly_reset_day TO quota_monthly_reset_day"}:                                                                       "egress_bandwidth_columns_v94",
	{94, "ALTER TABLE egress_interfaces DROP COLUMN max_download_speed"}:                                                                                                                  "egress_bandwidth_columns_v94",
	{94, "ALTER TABLE egress_interfaces DROP COLUMN download_quota_enabled"}:                                                                                                              "egress_bandwidth_columns_v94",
	{94, "ALTER TABLE egress_interfaces DROP COLUMN download_quota_limit_bytes"}:                                                                                                          "egress_bandwidth_columns_v94",
	{112, "DELETE FROM settings WHERE key = 'torrent.mapped_ports'"}:                                                                                                                      "forget_saved_mapped_ports_v112",
}

// collapseSQL is a statement with each run of whitespace one space.
func collapseSQL(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}

// egressColumnsV94 are the egress columns migration 94 renames, older name first.
var egressColumnsV94 = [][2]string{
	{"download_quota_period", "quota_period"},
	{"download_quota_reset_time_minutes_local", "quota_reset_time_minutes_local"},
	{"download_quota_weekly_reset_weekday", "quota_weekly_reset_weekday"},
	{"download_quota_monthly_reset_day", "quota_monthly_reset_day"},
}

// droppedEgressColumnsV94 are the egress columns migration 94 drops, as
// migration 53 made them; a column added to a table holding rows needs its
// default.
var droppedEgressColumnsV94 = [][2]string{
	{"max_download_speed", "BIGINT NOT NULL DEFAULT 0 CHECK (max_download_speed >= 0)"},
	{"download_quota_enabled", "BOOLEAN NOT NULL DEFAULT FALSE"},
	{"download_quota_limit_bytes", "BIGINT NOT NULL DEFAULT 0 CHECK (download_quota_limit_bytes >= 0)"},
}

func reverseGlobalBandwidthV94(shape schemaShape) []string {
	if !shape.Tables["network_bandwidth"] {
		return nil
	}
	return []string{
		`INSERT INTO settings (key, value) SELECT 'max_download_speed', CAST(download_bps AS TEXT) FROM network_bandwidth WHERE id = 1 AND download_bps IS NOT NULL ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
	}
}

func reverseEgressBandwidthV94(shape schemaShape) []string {
	columns := shape.Columns["egress_interfaces"]
	if columns == nil {
		return nil
	}
	var statements []string
	for _, rename := range egressColumnsV94 {
		if columns[rename[1]] && !columns[rename[0]] {
			statements = append(statements, fmt.Sprintf(`ALTER TABLE egress_interfaces RENAME COLUMN %s TO %s`, rename[1], rename[0]))
		}
	}
	for _, add := range droppedEgressColumnsV94 {
		if !columns[add[0]] {
			statements = append(statements, fmt.Sprintf(`ALTER TABLE egress_interfaces ADD COLUMN %s %s`, add[0], add[1]))
		}
	}
	if columns["download_bps"] {
		statements = append(statements, `UPDATE egress_interfaces SET max_download_speed = COALESCE(download_bps, 0), download_quota_enabled = quota_download_enabled, download_quota_limit_bytes = COALESCE(quota_download_bytes, 0)`)
	}
	return statements
}

// bandwidthStateV94 is a value an older build has no place for, or spells
// another way: where it is kept, the rows holding it, and what it is.
type bandwidthStateV94 struct {
	Table, Column, Where, Says string
}

// bandwidthStatesV94 is every bandwidth value from migration 94 on that the
// rewind cannot put back. Upload limits, upload quotas and upload counters
// came with 94, and the usenet split with 95: an older build drops them. A
// limit of 0 lets nothing through since 94 and meant no limit before it.
var bandwidthStatesV94 = []bandwidthStateV94{
	{"network_bandwidth", "download_bps", "download_bps = 0", "the global download limit is 0, which lets nothing through since migration 94 and meant no limit before it"},
	{"network_bandwidth", "upload_bps", "upload_bps IS NOT NULL", "the global upload limit is set"},
	{"network_bandwidth", "usenet_split_pct", "usenet_split_pct IS NOT NULL", "the global download limit is split between usenet and torrents"},
	{"egress_interfaces", "download_bps", "download_bps = 0", "egresses have a download limit of 0, which lets nothing through since migration 94 and meant no limit before it"},
	{"egress_interfaces", "upload_bps", "upload_bps IS NOT NULL", "egresses have an upload limit"},
	{"egress_interfaces", "quota_upload_soft_bytes", "quota_upload_soft_bytes IS NOT NULL", "egresses have an upload quota"},
	{"egress_interfaces", "quota_upload_hard_bytes", "quota_upload_hard_bytes IS NOT NULL", "egresses have a hard upload quota"},
	{"egress_interfaces", "quota_upload_trickle_bps", "quota_upload_trickle_bps <> 2048", "egresses have an upload trickle other than the default"},
	{"egress_download_usage", "upload_lifetime_bytes", "upload_lifetime_bytes <> 0", "egresses have counted uploads"},
	{"egress_download_usage", "upload_quota_baseline_bytes", "upload_quota_baseline_bytes <> 0", "egresses have an upload quota baseline"},
}

func refuseBandwidthV94(db *stateConn) error {
	tables, err := existingTables(db)
	if err != nil {
		return err
	}
	columns := map[string]map[string]bool{}
	for _, state := range bandwidthStatesV94 {
		if !tables[state.Table] {
			continue
		}
		if columns[state.Table] == nil {
			if columns[state.Table], err = tableColumns(db, state.Table); err != nil {
				return err
			}
		}
		if !columns[state.Table][state.Column] {
			continue
		}
		var count int
		if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`, state.Table, state.Where)).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("%d rows of %s: %s; the rewind cannot put it back", count, state.Table, state.Says)
		}
	}
	raw, found, err := savedSchedules(db)
	if err != nil || !found {
		return err
	}
	return refuseScheduleLimitsV94(raw)
}

// savedSchedules is the saved schedules setting, and whether it is saved.
func savedSchedules(db *stateConn) (string, bool, error) {
	var raw string
	switch err := db.QueryRow(`SELECT value FROM settings WHERE key = 'schedules'`).Scan(&raw); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	return raw, true, nil
}

// scheduleActionsSinceV94 are the schedule actions that came with migrations
// 94 and 95, which an older build cannot read.
var scheduleActionsSinceV94 = map[string]string{
	"bandwidth_split": "shares the download limit between usenet and torrents, which came with migration 95",
}

// refuseScheduleLimitsV94 refuses a saved schedule a rule of which an older
// build cannot read or reads otherwise. Since 94, 0 lets nothing through;
// before it 0 lifted the limit. An upload limit came with 94, and the split
// action with 95. Schedules that cannot be read are left to the replayed
// step, which leaves them as they are.
func refuseScheduleLimitsV94(raw string) error {
	var rules []struct {
		Action struct {
			Type   string                       `json:"type"`
			Limits []map[string]json.RawMessage `json:"limits"`
		} `json:"action"`
	}
	if json.Unmarshal([]byte(raw), &rules) != nil {
		return nil
	}
	for _, rule := range rules {
		if says, newer := scheduleActionsSinceV94[rule.Action.Type]; newer {
			return fmt.Errorf("a schedule rule %s; the rewind cannot put it back", says)
		}
		if rule.Action.Type != "speed_limit" {
			continue
		}
		for _, limit := range rule.Action.Limits {
			if string(limit["bytes_per_sec"]) == "0" {
				return fmt.Errorf("a schedule rule lets nothing through with 0, which before migration 94 lifted the limit; the rewind cannot put it back")
			}
			if upload, set := limit["upload_bytes_per_sec"]; set && string(upload) != "null" {
				return fmt.Errorf("a schedule rule sets an upload limit, which came with migration 94; the rewind cannot put it back")
			}
		}
	}
	return nil
}

// rewriteScheduleLimitsV94 writes each speed rule's null limit back as 0, how
// a build before 94 lifts a limit. The replayed step turns each 0 into null
// again, and no other value, so the round trip is exact.
func rewriteScheduleLimitsV94(db *stateConn) ([]string, error) {
	raw, found, err := savedSchedules(db)
	if err != nil || !found {
		return nil, err
	}
	rewritten, changed := schedulesWithZeroLimits(raw)
	if !changed {
		return nil, nil
	}
	return []string{fmt.Sprintf(`UPDATE settings SET value = '%s' WHERE key = 'schedules'`, strings.ReplaceAll(rewritten, "'", "''"))}, nil
}

// schedulesWithZeroLimits is the saved schedules with each speed rule's null
// limit written as 0, and whether any was. Schedules that cannot be read are
// left as they are.
func schedulesWithZeroLimits(raw string) (string, bool) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var rules []map[string]any
	if decoder.Decode(&rules) != nil {
		return raw, false
	}
	changed := false
	for _, rule := range rules {
		action, _ := rule["action"].(map[string]any)
		if action == nil || action["type"] != "speed_limit" {
			continue
		}
		limits, _ := action["limits"].([]any)
		for _, entry := range limits {
			limit, _ := entry.(map[string]any)
			if download, set := limit["bytes_per_sec"]; set && download == nil {
				limit["bytes_per_sec"] = json.Number("0")
				changed = true
			}
		}
	}
	if !changed {
		return raw, false
	}
	rewritten, err := json.Marshal(rules)
	if err != nil {
		return raw, false
	}
	return string(rewritten), true
}

func refuseDNSProfilesV125(db *stateConn) error {
	tables, err := existingTables(db)
	if err != nil {
		return err
	}
	for _, table := range []string{"dns_server_lists", "dns_profiles"} {
		if !tables[table] {
			continue
		}
		var count int
		if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("the database stores %d rows in %s, DNS settings the rewind cannot write back onto the proxies they came from", count, table)
		}
	}
	return nil
}

// serverLimitV92 is the server limit migration 92 trims a stored list to.
const serverLimitV92 = 50

func refuseServersPastLimitV92(db *stateConn) error {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM servers`).Scan(&count); err != nil {
		return err
	}
	if count > serverLimitV92 {
		return fmt.Errorf("the database stores %d servers, past the limit of %d migration 92 trims to; the rewind cannot undo that trim", count, serverLimitV92)
	}
	return nil
}

func reverseHistorySearchV88(shape schemaShape) []string {
	if shape.Datastore != weaverDatastorePostgres {
		return nil
	}
	return []string{
		`DROP INDEX IF EXISTS idx_job_history_search_key_trgm`,
		`DROP FUNCTION IF EXISTS weaver_history_fuzzy`,
	}
}

// reverseNzbMoveV85 undoes migration 85: each job's stored NZB goes back
// from job_nzbs onto its active and history rows, in the nullable column
// those rows held it in, before the rewind drops job_nzbs. The outlines are
// not put back: a job with no outline reads its stored NZB again.
func reverseNzbMoveV85(shape schemaShape) []string {
	blob := "BLOB"
	if shape.Datastore == weaverDatastorePostgres {
		blob = "BYTEA"
	}
	var statements []string
	for _, table := range []string{"active_jobs", "job_history"} {
		if !shape.Tables[table] {
			continue
		}
		if !shape.Columns[table]["nzb_zstd"] {
			statements = append(statements, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN nzb_zstd %s`, table, blob))
		}
		if shape.Tables["job_nzbs"] {
			statements = append(statements, fmt.Sprintf(`UPDATE %[1]s SET nzb_zstd = (SELECT n.nzb_zstd FROM job_nzbs AS n WHERE n.job_id = %[1]s.job_id) WHERE nzb_zstd IS NULL`, table))
		}
	}
	return statements
}

// plan is the statements that rewind a database of the given shape. Only what
// is there is dropped and only what is missing is put back, so the same rewind
// serves whether or not a later migration has already run.
func (rewind schemaRewind) plan(shape schemaShape) []string {
	var statements []string
	for _, hook := range rewind.Reverses {
		statements = append(statements, rustStepReverses[hook].Plan(shape)...)
	}
	for _, index := range rewind.DropIndexes {
		statements = append(statements, fmt.Sprintf(`DROP INDEX IF EXISTS %s`, index))
	}
	droppedTable := map[string]bool{}
	for _, table := range rewind.DropTables {
		droppedTable[table] = true
	}
	for _, drop := range rewind.DropColumns {
		if droppedTable[drop.Table] || !shape.Columns[drop.Table][drop.Column] {
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
			statements = append(statements, table.in(shape.Datastore))
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

// tableColumns is the set of a table's column names.
func tableColumns(db *stateConn, table string) (map[string]bool, error) {
	query := fmt.Sprintf(`SELECT name FROM pragma_table_info('%s')`, table)
	if db.datastore == weaverDatastorePostgres {
		query = fmt.Sprintf(`SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '%s'`, table)
	}
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns[name] = true
	}
	return columns, rows.Err()
}

// readSchemaShape reads the shape of the tables the rewind touches.
func readSchemaShape(db *stateConn, rewind schemaRewind) (schemaShape, error) {
	tables, err := existingTables(db)
	if err != nil {
		return schemaShape{}, err
	}
	shape := schemaShape{Datastore: db.datastore, Tables: tables, Columns: map[string]map[string]bool{}, Indexes: map[string]map[string]string{}}
	touched := map[string]bool{}
	for _, drop := range rewind.DropColumns {
		touched[drop.Table] = true
	}
	for _, add := range rewind.AddColumns {
		touched[add.Table] = true
	}
	for _, hook := range rewind.Reverses {
		for _, table := range rustStepReverses[hook].Tables {
			touched[table] = true
		}
	}
	for table := range touched {
		if !tables[table] {
			continue
		}
		columns, err := tableColumns(db, table)
		if err != nil {
			return schemaShape{}, err
		}
		shape.Columns[table] = columns
		indexes := map[string]string{}
		shape.Indexes[table] = indexes
		// Postgres drops every index on a column with the column, so only
		// SQLite's have to be named and dropped first.
		if db.datastore == weaverDatastorePostgres {
			continue
		}
		rows, err := db.Query(`SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL`, table)
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
	}
	return shape, nil
}

// applySchemaRewind rewinds the database and then runs the statements that
// plant what the older build would have left in it.
func applySchemaRewind(dbPath string, rewind schemaRewind, plant []string) error {
	return withStateDB(dbPath, func(db *stateConn) error {
		for _, hook := range rewind.Reverses {
			if refuse := rustStepReverses[hook].Refuse; refuse != nil {
				if err := refuse(db); err != nil {
					return fmt.Errorf("reverse of %s: %w", hook, err)
				}
			}
		}
		var rewrites []string
		for _, hook := range rewind.Reverses {
			if rewrite := rustStepReverses[hook].Rewrite; rewrite != nil {
				statements, err := rewrite(db)
				if err != nil {
					return fmt.Errorf("reverse of %s: %w", hook, err)
				}
				rewrites = append(rewrites, statements...)
			}
		}
		shape, err := readSchemaShape(db, rewind)
		if err != nil {
			return err
		}
		for _, statement := range append(append(rewrites, rewind.plan(shape)...), plant...) {
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
// checkpoint, and it still has every classic-only table. The fixed part
// undoes migrations 66 to 74; what came after the graph-engine migration is
// undone as the later migrations say, the same way the engine-stamped rewind
// undoes it.
func rewindTo0165(later laterMigrations) schemaRewind {
	return schemaRewind{
		Reverses:    later.Hooks,
		DropIndexes: later.Indexes,
		DropTables:  appendNew(append([]string(nil), graphTables...), later.Tables...),
		DropColumns: appendNew([]tableColumn{
			{"active_jobs", "engine"},
			{"active_jobs", "root_phase"},
			{"active_jobs", "nzb_admitted"},
			{"active_jobs", "nzb_release_pending"},
			{"job_history", "engine"},
		}, later.Columns...),
		RestoreTables: classicOnlyTables,
		LedgerFrom:    66,
	}
}

// appendNew appends each item base does not already hold.
func appendNew[T comparable](base []T, items ...T) []T {
	seen := map[T]bool{}
	for _, item := range base {
		seen[item] = true
	}
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			base = append(base, item)
		}
	}
	return base
}

// graphEngineMigration is the migration that leaves the graph engine the only
// engine: it drops active_jobs.engine, job_history.engine and the ten
// classicOnlyTables.
const graphEngineMigration = 74

// weaverMigrationsDir is the migration folder of the weaver under test, the
// one its manifest and SQL steps are read from.
func weaverMigrationsDir() string {
	return filepath.Join(weaverRepoPath(), "server", "crates", "weaver-server-core", "src", "db", "migrations")
}

// migrationStep is one step of a migration as the manifest lists it: its kind
// and, for an SQL step, its file relative to the migration folder's parent
// and the engine it runs on, when it names one.
type migrationStep struct {
	Kind   string
	File   string
	HookID string
	Engine string
}

// migrationEntry is one migration the manifest lists, with its steps.
type migrationEntry struct {
	Version int
	Steps   []migrationStep
}

var (
	manifestMigrationLine = regexp.MustCompile(`^\[\[migration\]\]$`)
	manifestStepLine      = regexp.MustCompile(`^\[\[migration\.steps\]\]$`)
	manifestVersionLine   = regexp.MustCompile(`^version\s*=\s*(\d+)$`)
	manifestFileLine      = regexp.MustCompile(`^file\s*=\s*"([^"]+)"$`)
	manifestKindLine      = regexp.MustCompile(`^kind\s*=\s*"([^"]+)"$`)
	manifestHookLine      = regexp.MustCompile(`^hook_id\s*=\s*"([^"]+)"$`)
	manifestEngineLine    = regexp.MustCompile(`^engine\s*=\s*"([^"]+)"$`)
	sqlLineComment        = regexp.MustCompile(`--[^\n]*`)
	sqlCreateTable        = regexp.MustCompile(`(?is)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\(`)
	sqlCreateIndex        = regexp.MustCompile(`(?is)^CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\b`)
	sqlDropIndex          = regexp.MustCompile(`(?is)^DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?\s*$`)
	sqlWriteTarget        = regexp.MustCompile(`(?is)^(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|DROP\s+TABLE(?:\s+IF\s+EXISTS)?|ALTER\s+TABLE)\s+(\w+)\b`)
	sqlRenameTable        = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+\w+\s+RENAME\s+TO\s+(\w+)\s*$`)
	sqlAddColumn          = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\b`)
	sqlDropTable          = regexp.MustCompile(`(?is)^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)\s*$`)
	sqlDropColumn         = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+(\w+)\s+DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?(\w+)\s*$`)
	sqlRenameTableFrom    = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+(\w+)\s+RENAME\s+TO\s+(\w+)\s*$`)
	sqlRenameColumn       = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+(\w+)\s+RENAME\s+COLUMN\s+(\w+)\s+TO\s+(\w+)\s*$`)
)

// readMigrationManifest is every migration the manifest in dir lists, in its
// order.
func readMigrationManifest(dir string) ([]migrationEntry, error) {
	path := filepath.Join(dir, "manifest.toml")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []migrationEntry
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		switch {
		case manifestMigrationLine.MatchString(line):
			entries = append(entries, migrationEntry{})
			continue
		case len(entries) == 0:
			continue
		}
		entry := &entries[len(entries)-1]
		if manifestStepLine.MatchString(line) {
			entry.Steps = append(entry.Steps, migrationStep{})
			continue
		}
		if match := manifestVersionLine.FindStringSubmatch(line); match != nil && len(entry.Steps) == 0 {
			entry.Version, _ = strconv.Atoi(match[1])
			continue
		}
		if len(entry.Steps) == 0 {
			continue
		}
		step := &entry.Steps[len(entry.Steps)-1]
		if match := manifestKindLine.FindStringSubmatch(line); match != nil {
			step.Kind = match[1]
		} else if match := manifestFileLine.FindStringSubmatch(line); match != nil {
			step.File = match[1]
		} else if match := manifestHookLine.FindStringSubmatch(line); match != nil {
			step.HookID = match[1]
		} else if match := manifestEngineLine.FindStringSubmatch(line); match != nil {
			step.Engine = match[1]
		}
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%s lists no migration", path)
	}
	for _, entry := range entries {
		if entry.Version == 0 {
			return nil, fmt.Errorf("%s lists a migration with no version", path)
		}
	}
	return entries, nil
}

// latestMigration is the last version the manifest in dir lists.
func latestMigration(dir string) (int, error) {
	entries, err := readMigrationManifest(dir)
	if err != nil {
		return 0, err
	}
	latest := 0
	for _, entry := range entries {
		if entry.Version > latest {
			latest = entry.Version
		}
	}
	return latest, nil
}

// laterMigrations is what the migrations after a version do: the tables
// they create, the columns they add, the indexes they create, and the Rust
// steps they run, each named once.
type laterMigrations struct {
	Tables  []string
	Columns []tableColumn
	Indexes []string
	Hooks   []string
}

// readLaterMigrations reads every step of each migration the manifest in dir
// lists after the given version. A rewind can undo only a table created, a
// column added, an index created (and dropped again by a later one of these
// migrations), or a Rust step rustStepReverses names, so a
// later step of any other kind is an error naming it, never something the
// rewind leaves in place.
func readLaterMigrations(dir string, after int) (laterMigrations, error) {
	entries, err := readMigrationManifest(dir)
	if err != nil {
		return laterMigrations{}, err
	}
	earlier, err := readEarlierObjects(dir, entries, after)
	if err != nil {
		return laterMigrations{}, err
	}
	var out laterMigrations
	seenTable := map[string]bool{}
	seenColumn := map[tableColumn]bool{}
	for _, entry := range entries {
		if entry.Version <= after {
			continue
		}
		// The rewind drops what a later migration creates. One that names, IF
		// NOT EXISTS, a table, index or column an earlier migration made would
		// have it dropped from under the older build.
		if made, found, err := earlierObjectRemade(dir, entry, earlier); err != nil {
			return laterMigrations{}, err
		} else if found {
			return laterMigrations{}, fmt.Errorf("migration %d creates %s, which a migration at or before %d made already; the rewind would drop it", entry.Version, made, after)
		}
		for _, step := range entry.Steps {
			if step.Kind == "rust" {
				if _, known := rustStepReverses[step.HookID]; !known {
					return laterMigrations{}, fmt.Errorf("migration %d has a %q step (hook %q) the rewind cannot undo", entry.Version, step.Kind, step.HookID)
				}
				out.Hooks = appendNew(out.Hooks, step.HookID)
				continue
			}
			if step.Kind != "sql" {
				return laterMigrations{}, fmt.Errorf("migration %d has a %q step the rewind cannot undo", entry.Version, step.Kind)
			}
			data, err := os.ReadFile(filepath.Join(filepath.Dir(dir), filepath.FromSlash(step.File)))
			if err != nil {
				return laterMigrations{}, fmt.Errorf("migration %d: %w", entry.Version, err)
			}
			for _, statement := range strings.Split(sqlLineComment.ReplaceAllString(string(data), ""), ";") {
				statement = strings.TrimSpace(statement)
				if statement == "" {
					continue
				}
				if create := sqlCreateTable.FindStringSubmatch(statement); create != nil {
					if !seenTable[create[1]] {
						seenTable[create[1]] = true
						out.Tables = append(out.Tables, create[1])
					}
					continue
				}
				if index := sqlCreateIndex.FindStringSubmatch(statement); index != nil {
					out.Indexes = appendNew(out.Indexes, index[1])
					continue
				}
				// An index a later migration made and a still later one drops
				// is gone either way once the rewind drops it. One made at or
				// before the version rewound to would have to be made again.
				if drop := sqlDropIndex.FindStringSubmatch(statement); drop != nil && slices.Contains(out.Indexes, drop[1]) {
					continue
				}
				if add := sqlAddColumn.FindStringSubmatch(statement); add != nil {
					column := tableColumn{add[1], add[2]}
					if !seenColumn[column] {
						seenColumn[column] = true
						out.Columns = append(out.Columns, column)
					}
					continue
				}
				if write := sqlWriteTarget.FindStringSubmatch(statement); write != nil {
					// A table one of these migrations made goes with the
					// rewind, and what was written to it with it; a table it
					// is renamed to goes too.
					if seenTable[write[1]] {
						if rename := sqlRenameTable.FindStringSubmatch(statement); rename != nil && !seenTable[rename[1]] {
							seenTable[rename[1]] = true
							out.Tables = append(out.Tables, rename[1])
						}
						continue
					}
					if reverse, known := sqlStepReverses[migrationStatement{entry.Version, collapseSQL(statement)}]; known {
						out.Hooks = appendNew(out.Hooks, reverse)
						continue
					}
				}
				return laterMigrations{}, fmt.Errorf("migration %d has a statement the rewind cannot undo: %s", entry.Version, statement)
			}
		}
	}
	// A table is dropped before the tables it references, which were made
	// before it.
	slices.Reverse(out.Tables)
	return out, nil
}

// migrationStatements is each SQL statement of a step, its comments removed.
func migrationStatements(dir string, version int, step migrationStep) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(filepath.Dir(dir), filepath.FromSlash(step.File)))
	if err != nil {
		return nil, fmt.Errorf("migration %d: %w", version, err)
	}
	var statements []string
	for _, statement := range strings.Split(sqlLineComment.ReplaceAllString(string(data), ""), ";") {
		if statement = strings.TrimSpace(statement); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements, nil
}

// createdObject names the table, index or column a statement makes, or "".
func createdObject(statement string) string {
	if create := sqlCreateTable.FindStringSubmatch(statement); create != nil {
		return "table " + create[1]
	}
	if index := sqlCreateIndex.FindStringSubmatch(statement); index != nil {
		return "index " + index[1]
	}
	if add := sqlAddColumn.FindStringSubmatch(statement); add != nil {
		return "column " + add[1] + "." + add[2]
	}
	return ""
}

// droppedObject names the table, index or column a statement drops, or "".
func droppedObject(statement string) string {
	if drop := sqlDropTable.FindStringSubmatch(statement); drop != nil {
		return "table " + drop[1]
	}
	if drop := sqlDropIndex.FindStringSubmatch(statement); drop != nil {
		return "index " + drop[1]
	}
	if drop := sqlDropColumn.FindStringSubmatch(statement); drop != nil {
		return "column " + drop[1] + "." + drop[2]
	}
	return ""
}

// readEarlierObjects is every table, index and column the SQL of the
// migrations at or before the given version leaves in place.
func readEarlierObjects(dir string, entries []migrationEntry, after int) (map[string]bool, error) {
	objects := map[string]bool{}
	for _, entry := range entries {
		if entry.Version > after {
			continue
		}
		for _, step := range entry.Steps {
			if step.Kind != "sql" {
				continue
			}
			statements, err := migrationStatements(dir, entry.Version, step)
			if err != nil {
				return nil, err
			}
			for _, statement := range statements {
				if made := createdObject(statement); made != "" {
					objects[made] = true
				} else if dropped := droppedObject(statement); dropped != "" {
					delete(objects, dropped)
					if table, ok := strings.CutPrefix(dropped, "table "); ok {
						for object := range objects {
							if strings.HasPrefix(object, "column "+table+".") {
								delete(objects, object)
							}
						}
					}
				} else if rename := sqlRenameTableFrom.FindStringSubmatch(statement); rename != nil {
					delete(objects, "table "+rename[1])
					objects["table "+rename[2]] = true
				} else if rename := sqlRenameColumn.FindStringSubmatch(statement); rename != nil {
					delete(objects, "column "+rename[1]+"."+rename[2])
					objects["column "+rename[1]+"."+rename[3]] = true
				}
			}
		}
	}
	return objects, nil
}

// earlierObjectRemade is the first table, index or column a later migration
// creates that an earlier one left in place.
func earlierObjectRemade(dir string, entry migrationEntry, earlier map[string]bool) (string, bool, error) {
	for _, step := range entry.Steps {
		if step.Kind != "sql" {
			continue
		}
		statements, err := migrationStatements(dir, entry.Version, step)
		if err != nil {
			return "", false, err
		}
		for _, statement := range statements {
			if made := createdObject(statement); made != "" && earlier[made] {
				return made, true, nil
			}
		}
	}
	return "", false, nil
}

// preGraphEngineRewind is a database written before the graph-engine
// migration: the engine stamps and every classic-only table are there, and
// nothing a later migration added is.
func preGraphEngineRewind(later laterMigrations) schemaRewind {
	return schemaRewind{
		Reverses:      later.Hooks,
		DropIndexes:   later.Indexes,
		DropTables:    later.Tables,
		DropColumns:   later.Columns,
		RestoreTables: classicOnlyTables,
		AddColumns: []columnAdd{
			{"active_jobs", "engine", "TEXT NOT NULL DEFAULT 'classic'"},
			{"job_history", "engine", "TEXT NOT NULL DEFAULT 'classic'"},
		},
		LedgerFrom: graphEngineMigration,
	}
}

// cloneHistoryRow copies a history row under another job id, every column
// as it stands, so a case has a second history row without a second
// download.
func cloneHistoryRow(from, to int) []string {
	return []string{
		`DROP TABLE IF EXISTS e2e_history_clone`,
		fmt.Sprintf(`CREATE TEMP TABLE e2e_history_clone AS SELECT * FROM job_history WHERE job_id = %d`, from),
		fmt.Sprintf(`UPDATE e2e_history_clone SET job_id = %d`, to),
		`INSERT INTO job_history SELECT * FROM e2e_history_clone`,
		`DROP TABLE e2e_history_clone`,
	}
}

// refuseArchive is the statements that make the database refuse the job's
// history row, as a write that cannot land would, and the statements that
// lift the refusal, in the current datastore's dialect.
func refuseArchive(jobID int) (refuse, lift []string) {
	if currentWeaverDatastore() == weaverDatastorePostgres {
		return []string{
			fmt.Sprintf(`CREATE FUNCTION e2e_refuse_archive() RETURNS trigger AS $body$ BEGIN IF NEW.job_id = %d THEN RAISE EXCEPTION 'e2e injected archive failure'; END IF; RETURN NEW; END $body$ LANGUAGE plpgsql`, jobID),
			`CREATE TRIGGER e2e_refuse_archive BEFORE INSERT ON job_history FOR EACH ROW EXECUTE FUNCTION e2e_refuse_archive()`,
		}, []string{
			`DROP TRIGGER e2e_refuse_archive ON job_history`,
			`DROP FUNCTION e2e_refuse_archive()`,
		}
	}
	return []string{
		fmt.Sprintf(`CREATE TRIGGER e2e_refuse_archive BEFORE INSERT ON job_history WHEN NEW.job_id = %d BEGIN SELECT RAISE(ABORT, 'e2e injected archive failure'); END`, jobID),
	}, []string{
		`DROP TRIGGER e2e_refuse_archive`,
	}
}

// migrationVersions is every version the ledger records as applied.
func migrationVersions(dbPath string) (map[int]bool, error) {
	out := map[int]bool{}
	err := withStateDB(dbPath, func(db *stateConn) error {
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
	err := withStateDB(dbPath, func(db *stateConn) error {
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
