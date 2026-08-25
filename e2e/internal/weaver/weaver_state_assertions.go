package weaver

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/blake3"
	_ "modernc.org/sqlite"
)

type weaverActiveStateTable struct {
	Name string
}

var weaverActiveStateTables = []weaverActiveStateTable{
	{Name: "active_jobs"},
	{Name: "active_file_progress"},
	{Name: "active_files"},
	{Name: "active_par2"},
	{Name: "active_par2_files"},
	{Name: "active_file_identities"},
	{Name: "active_extracted"},
	{Name: "active_failed_extractions"},
	{Name: "active_extraction_chunks"},
	{Name: "active_archive_headers"},
	{Name: "active_rar_volume_facts"},
	{Name: "active_volume_status"},
	{Name: "active_rar_verified_suspect"},
}

var requiredJobEventTimeout = 5 * time.Second
var requiredJobEventPollInterval = 50 * time.Millisecond
var requiredJobEventStabilityWindow = 500 * time.Millisecond
var terminalFixtureStateTimeout = 5 * time.Second
var terminalFixtureStatePollInterval = 100 * time.Millisecond

const (
	fileIdentityRewriteObserverTable         = "e2e_file_identity_rewrite_observations"
	fileIdentityRewriteObserverInsertTrigger = "e2e_file_identity_rewrite_observations_insert"
	fileIdentityRewriteObserverUpdateTrigger = "e2e_file_identity_rewrite_observations_update"
	fileIdentityRewriteObserverFunction      = "e2e_file_identity_rewrite_observations_capture"
	fileIdentityRewriteObservedAtEpochMS     = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
)

type fileIdentityRewriteObservation struct {
	Observed                     bool
	RequiredClassificationSource string
	MissingCurrentFilenames      []string
	ForbiddenCurrentFilenames    []string
	WrongClassificationSources   []string
	ObservedCurrentFilenames     []string
}

type fileIdentityRewriteObserver struct {
	db        *sql.DB
	datastore weaverDatastore
}

func localWeaverDBPath() string {
	return filepath.Join(localWeaverDir(), "weaver.db")
}

func installFileIdentityRewriteObserver(dbPath string) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return fmt.Errorf("open weaver state db: %w", err)
	}
	defer db.Close()

	statements := sqliteFileIdentityRewriteObserverStatements()
	if datastore == weaverDatastorePostgres {
		statements = postgresFileIdentityRewriteObserverStatements()
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("install file identity rewrite observer: %w", err)
		}
	}
	return nil
}

func openFileIdentityRewriteObserver(dbPath string) (*fileIdentityRewriteObserver, error) {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open weaver state db: %w", err)
	}
	return &fileIdentityRewriteObserver{
		db:        db,
		datastore: datastore,
	}, nil
}

func (o *fileIdentityRewriteObserver) Close() error {
	if o == nil || o.db == nil {
		return nil
	}
	return o.db.Close()
}

func (o *fileIdentityRewriteObserver) Observe(
	jobID int,
	assertion *ScenarioFileIdentityRewriteAssertion,
) (fileIdentityRewriteObservation, error) {
	if o == nil || o.db == nil {
		return newFileIdentityRewriteObservation(assertion), fmt.Errorf("file identity rewrite observer is closed")
	}
	return evaluateActiveFileIdentityRewrite(o.db, o.datastore, jobID, assertion)
}

func sqliteFileIdentityRewriteObserverStatements() []string {
	return []string{
		"DROP TRIGGER IF EXISTS " + fileIdentityRewriteObserverInsertTrigger,
		"DROP TRIGGER IF EXISTS " + fileIdentityRewriteObserverUpdateTrigger,
		"DROP TABLE IF EXISTS " + fileIdentityRewriteObserverTable,
		`CREATE TABLE ` + fileIdentityRewriteObserverTable + ` (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			observed_at_epoch_ms INTEGER NOT NULL,
			operation TEXT NOT NULL,
			job_id INTEGER NOT NULL,
			file_index INTEGER NOT NULL,
			source_filename TEXT NOT NULL,
			current_filename TEXT NOT NULL,
			canonical_filename TEXT,
			classification_kind TEXT,
			classification_set_name TEXT,
			classification_volume_index INTEGER,
			classification_source TEXT NOT NULL
		)`,
		`CREATE INDEX e2e_file_identity_rewrite_observations_job_id_idx
			ON ` + fileIdentityRewriteObserverTable + ` (job_id, current_filename, classification_source)`,
		`CREATE TRIGGER ` + fileIdentityRewriteObserverInsertTrigger + `
			AFTER INSERT ON active_file_identities
			BEGIN
				INSERT INTO ` + fileIdentityRewriteObserverTable + ` (
					observed_at_epoch_ms,
					operation,
					job_id,
					file_index,
					source_filename,
					current_filename,
					canonical_filename,
					classification_kind,
					classification_set_name,
					classification_volume_index,
					classification_source
				) VALUES (
					` + fileIdentityRewriteObservedAtEpochMS + `,
					'insert',
					NEW.job_id,
					NEW.file_index,
					NEW.source_filename,
					NEW.current_filename,
					NEW.canonical_filename,
					NEW.classification_kind,
					NEW.classification_set_name,
					NEW.classification_volume_index,
					NEW.classification_source
				);
			END`,
		`CREATE TRIGGER ` + fileIdentityRewriteObserverUpdateTrigger + `
			AFTER UPDATE ON active_file_identities
			BEGIN
				INSERT INTO ` + fileIdentityRewriteObserverTable + ` (
					observed_at_epoch_ms,
					operation,
					job_id,
					file_index,
					source_filename,
					current_filename,
					canonical_filename,
					classification_kind,
					classification_set_name,
					classification_volume_index,
					classification_source
				) VALUES (
					` + fileIdentityRewriteObservedAtEpochMS + `,
					'update',
					NEW.job_id,
					NEW.file_index,
					NEW.source_filename,
					NEW.current_filename,
					NEW.canonical_filename,
					NEW.classification_kind,
					NEW.classification_set_name,
					NEW.classification_volume_index,
					NEW.classification_source
				);
			END`,
	}
}

func postgresFileIdentityRewriteObserverStatements() []string {
	return []string{
		"DROP TRIGGER IF EXISTS " + fileIdentityRewriteObserverInsertTrigger + " ON active_file_identities",
		"DROP TRIGGER IF EXISTS " + fileIdentityRewriteObserverUpdateTrigger + " ON active_file_identities",
		"DROP FUNCTION IF EXISTS " + fileIdentityRewriteObserverFunction + "()",
		"DROP TABLE IF EXISTS " + fileIdentityRewriteObserverTable,
		`CREATE TABLE ` + fileIdentityRewriteObserverTable + ` (
			id BIGSERIAL PRIMARY KEY,
			observed_at_epoch_ms BIGINT NOT NULL,
			operation TEXT NOT NULL,
			job_id BIGINT NOT NULL,
			file_index INTEGER NOT NULL,
			source_filename TEXT NOT NULL,
			current_filename TEXT NOT NULL,
			canonical_filename TEXT,
			classification_kind TEXT,
			classification_set_name TEXT,
			classification_volume_index INTEGER,
			classification_source TEXT NOT NULL
		)`,
		`CREATE INDEX e2e_file_identity_rewrite_observations_job_id_idx
			ON ` + fileIdentityRewriteObserverTable + ` (job_id, current_filename, classification_source)`,
		`CREATE FUNCTION ` + fileIdentityRewriteObserverFunction + `()
			RETURNS trigger
			LANGUAGE plpgsql
			AS $$
			BEGIN
				INSERT INTO ` + fileIdentityRewriteObserverTable + ` (
					observed_at_epoch_ms,
					operation,
					job_id,
					file_index,
					source_filename,
					current_filename,
					canonical_filename,
					classification_kind,
					classification_set_name,
					classification_volume_index,
					classification_source
				) VALUES (
					(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
					lower(TG_OP),
					NEW.job_id,
					NEW.file_index,
					NEW.source_filename,
					NEW.current_filename,
					NEW.canonical_filename,
					NEW.classification_kind,
					NEW.classification_set_name,
					NEW.classification_volume_index,
					NEW.classification_source
				);
				RETURN NEW;
			END;
			$$`,
		`CREATE TRIGGER ` + fileIdentityRewriteObserverInsertTrigger + `
			AFTER INSERT ON active_file_identities
			FOR EACH ROW
			EXECUTE FUNCTION ` + fileIdentityRewriteObserverFunction + `()`,
		`CREATE TRIGGER ` + fileIdentityRewriteObserverUpdateTrigger + `
			AFTER UPDATE ON active_file_identities
			FOR EACH ROW
			EXECUTE FUNCTION ` + fileIdentityRewriteObserverFunction + `()`,
	}
}

func observeActiveFileIdentityRewrite(
	dbPath string,
	jobID int,
	assertion *ScenarioFileIdentityRewriteAssertion,
) (fileIdentityRewriteObservation, error) {
	observation := newFileIdentityRewriteObservation(assertion)
	if jobID <= 0 || assertion == nil || !assertion.enabled() {
		return observation, nil
	}

	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return observation, fmt.Errorf("open weaver state db: %w", err)
	}
	defer db.Close()

	return evaluateActiveFileIdentityRewrite(db, datastore, jobID, assertion)
}

func applyRuntimeFileIdentityRewriteTerminalCheck(
	status string,
	assertion *ScenarioFileIdentityRewriteAssertion,
	observed bool,
	observation fileIdentityRewriteObservation,
	lastQueryError string,
) (string, string, bool) {
	if assertion == nil || !assertion.enabled() || !facadeTerminalStatus(status) || observed {
		return "", "", false
	}
	return "RUNTIME_ASSERTION_ERROR", observation.terminalFailureMessage(status, lastQueryError), true
}

func assertTerminalFixtureState(dbPath string, jobID int, expectedStatus string) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return fmt.Errorf("open weaver state db: %w", err)
	}
	defer db.Close()

	historyStatus, historyFound, err := loadJobHistoryStatus(db, datastore, jobID)
	if err != nil {
		return err
	}
	if !historyFound {
		return fmt.Errorf("job %d missing job_history row", jobID)
	}
	if normalizeFacadeState(historyStatus) != normalizeFacadeState(expectedStatus) {
		return fmt.Errorf(
			"job %d history status mismatch: expected %s, found %s",
			jobID,
			normalizeFacadeState(expectedStatus),
			normalizeFacadeState(historyStatus),
		)
	}

	var lingering []string
	for _, table := range weaverActiveStateTables {
		count, err := countJobRows(db, datastore, table.Name, jobID)
		if err != nil {
			return err
		}
		if count > 0 {
			lingering = append(lingering, fmt.Sprintf("%s=%d", table.Name, count))
		}
	}
	if len(lingering) > 0 {
		return fmt.Errorf(
			"job %d archived as %s but lingering active state remains: %s",
			jobID,
			normalizeFacadeState(historyStatus),
			strings.Join(lingering, ", "),
		)
	}

	if err := assertNoOrphanActiveState(db); err != nil {
		return err
	}

	return nil
}

func assertNoLingeringWeaverActiveState(dbPath string) error {
	db, _, err := openWeaverStateDB(dbPath)
	if err != nil {
		return fmt.Errorf("open weaver state db: %w", err)
	}
	defer db.Close()

	var lingering []string
	for _, table := range weaverActiveStateTables {
		count, err := countTableRows(db, table.Name)
		if err != nil {
			return err
		}
		if count > 0 {
			lingering = append(lingering, fmt.Sprintf("%s=%d", table.Name, count))
		}
	}
	if len(lingering) > 0 {
		return fmt.Errorf("lingering active state remains: %s", strings.Join(lingering, ", "))
	}

	return nil
}

func assertNoOrphanActiveStatePath(dbPath string) error {
	db, _, err := openWeaverStateDB(dbPath)
	if err != nil {
		return fmt.Errorf("open weaver state db: %w", err)
	}
	defer db.Close()

	return assertNoOrphanActiveState(db)
}

func loadJobHistoryStatus(db *sql.DB, datastore weaverDatastore, jobID int) (string, bool, error) {
	var status string
	query := rebindWeaverSQL(datastore, `SELECT status FROM job_history WHERE job_id = ?`)
	err := db.QueryRow(query, jobID).Scan(&status)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load job_history status for job %d: %w", jobID, err)
	}
	return status, true, nil
}

func countTableRows(db *sql.DB, table string) (int, error) {
	var count int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)
	if err := db.QueryRow(query).Scan(&count); err != nil {
		return 0, fmt.Errorf("count rows in %s: %w", table, err)
	}
	return count, nil
}

func countJobRows(db *sql.DB, datastore weaverDatastore, table string, jobID int) (int, error) {
	var count int
	query := rebindWeaverSQL(datastore, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE job_id = ?`, table))
	if err := db.QueryRow(query, jobID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count rows in %s for job %d: %w", table, jobID, err)
	}
	return count, nil
}

func weaverStateTableExists(db *sql.DB, datastore weaverDatastore, table string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)`
	if datastore == weaverDatastorePostgres {
		query = `SELECT EXISTS(
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_name = ?
		)`
	}
	err := db.QueryRow(rebindWeaverSQL(datastore, query), table).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("inspect weaver state table %s: %w", table, err)
	}
	return exists, nil
}

func assertNoOrphanActiveState(db *sql.DB) error {
	var orphaned []string
	for _, table := range weaverActiveStateTables[1:] {
		var count int
		query := fmt.Sprintf(
			`SELECT COUNT(*) FROM %s WHERE NOT EXISTS (
				SELECT 1 FROM active_jobs WHERE active_jobs.job_id = %s.job_id
			)`,
			table.Name,
			table.Name,
		)
		if err := db.QueryRow(query).Scan(&count); err != nil {
			return fmt.Errorf("count orphan rows in %s: %w", table.Name, err)
		}
		if count > 0 {
			orphaned = append(orphaned, fmt.Sprintf("%s=%d", table.Name, count))
		}
	}
	if len(orphaned) > 0 {
		return fmt.Errorf("orphaned active rows remain: %s", strings.Join(orphaned, ", "))
	}
	return nil
}

func evaluateActiveFileIdentityRewrite(
	db *sql.DB,
	datastore weaverDatastore,
	jobID int,
	assertion *ScenarioFileIdentityRewriteAssertion,
) (fileIdentityRewriteObservation, error) {
	observation := newFileIdentityRewriteObservation(assertion)
	if jobID <= 0 || assertion == nil || !assertion.enabled() {
		return observation, nil
	}

	presentSources := make(map[string]map[string]struct{})
	activeRows, err := collectFileIdentityRewriteRows(
		db,
		presentSources,
		rebindWeaverSQL(
			datastore,
			`SELECT current_filename, classification_source FROM active_file_identities WHERE job_id = ?`,
		),
		jobID,
	)
	if err != nil {
		return observation, fmt.Errorf("query active_file_identities for job %d: %w", jobID, err)
	}

	if activeRows == 0 {
		observerExists, err := weaverStateTableExists(db, datastore, fileIdentityRewriteObserverTable)
		if err != nil {
			return observation, err
		}
		if observerExists {
			if _, err := collectFileIdentityRewriteRows(
				db,
				presentSources,
				rebindWeaverSQL(
					datastore,
					`SELECT current_filename, classification_source
					 FROM `+fileIdentityRewriteObserverTable+`
					 WHERE id IN (
						SELECT MAX(id)
						FROM `+fileIdentityRewriteObserverTable+`
						WHERE job_id = ?
						GROUP BY file_index
					 )`,
				),
				jobID,
			); err != nil {
				return observation, fmt.Errorf("query latest %s rows for job %d: %w", fileIdentityRewriteObserverTable, jobID, err)
			}
		}
	}

	requiredCurrentFilenames := normalizeManifestFilenames(assertion.RequiredCurrentFilenames)
	forbiddenCurrentFilenames := normalizeManifestFilenames(assertion.ForbiddenCurrentFilenames)
	requiredSource := observation.RequiredClassificationSource

	for _, currentFilename := range requiredCurrentFilenames {
		sources, ok := presentSources[currentFilename]
		if !ok {
			observation.MissingCurrentFilenames = append(observation.MissingCurrentFilenames, currentFilename)
			continue
		}
		if requiredSource != "" {
			if _, ok := sources[requiredSource]; !ok {
				observation.WrongClassificationSources = append(
					observation.WrongClassificationSources,
					fmt.Sprintf("%s (found: %s)", currentFilename, joinSortedSet(sources)),
				)
			}
		}
	}

	for _, currentFilename := range forbiddenCurrentFilenames {
		if _, ok := presentSources[currentFilename]; ok {
			observation.ForbiddenCurrentFilenames = append(observation.ForbiddenCurrentFilenames, currentFilename)
		}
	}

	if len(presentSources) > 0 {
		observedCurrentFilenames := make([]string, 0, len(presentSources))
		for currentFilename, sources := range presentSources {
			observedCurrentFilenames = append(
				observedCurrentFilenames,
				fmt.Sprintf("%s (%s)", currentFilename, joinSortedSet(sources)),
			)
		}
		sort.Strings(observedCurrentFilenames)
		observation.ObservedCurrentFilenames = observedCurrentFilenames
	}

	observation.Observed =
		len(observation.MissingCurrentFilenames) == 0 &&
			len(observation.ForbiddenCurrentFilenames) == 0 &&
			len(observation.WrongClassificationSources) == 0

	return observation, nil
}

func collectFileIdentityRewriteRows(
	db *sql.DB,
	presentSources map[string]map[string]struct{},
	query string,
	args ...interface{},
) (int, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			currentFilename      string
			classificationSource string
		)
		if err := rows.Scan(&currentFilename, &classificationSource); err != nil {
			return count, err
		}
		if _, ok := presentSources[currentFilename]; !ok {
			presentSources[currentFilename] = make(map[string]struct{})
		}
		presentSources[currentFilename][normalizeFileIdentityClassificationSource(classificationSource)] = struct{}{}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, err
	}
	return count, nil
}

func applyTerminalStateCheck(dbPath string, jobID int, slug string, status string) (string, string) {
	if jobID <= 0 || !facadeTerminalStatus(status) {
		return status, ""
	}
	if err := assertTerminalFixtureStateEventually(dbPath, jobID, status); err != nil {
		log.Printf("  %s: state mismatch after %s: %v", slug, status, err)
		return "DB_STATE_ERROR", err.Error()
	}
	if normalizeFacadeState(status) == "COMPLETE" {
		scenario, err := loadScenario(filepath.Join(testdataDir(), slug))
		if err == nil && len(scenario.RequiredJobEvents) > 0 {
			if err := assertRequiredJobEvents(dbPath, jobID, scenario.RequiredJobEvents); err != nil {
				log.Printf("  %s: required job event mismatch after %s: %v", slug, status, err)
				return "EVENT_STATE_ERROR", err.Error()
			}
		}
		if err == nil && len(scenario.ForbiddenJobEvents) > 0 {
			if err := assertForbiddenJobEvents(dbPath, jobID, scenario.ForbiddenJobEvents); err != nil {
				log.Printf("  %s: forbidden job event mismatch after %s: %v", slug, status, err)
				return "EVENT_STATE_ERROR", err.Error()
			}
		}
		if err == nil && len(scenario.MaxJobEventCounts) > 0 {
			if err := assertMaxJobEventCounts(dbPath, jobID, scenario.MaxJobEventCounts); err != nil {
				log.Printf("  %s: job event count mismatch after %s: %v", slug, status, err)
				return "EVENT_STATE_ERROR", err.Error()
			}
		}
		if err == nil && len(scenario.ExpectedOutputBLAKE3) > 0 {
			if err := assertOutputBLAKE3(dbPath, jobID, scenario.ExpectedOutputBLAKE3); err != nil {
				log.Printf("  %s: output digest mismatch after %s: %v", slug, status, err)
				return "OUTPUT_DIGEST_ERROR", err.Error()
			}
		}
		if err == nil && len(scenario.ForbiddenOutputPaths) > 0 {
			if err := assertForbiddenOutputPaths(dbPath, jobID, scenario.ForbiddenOutputPaths); err != nil {
				log.Printf("  %s: forbidden output present after %s: %v", slug, status, err)
				return "OUTPUT_LEFTOVER_ERROR", err.Error()
			}
		}
		if err == nil && scenario.par2CleanSettlementAssertion() != nil {
			if err := assertPar2CleanSettlement(jobID, scenario.par2CleanSettlementAssertion()); err != nil {
				log.Printf("  %s: PAR2 clean-set settlement mismatch after %s: %v", slug, status, err)
				return "PAR2_SETTLEMENT_ERROR", err.Error()
			}
		}
	}
	return status, ""
}

const cleanPar2SettlementMessage = "PAR2 set settled clean from in-stream grid evidence"
const cleanPar2VerificationSourceMessage = "PAR2 clean set verification source"

var cleanPar2VerificationModes = []string{"grid", "quick_digest", "strong_decode", "authoritative"}

func assertPar2CleanSettlement(jobID int, assertion *ScenarioPar2CleanSettlementAssertion) error {
	if assertion == nil || len(assertion.ExpectedSetSliceSizes) == 0 {
		return nil
	}
	if len(assertion.ExpectedSetVerificationModes) == 0 {
		return fmt.Errorf("clean PAR2 settlement assertion requires expected verification modes")
	}
	for setID := range assertion.ExpectedSetSliceSizes {
		if _, ok := assertion.ExpectedSetVerificationModes[setID]; !ok {
			return fmt.Errorf("clean PAR2 set %s has no expected verification mode", setID)
		}
	}
	for setID, modes := range assertion.ExpectedSetVerificationModes {
		if _, ok := assertion.ExpectedSetSliceSizes[setID]; !ok {
			return fmt.Errorf("clean PAR2 set %s has no expected slice size", setID)
		}
		for _, mode := range modes {
			if !slices.Contains(cleanPar2VerificationModes, mode) {
				return fmt.Errorf("clean PAR2 set %s has unknown verification mode %q", setID, mode)
			}
		}
	}
	raw, err := os.ReadFile(localWeaverLogPath())
	if err != nil {
		return fmt.Errorf("read weaver log: %w", err)
	}
	wantJobID := strconv.Itoa(jobID)
	observedModes := make(map[string]string, len(assertion.ExpectedSetVerificationModes))
	for _, rawLine := range strings.Split(string(raw), "\n") {
		line := ansiEscape.ReplaceAllString(rawLine, "")
		if !strings.Contains(line, cleanPar2VerificationSourceMessage) || directLogJobID(line) != wantJobID {
			continue
		}
		setID := weaverLogField(line, "recovery_set_id")
		wantModes, wanted := assertion.ExpectedSetVerificationModes[setID]
		if !wanted {
			return fmt.Errorf("clean PAR2 verification source observed unexpected set %s", setID)
		}
		mode := weaverLogField(line, "verification_mode")
		if !slices.Contains(wantModes, mode) {
			return fmt.Errorf("clean PAR2 verification source for set %s has mode %q, want one of %v", setID, mode, wantModes)
		}
		sliceSize, err := parseWeaverLogUint(line, "slice_size")
		if err != nil {
			return fmt.Errorf("clean PAR2 verification source for set %s: %w", setID, err)
		}
		if wantSliceSize := assertion.ExpectedSetSliceSizes[setID]; sliceSize != wantSliceSize {
			return fmt.Errorf("clean PAR2 verification source for set %s has slice size %d, want %d", setID, sliceSize, wantSliceSize)
		}
		if _, duplicate := observedModes[setID]; duplicate {
			return fmt.Errorf("clean PAR2 verification source observed set %s more than once", setID)
		}
		observedModes[setID] = mode
	}
	if len(observedModes) != len(assertion.ExpectedSetVerificationModes) {
		missing := make([]string, 0, len(assertion.ExpectedSetVerificationModes)-len(observedModes))
		for setID := range assertion.ExpectedSetVerificationModes {
			if _, ok := observedModes[setID]; !ok {
				missing = append(missing, setID)
			}
		}
		sort.Strings(missing)
		return fmt.Errorf("clean PAR2 verification source did not observe set(s): %s", strings.Join(missing, ", "))
	}

	observedGridSets := make(map[string]uint64)
	for _, rawLine := range strings.Split(string(raw), "\n") {
		line := ansiEscape.ReplaceAllString(rawLine, "")
		if !strings.Contains(line, cleanPar2SettlementMessage) || directLogJobID(line) != wantJobID {
			continue
		}
		setID := weaverLogField(line, "recovery_set_id")
		mode, wanted := observedModes[setID]
		if !wanted {
			return fmt.Errorf("clean PAR2 settlement observed unexpected set %s", setID)
		}
		if mode != "grid" {
			return fmt.Errorf("clean PAR2 settlement observed set %s with non-grid mode %q", setID, mode)
		}
		sliceSize, err := parseWeaverLogUint(line, "slice_size")
		if err != nil {
			return fmt.Errorf("clean PAR2 settlement for set %s: %w", setID, err)
		}
		if wantSliceSize := assertion.ExpectedSetSliceSizes[setID]; sliceSize != wantSliceSize {
			return fmt.Errorf("clean PAR2 settlement for set %s has slice size %d, want %d", setID, sliceSize, wantSliceSize)
		}
		if verdict := weaverLogField(line, "verdict"); verdict != "clean" {
			return fmt.Errorf("clean PAR2 settlement for set %s has verdict %q, want clean", setID, verdict)
		}
		readBytes, err := parseWeaverLogUint(line, "verification_read_bytes")
		if err != nil {
			return fmt.Errorf("clean PAR2 settlement for set %s: %w", setID, err)
		}
		if readBytes != assertion.VerificationReadBytes {
			return fmt.Errorf("clean PAR2 settlement for set %s read %d verification bytes, want %d", setID, readBytes, assertion.VerificationReadBytes)
		}
		if _, duplicate := observedGridSets[setID]; duplicate {
			return fmt.Errorf("clean PAR2 settlement observed set %s more than once", setID)
		}
		observedGridSets[setID] = sliceSize
	}
	missingGridSets := make([]string, 0)
	for setID, mode := range observedModes {
		if mode == "grid" {
			if _, ok := observedGridSets[setID]; !ok {
				missingGridSets = append(missingGridSets, setID)
			}
		}
	}
	if len(missingGridSets) > 0 {
		sort.Strings(missingGridSets)
		return fmt.Errorf("clean PAR2 settlement did not observe grid set(s): %s", strings.Join(missingGridSets, ", "))
	}
	return nil
}

func parseWeaverLogUint(line, field string) (uint64, error) {
	value := weaverLogField(line, field)
	if value == "" {
		return 0, fmt.Errorf("omitted %s", field)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", field, value, err)
	}
	return parsed, nil
}

func weaverLogField(line, field string) string {
	key := field + "="
	start := strings.Index(line, key)
	if start < 0 {
		return ""
	}
	rest := strings.TrimLeft(line[start+len(key):], `"`)
	end := strings.IndexAny(rest, "\" \t")
	if end < 0 {
		return strings.TrimSpace(rest)
	}
	return rest[:end]
}

func assertTerminalFixtureStateEventually(dbPath string, jobID int, expectedStatus string) error {
	deadline := time.Now().Add(terminalFixtureStateTimeout)
	var lastErr error
	for {
		err := assertTerminalFixtureState(dbPath, jobID, expectedStatus)
		if err == nil {
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(terminalFixtureStatePollInterval)
	}
}

func assertRequiredJobEvents(dbPath string, jobID int, required []string) error {
	if len(required) == 0 {
		return nil
	}

	deadline := time.Now().Add(requiredJobEventTimeout)
	lastChangeAt := time.Now()
	var lastEvents []string
	initialized := false
	for {
		now := time.Now()
		events, err := jobEventKinds(dbPath, jobID)
		if err != nil {
			if isTransientSQLiteBusy(err) && now.Before(deadline.Add(requiredJobEventStabilityWindow)) {
				time.Sleep(requiredJobEventPollInterval)
				continue
			}
			return fmt.Errorf("load job events for job %d: %w", jobID, err)
		}
		if !initialized || !sameJobEventSnapshot(lastEvents, events) {
			lastEvents = append([]string(nil), events...)
			lastChangeAt = now
			initialized = true
		}
		missing := missingJobEvents(lastEvents, required)
		if len(missing) == 0 {
			return nil
		}
		if now.After(deadline) && now.Sub(lastChangeAt) >= requiredJobEventStabilityWindow {
			return fmt.Errorf(
				"job %d missing required job event(s): %s after waiting for a stable event snapshot; observed: %s",
				jobID,
				strings.Join(missing, ", "),
				strings.Join(lastEvents, ", "),
			)
		}
		time.Sleep(requiredJobEventPollInterval)
	}
}

func isTransientSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "SQLITE_BUSY") || strings.Contains(message, "database is locked")
}

func assertForbiddenJobEvents(dbPath string, jobID int, forbidden []string) error {
	if len(forbidden) == 0 {
		return nil
	}

	events, err := jobEventKinds(dbPath, jobID)
	if err != nil {
		return fmt.Errorf("load job events for job %d: %w", jobID, err)
	}

	present := make(map[string]struct{}, len(events))
	for _, event := range events {
		present[event] = struct{}{}
	}

	var violations []string
	for _, event := range forbidden {
		if _, ok := present[event]; ok {
			violations = append(violations, event)
		}
	}
	if len(violations) > 0 {
		return fmt.Errorf(
			"job %d emitted forbidden job event(s): %s; observed: %s",
			jobID,
			strings.Join(violations, ", "),
			strings.Join(events, ", "),
		)
	}

	return nil
}

func assertMaxJobEventCounts(dbPath string, jobID int, maximums map[string]int) error {
	events, err := jobEventKinds(dbPath, jobID)
	if err != nil {
		return fmt.Errorf("load job events for job %d: %w", jobID, err)
	}

	counts := make(map[string]int, len(events))
	for _, event := range events {
		counts[event]++
	}
	for event, maximum := range maximums {
		if maximum < 0 {
			return fmt.Errorf("job %d has negative maximum for event %q", jobID, event)
		}
		if observed := counts[event]; observed > maximum {
			return fmt.Errorf(
				"job %d emitted %q %d times, maximum is %d; observed: %s",
				jobID,
				event,
				observed,
				maximum,
				strings.Join(events, ", "),
			)
		}
	}

	return nil
}

func assertOutputBLAKE3(dbPath string, jobID int, expectedByRelativePath map[string]string) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return fmt.Errorf("open weaver state db: %w", err)
	}
	defer db.Close()

	var outputDir string
	query := rebindWeaverSQL(datastore, `SELECT output_dir FROM job_history WHERE job_id = ?`)
	if err := db.QueryRow(query, jobID).Scan(&outputDir); err != nil {
		return fmt.Errorf("load output directory for job %d: %w", jobID, err)
	}
	if outputDir == "" {
		return fmt.Errorf("job %d has no output directory", jobID)
	}

	relativePaths := make([]string, 0, len(expectedByRelativePath))
	for relativePath := range expectedByRelativePath {
		relativePaths = append(relativePaths, relativePath)
	}
	sort.Strings(relativePaths)

	for _, relativePath := range relativePaths {
		localRelativePath := filepath.FromSlash(relativePath)
		if localRelativePath == "." || !filepath.IsLocal(localRelativePath) {
			return fmt.Errorf("job %d output digest path %q is not relative", jobID, relativePath)
		}

		file, err := os.Open(filepath.Join(outputDir, localRelativePath))
		if err != nil {
			return fmt.Errorf("open output %q for job %d: %w", relativePath, jobID, err)
		}
		hash := blake3.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil {
			return fmt.Errorf("hash output %q for job %d: %w", relativePath, jobID, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close output %q for job %d: %w", relativePath, jobID, closeErr)
		}

		actual := fmt.Sprintf("%x", hash.Sum(nil))
		expected := strings.ToLower(strings.TrimSpace(expectedByRelativePath[relativePath]))
		if actual != expected {
			return fmt.Errorf(
				"job %d output %q BLAKE3 mismatch: got %s, want %s",
				jobID,
				relativePath,
				actual,
				expected,
			)
		}
	}

	return nil
}

// assertForbiddenOutputPaths fails when the job's completed output still holds
// a file the scenario says is not part of the release. It is the counterpart of
// assertOutputBLAKE3: that one says the release is right, this one says nothing
// the pipeline used along the way was handed over with it — split parts a join
// consumed, backups a repair wrote, an archive an extraction opened.
func assertForbiddenOutputPaths(dbPath string, jobID int, patterns []string) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return fmt.Errorf("open weaver state db: %w", err)
	}
	defer db.Close()

	var outputDir string
	query := rebindWeaverSQL(datastore, `SELECT output_dir FROM job_history WHERE job_id = ?`)
	if err := db.QueryRow(query, jobID).Scan(&outputDir); err != nil {
		return fmt.Errorf("load output directory for job %d: %w", jobID, err)
	}
	if outputDir == "" {
		return fmt.Errorf("job %d has no output directory", jobID)
	}

	var delivered []string
	walkErr := filepath.WalkDir(outputDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(outputDir, path)
		if err != nil {
			return err
		}
		delivered = append(delivered, filepath.ToSlash(relative))
		return nil
	})
	if walkErr != nil {
		return fmt.Errorf("read output directory for job %d: %w", jobID, walkErr)
	}

	found, err := matchForbiddenOutputPaths(patterns, delivered)
	if err != nil {
		return fmt.Errorf("job %d: %w", jobID, err)
	}
	if len(found) > 0 {
		sort.Strings(delivered)
		return fmt.Errorf(
			"job %d delivered forbidden output(s): %s; delivered: %s",
			jobID,
			strings.Join(found, ", "),
			strings.Join(delivered, ", "),
		)
	}
	return nil
}

// matchForbiddenOutputPaths reports which delivered paths a pattern claims. A
// pattern carrying a separator is matched against the whole slash-relative path
// under the output directory; one without is matched against a file's name
// wherever in the tree it landed, so a scenario does not have to know which
// subdirectory an extraction chose. Patterns are path.Match globs, and a
// literal name is simply a glob with nothing to expand.
func matchForbiddenOutputPaths(patterns []string, delivered []string) ([]string, error) {
	var found []string
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if _, err := path.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("forbidden output pattern %q is malformed: %w", pattern, err)
		}
		for _, relative := range delivered {
			subject := relative
			if !strings.Contains(pattern, "/") {
				subject = path.Base(relative)
			}
			matched, err := path.Match(pattern, subject)
			if err != nil {
				return nil, fmt.Errorf("forbidden output pattern %q is malformed: %w", pattern, err)
			}
			if matched {
				found = append(found, relative)
			}
		}
	}
	sort.Strings(found)
	return uniqueStrings(found), nil
}

func uniqueStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func missingJobEvents(events []string, required []string) []string {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		seen[event] = struct{}{}
	}

	var missing []string
	for _, event := range required {
		if _, ok := seen[event]; !ok {
			missing = append(missing, event)
		}
	}
	return missing
}

func sameJobEventSnapshot(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx] != right[idx] {
			return false
		}
	}
	return true
}

func newFileIdentityRewriteObservation(
	assertion *ScenarioFileIdentityRewriteAssertion,
) fileIdentityRewriteObservation {
	if assertion == nil {
		return fileIdentityRewriteObservation{}
	}
	return fileIdentityRewriteObservation{
		RequiredClassificationSource: normalizeFileIdentityClassificationSource(assertion.RequiredClassificationSource),
	}
}

func (a *ScenarioFileIdentityRewriteAssertion) enabled() bool {
	if a == nil {
		return false
	}
	return len(normalizeManifestFilenames(a.RequiredCurrentFilenames)) > 0 ||
		len(normalizeManifestFilenames(a.ForbiddenCurrentFilenames)) > 0
}

func (o fileIdentityRewriteObservation) terminalFailureMessage(status string, lastQueryError string) string {
	parts := []string{fmt.Sprintf(
		"job reached %s before file identity rewrite oracle observed",
		normalizeFacadeState(status),
	)}
	if len(o.MissingCurrentFilenames) > 0 {
		parts = append(parts, "missing current filenames: "+strings.Join(o.MissingCurrentFilenames, ", "))
	}
	if len(o.ForbiddenCurrentFilenames) > 0 {
		parts = append(parts, "forbidden current filenames still present: "+strings.Join(o.ForbiddenCurrentFilenames, ", "))
	}
	if len(o.WrongClassificationSources) > 0 {
		if o.RequiredClassificationSource != "" {
			parts = append(
				parts,
				fmt.Sprintf(
					"current filenames missing classification_source=%s: %s",
					o.RequiredClassificationSource,
					strings.Join(o.WrongClassificationSources, ", "),
				),
			)
		} else {
			parts = append(parts, "classification source mismatches: "+strings.Join(o.WrongClassificationSources, ", "))
		}
	}
	if len(parts) == 1 {
		parts = append(parts, "no matching active_file_identities rows were observed")
	}
	if len(o.ObservedCurrentFilenames) > 0 {
		parts = append(parts, "effective file identity rows: "+strings.Join(o.ObservedCurrentFilenames, ", "))
	}
	if strings.TrimSpace(lastQueryError) != "" {
		parts = append(parts, "last oracle query error: "+strings.TrimSpace(lastQueryError))
	}
	return strings.Join(parts, "; ")
}

func normalizeManifestFilenames(names []string) []string {
	seen := make(map[string]struct{}, len(names))
	normalized := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	sort.Strings(normalized)
	return normalized
}

func normalizeFileIdentityClassificationSource(source string) string {
	return strings.ToLower(strings.TrimSpace(source))
}

func joinSortedSet(values map[string]struct{}) string {
	items := make([]string, 0, len(values))
	for value := range values {
		items = append(items, value)
	}
	sort.Strings(items)
	return strings.Join(items, ", ")
}
