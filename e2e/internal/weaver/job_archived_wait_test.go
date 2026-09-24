package weaver

import (
	"path/filepath"
	"testing"
)

func TestWaitForJobArchivedWaitsForThisJobsHistoryRow(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, "sqlite")
	dbPath := filepath.Join(t.TempDir(), "weaver.db")
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()
	mustExecWeaverStateSQL(t, db, "CREATE TABLE job_history (job_id INTEGER PRIMARY KEY)")
	// Another job's archive must not satisfy the wait: it is keyed by id.
	mustExecWeaverStateSQL(t, db, "INSERT INTO job_history (job_id) VALUES (7)")

	pauses := 0
	err := awaitJobArchived(func() (bool, error) { return jobArchived(dbPath, 8) }, func() {
		pauses++
		switch pauses {
		case 1:
			// Still unarchived after the first look: the job's terminal status
			// is visible, its archive is queued behind other writes.
		case 2:
			mustExecWeaverStateSQL(t, db, "INSERT INTO job_history (job_id) VALUES (8)")
		default:
			t.Fatalf("the wait must end at the first look after the row commits; paused %d times", pauses)
		}
	})
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if pauses != 2 {
		t.Errorf("non-vacuity: the wait must have looked before the row existed and ended once it did; paused %d times", pauses)
	}
}

func TestWaitForJobArchivedReportsAnUnreadableStore(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, "sqlite")
	dbPath := filepath.Join(t.TempDir(), "weaver.db")
	// No job_history table: the query fails, and the wait returns that rather
	// than spinning on it.
	err := awaitJobArchived(func() (bool, error) { return jobArchived(dbPath, 8) }, func() {
		t.Fatal("an unreadable store must not be polled again")
	})
	if err == nil {
		t.Fatal("expected the read failure to be returned")
	}
}
