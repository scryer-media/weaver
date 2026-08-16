package weaver

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type weaverDatastore string

const (
	weaverDatastoreSQLite   weaverDatastore = "sqlite"
	weaverDatastorePostgres weaverDatastore = "postgres"

	weaverDatastoreEnv = "E2E_WEAVER_DATASTORE"
)

func parseWeaverDatastore(value string) (weaverDatastore, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "sqlite":
		return weaverDatastoreSQLite, nil
	case "postgres", "postgresql":
		return weaverDatastorePostgres, nil
	default:
		return "", fmt.Errorf("invalid %s=%q (expected sqlite|postgres)", weaverDatastoreEnv, value)
	}
}

func currentWeaverDatastore() weaverDatastore {
	datastore, err := parseWeaverDatastore(os.Getenv(weaverDatastoreEnv))
	if err != nil {
		log.Fatal(err)
	}
	return datastore
}

func normalizedWeaverDatastoreForPhase(value string) string {
	datastore, err := parseWeaverDatastore(value)
	if err != nil {
		log.Fatal(err)
	}
	return string(datastore)
}

func weaverUsesPostgresDatastore() bool {
	return currentWeaverDatastore() == weaverDatastorePostgres
}

func applyWeaverPostgresPhaseEnv(env map[string]string) {
	env["E2E_WEAVER_POSTGRES_DB"] = firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_DB"), "weaver")
	env["E2E_WEAVER_POSTGRES_USER"] = firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_USER"), "weaver")
	env["E2E_WEAVER_POSTGRES_PASSWORD"] = firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_PASSWORD"), "weaver-pass")
	env["E2E_WEAVER_DATABASE_URL"] = weaverComposePostgresURL()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func weaverPostgresDBName() string {
	return firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_DB"), "weaver")
}

func weaverPostgresUser() string {
	return firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_USER"), "weaver")
}

func weaverPostgresPassword() string {
	return firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_PASSWORD"), "weaver-pass")
}

func weaverPostgresPort() string {
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_POSTGRES_PORT")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return os.Getenv("E2E_WEAVER_POSTGRES_PORT")
}

func weaverPostgresURL() string {
	if value := strings.TrimSpace(os.Getenv("WEAVER_DATABASE_URL")); value != "" {
		return value
	}

	host := firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_HOST"), "localhost")
	postgresURL := url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(host, weaverPostgresPort()),
		Path:   "/" + weaverPostgresDBName(),
	}
	postgresURL.User = url.UserPassword(weaverPostgresUser(), weaverPostgresPassword())
	query := postgresURL.Query()
	query.Set("sslmode", firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_SSLMODE"), "require"))
	postgresURL.RawQuery = query.Encode()
	return postgresURL.String()
}

func weaverComposePostgresURL() string {
	postgresURL := url.URL{
		Scheme: "postgres",
		Host:   "weaver-postgres:5432",
		Path:   "/" + weaverPostgresDBName(),
	}
	postgresURL.User = url.UserPassword(weaverPostgresUser(), weaverPostgresPassword())
	query := postgresURL.Query()
	query.Set("sslmode", firstNonEmpty(os.Getenv("E2E_WEAVER_POSTGRES_SSLMODE"), "require"))
	postgresURL.RawQuery = query.Encode()
	return postgresURL.String()
}

func appendOrReplaceEnv(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	replaced := false
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			if !replaced {
				out = append(out, prefix+value)
				replaced = true
			}
			continue
		}
		out = append(out, entry)
	}
	if !replaced {
		out = append(out, prefix+value)
	}
	return out
}

func openWeaverPostgresDB() (*sql.DB, error) {
	db, err := sql.Open("postgres", weaverPostgresURL())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(time.Minute)
	return db, nil
}

func waitForWeaverPostgresReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := openWeaverPostgresDB()
		if err != nil {
			lastErr = err
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			lastErr = db.PingContext(ctx)
			cancel()
			_ = db.Close()
			if lastErr == nil {
				return nil
			}
		}
		if err := sleepWithSuspendDetection(time.Second, "waiting for Weaver Postgres"); err != nil {
			return err
		}
	}
	return fmt.Errorf("timeout waiting for Weaver Postgres on localhost:%s: %w", weaverPostgresPort(), lastErr)
}

func resetWeaverPostgresDatabase() error {
	db, err := openWeaverPostgresDB()
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, stmt := range []string{
		`DROP SCHEMA IF EXISTS public CASCADE`,
		`CREATE SCHEMA public`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func weaverStateDBAvailable(sqlitePath string) (bool, error) {
	if weaverUsesPostgresDatastore() {
		return true, nil
	}
	if _, err := os.Stat(sqlitePath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func openWeaverStateDB(sqlitePath string) (*sql.DB, weaverDatastore, error) {
	datastore := currentWeaverDatastore()
	if datastore == weaverDatastorePostgres {
		db, err := openWeaverPostgresDB()
		return db, datastore, err
	}
	db, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		return nil, datastore, err
	}
	db.SetMaxOpenConns(1)
	return db, datastore, nil
}

func rebindWeaverSQL(datastore weaverDatastore, query string) string {
	if datastore != weaverDatastorePostgres {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	index := 1
	for _, r := range query {
		if r == '?' {
			b.WriteString(fmt.Sprintf("$%d", index))
			index++
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
