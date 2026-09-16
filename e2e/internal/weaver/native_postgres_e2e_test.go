package weaver

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Every native test root owns a fresh database, retained across only its own
// process restarts. The opt-in URL must identify an operator-approved local
// fixture; it is never inherited by normal application processes.
var nativePostgres = struct {
	sync.Mutex
	urls map[string]string
}{urls: make(map[string]string)}

func nativeUnpackPostgresURL(t *testing.T, root string) string {
	t.Helper()
	raw := os.Getenv("WEAVER_NATIVE_E2E_POSTGRES_URL")
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal("invalid native PostgreSQL fixture URL")
	}
	host := net.ParseIP(parsed.Hostname())
	if (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || host == nil || !host.IsLoopback() {
		t.Fatal("native PostgreSQL fixture must use an explicit loopback IP")
	}
	nativePostgres.Lock()
	defer nativePostgres.Unlock()
	if existing, ok := nativePostgres.urls[root]; ok {
		return existing
	}
	digest := sha256.Sum256([]byte(root))
	name := fmt.Sprintf("weaver_native_%x", digest[:12])
	admin, err := sql.Open("postgres", raw)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	admin.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	databaseURL := parsed.String()
	nativePostgres.urls[root] = databaseURL
	t.Cleanup(func() {
		// Registered before the owned processes: their cleanup runs first.
		cleanup, err := sql.Open("postgres", raw)
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanup.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := cleanup.ExecContext(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
		nativePostgres.Lock()
		delete(nativePostgres.urls, root)
		nativePostgres.Unlock()
	})
	return databaseURL
}

func openNativeUnpackDB(t *testing.T, root string, readOnly bool) *sql.DB {
	t.Helper()
	nativePostgres.Lock()
	databaseURL := nativePostgres.urls[root]
	nativePostgres.Unlock()
	driver, dsn := "sqlite", filepath.Join(root, "weaver.db")
	if databaseURL != "" {
		driver, dsn = "postgres", databaseURL
	} else if readOnly {
		dsn = "file:" + filepath.ToSlash(dsn) + "?mode=ro"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}
