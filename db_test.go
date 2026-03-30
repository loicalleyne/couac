package couac_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/loicalleyne/couac"
)

// newTestDB creates an in-memory DuckDB for testing. It uses WithDriverPath
// pointing to the dbc-managed driver or falls back to common system paths.
// Tests requiring DuckDB must call this; if the driver is not found, the
// test is skipped.
func newTestDB(t *testing.T, opts ...couac.Option) *couac.DB {
	t.Helper()
	allOpts := append([]couac.Option{couac.WithDriverName("duckdb")}, opts...)
	db, err := couac.NewDuck(allOpts...)
	if err != nil {
		t.Skipf("skipping: cannot open DuckDB (driver not found?): %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// newTestConn creates an in-memory DuckDB with one connection for testing.
func newTestConn(t *testing.T) (*couac.DB, *couac.Conn) {
	t.Helper()
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return db, conn
}

func TestNewDuck_InMemory(t *testing.T) {
	db := newTestDB(t)
	if db.Path() != "" {
		t.Errorf("expected empty path for in-memory DB, got %q", db.Path())
	}
}

func TestNewDuck_FileBacked(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := couac.NewDuck(
		couac.WithPath(dbPath),
		couac.WithDriverName("duckdb"),
	)
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer db.Close()

	if db.Path() == "" {
		t.Error("expected non-empty path for file-backed DB")
	}
}

func TestNewDuck_SamePathGuard(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "guard_test.db")

	db1, err := couac.NewDuck(
		couac.WithPath(dbPath),
		couac.WithDriverName("duckdb"),
	)
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer db1.Close()

	// Second open to the same path should fail
	_, err = couac.NewDuck(
		couac.WithPath(dbPath),
		couac.WithDriverName("duckdb"),
	)
	if err == nil {
		t.Fatal("expected error opening same path twice, got nil")
	}
}

func TestNewDuck_TwoInMemory(t *testing.T) {
	db1 := newTestDB(t)
	db2 := newTestDB(t)

	// Both should work independently
	ctx := context.Background()
	c1, err := db1.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	c2, err := db2.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	_, err = c1.Exec(ctx, "CREATE TABLE t1 (x INT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c2.Exec(ctx, "CREATE TABLE t2 (y INT)")
	if err != nil {
		t.Fatal(err)
	}
}

func TestPing(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	if err := db.Ping(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestPing_AfterClose(t *testing.T) {
	db := newTestDB(t)
	db.Close()
	ctx := context.Background()
	if err := db.Ping(ctx); err == nil {
		t.Fatal("expected error pinging closed DB")
	}
}

func TestClose_Idempotent(t *testing.T) {
	db := newTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close should be nil, got: %v", err)
	}
}

func TestClose_ReturnsErrors(t *testing.T) {
	db := newTestDB(t)
	// Opening connections and closing DB should close all connections cleanly
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	_ = conn
	if err := db.Close(); err != nil {
		t.Fatalf("Close with open connection should succeed, got: %v", err)
	}
}

func TestFileBacked_PersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist.db")
	ctx := context.Background()

	// Create and write
	db1, err := couac.NewDuck(
		couac.WithPath(dbPath),
		couac.WithDriverName("duckdb"),
	)
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	c1, err := db1.Connect()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c1.Exec(ctx, "CREATE TABLE persist_test (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c1.Exec(ctx, "INSERT INTO persist_test VALUES (1, 'hello'), (2, 'world')")
	if err != nil {
		t.Fatal(err)
	}
	c1.Close()
	db1.Close()

	// Reopen and read
	db2, err := couac.NewDuck(
		couac.WithPath(dbPath),
		couac.WithDriverName("duckdb"),
	)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	c2, err := db2.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	res, err := c2.Query(ctx, "SELECT count(*) FROM persist_test")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	if !res.Reader.Next() {
		t.Fatal("expected result row")
	}
	count := res.Reader.RecordBatch().Column(0).ValueStr(0)
	if count != "2" {
		t.Errorf("expected count 2, got %s", count)
	}
}

func TestDriverPath(t *testing.T) {
	db := newTestDB(t)
	dp := db.DriverPath()
	if dp == "" {
		t.Error("expected non-empty driver path")
	}
}

func TestDefaultContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), "key", "value")
	db := newTestDB(t, couac.WithContext(ctx))
	if db.DefaultContext().Value("key") != "value" {
		t.Error("expected custom context to be set")
	}
}

func TestExtensionsDir(t *testing.T) {
	dir := couac.ExtensionsDir()
	if dir == "" {
		t.Skip("could not determine home directory")
	}
	// Just verify it's a reasonable path
	home, _ := os.UserHomeDir()
	if home != "" && dir[:len(home)] != home {
		t.Errorf("expected path starting with %s, got %s", home, dir)
	}
}

func TestSecretsDir(t *testing.T) {
	dir := couac.SecretsDir()
	if dir == "" {
		t.Skip("could not determine home directory")
	}
}
