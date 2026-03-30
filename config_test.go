package couac_test

import (
	"context"
	"testing"
)

func TestSet_Setting(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	// Get initial threads setting
	threads, err := conn.Setting(ctx, "threads")
	if err != nil {
		t.Fatal(err)
	}
	if threads == "" {
		t.Error("expected non-empty threads value")
	}
}

func TestSetMemoryLimit(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	if err := conn.SetMemoryLimit(ctx, "1GB"); err != nil {
		t.Fatal(err)
	}

	val, err := conn.Setting(ctx, "memory_limit")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("memory_limit after set: %s", val)
}

func TestSetThreads(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	if err := conn.SetThreads(ctx, 2); err != nil {
		t.Fatal(err)
	}

	val, err := conn.Setting(ctx, "threads")
	if err != nil {
		t.Fatal(err)
	}
	if val != "2" {
		t.Errorf("expected threads=2, got %s", val)
	}
}

func TestSettings(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	settings, err := conn.Settings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(settings) == 0 {
		t.Error("expected at least one setting")
	}
}

func TestDescribe(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE describe_test (id INT PRIMARY KEY, name VARCHAR NOT NULL, val DOUBLE)")
	if err != nil {
		t.Fatal(err)
	}

	cols, err := conn.Describe(ctx, "describe_test")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}

	// Check first column
	if cols[0].Name != "id" {
		t.Errorf("expected column name 'id', got %q", cols[0].Name)
	}
	if cols[0].Type != "INTEGER" {
		t.Errorf("expected type 'INTEGER', got %q", cols[0].Type)
	}
}

func TestShowTables(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE show_test1 (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "CREATE TABLE show_test2 (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	tables, err := conn.ShowTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) < 2 {
		t.Errorf("expected at least 2 tables, got %d", len(tables))
	}

	found1, found2 := false, false
	for _, tbl := range tables {
		if tbl == "show_test1" {
			found1 = true
		}
		if tbl == "show_test2" {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Errorf("expected both tables in show tables, got %v", tables)
	}
}

func TestVersion(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	ver, err := conn.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ver == "" {
		t.Error("expected non-empty version")
	}
	t.Logf("DuckDB version: %s", ver)
}

func TestPlatform(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	platform, err := conn.Platform(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if platform == "" {
		t.Error("expected non-empty platform")
	}
	t.Logf("DuckDB platform: %s", platform)
}

func TestExplain(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE explain_test (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	plan, err := conn.Explain(ctx, "SELECT * FROM explain_test WHERE id > 5")
	if err != nil {
		t.Fatal(err)
	}
	if plan == "" {
		t.Error("expected non-empty explain plan")
	}
	t.Logf("Plan: %s", plan)
}

func TestReset(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	// Set and reset threads
	if err := conn.SetThreads(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := conn.Reset(ctx, "threads"); err != nil {
		t.Fatal(err)
	}
}
