package couac_test

import (
	"context"
	"testing"

	"github.com/loicalleyne/couac"
)

func TestExec_CreateAndInsert(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE test_exec (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	n, err := conn.Exec(ctx, "INSERT INTO test_exec VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("expected 3 rows affected, got %d", n)
	}
}

func TestExec_BadSQL(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "THIS IS NOT VALID SQL")
	if err == nil {
		t.Fatal("expected error for bad SQL")
	}
}

func TestQuery_Select(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE test_query (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO test_query VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatal(err)
	}

	res, err := conn.Query(ctx, "SELECT * FROM test_query ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	// Check schema
	schema := res.Schema()
	if schema == nil {
		t.Fatal("expected non-nil schema")
	}
	if len(schema.Fields()) != 2 {
		t.Errorf("expected 2 fields, got %d", len(schema.Fields()))
	}

	// Read data
	rowCount := 0
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		rowCount += int(rec.NumRows())
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows, got %d", rowCount)
	}
}

func TestQueryRaw(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	rr, stmt, _, err := conn.QueryRaw(ctx, "SELECT 42 AS answer")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	defer rr.Release()

	if !rr.Next() {
		t.Fatal("expected a record batch")
	}
	val := rr.RecordBatch().Column(0).ValueStr(0)
	if val != "42" {
		t.Errorf("expected '42', got %q", val)
	}
}

func TestQueryResult_Close(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	res, err := conn.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	// Close without reading - should not error
	if err := res.Close(); err != nil {
		t.Fatalf("QueryResult.Close: %v", err)
	}
	// Close again - idempotent
	if err := res.Close(); err != nil {
		t.Fatalf("second QueryResult.Close: %v", err)
	}
}

func TestPrepare(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE test_prepare (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare(ctx, "INSERT INTO test_prepare VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// Prepared statement should exist; we can't easily bind without Arrow record batches
	// but we verify the preparation succeeded.
}

func TestWithTransaction_Commit(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()

	// Create a table first outside the transaction
	_, err = conn.Exec(ctx, "CREATE TABLE tx_test (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	// Run a transaction
	err = db.WithTransaction(ctx, func(tx *couac.Conn) error {
		_, err := tx.Exec(ctx, "INSERT INTO tx_test VALUES (1)")
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO tx_test VALUES (2)")
		return err
	})
	if err != nil {
		t.Fatalf("WithTransaction commit: %v", err)
	}

	// Verify data was committed
	res, err := conn.Query(ctx, "SELECT count(*) FROM tx_test")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "2" {
			t.Errorf("expected 2 rows, got %s", count)
		}
	}
}

func TestWithTransaction_Rollback(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.NewConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()

	_, err = conn.Exec(ctx, "CREATE TABLE tx_rollback (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	// Run a transaction that returns an error
	err = db.WithTransaction(ctx, func(tx *couac.Conn) error {
		_, err := tx.Exec(ctx, "INSERT INTO tx_rollback VALUES (1)")
		if err != nil {
			return err
		}
		return context.Canceled // simulate error
	})
	if err == nil {
		t.Fatal("expected error from WithTransaction")
	}

	// Verify data was NOT committed
	res, err := conn.Query(ctx, "SELECT count(*) FROM tx_rollback")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "0" {
			t.Errorf("expected 0 rows after rollback, got %s", count)
		}
	}
}
