package couac_test

import (
	"context"
	"sync"
	"testing"
)

func TestConnect(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if db.ConnectionCount() != 1 {
		t.Errorf("expected 1 connection, got %d", db.ConnectionCount())
	}
}

func TestConnectAs(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.ConnectAs("memory", "main")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if conn.Catalog() != "memory" {
		t.Errorf("expected catalog 'memory', got %q", conn.Catalog())
	}
	if conn.DBSchema() != "main" {
		t.Errorf("expected schema 'main', got %q", conn.DBSchema())
	}
}

func TestCatalog_Default(t *testing.T) {
	_, conn := newTestConn(t)
	if conn.Catalog() != "" {
		t.Errorf("expected empty default catalog, got %q", conn.Catalog())
	}
	if conn.DBSchema() != "" {
		t.Errorf("expected empty default schema, got %q", conn.DBSchema())
	}
}

func TestConnectionCount(t *testing.T) {
	db := newTestDB(t)
	if db.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections initially, got %d", db.ConnectionCount())
	}

	c1, _ := db.Connect()
	c2, _ := db.Connect()
	if db.ConnectionCount() != 2 {
		t.Errorf("expected 2 connections, got %d", db.ConnectionCount())
	}

	c1.Close()
	if db.ConnectionCount() != 1 {
		t.Errorf("expected 1 connection after close, got %d", db.ConnectionCount())
	}

	c2.Close()
	if db.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections after all closed, got %d", db.ConnectionCount())
	}
}

func TestConnection_CloseIdempotent(t *testing.T) {
	_, conn := newTestConn(t)
	if err := conn.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close should be nil, got: %v", err)
	}
}

func TestConnection_OperationAfterClose(t *testing.T) {
	_, conn := newTestConn(t)
	conn.Close()

	_, err := conn.Exec(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error on closed connection")
	}
}

func TestConcurrentConnectionCreateClose(t *testing.T) {
	db := newTestDB(t)
	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			c, err := db.Connect()
			if err != nil {
				t.Errorf("concurrent Connect: %v", err)
				return
			}
			c.Close()
		}()
	}
	wg.Wait()

	if db.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections after concurrent test, got %d", db.ConnectionCount())
	}
}
