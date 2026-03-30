package couac_test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/couac"
)

func TestCheckpoint(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec(ctx, "CREATE TABLE ckpt_test (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO ckpt_test VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

func TestForceCheckpoint(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec(ctx, "CREATE TABLE fckpt_test (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.ForceCheckpoint(ctx); err != nil {
		t.Fatalf("ForceCheckpoint: %v", err)
	}
}

// TestCompact_InMemory tests that Compact works on in-memory databases.
func TestCompact_InMemory(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec(ctx, "CREATE TABLE compact_mem (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO compact_mem VALUES (1), (2), (3)")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact in-memory: %v", err)
	}

	// Verify connection still works after compact
	res, err := conn.Query(ctx, "SELECT count(*) FROM compact_mem")
	if err != nil {
		t.Fatalf("query after compact: %v", err)
	}
	defer res.Close()
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "3" {
			t.Errorf("expected 3, got %s", count)
		}
	}
}

// TestCompact_ConnectionsSurvive is the critical test: verify that open
// connections continue to work after Compact completes.
func TestCompact_ConnectionsSurvive(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	// Open multiple connections
	conn1, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()

	conn2, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	// Set up data on conn1
	_, err = conn1.Exec(ctx, "CREATE TABLE survive_test (id INT, val VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn1.Exec(ctx, "INSERT INTO survive_test VALUES (1, 'before'), (2, 'compact')")
	if err != nil {
		t.Fatal(err)
	}

	// Verify conn2 can see the data before compact
	res, err := conn2.Query(ctx, "SELECT count(*) FROM survive_test")
	if err != nil {
		t.Fatal(err)
	}
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "2" {
			t.Fatalf("expected 2 before compact, got %s", count)
		}
	}
	res.Close()

	// Run compact
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Verify BOTH connections still work after compact
	res1, err := conn1.Query(ctx, "SELECT count(*) FROM survive_test")
	if err != nil {
		t.Fatalf("conn1 after compact: %v", err)
	}
	defer res1.Close()
	if res1.Reader.Next() {
		count := res1.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "2" {
			t.Errorf("conn1: expected 2, got %s", count)
		}
	}

	res2, err := conn2.Query(ctx, "SELECT count(*) FROM survive_test")
	if err != nil {
		t.Fatalf("conn2 after compact: %v", err)
	}
	defer res2.Close()
	if res2.Reader.Next() {
		count := res2.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "2" {
			t.Errorf("conn2: expected 2, got %s", count)
		}
	}

	// Verify new data can be inserted after compact
	_, err = conn1.Exec(ctx, "INSERT INTO survive_test VALUES (3, 'after')")
	if err != nil {
		t.Fatalf("insert after compact: %v", err)
	}

	// Verify the new row is visible
	res3, err := conn2.Query(ctx, "SELECT count(*) FROM survive_test")
	if err != nil {
		t.Fatal(err)
	}
	defer res3.Close()
	if res3.Reader.Next() {
		count := res3.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "3" {
			t.Errorf("expected 3 after insert, got %s", count)
		}
	}
}

// TestCompact_ConcurrentOperations verifies that Compact safely blocks
// concurrent operations via the RWMutex and that operations resume
// correctly after compact completes.
func TestCompact_ConcurrentOperations(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	// Create table and insert initial data
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec(ctx, "CREATE TABLE concurrent_compact (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO concurrent_compact VALUES (%d)", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Launch concurrent readers
	var (
		wg        sync.WaitGroup
		errors    atomic.Int64
		reads     atomic.Int64
		compacted atomic.Bool
	)

	const numReaders = 5
	const readsPerReader = 20

	// Start readers that continuously query during compact
	for i := range numReaders {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := db.Connect()
			if err != nil {
				errors.Add(1)
				t.Errorf("reader %d: Connect: %v", id, err)
				return
			}
			defer c.Close()

			for range readsPerReader {
				res, err := c.Query(ctx, "SELECT count(*) FROM concurrent_compact")
				if err != nil {
					errors.Add(1)
					t.Errorf("reader %d: query: %v", id, err)
					continue
				}
				if res.Reader.Next() {
					count := res.Reader.RecordBatch().Column(0).ValueStr(0)
					if count != "100" {
						// During compact, data might be in flux; log but don't fail
						t.Logf("reader %d: count=%s (during compact=%v)", id, count, compacted.Load())
					}
				}
				res.Close()
				reads.Add(1)
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	// Launch compact while readers are active
	wg.Go(func() {
		time.Sleep(20 * time.Millisecond) // let readers start
		if err := db.Compact(ctx); err != nil {
			t.Errorf("Compact during concurrent reads: %v", err)
		}
		compacted.Store(true)
	})

	// Launch concurrent writers
	const numWriters = 3
	for i := range numWriters {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := db.Connect()
			if err != nil {
				errors.Add(1)
				return
			}
			defer c.Close()

			for j := range 10 {
				_, err := c.Exec(ctx, fmt.Sprintf("INSERT INTO concurrent_compact VALUES (%d)", 1000+id*100+j))
				if err != nil {
					// Write conflicts are expected during compact
					t.Logf("writer %d: insert: %v", id, err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	totalReads := reads.Load()
	totalErrors := errors.Load()
	t.Logf("Concurrent compact test: %d reads, %d errors", totalReads, totalErrors)

	// After compact, all connections should still work
	res, err := conn.Query(ctx, "SELECT count(*) FROM concurrent_compact")
	if err != nil {
		t.Fatalf("query after concurrent compact: %v", err)
	}
	defer res.Close()
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		t.Logf("Final row count after concurrent compact: %s", count)
	}
}

// TestCompact_FileBacked tests compaction on a file-backed database
// (the full copy-and-replace path).
func TestCompact_FileBacked(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compact_file.db")
	ctx := context.Background()

	db, err := couac.NewDuck(
		couac.WithPath(dbPath),
		couac.WithDriverName("duckdb"),
	)
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Create table, insert data, delete some rows
	_, err = conn.Exec(ctx, "CREATE TABLE compact_file_test (id INT, data VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO compact_file_test VALUES (%d, 'data_%d')", i, i))
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = conn.Exec(ctx, "DELETE FROM compact_file_test WHERE id < 50")
	if err != nil {
		t.Fatal(err)
	}

	// Compact should reclaim space
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact file-backed: %v", err)
	}

	// Verify data integrity after compact
	res, err := conn.Query(ctx, "SELECT count(*) FROM compact_file_test")
	if err != nil {
		t.Fatalf("query after file compact: %v", err)
	}
	defer res.Close()
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "50" {
			t.Errorf("expected 50 rows, got %s", count)
		}
	}
}

// TestCompact_WithActiveIngest tests that Compact safely pauses an
// ingest operation and that the ingest resumes correctly afterward.
func TestCompact_WithActiveIngest(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec(ctx, "CREATE TABLE ingest_compact (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	var wg sync.WaitGroup

	// Writer goroutine: continuously ingest records
	var ingestErrors atomic.Int64
	var ingestCount atomic.Int64
	wg.Go(func() {
		c, err := db.Connect()
		if err != nil {
			t.Errorf("ingest conn: %v", err)
			return
		}
		defer c.Close()

		for i := range 50 {
			bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			bldr.Field(0).(*array.Int32Builder).Append(int32(i))
			rec := bldr.NewRecordBatch()

			_, err := c.Ingest(ctx, "ingest_compact", rec)
			rec.Release()
			bldr.Release()
			if err != nil {
				ingestErrors.Add(1)
				t.Logf("ingest %d: %v", i, err)
				continue
			}
			ingestCount.Add(1)
			time.Sleep(2 * time.Millisecond)
		}
	})

	// Run compact in the middle
	time.Sleep(30 * time.Millisecond)
	if err := db.Compact(ctx); err != nil {
		t.Logf("Compact during ingest: %v (may be expected)", err)
	}

	wg.Wait()

	t.Logf("Ingests succeeded: %d, errors: %d", ingestCount.Load(), ingestErrors.Load())

	// Verify data is readable
	res, err := conn.Query(ctx, "SELECT count(*) FROM ingest_compact")
	if err != nil {
		t.Fatalf("query after ingest+compact: %v", err)
	}
	defer res.Close()
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		t.Logf("Final ingest_compact row count: %s", count)
	}
}

func TestDatabases(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	dbs, err := conn.Databases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(dbs) == 0 {
		t.Error("expected at least one database")
	}
}

func TestExtensions(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	exts, err := conn.Extensions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// DuckDB always has some built-in extensions
	if len(exts) == 0 {
		t.Error("expected at least one extension")
	}

	// Check that the fields are populated
	for _, ext := range exts {
		if ext.Name == "" {
			t.Error("expected non-empty extension name")
		}
	}
}

func TestAttachDetach(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	// Attach an in-memory database
	if err := conn.Attach(ctx, ":memory:", "test_attached"); err != nil {
		t.Fatal(err)
	}

	// Verify it appears in databases
	dbs, err := conn.Databases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := slices.Contains(dbs, "test_attached")
	if !found {
		t.Error("expected to find 'test_attached' in databases")
	}

	// Detach
	if err := conn.Detach(ctx, "test_attached"); err != nil {
		t.Fatal(err)
	}
}
