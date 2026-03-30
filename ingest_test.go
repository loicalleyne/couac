package couac_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func makeTestRecord(t *testing.T, nRows int) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	for i := range nRows {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i + 1))
		bldr.Field(1).(*array.StringBuilder).Append("row_" + string(rune('a'+i%26)))
	}
	rec := bldr.NewRecordBatch()
	return rec
}

func makeTestRecordExtended(t *testing.T, nRows int) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	for i := range nRows {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i + 100))
		bldr.Field(1).(*array.StringBuilder).Append("ext_row")
		bldr.Field(2).(*array.Int32Builder).Append(int32(20 + i))
	}
	return bldr.NewRecordBatch()
}

func TestIngest_Create(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()
	rec := makeTestRecord(t, 3)
	defer rec.Release()

	n, err := conn.Ingest(ctx, "ingest_test", rec)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("expected 3 rows, got %d", n)
	}

	// Verify table was created
	res, err := conn.Query(ctx, "SELECT count(*) FROM ingest_test")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "3" {
			t.Errorf("expected 3, got %s", count)
		}
	}
}

func TestIngest_Append(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	rec1 := makeTestRecord(t, 2)
	defer rec1.Release()
	rec2 := makeTestRecord(t, 3)
	defer rec2.Release()

	_, err := conn.Ingest(ctx, "ingest_append", rec1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Ingest(ctx, "ingest_append", rec2)
	if err != nil {
		t.Fatal(err)
	}

	res, err := conn.Query(ctx, "SELECT count(*) FROM ingest_append")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "5" {
			t.Errorf("expected 5, got %s", count)
		}
	}
}

func TestIngest_NilRecord(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Ingest(ctx, "test", nil)
	if err == nil {
		t.Fatal("expected error for nil record")
	}
}

func TestIngest_EmptyTable(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()
	rec := makeTestRecord(t, 1)
	defer rec.Release()

	_, err := conn.Ingest(ctx, "", rec)
	if err == nil {
		t.Fatal("expected error for empty table name")
	}
}

func TestIngestMerge_SchemaEvolution(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	// First ingest: 2 columns (id, name)
	rec1 := makeTestRecord(t, 2)
	defer rec1.Release()
	_, err := conn.IngestMerge(ctx, "merge_test", rec1)
	if err != nil {
		t.Fatal(err)
	}

	// Second ingest: 3 columns (id, name, age) - schema mismatch triggers merge
	rec2 := makeTestRecordExtended(t, 3)
	defer rec2.Release()
	_, err = conn.IngestMerge(ctx, "merge_test", rec2)
	if err != nil {
		t.Fatal(err)
	}

	// Verify: table should have 3 columns and 5 rows
	res, err := conn.Query(ctx, "SELECT count(*) FROM merge_test")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "5" {
			t.Errorf("expected 5, got %s", count)
		}
	}

	// Verify schema has 3 columns
	cols, err := conn.Describe(ctx, "merge_test")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 3 {
		t.Errorf("expected 3 columns after merge, got %d", len(cols))
	}
}

func TestIngestReplace(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	rec1 := makeTestRecord(t, 5)
	defer rec1.Release()
	_, err := conn.Ingest(ctx, "replace_test", rec1)
	if err != nil {
		t.Fatal(err)
	}

	// Replace with 2 rows
	rec2 := makeTestRecord(t, 2)
	defer rec2.Release()
	_, err = conn.IngestReplace(ctx, "replace_test", rec2)
	if err != nil {
		t.Fatal(err)
	}

	res, err := conn.Query(ctx, "SELECT count(*) FROM replace_test")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if res.Reader.Next() {
		count := res.Reader.RecordBatch().Column(0).ValueStr(0)
		if count != "2" {
			t.Errorf("expected 2 after replace, got %s", count)
		}
	}
}

func TestIngestStream(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i := range 10 {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
	}
	rec := bldr.NewRecordBatch()
	bldr.Release()
	defer rec.Release()

	// Create a simple RecordReader from the record
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	n, err := conn.IngestStream(ctx, "stream_test", reader)
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Errorf("expected 10 rows, got %d", n)
	}
}
