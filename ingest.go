package couac

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Ingest ingests an Arrow record batch into the DuckDB database,
// creating the table from the record's schema if it does not exist, or
// appending if it does.
//
// DuckDB does not support ADBC's CreateAppend ingest mode, so this
// method probes for the table via GetTableSchema and switches between
// Create and Append modes accordingly.
//
// It returns the number of rows affected if known, otherwise -1.
//
// The connection's Catalog and DBSchema are used as the target catalog
// and schema if set.
func (q *Conn) Ingest(ctx context.Context, destTable string, rec arrow.RecordBatch) (int64, error) {
	if err := q.ensureConnOpen(); err != nil {
		return 0, err
	}
	if destTable == "" {
		return 0, ErrEmptyTable
	}
	if rec == nil {
		return 0, ErrNilRecord
	}

	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	// Probe for existing table
	schema, _ := q.conn.GetTableSchema(ctx, q.catalogPtr(), q.dbSchemaPtr(), destTable)

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return 0, fmt.Errorf("couac: new statement: %w", err)
	}
	defer stmt.Close()

	if schema == nil {
		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
			return 0, fmt.Errorf("couac: set ingest mode create: %w", err)
		}
	} else {
		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend); err != nil {
			return 0, fmt.Errorf("couac: set ingest mode append: %w", err)
		}
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, destTable); err != nil {
		return 0, fmt.Errorf("couac: set target table: %w", err)
	}
	if err := stmt.Bind(ctx, rec); err != nil {
		return 0, fmt.Errorf("couac: bind record: %w", err)
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return n, fmt.Errorf("couac: execute ingest: %w", err)
	}
	return n, nil
}

// IngestCreateAppend is a backward-compatible alias for [Conn.Ingest].
//
// Deprecated: Use [Conn.Ingest] instead.
func (q *Conn) IngestCreateAppend(ctx context.Context, destTable string, rec arrow.RecordBatch) (int64, error) {
	return q.Ingest(ctx, destTable, rec)
}

// IngestMerge ingests an Arrow record batch with automatic schema
// evolution. If the target table does not exist, it is created. If the
// table exists and the record's schema matches, data is appended. If
// the schemas differ, the record is ingested into a temporary merge
// table and then merged into the target using UNION BY NAME, which
// adds any new columns.
//
// It returns the number of rows affected if known, otherwise -1.
//
// This is useful when the schema of incoming data may evolve over time
// (e.g. new fields added to a protobuf message).
func (q *Conn) IngestMerge(ctx context.Context, destTable string, rec arrow.RecordBatch) (int64, error) {
	if err := q.ensureConnOpen(); err != nil {
		return 0, err
	}
	if destTable == "" {
		return 0, ErrEmptyTable
	}
	if rec == nil {
		return 0, ErrNilRecord
	}

	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	mergeTable := destTable + "mergetmp"
	quotedDest := quoteIdentifier(destTable)
	quotedMerge := quoteIdentifier(mergeTable)

	// Clean up any leftover merge table from a previous failed operation.
	mergeSchema, _ := q.conn.GetTableSchema(ctx, q.catalogPtr(), q.dbSchemaPtr(), mergeTable)
	if mergeSchema != nil {
		mergeQuery := fmt.Sprintf(`CREATE OR REPLACE TABLE %s AS SELECT * FROM %s UNION BY NAME SELECT * FROM %s`,
			quotedDest, quotedDest, quotedMerge)
		q.execInternal(ctx, mergeQuery)
		q.dropTableRetry(ctx, quotedMerge, mergeTable)
	}

	// Probe for existing table
	schema, _ := q.conn.GetTableSchema(ctx, q.catalogPtr(), q.dbSchemaPtr(), destTable)
	schemaMismatch := schema != nil && schema.String() != rec.Schema().String()

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return 0, fmt.Errorf("couac: new statement: %w", err)
	}
	defer stmt.Close()

	// Choose ingest mode and target table
	if schema == nil || schemaMismatch {
		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
			return 0, fmt.Errorf("couac: set ingest mode create: %w", err)
		}
	} else {
		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend); err != nil {
			return 0, fmt.Errorf("couac: set ingest mode append: %w", err)
		}
	}

	targetTable := destTable
	if schemaMismatch {
		targetTable = mergeTable
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, targetTable); err != nil {
		return 0, fmt.Errorf("couac: set target table: %w", err)
	}
	if err := stmt.Bind(ctx, rec); err != nil {
		return 0, fmt.Errorf("couac: bind record: %w", err)
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return n, fmt.Errorf("couac: execute ingest: %w", err)
	}

	// Merge if schema mismatch
	if schemaMismatch {
		mergeQuery := fmt.Sprintf(`CREATE OR REPLACE TABLE %s AS
		SELECT * FROM %s
		UNION BY NAME
		SELECT * FROM %s`, quotedDest, quotedDest, quotedMerge)

		var mergeErr error
		for range 3 {
			_, mergeErr = q.execInternal(ctx, mergeQuery)
			if mergeErr == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if mergeErr != nil {
			return 0, fmt.Errorf("couac: merge into %s: %w", destTable, mergeErr)
		}
		q.dropTableRetry(ctx, quotedMerge, mergeTable)
	}
	return n, nil
}

// IngestCreateAppendMerge is a backward-compatible alias for [Conn.IngestMerge].
//
// Deprecated: Use [Conn.IngestMerge] instead.
func (q *Conn) IngestCreateAppendMerge(ctx context.Context, destTable string, rec arrow.RecordBatch) (int64, error) {
	return q.IngestMerge(ctx, destTable, rec)
}

// IngestReplace ingests an Arrow record batch, replacing the target table
// entirely (DROP + CREATE). It returns the number of rows affected
// if known, otherwise -1.
//
// This uses ADBC's Replace ingest mode, which is supported since
// ADBC 1.1.0 / DuckDB 0.9.0+.
func (q *Conn) IngestReplace(ctx context.Context, destTable string, rec arrow.RecordBatch) (int64, error) {
	if err := q.ensureConnOpen(); err != nil {
		return 0, err
	}
	if destTable == "" {
		return 0, ErrEmptyTable
	}
	if rec == nil {
		return 0, ErrNilRecord
	}

	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return 0, fmt.Errorf("couac: new statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace); err != nil {
		return 0, fmt.Errorf("couac: set ingest mode replace: %w", err)
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, destTable); err != nil {
		return 0, fmt.Errorf("couac: set target table: %w", err)
	}
	if err := stmt.Bind(ctx, rec); err != nil {
		return 0, fmt.Errorf("couac: bind record: %w", err)
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return n, fmt.Errorf("couac: execute ingest: %w", err)
	}
	return n, nil
}

// IngestStream ingests data from an Arrow RecordReader, which provides
// streaming access to record batches. This is more memory-efficient
// than [Ingest] for large datasets because it doesn't
// require holding all data in memory at once.
//
// The table is created if it does not exist, or appended to if it does.
func (q *Conn) IngestStream(ctx context.Context, destTable string, reader array.RecordReader) (int64, error) {
	if err := q.ensureConnOpen(); err != nil {
		return 0, err
	}
	if destTable == "" {
		return 0, ErrEmptyTable
	}
	if reader == nil {
		return 0, ErrNilRecord
	}

	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	// Probe for existing table
	schema, _ := q.conn.GetTableSchema(ctx, q.catalogPtr(), q.dbSchemaPtr(), destTable)

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return 0, fmt.Errorf("couac: new statement: %w", err)
	}
	defer stmt.Close()

	if schema == nil {
		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
			return 0, fmt.Errorf("couac: set ingest mode create: %w", err)
		}
	} else {
		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend); err != nil {
			return 0, fmt.Errorf("couac: set ingest mode append: %w", err)
		}
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, destTable); err != nil {
		return 0, fmt.Errorf("couac: set target table: %w", err)
	}
	if err := stmt.BindStream(ctx, reader); err != nil {
		return 0, fmt.Errorf("couac: bind stream: %w", err)
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return n, fmt.Errorf("couac: execute stream ingest: %w", err)
	}
	return n, nil
}

// execInternal executes a query without acquiring the parent's RWMutex.
// Caller must already hold the appropriate lock.
func (q *Conn) execInternal(ctx context.Context, query string) (int64, error) {
	stmt, err := q.conn.NewStatement()
	if err != nil {
		return 0, fmt.Errorf("couac: new statement: %w", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		return 0, fmt.Errorf("couac: set sql query: %w", err)
	}
	return stmt.ExecuteUpdate(ctx)
}

// dropTableRetry attempts to drop a table, retrying up to 5 times.
// Caller must already hold the parent's RWMutex read lock.
func (q *Conn) dropTableRetry(ctx context.Context, quotedName, rawName string) {
	for range 6 {
		schema, _ := q.conn.GetTableSchema(ctx, q.catalogPtr(), q.dbSchemaPtr(), rawName)
		if schema == nil {
			return
		}
		dropQuery := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, quotedName)
		q.execInternal(ctx, dropQuery)
		time.Sleep(50 * time.Millisecond)
	}
}
