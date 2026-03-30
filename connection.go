package couac

import (
	"errors"
	"fmt"
)

// Connect opens a new connection to the database and tracks it
// in the parent [DB]. The connection will be closed automatically
// when the parent's [DB.Close] is called, but it is good practice
// to close connections when no longer needed.
//
// Connect acquires a read lock on the parent; it will block if
// a maintenance operation (e.g. [DB.Compact]) is in progress.
func (q *DB) Connect() (*Conn, error) {
	if err := q.ensureOpen(); err != nil {
		return nil, err
	}
	q.mu.RLock()
	defer q.mu.RUnlock()

	qc := &Conn{}
	var err error
	qc.conn, err = q.db.Open(q.ctx)
	if err != nil {
		return nil, fmt.Errorf("couac: open connection: %w", err)
	}
	qc.parent = q

	q.mu.RUnlock()
	q.mu.Lock()
	q.ducklings = append(q.ducklings, qc)
	q.mu.Unlock()
	q.mu.RLock()

	return qc, nil
}

// NewConnection is a backward-compatible alias for [DB.Connect].
//
// Deprecated: Use [DB.Connect] instead.
func (q *DB) NewConnection() (*Conn, error) {
	return q.Connect()
}

// ConnectAs opens a new connection with the specified catalog
// and schema. The catalog and schema are used as defaults for metadata
// and ingest operations on this connection.
//
// Pass empty strings to use the default catalog ("memory" for in-memory
// databases, or the filename for file-backed databases) and default
// schema ("main").
func (q *DB) ConnectAs(catalog, schema string) (*Conn, error) {
	if err := q.ensureOpen(); err != nil {
		return nil, err
	}
	q.mu.RLock()
	defer q.mu.RUnlock()

	qc := &Conn{
		catalog:  catalog,
		dbSchema: schema,
	}
	var err error
	qc.conn, err = q.db.Open(q.ctx)
	if err != nil {
		return nil, fmt.Errorf("couac: open connection: %w", err)
	}
	qc.parent = q

	q.mu.RUnlock()
	q.mu.Lock()
	q.ducklings = append(q.ducklings, qc)
	q.mu.Unlock()
	q.mu.RLock()

	return qc, nil
}

// NewConnectionWithOpts is a backward-compatible alias for [DB.ConnectAs].
//
// Deprecated: Use [DB.ConnectAs] instead.
func (q *DB) NewConnectionWithOpts(catalog, schema string) (*Conn, error) {
	return q.ConnectAs(catalog, schema)
}

// ConnectionCount returns the number of currently tracked open connections.
func (q *DB) ConnectionCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.ducklings)
}

// Catalog returns the connection's catalog name. An empty string
// indicates the default catalog.
func (q *Conn) Catalog() string { return q.catalog }

// DBSchema returns the connection's database schema. An empty string
// indicates the default schema ("main").
func (q *Conn) DBSchema() string { return q.dbSchema }

// catalogPtr returns a pointer to the catalog name for use with ADBC
// methods that accept *string. Returns nil if the catalog is the default
// (empty string).
func (q *Conn) catalogPtr() *string {
	if q.catalog == "" {
		return nil
	}
	s := q.catalog
	return &s
}

// dbSchemaPtr returns a pointer to the schema name for use with ADBC
// methods that accept *string. Returns nil if the schema is the default
// (empty string).
func (q *Conn) dbSchemaPtr() *string {
	if q.dbSchema == "" {
		return nil
	}
	s := q.dbSchema
	return &s
}

// Close closes this connection and removes it from the parent's
// connection tracking. Close is idempotent; calling it more than once
// returns nil.
//
// It is important to close connections to allow DuckDB to properly
// commit WAL file changes.
func (q *Conn) Close() error {
	if q.closed.Swap(true) {
		return nil // already closed
	}

	// Remove from parent's tracking
	if q.parent != nil {
		q.parent.mu.Lock()
		for i, v := range q.parent.ducklings {
			if v == q {
				q.parent.removeConn(i)
				break
			}
		}
		q.parent.mu.Unlock()
		q.parent = nil
	}

	var errs []error
	if err := q.conn.Close(); err != nil {
		errs = append(errs, fmt.Errorf("couac: close connection: %w", err))
	}
	return errors.Join(errs...)
}

// removeConn removes the connection at index i using swap-with-last for
// O(1) removal. Caller must hold q.mu write lock.
func (q *DB) removeConn(i int) {
	last := len(q.ducklings) - 1
	if i != last {
		q.ducklings[i] = q.ducklings[last]
	}
	q.ducklings[last] = nil // avoid memory leak
	q.ducklings = q.ducklings[:last]
}

// ensureConnOpen returns ErrConnectionClosed if this connection has been closed.
func (q *Conn) ensureConnOpen() error {
	if q.closed.Load() {
		return ErrConnectionClosed
	}
	return nil
}
