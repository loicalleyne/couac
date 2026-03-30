package couac

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// execOnTxConn executes a SQL statement on a raw ADBC connection.
func execOnTxConn(ctx context.Context, conn adbc.Connection, sql string) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

// rollbackConn issues a ROLLBACK on a raw ADBC connection, ignoring errors.
func rollbackConn(ctx context.Context, conn adbc.Connection) {
	_ = execOnTxConn(ctx, conn, "ROLLBACK")
}

// Exec executes a statement that does not generate a result set (DDL, DML).
// It returns the number of rows affected if known, otherwise -1.
//
// Exec acquires a read lock on the parent database, allowing concurrent
// execution with other operations but blocking during maintenance.
func (q *Conn) Exec(ctx context.Context, query string) (int64, error) {
	if err := q.ensureConnOpen(); err != nil {
		return 0, err
	}
	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return 0, fmt.Errorf("couac: new statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return 0, fmt.Errorf("couac: set sql query: %w", err)
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return n, fmt.Errorf("couac: execute update: %w", err)
	}
	return n, nil
}

// Query executes a SQL query and returns a [QueryResult] containing a
// streaming Arrow [array.RecordReader]. The caller must call
// [QueryResult.Close] when done reading results.
//
// Query acquires a read lock only for the statement execution; the
// returned RecordReader can be consumed after the lock is released.
//
// Example:
//
//	res, err := conn.Query(ctx, "SELECT * FROM users WHERE age > 21")
//	if err != nil {
//	    return err
//	}
//	defer res.Close()
//	for res.Reader.Next() {
//	    rec := res.Reader.RecordBatch()
//	    // process rec...
//	}
func (q *Conn) Query(ctx context.Context, query string) (*QueryResult, error) {
	if err := q.ensureConnOpen(); err != nil {
		return nil, err
	}
	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("couac: new statement: %w", err)
	}

	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("couac: set sql query: %w", err)
	}

	rr, n, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("couac: execute query: %w", err)
	}

	return &QueryResult{
		Reader:       rr,
		stmt:         stmt,
		RowsAffected: n,
	}, nil
}

// QueryRaw executes a SQL query and returns the raw components: a
// RecordReader, the underlying ADBC Statement, and the number of rows
// affected. This is the original 4-return-value form for callers that
// need direct access to the statement.
//
// The caller is responsible for closing both the RecordReader and the
// Statement. Prefer [Conn.Query] for simpler resource management.
func (q *Conn) QueryRaw(ctx context.Context, query string) (array.RecordReader, adbc.Statement, int64, error) {
	if err := q.ensureConnOpen(); err != nil {
		return nil, nil, 0, err
	}
	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("couac: new statement: %w", err)
	}
	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, nil, 0, fmt.Errorf("couac: set sql query: %w", err)
	}
	rr, n, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, nil, 0, fmt.Errorf("couac: execute query: %w", err)
	}
	return rr, stmt, n, nil
}

// NewStatement creates a raw ADBC [Statement] on this connection.
// The caller must close the statement when done.
//
// Use this for advanced ADBC operations not covered by the convenience
// methods. For most use cases, prefer [Conn.Exec], [Conn.Query],
// or [Conn.Prepare].
func (q *Conn) NewStatement() (Statement, error) {
	if err := q.ensureConnOpen(); err != nil {
		return nil, err
	}
	return q.conn.NewStatement()
}

// Prepare creates a prepared statement for the given SQL query. The
// caller must close the statement when done.
//
// Use prepared statements for repeatedly executing the same query with
// different parameters. Bind parameters with [Statement.Bind] before
// executing.
//
// Example:
//
//	stmt, err := conn.Prepare(ctx, "INSERT INTO t VALUES (?, ?)")
//	if err != nil { ... }
//	defer stmt.Close()
//	stmt.Bind(ctx, record)
//	n, err := stmt.ExecuteUpdate(ctx)
func (q *Conn) Prepare(ctx context.Context, query string) (Statement, error) {
	if err := q.ensureConnOpen(); err != nil {
		return nil, err
	}
	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("couac: new statement: %w", err)
	}
	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("couac: set sql query: %w", err)
	}
	if err := stmt.Prepare(ctx); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("couac: prepare: %w", err)
	}
	return stmt, nil
}

// WithTransaction executes fn within a transaction. It opens a dedicated
// connection, disables auto-commit, executes fn, and then either commits
// (if fn returns nil) or rolls back (if fn returns an error or panics).
// The dedicated connection is closed after the transaction completes.
//
// DuckDB uses optimistic concurrency control. If fn modifies rows that
// are concurrently modified by another transaction, DuckDB may return a
// "Transaction conflict" error.
//
// Example:
//
//	err := db.WithTransaction(ctx, func(tx *couac.Conn) error {
//	    _, err := tx.Exec(ctx, "INSERT INTO t1 VALUES (1)")
//	    if err != nil { return err }
//	    _, err = tx.Exec(ctx, "INSERT INTO t2 VALUES (2)")
//	    return err
//	})
func (q *DB) WithTransaction(ctx context.Context, fn func(*Conn) error) (retErr error) {
	if err := q.ensureOpen(); err != nil {
		return err
	}

	// Open a dedicated connection for the transaction
	conn, err := q.db.Open(ctx)
	if err != nil {
		return fmt.Errorf("couac: open transaction connection: %w", err)
	}

	txConn := &Conn{
		parent: q,
		conn:   conn,
	}
	defer func() {
		// Don't track this connection in ducklings - it's ephemeral
		if r := recover(); r != nil {
			rollbackConn(ctx, conn)
			txConn.closed.Store(true)
			conn.Close()
			panic(r) // re-panic after rollback
		}
		txConn.closed.Store(true)
		conn.Close()
	}()

	// Disable auto-commit to start a transaction by executing BEGIN
	if err := execOnTxConn(ctx, conn, "BEGIN TRANSACTION"); err != nil {
		return fmt.Errorf("couac: begin transaction: %w", err)
	}

	retErr = fn(txConn)

	if retErr != nil {
		rollbackConn(ctx, conn)
		return retErr
	}

	if err := execOnTxConn(ctx, conn, "COMMIT"); err != nil {
		return fmt.Errorf("couac: commit: %w", err)
	}
	return nil
}
