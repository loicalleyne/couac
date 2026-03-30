package couac

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// compile-time interface checks for the database/sql/driver bridge.
var (
	_ driver.Connector        = (*dbConnector)(nil)
	_ driver.Conn             = (*sqlConn)(nil)
	_ driver.ConnPrepareContext = (*sqlConn)(nil)
	_ driver.ConnBeginTx      = (*sqlConn)(nil)
	_ driver.ExecerContext     = (*sqlConn)(nil)
	_ driver.QueryerContext    = (*sqlConn)(nil)
	_ driver.NamedValueChecker = (*sqlConn)(nil)
	_ driver.Stmt             = (*sqlStmt)(nil)
	_ driver.StmtExecContext  = (*sqlStmt)(nil)
	_ driver.StmtQueryContext = (*sqlStmt)(nil)
	_ driver.Tx               = (*sqlTx)(nil)
	_ driver.Rows             = (*sqlRows)(nil)
	_ driver.Result           = (*sqlResult)(nil)
)

// StdDB returns a [*sql.DB] backed by this DuckDB database. The
// returned sql.DB shares the same underlying ADBC database and
// connections.
//
// This is useful for integrating with code that expects a standard
// [*sql.DB], such as ORMs, migration tools, or test harnesses.
//
// The returned [*sql.DB] should be closed when no longer needed, but
// closing it does NOT close the underlying couac [DB].
//
// Column values are returned as native Go types matching the Arrow column
// type (int8–uint64, float32/float64, bool, string, []byte, time.Time,
// [Decimal]). Nested Arrow types are returned as [List], [Struct], and
// [Map] with full recursive conversion to Go-native values.
// See [arrowToDriverValue] for the complete mapping.
//
// Parameterized queries are supported with both ? and $N placeholder
// styles (e.g. "SELECT * FROM t WHERE id = ?" or "WHERE id = $1").
// Named parameters ($name) are not supported by DuckDB's ADBC driver.
// Supported Go parameter types: int64, float64, bool, string, []byte,
// time.Time, [Decimal], and nil.
//
// Limitations of the database/sql bridge:
//   - For full Arrow-native performance, prefer [Conn.Query] and
//     [Conn.Ingest] over database/sql.
//
// Example:
//
//	db, _ := couac.NewDuck()
//	defer db.Close()
//
//	stdDB := db.StdDB()
//	defer stdDB.Close()
//
//	var count int
//	stdDB.QueryRowContext(ctx, "SELECT count(*) FROM users").Scan(&count)
func (q *DB) StdDB() *sql.DB {
	return sql.OpenDB(&dbConnector{db: q})
}

// dbConnector implements [database/sql/driver.Connector].
type dbConnector struct {
	db *DB
}

// Connect opens a new ADBC connection and wraps it as a [driver.Conn].
func (c *dbConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := c.db.ensureOpen(); err != nil {
		return nil, err
	}
	conn, err := c.db.db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("couac: sql connect: %w", err)
	}
	return &sqlConn{conn: conn, db: c.db}, nil
}

// Driver returns the underlying [driver.Driver]. Since connections are
// created via [dbConnector.Connect], this driver's Open method is a
// no-op that returns an error directing callers to use [DB.StdDB].
func (c *dbConnector) Driver() driver.Driver {
	return &sqlDriver{}
}

// sqlDriver is a placeholder [driver.Driver]. Use [DB.StdDB] instead
// of [sql.Open].
type sqlDriver struct{}

// Open implements [driver.Driver]. It always returns an error because
// couac connections must be created via [DB.StdDB], not [sql.Open].
func (d *sqlDriver) Open(_ string) (driver.Conn, error) {
	return nil, fmt.Errorf("couac: use DB.StdDB() instead of sql.Open()")
}

// sqlConn wraps an ADBC connection to implement [driver.Conn].
type sqlConn struct {
	conn adbc.Connection
	db   *DB
}

// Prepare implements [driver.Conn]. It delegates to [sqlConn.PrepareContext]
// with a background context.
func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext implements [driver.ConnPrepareContext]. It creates a new
// ADBC statement and sets the SQL query text. The returned [driver.Stmt]
// supports parameterized execution via ? or $N placeholders.
func (c *sqlConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("couac: sql prepare: %w", err)
	}
	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("couac: sql set query: %w", err)
	}
	return &sqlStmt{stmt: stmt}, nil
}

// Close implements [driver.Conn]. It closes the underlying ADBC connection.
func (c *sqlConn) Close() error {
	return c.conn.Close()
}

// ExecContext implements [driver.ExecerContext], allowing one-shot
// parameterized statements without an explicit Prepare round-trip.
func (c *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("couac: sql exec: %w", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		return nil, fmt.Errorf("couac: sql set query: %w", err)
	}
	if err := bindArgs(ctx, stmt, args); err != nil {
		return nil, fmt.Errorf("couac: sql bind: %w", err)
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return nil, fmt.Errorf("couac: sql exec: %w", err)
	}
	return &sqlResult{rowsAffected: n}, nil
}

// QueryContext implements [driver.QueryerContext], allowing one-shot
// parameterized queries without an explicit Prepare round-trip.
func (c *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("couac: sql query: %w", err)
	}
	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("couac: sql set query: %w", err)
	}
	if err := bindArgs(ctx, stmt, args); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("couac: sql bind: %w", err)
	}
	rr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("couac: sql query: %w", err)
	}
	// stmt is intentionally NOT closed here — the Arrow RecordReader
	// returned by ExecuteQuery holds a reference to the statement's
	// result set. Closing stmt would invalidate the reader. The
	// statement will be closed when sqlRows.Close() releases the reader
	// (ADBC manages the lifecycle).
	return &sqlRows{rr: rr, stmt: stmt}, nil
}

// CheckNamedValue implements [driver.NamedValueChecker]. It allows
// [Decimal], [NullDecimal], [List], [NullList], [Struct], [NullStruct],
// [Map], and [NullMap] values to be passed as query parameters without
// database/sql rejecting them as unsupported types.
// Nested types are JSON-serialized for parameter binding.
func (c *sqlConn) CheckNamedValue(nv *driver.NamedValue) error {
	switch v := nv.Value.(type) {
	case Decimal:
		return nil // accepted as-is; namedValuesToRecord handles it
	case NullDecimal:
		if !v.Valid {
			nv.Value = nil
		} else {
			nv.Value = v.Decimal
		}
		return nil
	case List:
		s, _ := v.Value()
		nv.Value = s
		return nil
	case NullList:
		if !v.Valid {
			nv.Value = nil
		} else {
			s, _ := v.List.Value()
			nv.Value = s
		}
		return nil
	case Struct:
		s, _ := v.Value()
		nv.Value = s
		return nil
	case NullStruct:
		if !v.Valid {
			nv.Value = nil
		} else {
			s, _ := v.Struct.Value()
			nv.Value = s
		}
		return nil
	case Map:
		s, _ := v.Value()
		nv.Value = s
		return nil
	case NullMap:
		if !v.Valid {
			nv.Value = nil
		} else {
			s, _ := v.Map.Value()
			nv.Value = s
		}
		return nil
	default:
		return driver.ErrSkip // let database/sql apply default conversion
	}
}

// Begin implements [driver.Conn]. It delegates to [sqlConn.BeginTx] with a
// background context and default transaction options.
func (c *sqlConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements [driver.ConnBeginTx]. It honours the caller's
// context and supports read-only transactions via [sql.TxOptions].
// DuckDB does not support configurable isolation levels; any level
// other than [sql.LevelDefault] returns an error.
func (c *sqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, fmt.Errorf("couac: unsupported isolation level %d; DuckDB only supports the default level", opts.Isolation)
	}
	beginSQL := "BEGIN TRANSACTION"
	if opts.ReadOnly {
		beginSQL = "BEGIN TRANSACTION READ ONLY"
	}
	if err := execOnTxConn(ctx, c.conn, beginSQL); err != nil {
		return nil, fmt.Errorf("couac: sql begin: %w", err)
	}
	return &sqlTx{conn: c}, nil
}

// sqlTx implements [driver.Tx].
type sqlTx struct {
	conn *sqlConn
}

// Commit implements [driver.Tx]. It commits the current transaction.
func (t *sqlTx) Commit() error {
	return execOnTxConn(context.Background(), t.conn.conn, "COMMIT")
}

// Rollback implements [driver.Tx]. It rolls back the current transaction.
func (t *sqlTx) Rollback() error {
	return execOnTxConn(context.Background(), t.conn.conn, "ROLLBACK")
}

// sqlStmt implements [driver.Stmt], [driver.StmtExecContext], and
// [driver.StmtQueryContext]. It wraps an ADBC statement and supports
// parameterized execution via ? or $N placeholders.
type sqlStmt struct {
	stmt adbc.Statement
}

// Close implements [driver.Stmt]. It closes the underlying ADBC statement.
func (s *sqlStmt) Close() error {
	return s.stmt.Close()
}

// NumInput returns -1 (unknown) because ADBC does not expose parameter
// count information.
func (s *sqlStmt) NumInput() int {
	return -1
}

// Exec implements [driver.Stmt]. It delegates to [sqlStmt.ExecContext]
// with a background context and no parameters.
func (s *sqlStmt) Exec(_ []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), nil)
}

// ExecContext implements [driver.StmtExecContext]. It binds the given
// parameters (if any) and executes the statement, returning the number
// of rows affected.
func (s *sqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := bindArgs(ctx, s.stmt, args); err != nil {
		return nil, fmt.Errorf("couac: sql bind: %w", err)
	}
	n, err := s.stmt.ExecuteUpdate(ctx)
	if err != nil {
		return nil, fmt.Errorf("couac: sql exec: %w", err)
	}
	return &sqlResult{rowsAffected: n}, nil
}

// Query implements [driver.Stmt]. It delegates to [sqlStmt.QueryContext]
// with a background context and no parameters.
func (s *sqlStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), nil)
}

// QueryContext implements [driver.StmtQueryContext]. It binds the given
// parameters (if any) and executes the query, returning a [driver.Rows]
// that iterates over Arrow record batches.
func (s *sqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := bindArgs(ctx, s.stmt, args); err != nil {
		return nil, fmt.Errorf("couac: sql bind: %w", err)
	}
	rr, _, err := s.stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("couac: sql query: %w", err)
	}
	return &sqlRows{rr: rr}, nil
}

// sqlResult implements [driver.Result].
type sqlResult struct {
	rowsAffected int64
}

// LastInsertId implements [driver.Result]. DuckDB does not support
// auto-increment IDs via ADBC, so this always returns an error.
func (r *sqlResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("couac: LastInsertId not supported by DuckDB")
}

// RowsAffected implements [driver.Result]. It returns the number of rows
// affected by the statement.
func (r *sqlResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// sqlRows wraps an Arrow RecordReader to implement [driver.Rows].
// Values are returned as native Go types matching the Arrow column type.
// See [arrowToDriverValue] for the complete type mapping.
type sqlRows struct {
	rr     array.RecordReader
	rec    arrow.RecordBatch
	stmt   adbc.Statement // non-nil when created by QueryerContext (owns the statement)
	rowIdx int
	cols   []string
	closed bool
}

// Columns implements [driver.Rows]. It returns the column names from the
// Arrow schema of the result set.
func (r *sqlRows) Columns() []string {
	if r.cols != nil {
		return r.cols
	}
	schema := r.rr.Schema()
	r.cols = make([]string, len(schema.Fields()))
	for i, f := range schema.Fields() {
		r.cols[i] = f.Name
	}
	return r.cols
}

// Close implements [driver.Rows]. It releases the underlying Arrow
// RecordReader and, if this result set was created by a one-shot
// QueryContext call, also closes the ADBC statement.
func (r *sqlRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.rr.Release()
	if r.stmt != nil {
		return r.stmt.Close()
	}
	return nil
}

// Next implements [driver.Rows]. It advances to the next row, populating
// dest with native Go values converted from the Arrow columns. Returns
// [io.EOF] when no more rows are available.
func (r *sqlRows) Next(dest []driver.Value) error {
	for {
		if r.rec != nil && r.rowIdx < int(r.rec.NumRows()) {
			for i := 0; i < int(r.rec.NumCols()); i++ {
				col := r.rec.Column(i)
				if col.IsNull(r.rowIdx) {
					dest[i] = nil
				} else {
					dest[i] = arrowToDriverValue(col, r.rowIdx)
				}
			}
			r.rowIdx++
			return nil
		}
		if !r.rr.Next() {
			if err := r.rr.Err(); err != nil {
				return err
			}
			return io.EOF
		}
		r.rec = r.rr.RecordBatch()
		r.rowIdx = 0
	}
}
