package couac

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	drivermgr "github.com/apache/arrow-adbc/go/adbc/drivermgr"
	dbcconfig "github.com/columnar-tech/dbc/config"
)

// openDatabases tracks file-backed databases that are currently open
// in this process. DuckDB only supports a single writer per file;
// this prevents confusing ADBC errors by catching duplicates early.
var openDatabases sync.Map // map[string]struct{}

// WithPath sets the database file path. If omitted or empty, the
// database is created in-memory.
func WithPath(path string) Option {
	return func(cfg config) {
		cfg.path = path
	}
}

// WithDriverName specifies the driver by name (e.g. "duckdb"). The
// ADBC driver manager resolves the name to a shared library using
// installed TOML driver manifests. This requires that the driver has
// been installed with the dbc CLI:
//
//	dbc install duckdb
//
// This is the default if no driver option is provided.
func WithDriverName(name string) Option {
	return func(cfg config) {
		cfg.driverPath = name
	}
}

// WithDriverPath specifies the explicit filesystem path to the DuckDB
// shared library (e.g. "/usr/local/lib/libduckdb.so" or
// "C:\\path\\to\\duckdb.dll"). Use this as an escape hatch when the
// driver is not installed via dbc.
func WithDriverPath(path string) Option {
	return func(cfg config) {
		cfg.driverPath = path
	}
}

// WithDriverLookup programmatically searches ADBC driver manifest
// directories for an installed "duckdb" driver, using the
// [github.com/columnar-tech/dbc/config] package. It searches in order:
//
//  1. ADBC_DRIVER_PATH environment variable
//  2. Virtual environment / Conda prefix
//  3. User config directory (~/.config/adbc/drivers on Linux,
//     ~/Library/Application Support/ADBC/Drivers on macOS,
//     %LOCALAPPDATA%\ADBC\Drivers on Windows)
//  4. System config directory (/etc/adbc/drivers on Linux,
//     C:\Program Files\ADBC\Drivers on Windows)
//
// If the driver is not found, [NewDuck] returns [ErrDriverNotFound].
// The driver must first be installed with:
//
//	dbc install duckdb
func WithDriverLookup() Option {
	return func(cfg config) {
		cfg.driverPath = "\x00lookup"
	}
}

// WithContext sets the default context used for opening new connections.
// If omitted, [context.Background] is used.
func WithContext(ctx context.Context) Option {
	return func(cfg config) {
		cfg.ctx = ctx
	}
}

// NewDuckDatabase is an alias for [NewDuck].
//
//go:fix inline
func NewDuckDatabase(opts ...Option) (*DB, error) {
	return NewDuck(opts...)
}

// NewDB is a deprecated alias for [NewDuck].
//
// Deprecated: Use [NewDuck] instead.
//
//go:fix inline
func NewDB(opts ...Option) (*DB, error) {
	return NewDuck(opts...)
}

// NewDuck opens a DuckDB database via ADBC.
//
// Options control the database file path, driver location, and default
// context. If no driver option is provided, the driver name "duckdb" is
// passed to the ADBC driver manager, which resolves it via TOML manifests
// installed by the dbc CLI.
//
// For file-backed databases, only one [DB] may be open per file path
// within a process. Attempting to open the same file twice returns
// [ErrPathAlreadyOpen].
//
// Example:
//
//	// In-memory database with default driver resolution:
//	db, err := couac.NewDuck()
//
//	// File-backed database with explicit driver path:
//	db, err := couac.NewDuck(
//	    couac.WithPath("analytics.db"),
//	    couac.WithDriverPath("/usr/local/lib/libduckdb.so"),
//	)
func NewDuck(opts ...Option) (*DB, error) {
	q := new(DB)
	for _, opt := range opts {
		opt(q)
	}

	// Resolve driver path
	driverPath, entrypoint, err := resolveDriver(q.driverPath)
	if err != nil {
		return nil, err
	}
	q.driverPath = driverPath

	if q.ctx == nil {
		q.ctx = context.Background()
	}

	// Guard against opening the same file twice
	if q.path != "" {
		absPath, err := filepath.Abs(q.path)
		if err == nil {
			q.path = absPath
		}
		if _, loaded := openDatabases.LoadOrStore(q.path, struct{}{}); loaded {
			return nil, ErrPathAlreadyOpen
		}
	}

	q.drv = drivermgr.Driver{}
	dbOpts := map[string]string{
		"driver":     driverPath,
		"entrypoint": entrypoint,
	}
	if q.path != "" {
		dbOpts["path"] = q.path
	}

	q.db, err = q.drv.NewDatabase(dbOpts)
	if err != nil {
		if q.path != "" {
			openDatabases.Delete(q.path)
		}
		return nil, fmt.Errorf("couac: new database: %w", err)
	}
	return q, nil
}

// resolveDriver determines the driver shared library path and entrypoint
// from the user's configuration.
func resolveDriver(configured string) (driverPath, entrypoint string, err error) {
	entrypoint = "duckdb_adbc_init"

	switch {
	case configured == "\x00lookup":
		// Programmatic manifest lookup via dbc/config
		return lookupDriver()
	case configured == "":
		// Default: pass "duckdb" by name for driver manager resolution
		return "duckdb", entrypoint, nil
	default:
		// Explicit path or name
		return configured, entrypoint, nil
	}
}

// lookupDriver searches installed ADBC driver manifests for the DuckDB
// driver using the dbc/config package.
func lookupDriver() (driverPath, entrypoint string, err error) {
	entrypoint = "duckdb_adbc_init"
	configs := dbcconfig.Get()

	// Search in priority order: Env > User > System
	for _, level := range []dbcconfig.ConfigLevel{
		dbcconfig.ConfigEnv,
		dbcconfig.ConfigUser,
		dbcconfig.ConfigSystem,
	} {
		cfg, ok := configs[level]
		if !ok {
			continue
		}
		info, lookupErr := dbcconfig.GetDriver(cfg, "duckdb")
		if lookupErr != nil {
			continue
		}
		dp := info.Driver.Shared.Get(dbcconfig.PlatformTuple())
		if dp != "" {
			if info.Driver.Entrypoint != "" {
				entrypoint = info.Driver.Entrypoint
			}
			return dp, entrypoint, nil
		}
	}
	return "", "", ErrDriverNotFound
}

// Close closes the database and all its open connections, releasing
// all associated resources. It is important to call Close to allow
// DuckDB to properly commit all WAL file changes.
//
// Close acquires the write lock, so it waits for any in-flight
// operations to complete. Close is idempotent; calling it more than
// once returns nil.
//
// Close returns an aggregated error if any connection or the database
// itself fails to close.
func (q *DB) Close() error {
	if q.closed.Swap(true) {
		return nil // already closed
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	var errs []error
	for _, d := range q.ducklings {
		if err := d.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("couac: close connection: %w", err))
		}
		d.closed.Store(true)
	}
	q.ducklings = nil

	if err := q.db.Close(); err != nil {
		errs = append(errs, fmt.Errorf("couac: close database: %w", err))
	}

	if q.path != "" {
		openDatabases.Delete(q.path)
	}

	return errors.Join(errs...)
}

// Path returns the path to the database file. An empty string indicates
// an in-memory database.
func (q *DB) Path() string { return q.path }

// DefaultContext returns the context used for opening new connections.
func (q *DB) DefaultContext() context.Context { return q.ctx }

// Ping verifies the database is reachable by executing SELECT 1 on a
// temporary connection. Returns an error if the database is closed or
// unreachable.
func (q *DB) Ping(ctx context.Context) error {
	if q.closed.Load() {
		return ErrDatabaseClosed
	}
	conn, err := q.db.Open(ctx)
	if err != nil {
		return fmt.Errorf("couac: ping: %w", err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return fmt.Errorf("couac: ping: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT 1"); err != nil {
		return fmt.Errorf("couac: ping: %w", err)
	}
	rr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("couac: ping: %w", err)
	}
	rr.Release()
	return nil
}

// DriverPath returns the resolved driver path being used.
func (q *DB) DriverPath() string { return q.driverPath }

// resetOpenDatabases is used by tests to clear the open-path guard.
func resetOpenDatabases() {
	openDatabases.Range(func(key, value any) bool {
		openDatabases.Delete(key)
		return true
	})
}

// ensureOpen returns ErrDatabaseClosed if the database has been closed.
func (q *DB) ensureOpen() error {
	if q.closed.Load() {
		return ErrDatabaseClosed
	}
	return nil
}

// internalConn opens a raw ADBC connection for internal maintenance
// operations. The caller must close the returned connection.
func (q *DB) internalConn(ctx context.Context) (adbc.Connection, error) {
	if err := q.ensureOpen(); err != nil {
		return nil, err
	}
	conn, err := q.db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("couac: open internal connection: %w", err)
	}
	return conn, nil
}

// driverPathFromEnv returns the ADBC_DRIVER_PATH environment variable, if set.
func driverPathFromEnv() string {
	return os.Getenv("ADBC_DRIVER_PATH")
}
