package couac

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// Compact reclaims disk space by checkpointing and, for file-backed
// databases, creating a compacted copy.
//
// Compact acquires the write lock on the database, which blocks until
// all in-flight read operations complete and prevents new operations
// from starting. Open connections remain valid after compaction — they
// are NOT closed or invalidated.
//
// For file-backed databases, Compact:
//  1. Runs FORCE CHECKPOINT to flush the WAL.
//  2. ATTACHes a temporary database file.
//  3. Runs COPY FROM DATABASE to create a compacted copy.
//  4. DETACHes the temporary database.
//  5. Replaces the original file with the compacted copy.
//  6. Runs FORCE CHECKPOINT again to sync.
//
// For in-memory databases, only FORCE CHECKPOINT is run (which reclaims
// space from deleted rows when the database was created with COMPRESS
// mode).
//
// DuckDB's in-memory cache is preserved because at least one internal
// connection remains open during the operation.
func (q *DB) Compact(ctx context.Context) error {
	if err := q.ensureOpen(); err != nil {
		return err
	}

	// Acquire write lock — blocks all concurrent operations
	q.mu.Lock()
	defer q.mu.Unlock()

	// Open a dedicated internal connection for maintenance
	conn, err := q.db.Open(ctx)
	if err != nil {
		return fmt.Errorf("couac: compact open connection: %w", err)
	}
	defer conn.Close()

	execSQL := func(sql string) error {
		stmt, err := conn.NewStatement()
		if err != nil {
			return fmt.Errorf("couac: compact new statement: %w", err)
		}
		defer stmt.Close()
		if err := stmt.SetSqlQuery(sql); err != nil {
			return fmt.Errorf("couac: compact set query: %w", err)
		}
		_, err = stmt.ExecuteUpdate(ctx)
		return err
	}

	// Step 1: Force checkpoint
	if err := execSQL("FORCE CHECKPOINT"); err != nil {
		return fmt.Errorf("couac: compact checkpoint: %w", err)
	}

	// For in-memory databases, checkpoint is all we can do
	if q.path == "" {
		return nil
	}

	// Step 2: Create compacted copy
	tmpPath := q.path + ".couac_compact_tmp"
	defer os.Remove(tmpPath) // clean up on any error

	quotedTmp := quoteString(tmpPath)

	if err := execSQL(fmt.Sprintf("ATTACH %s AS _couac_compact", quotedTmp)); err != nil {
		return fmt.Errorf("couac: compact attach: %w", err)
	}

	// Step 3: Copy data to compacted file
	copyErr := execSQL("COPY FROM DATABASE memory TO _couac_compact")
	// Try to figure out the current database name for file-backed DBs
	if copyErr != nil {
		// For file-backed DBs the catalog name may differ; try generic approach
		baseName := filepath.Base(q.path)
		baseName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
		copyErr = execSQL(fmt.Sprintf("COPY FROM DATABASE %s TO _couac_compact", quoteIdentifier(baseName)))
	}

	// Always try to detach, even on copy error
	detachErr := execSQL("DETACH _couac_compact")

	if copyErr != nil {
		return fmt.Errorf("couac: compact copy: %w", copyErr)
	}
	if detachErr != nil {
		return fmt.Errorf("couac: compact detach: %w", detachErr)
	}

	// Step 4: Replace original with compacted copy
	if err := replaceFile(tmpPath, q.path); err != nil {
		return fmt.Errorf("couac: compact replace: %w", err)
	}

	// Step 5: Final checkpoint to sync
	if err := execSQL("FORCE CHECKPOINT"); err != nil {
		// Non-fatal: the compaction succeeded, just the final sync failed
		return fmt.Errorf("couac: compact final checkpoint: %w", err)
	}

	return nil
}

// Checkpoint synchronizes the WAL to the database file. This fails if
// any connections have running transactions. For a version that waits
// for running transactions, use [DB.ForceCheckpoint].
//
// Checkpoint acquires a read lock, allowing it to run concurrently
// with queries but not with maintenance operations.
func (q *DB) Checkpoint(ctx context.Context) error {
	if err := q.ensureOpen(); err != nil {
		return err
	}
	q.mu.RLock()
	defer q.mu.RUnlock()

	conn, err := q.db.Open(ctx)
	if err != nil {
		return fmt.Errorf("couac: checkpoint open connection: %w", err)
	}
	defer conn.Close()

	return execOnConn(ctx, conn, "CHECKPOINT")
}

// ForceCheckpoint synchronizes the WAL to the database file, waiting
// for any running transactions to complete first (DuckDB v1.4+).
//
// ForceCheckpoint acquires the write lock, blocking all concurrent
// operations until the checkpoint completes. Open connections remain
// valid afterward.
func (q *DB) ForceCheckpoint(ctx context.Context) error {
	if err := q.ensureOpen(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	conn, err := q.db.Open(ctx)
	if err != nil {
		return fmt.Errorf("couac: force checkpoint open connection: %w", err)
	}
	defer conn.Close()

	return execOnConn(ctx, conn, "FORCE CHECKPOINT")
}

// Attach attaches an additional database file to the current DuckDB
// instance under the given alias. The attached database appears as a
// separate catalog and can be queried with fully-qualified names
// (e.g. alias.schema.table).
//
// Attachment definitions are NOT persisted between DuckDB sessions;
// you must re-attach on each [NewDuck] call.
//
// Options:
//   - [ReadOnly]: open in read-only mode
//   - [WithBlockSize]: set the block size (power of 2, 16384–262144)
//   - [WithEncryptionKey]: set an encryption key
//
// Example:
//
//	err := conn.Attach(ctx, "other.db", "other_db", couac.ReadOnly())
//	res, _ := conn.Query(ctx, "SELECT * FROM other_db.main.users")
func (q *Conn) Attach(ctx context.Context, path, alias string, opts ...AttachOption) error {
	if err := q.ensureConnOpen(); err != nil {
		return err
	}

	cfg := &attachConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	var optParts []string
	if cfg.readOnly {
		optParts = append(optParts, "READ_ONLY")
	}
	if cfg.blockSize > 0 {
		optParts = append(optParts, fmt.Sprintf("BLOCK_SIZE %d", cfg.blockSize))
	}
	if cfg.encryptionKey != "" {
		optParts = append(optParts, fmt.Sprintf("ENCRYPTION_KEY '%s'", cfg.encryptionKey))
	}

	sql := fmt.Sprintf("ATTACH %s AS %s", quoteString(path), quoteIdentifier(alias))
	if len(optParts) > 0 {
		sql += " (" + strings.Join(optParts, ", ") + ")"
	}

	_, err := q.Exec(ctx, sql)
	return err
}

// Detach detaches a previously attached database. The alias must match
// the one used in [Conn.Attach].
func (q *Conn) Detach(ctx context.Context, alias string) error {
	_, err := q.Exec(ctx, fmt.Sprintf("DETACH %s", quoteIdentifier(alias)))
	return err
}

// CopyDatabase copies all data from the source database (identified by
// its catalog alias) to a new database file at dstPath. This creates
// a perfectly compacted copy.
//
// Example:
//
//	conn.CopyDatabase(ctx, "memory", "/tmp/backup.db")
func (q *Conn) CopyDatabase(ctx context.Context, srcAlias, dstPath string) error {
	if err := q.ensureConnOpen(); err != nil {
		return err
	}

	quotedDst := quoteString(dstPath)
	quotedSrc := quoteIdentifier(srcAlias)

	if _, err := q.Exec(ctx, fmt.Sprintf("ATTACH %s AS _couac_copy", quotedDst)); err != nil {
		return fmt.Errorf("couac: copy database attach: %w", err)
	}

	copyErr := func() error {
		_, err := q.Exec(ctx, fmt.Sprintf("COPY FROM DATABASE %s TO _couac_copy", quotedSrc))
		return err
	}()

	detachErr := func() error {
		_, err := q.Exec(ctx, "DETACH _couac_copy")
		return err
	}()

	if copyErr != nil {
		return fmt.Errorf("couac: copy database: %w", copyErr)
	}
	if detachErr != nil {
		return fmt.Errorf("couac: copy database detach: %w", detachErr)
	}
	return nil
}

// Databases returns the list of all attached database names (catalogs),
// including the default database.
func (q *Conn) Databases(ctx context.Context) ([]string, error) {
	res, err := q.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var dbs []string
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		col := rec.Column(0)
		for i := 0; i < col.Len(); i++ {
			dbs = append(dbs, cloneStr(col.ValueStr(i)))
		}
	}
	return dbs, nil
}

// Extensions returns the list of DuckDB extensions with their status.
func (q *Conn) Extensions(ctx context.Context) ([]Extension, error) {
	res, err := q.Query(ctx, "SELECT extension_name, loaded, installed, COALESCE(extension_version, '') as extension_version, COALESCE(description, '') as description, COALESCE(install_mode, '') as install_mode FROM duckdb_extensions()")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var exts []Extension
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			ext := Extension{
				Name:        cloneStr(rec.Column(0).ValueStr(i)),
				Loaded:      rec.Column(1).ValueStr(i) == "true",
				Installed:   rec.Column(2).ValueStr(i) == "true",
				Version:     cloneStr(rec.Column(3).ValueStr(i)),
				Description: cloneStr(rec.Column(4).ValueStr(i)),
				InstallMode: cloneStr(rec.Column(5).ValueStr(i)),
			}
			exts = append(exts, ext)
		}
	}
	return exts, nil
}

// InstallExtension installs a DuckDB extension by name. The extension
// is downloaded from the DuckDB extension repository if not already
// installed. Use [QuackCon.LoadExtension] to load it after installation.
//
// Example:
//
//	conn.InstallExtension(ctx, "httpfs")
//	conn.LoadExtension(ctx, "httpfs")
func (q *Conn) InstallExtension(ctx context.Context, name string) error {
	_, err := q.Exec(ctx, fmt.Sprintf("INSTALL %s", quoteIdentifier(name)))
	if err != nil {
		return fmt.Errorf("couac: install extension %q: %w", name, err)
	}
	return nil
}

// LoadExtension loads a previously installed DuckDB extension. Extensions
// cannot be unloaded or reloaded at runtime.
func (q *Conn) LoadExtension(ctx context.Context, name string) error {
	_, err := q.Exec(ctx, fmt.Sprintf("LOAD %s", quoteIdentifier(name)))
	if err != nil {
		return fmt.Errorf("couac: load extension %q: %w", name, err)
	}
	return nil
}

// Secrets returns the list of stored DuckDB secrets. Sensitive fields
// are redacted by DuckDB.
func (q *Conn) Secrets(ctx context.Context) ([]Secret, error) {
	res, err := q.Query(ctx, "SELECT name, type, provider, COALESCE(scope, '') as scope FROM duckdb_secrets()")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var secrets []Secret
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			s := Secret{
				Name:     cloneStr(rec.Column(0).ValueStr(i)),
				Type:     cloneStr(rec.Column(1).ValueStr(i)),
				Provider: cloneStr(rec.Column(2).ValueStr(i)),
				Scope:    cloneStr(rec.Column(3).ValueStr(i)),
			}
			secrets = append(secrets, s)
		}
	}
	return secrets, nil
}

// ExtensionsDir returns the default directory where DuckDB stores
// installed extension binaries (~/.duckdb/extensions/).
func ExtensionsDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".duckdb", "extensions")
}

// SecretsDir returns the default directory where DuckDB stores
// persistent secrets (~/.duckdb/stored_secrets/).
func SecretsDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".duckdb", "stored_secrets")
}

// execOnConn executes a single SQL statement on a raw ADBC connection.
func execOnConn(ctx context.Context, conn interface{ NewStatement() (adbc.Statement, error) }, sql string) error {
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

// replaceFile atomically replaces dst with src. On Windows, os.Rename
// may fail if dst is open, so we remove dst first.
func replaceFile(src, dst string) error {
	if runtime.GOOS == "windows" {
		// On Windows, rename fails if dst exists and is open.
		// Remove the WAL file first, then the main file.
		os.Remove(dst + ".wal")
		if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.Rename(src, dst)
}
