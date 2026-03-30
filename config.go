package couac

import (
	"context"
	"fmt"
	"strconv"
)

// Set sets a DuckDB configuration option. Most options are GLOBAL
// (instance-wide); some are SESSION-scoped (connection-specific).
// Use [Conn.Setting] to read the current value, and
// [Conn.Reset] to restore the default.
//
// Example:
//
//	conn.Set(ctx, "memory_limit", "4GB")
//	conn.Set(ctx, "threads", "8")
func (q *Conn) Set(ctx context.Context, key, value string) error {
	_, err := q.Exec(ctx, fmt.Sprintf("SET %s = '%s'", key, value))
	return err
}

// Reset restores a DuckDB configuration option to its default value.
func (q *Conn) Reset(ctx context.Context, key string) error {
	_, err := q.Exec(ctx, fmt.Sprintf("RESET %s", key))
	return err
}

// Setting reads the current value of a DuckDB configuration option.
//
// Example:
//
//	threads, err := conn.Setting(ctx, "threads")
//	memLimit, err := conn.Setting(ctx, "memory_limit")
func (q *Conn) Setting(ctx context.Context, key string) (string, error) {
	res, err := q.Query(ctx, fmt.Sprintf("SELECT current_setting('%s')", key))
	if err != nil {
		return "", err
	}
	defer res.Close()

	if res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		if rec.NumRows() > 0 {
			return cloneStr(rec.Column(0).ValueStr(0)), nil
		}
	}
	return "", fmt.Errorf("couac: setting %q not found", key)
}

// GetSetting is a backward-compatible alias for [Conn.Setting].
//
// Deprecated: Use [Conn.Setting] instead.
func (q *Conn) GetSetting(ctx context.Context, key string) (string, error) {
	return q.Setting(ctx, key)
}

// Settings returns all DuckDB configuration settings with their
// current values, descriptions, types, and scopes.
func (q *Conn) Settings(ctx context.Context) ([]Setting, error) {
	res, err := q.Query(ctx, "SELECT name, value, description, input_type, scope FROM duckdb_settings()")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var settings []Setting
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			s := Setting{
				Name:        cloneStr(rec.Column(0).ValueStr(i)),
				Value:       cloneStr(rec.Column(1).ValueStr(i)),
				Description: cloneStr(rec.Column(2).ValueStr(i)),
				InputType:   cloneStr(rec.Column(3).ValueStr(i)),
				Scope:       cloneStr(rec.Column(4).ValueStr(i)),
			}
			settings = append(settings, s)
		}
	}
	return settings, nil
}

// LockConfiguration prevents further configuration changes for the
// remainder of this session. This is useful to lock in settings during
// stable operation.
func (q *Conn) LockConfiguration(ctx context.Context) error {
	return q.Set(ctx, "lock_configuration", "true")
}

// --- Performance Tuning ---

// SetMemoryLimit sets the buffer manager's memory limit (e.g. "4GB",
// "512MB"). Note: actual memory consumption may exceed this limit
// because vectors, query results, and some aggregate states are
// allocated outside the buffer manager.
func (q *Conn) SetMemoryLimit(ctx context.Context, limit string) error {
	return q.Set(ctx, "memory_limit", limit)
}

// SetThreads sets the number of threads used for parallel query
// execution. Defaults to the number of CPU cores. Consider setting
// this to the number of physical cores (not hyperthreads) if
// hyperthreading causes slowdowns.
func (q *Conn) SetThreads(ctx context.Context, n int) error {
	return q.Set(ctx, "threads", strconv.Itoa(n))
}

// SetTempDirectory sets the directory used for spilling data to disk
// when memory is insufficient. Defaults to <database_file>.tmp for
// file-backed databases or .tmp for in-memory databases.
func (q *Conn) SetTempDirectory(ctx context.Context, path string) error {
	return q.Set(ctx, "temp_directory", path)
}

// SetMaxTempDirectorySize sets the maximum size of the temp directory
// (e.g. "50GB"). Defaults to 90% of available disk space.
func (q *Conn) SetMaxTempDirectorySize(ctx context.Context, size string) error {
	return q.Set(ctx, "max_temp_directory_size", size)
}

// SetPreserveInsertionOrder controls whether result ordering matches
// insertion order. Disabling this (false) can improve performance and
// reduce memory usage for larger-than-memory workloads.
func (q *Conn) SetPreserveInsertionOrder(ctx context.Context, preserve bool) error {
	return q.Set(ctx, "preserve_insertion_order", strconv.FormatBool(preserve))
}

// --- Introspection ---

// Describe returns column information for a table or query result.
// Pass either a table name or a full SELECT statement.
//
// Example:
//
//	cols, err := conn.Describe(ctx, "users")
//	cols, err := conn.Describe(ctx, "SELECT * FROM users WHERE age > 21")
func (q *Conn) Describe(ctx context.Context, tableOrQuery string) ([]ColumnInfo, error) {
	res, err := q.Query(ctx, fmt.Sprintf("DESCRIBE %s", tableOrQuery))
	if err != nil {
		return nil, fmt.Errorf("couac: describe: %w", err)
	}
	defer res.Close()

	var cols []ColumnInfo
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			c := ColumnInfo{
				Name:    cloneStr(rec.Column(0).ValueStr(i)),
				Type:    cloneStr(rec.Column(1).ValueStr(i)),
				Null:    cloneStr(rec.Column(2).ValueStr(i)),
				Key:     cloneStr(rec.Column(3).ValueStr(i)),
				Default: cloneStr(rec.Column(4).ValueStr(i)),
				Extra:   cloneStr(rec.Column(5).ValueStr(i)),
			}
			cols = append(cols, c)
		}
	}
	return cols, nil
}

// Summarize computes aggregate statistics (min, max, approx_unique,
// avg, std, q25, q50, q75, count, null_percentage) over all columns
// of a table or query result. Returns a [QueryResult] for streaming
// access to the statistics.
func (q *Conn) Summarize(ctx context.Context, tableOrQuery string) (*QueryResult, error) {
	return q.Query(ctx, fmt.Sprintf("SUMMARIZE %s", tableOrQuery))
}

// ShowTables returns the names of all tables in the current schema.
func (q *Conn) ShowTables(ctx context.Context) ([]string, error) {
	res, err := q.Query(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var tables []string
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		col := rec.Column(0)
		for i := 0; i < col.Len(); i++ {
			tables = append(tables, cloneStr(col.ValueStr(i)))
		}
	}
	return tables, nil
}

// ShowAllTables returns information about all tables across all
// databases and schemas.
func (q *Conn) ShowAllTables(ctx context.Context) ([]TableInfo, error) {
	res, err := q.Query(ctx, "SHOW ALL TABLES")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var tables []TableInfo
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			t := TableInfo{
				Database:  cloneStr(rec.Column(0).ValueStr(i)),
				Schema:    cloneStr(rec.Column(1).ValueStr(i)),
				TableName: cloneStr(rec.Column(2).ValueStr(i)),
				Temporary: rec.Column(5).ValueStr(i) == "true",
			}
			tables = append(tables, t)
		}
	}
	return tables, nil
}

// --- Environment Information ---

// Version returns the DuckDB version string (e.g. "v1.5.1").
func (q *Conn) Version(ctx context.Context) (string, error) {
	res, err := q.Query(ctx, "SELECT version()")
	if err != nil {
		return "", err
	}
	defer res.Close()
	if res.Reader.Next() {
		return cloneStr(res.Reader.RecordBatch().Column(0).ValueStr(0)), nil
	}
	return "", fmt.Errorf("couac: no version result")
}

// Platform returns the DuckDB platform identifier (e.g. "linux_amd64",
// "osx_arm64", "windows_amd64").
func (q *Conn) Platform(ctx context.Context) (string, error) {
	res, err := q.Query(ctx, "CALL pragma_platform()")
	if err != nil {
		return "", err
	}
	defer res.Close()
	if res.Reader.Next() {
		return cloneStr(res.Reader.RecordBatch().Column(0).ValueStr(0)), nil
	}
	return "", fmt.Errorf("couac: no platform result")
}

// UserAgent returns the DuckDB user agent string (e.g.
// "duckdb/v1.5.1(windows_amd64)").
func (q *Conn) UserAgent(ctx context.Context) (string, error) {
	res, err := q.Query(ctx, "PRAGMA user_agent")
	if err != nil {
		return "", err
	}
	defer res.Close()
	if res.Reader.Next() {
		return cloneStr(res.Reader.RecordBatch().Column(0).ValueStr(0)), nil
	}
	return "", fmt.Errorf("couac: no user agent result")
}

// DatabaseSize returns size information for all databases (including
// attached databases).
func (q *Conn) DatabaseSize(ctx context.Context) ([]DatabaseSize, error) {
	res, err := q.Query(ctx, "PRAGMA database_size")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var sizes []DatabaseSize
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			s := DatabaseSize{
				DatabaseName: cloneStr(rec.Column(0).ValueStr(i)),
				DatabaseSize: cloneStr(rec.Column(1).ValueStr(i)),
			}
			sizes = append(sizes, s)
		}
	}
	return sizes, nil
}

// StorageInfo returns detailed storage information for a table,
// including row group and compression details.
func (q *Conn) StorageInfo(ctx context.Context, table string) (*QueryResult, error) {
	return q.Query(ctx, fmt.Sprintf("PRAGMA storage_info(%s)", quoteIdentifier(table)))
}

// --- Profiling ---

// EnableProfiling enables query profiling. Format can be "json",
// "query_tree", "query_tree_optimizer", or "no_output".
func (q *Conn) EnableProfiling(ctx context.Context, format string) error {
	if format == "" {
		format = "query_tree"
	}
	return q.Set(ctx, "enable_profiling", format)
}

// DisableProfiling disables query profiling.
func (q *Conn) DisableProfiling(ctx context.Context) error {
	_, err := q.Exec(ctx, "PRAGMA disable_profiling")
	return err
}

// SetProfilingOutput sets the file path for profiling output.
func (q *Conn) SetProfilingOutput(ctx context.Context, path string) error {
	return q.Set(ctx, "profiling_output", path)
}

// Explain returns the query plan for a SQL statement without executing it.
func (q *Conn) Explain(ctx context.Context, query string) (string, error) {
	res, err := q.Query(ctx, fmt.Sprintf("EXPLAIN %s", query))
	if err != nil {
		return "", err
	}
	defer res.Close()

	var plan string
	if res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		// EXPLAIN returns two columns: explain_key, explain_value
		if rec.NumCols() >= 2 {
			plan = cloneStr(rec.Column(1).ValueStr(0))
		}
	}
	return plan, nil
}
