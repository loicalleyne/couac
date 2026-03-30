// Package couac provides a convenient, ergonomic wrapper around ADBC
// (Arrow Database Connectivity) for DuckDB.
//
// Couac simplifies working with DuckDB through ADBC by providing:
//   - Database lifecycle management with automatic driver discovery
//   - Connection pooling with concurrency-safe tracking
//   - Query execution returning Apache Arrow record batches
//   - Bulk ingestion from Arrow record batches with automatic table creation
//   - Schema evolution via UNION BY NAME merge strategy
//   - Hierarchical catalog/schema/table metadata with typed navigation
//   - Safe database compaction without disrupting open connections
//   - Extension and secret management
//   - Pragma, tuning, and introspection convenience wrappers
//
// DuckDB's ADBC driver supports the full ADBC specification except for
// ReadPartition and ExecutePartitions (not applicable to a non-distributed
// database). Notably, DuckDB does not support the CreateAppend ingest mode
// or statement-level AutoCommit; couac works around both limitations.
//
// # Concurrency Model
//
// DuckDB uses MVCC with optimistic concurrency control. Within a single
// process, multiple connections may read and write concurrently:
//   - Appends never conflict, even on the same table.
//   - Concurrent updates/deletes on different rows succeed.
//   - Concurrent updates/deletes on the same row produce a conflict error.
//   - Only one process may open a database file for writing at a time.
//
// Couac protects its internal connection list with a sync.RWMutex. Normal
// operations acquire a read lock (allowing full concurrency). Maintenance
// operations like [DB.Compact] acquire a write lock, blocking new
// operations until maintenance completes.
//
// # Driver Discovery
//
// Couac supports three modes for locating the DuckDB shared library:
//   - [WithDriverName]: pass a bare name (e.g. "duckdb") and let the ADBC
//     driver manager resolve it via installed TOML manifests (requires
//     [dbc install duckdb]).
//   - [WithDriverPath]: explicit path to libduckdb.so / .dylib / .dll.
//   - [WithDriverLookup]: programmatic search of ADBC manifest directories
//     using the [github.com/columnar-tech/dbc/config] package.
//
// If no driver option is supplied, couac defaults to WithDriverName("duckdb").
package couac

import (
	"errors"
	"sync"
	"sync/atomic"

	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Sentinel errors returned by couac functions.
var (
	// ErrNilRecord is returned when a nil arrow.RecordBatch is passed to an ingest function.
	ErrNilRecord = errors.New("couac: nil arrow record batch")
	// ErrEmptyTable is returned when an empty destination table name is provided.
	ErrEmptyTable = errors.New("couac: empty destination table name")
	// ErrDatabaseClosed is returned when an operation is attempted on a closed database.
	ErrDatabaseClosed = errors.New("couac: database is closed")
	// ErrConnectionClosed is returned when an operation is attempted on a closed connection.
	ErrConnectionClosed = errors.New("couac: connection is closed")
	// ErrDriverNotFound is returned when the DuckDB driver cannot be located.
	ErrDriverNotFound = errors.New("couac: duckdb driver not found; install with: dbc install duckdb")
	// ErrPathAlreadyOpen is returned when attempting to open a database file that
	// is already open in this process.
	ErrPathAlreadyOpen = errors.New("couac: database file is already open in this process")
)

// ObjectDepth controls how deep [Conn.Objects] recurses into the
// catalog hierarchy.
type ObjectDepth int

const (
	// ObjectDepthAll requests catalogs, schemas, tables, and columns.
	ObjectDepthAll ObjectDepth = iota
	// ObjectDepthCatalogs requests only catalog names.
	ObjectDepthCatalogs
	// ObjectDepthDBSchemas requests catalogs and their schemas.
	ObjectDepthDBSchemas
	// ObjectDepthTables requests catalogs, schemas, and tables (no columns).
	ObjectDepthTables
	// ObjectDepthColumns is an alias for ObjectDepthAll.
	ObjectDepthColumns = ObjectDepthAll
)

// Quacker is a backward-compatible alias for [DB].
//
//go:fix inline
type Quacker = DB

// QuackCon is a backward-compatible alias for [Conn].
//
//go:fix inline
type QuackCon = Conn

// DuckDatabase is a backward-compatible alias for [DB].
//
//go:fix inline
type DuckDatabase = DB

// Connection is a backward-compatible alias for [Conn].
//
//go:fix inline
type Connection = Conn

// Statement is an alias for [adbc.Statement].
type Statement = adbc.Statement

// DB represents an open DuckDB database accessed through ADBC.
//
// A DB holds the ADBC driver, database handle, and tracks all open
// connections. It is safe for concurrent use; internal state is protected
// by a [sync.RWMutex]. Normal operations (queries, ingests) acquire a read
// lock, while maintenance operations ([Compact], [ForceCheckpoint]) acquire
// a write lock to safely pause concurrent work.
//
// Call [NewDuck] to create a DB and [DB.Close] to release resources.
type DB struct {
	ctx context.Context
	drv adbc.Driver
	db  adbc.Database
	// mu protects ducklings and is used by Compact/ForceCheckpoint to
	// pause all concurrent operations.
	mu sync.RWMutex
	// duckdb database connections
	ducklings []*Conn
	// path to database file, in-memory if empty
	path       string
	driverPath string
	closed     atomic.Bool
}

// Conn represents a single connection to a DuckDB database.
//
// Conn wraps an ADBC connection and provides methods for executing
// queries, ingesting data, and inspecting metadata. Each Conn is
// tracked by its parent [DB] and will be closed automatically when
// the parent is closed.
//
// Conn methods that perform database operations acquire a read lock
// on the parent DB's mutex, allowing concurrent use. Maintenance
// operations on the parent (e.g. [DB.Compact]) will block until
// all in-flight operations complete.
type Conn struct {
	parent   *DB
	conn     adbc.Connection
	catalog  string
	dbSchema string
	closed   atomic.Bool
}

// Option configures a [DB] during construction via [NewDuck].
type (
	Option func(config)
	config *DB
)

// QueryResult holds the results of a [Conn.Query] call.
//
// The Reader field provides streaming access to Arrow record batches.
// RowsAffected contains the number of rows affected if known, otherwise -1.
// The caller must call Close when done reading to release resources.
//
// Since ADBC 1.1.0, releasing the Reader without fully consuming it is
// equivalent to calling AdbcStatementCancel.
type QueryResult struct {
	// Reader provides streaming access to Arrow record batches.
	Reader array.RecordReader
	// stmt is the underlying ADBC statement (closed by QueryResult.Close).
	stmt adbc.Statement
	// RowsAffected is the number of rows affected, or -1 if unknown.
	RowsAffected int64
}

// Close releases the resources associated with the query result.
// It closes both the underlying statement and the record reader.
// Close is idempotent.
func (qr *QueryResult) Close() error {
	var errs []error
	if qr.Reader != nil {
		qr.Reader.Release()
		qr.Reader = nil
	}
	if qr.stmt != nil {
		if err := qr.stmt.Close(); err != nil {
			errs = append(errs, err)
		}
		qr.stmt = nil
	}
	return errors.Join(errs...)
}

// Schema returns the Arrow schema of the result set, or nil if the
// Reader has been closed.
func (qr *QueryResult) Schema() *arrow.Schema {
	if qr.Reader == nil {
		return nil
	}
	return qr.Reader.Schema()
}

// ObjectsOption configures a [Conn.Objects] call.
type ObjectsOption func(*objectsConfig)

type objectsConfig struct {
	depth      ObjectDepth
	catalog    *string
	dbSchema   *string
	tableName  *string
	columnName *string
	tableTypes []string
}

// WithDepth sets the [ObjectDepth] for an Objects call, controlling
// how deep to recurse into the catalog hierarchy.
func WithDepth(depth ObjectDepth) ObjectsOption {
	return func(cfg *objectsConfig) {
		cfg.depth = depth
	}
}

// WithCatalogFilter filters Objects results to catalogs matching the
// given name. An empty string matches only objects without a catalog.
func WithCatalogFilter(catalog string) ObjectsOption {
	return func(cfg *objectsConfig) {
		cfg.catalog = &catalog
	}
}

// WithSchemaFilter filters Objects results to schemas matching the
// given name.
func WithSchemaFilter(schema string) ObjectsOption {
	return func(cfg *objectsConfig) {
		cfg.dbSchema = &schema
	}
}

// WithTableFilter filters Objects results to tables matching the
// given name.
func WithTableFilter(table string) ObjectsOption {
	return func(cfg *objectsConfig) {
		cfg.tableName = &table
	}
}

// WithColumnFilter filters Objects results to columns matching the
// given name.
func WithColumnFilter(column string) ObjectsOption {
	return func(cfg *objectsConfig) {
		cfg.columnName = &column
	}
}

// WithTableTypes filters Objects results to the given table types
// (e.g. []string{"TABLE", "VIEW"}).
func WithTableTypes(types []string) ObjectsOption {
	return func(cfg *objectsConfig) {
		cfg.tableTypes = types
	}
}

// AttachOption configures a [Conn.Attach] call.
type AttachOption func(*attachConfig)

type attachConfig struct {
	readOnly      bool
	blockSize     int
	encryptionKey string
}

// ReadOnly configures an ATTACH operation to open the database in
// read-only mode.
func ReadOnly() AttachOption {
	return func(cfg *attachConfig) {
		cfg.readOnly = true
	}
}

// WithBlockSize sets the block size for an attached database. Must be a
// power of 2 between 16384 (16 KB) and 262144 (256 KB).
func WithBlockSize(size int) AttachOption {
	return func(cfg *attachConfig) {
		cfg.blockSize = size
	}
}

// WithEncryptionKey sets the encryption key for an attached database.
func WithEncryptionKey(key string) AttachOption {
	return func(cfg *attachConfig) {
		cfg.encryptionKey = key
	}
}

// CatalogInfo represents a single catalog in the DuckDB database hierarchy.
// When multiple databases are ATTACHed, each appears as a separate CatalogInfo.
type CatalogInfo struct {
	CatalogName      string       `json:"catalog_name"`
	CatalogDBSchemas []SchemaInfo `json:"catalog_db_schemas"`
}

// SchemaInfo represents a schema within a catalog.
type SchemaInfo struct {
	DBSchemaName   string        `json:"db_schema_name"`
	DBSchemaTables []TableSchema `json:"db_schema_tables"`
}

// TableSchema describes a table or view within a schema.
type TableSchema struct {
	TableName        string             `json:"table_name"`
	TableType        string             `json:"table_type"`
	TableColumns     []ColumnSchema     `json:"table_columns"`
	TableConstraints []ConstraintSchema `json:"table_constraints"`
}

// ColumnSchema describes a single column within a table.
type ColumnSchema struct {
	ColumnName            string `json:"column_name"`
	OrdinalPosition       int32  `json:"ordinal_position"`
	Remarks               string `json:"remarks"`
	XdbcDataType          int16  `json:"xdbc_data_type"`
	XdbcTypeName          string `json:"xdbc_type_name"`
	XdbcColumnSize        int32  `json:"xdbc_column_size"`
	XdbcDecimalDigits     int16  `json:"xdbc_decimal_digits"`
	XdbcNumPrecRadix      int16  `json:"xdbc_num_prec_radix"`
	XdbcNullable          int16  `json:"xdbc_nullable"`
	XdbcColumnDef         string `json:"xdbc_column_def"`
	XdbcSqlDataType       int16  `json:"xdbc_sql_data_type"`
	XdbcDatetimeSub       int16  `json:"xdbc_datetime_sub"`
	XdbcCharOctetLength   int32  `json:"xdbc_char_octet_length"`
	XdbcIsNullable        string `json:"xdbc_is_nullable"`
	XdbcScopeCatalog      string `json:"xdbc_scope_catalog"`
	XdbcScopeSchema       string `json:"xdbc_scope_schema"`
	XdbcScopeTable        string `json:"xdbc_scope_table"`
	XdbcIsAutoincrement   bool   `json:"xdbc_is_autoincrement"`
	XdbcIsGeneratedColumn bool   `json:"xdbc_is_generatedcolumn"`
}

// ConstraintSchema describes a constraint on a table (CHECK, FOREIGN KEY,
// PRIMARY KEY, or UNIQUE).
type ConstraintSchema struct {
	ConstraintName        string        `json:"constraint_name"`
	ConstraintType        string        `json:"constraint_type"`
	ConstraintColumnNames []string      `json:"constraint_column_names"`
	ConstraintColumnUsage []UsageSchema `json:"constraint_column_usage"`
}

// UsageSchema describes a foreign key reference to another table.
type UsageSchema struct {
	FKCatalog    string `json:"fk_catalog"`
	FKDBSchema   string `json:"fk_db_schema"`
	FKTable      string `json:"fk_table"`
	FKColumnName string `json:"fk_column_name"`
}

// DBObject is a backward-compatible alias for [CatalogInfo].
//
//go:fix inline
type DBObject = CatalogInfo

// DBSchema is a backward-compatible alias for [SchemaInfo].
//
//go:fix inline
type DBSchema = SchemaInfo

// DBObjects is a backward-compatible alias for [CatalogTree].
//
//go:fix inline
type DBObjects = CatalogTree

// GetObjectsOption is a backward-compatible alias for [ObjectsOption].
//
//go:fix inline
type GetObjectsOption = ObjectsOption

// CatalogTree wraps a []CatalogInfo and provides typed navigation methods
// for exploring catalog metadata without additional ADBC round-trips.
//
// Use [Conn.Objects] to obtain a CatalogTree instance.
type CatalogTree struct {
	objects []CatalogInfo
}

// Catalogs returns the names of all catalogs in the result set.
func (o *CatalogTree) Catalogs() []string {
	names := make([]string, 0, len(o.objects))
	for _, obj := range o.objects {
		names = append(names, obj.CatalogName)
	}
	return names
}

// Schemas returns the schema names within the given catalog.
// Returns nil if the catalog is not found.
func (o *CatalogTree) Schemas(catalog string) []string {
	for _, obj := range o.objects {
		if obj.CatalogName == catalog {
			names := make([]string, 0, len(obj.CatalogDBSchemas))
			for _, s := range obj.CatalogDBSchemas {
				names = append(names, s.DBSchemaName)
			}
			return names
		}
	}
	return nil
}

// Tables returns the tables within the given catalog and schema.
// Returns nil if the catalog or schema is not found.
func (o *CatalogTree) Tables(catalog, schema string) []TableSchema {
	for _, obj := range o.objects {
		if obj.CatalogName == catalog {
			for _, s := range obj.CatalogDBSchemas {
				if s.DBSchemaName == schema {
					return s.DBSchemaTables
				}
			}
		}
	}
	return nil
}

// Columns returns the columns for the specified table.
// Returns nil if the catalog, schema, or table is not found.
func (o *CatalogTree) Columns(catalog, schema, table string) []ColumnSchema {
	for _, t := range o.Tables(catalog, schema) {
		if t.TableName == table {
			return t.TableColumns
		}
	}
	return nil
}

// FindTable searches all catalogs and schemas for a table with the given
// name. Returns the catalog, schema, table, and whether it was found.
// If multiple tables share the same name across catalogs/schemas, the
// first match is returned.
func (o *CatalogTree) FindTable(name string) (catalog, schema string, table *TableSchema, found bool) {
	for _, obj := range o.objects {
		for _, s := range obj.CatalogDBSchemas {
			for i := range s.DBSchemaTables {
				if s.DBSchemaTables[i].TableName == name {
					return obj.CatalogName, s.DBSchemaName, &s.DBSchemaTables[i], true
				}
			}
		}
	}
	return "", "", nil, false
}

// TableExists reports whether a table with the given name exists in the
// specified catalog and schema.
func (o *CatalogTree) TableExists(catalog, schema, table string) bool {
	for _, t := range o.Tables(catalog, schema) {
		if t.TableName == table {
			return true
		}
	}
	return false
}

// Constraints returns the constraints for the specified table.
// Returns nil if the catalog, schema, or table is not found.
func (o *CatalogTree) Constraints(catalog, schema, table string) []ConstraintSchema {
	for _, t := range o.Tables(catalog, schema) {
		if t.TableName == table {
			return t.TableConstraints
		}
	}
	return nil
}

// Map returns the catalog hierarchy as a nested map:
// catalog name → schema name → []TableSchema.
// This is useful for bulk iteration over all metadata.
func (o *CatalogTree) Map() map[string]map[string][]TableSchema {
	m := make(map[string]map[string][]TableSchema, len(o.objects))
	for _, obj := range o.objects {
		sm := make(map[string][]TableSchema, len(obj.CatalogDBSchemas))
		for _, s := range obj.CatalogDBSchemas {
			sm[s.DBSchemaName] = s.DBSchemaTables
		}
		m[obj.CatalogName] = sm
	}
	return m
}

// Raw returns the underlying []CatalogInfo slice for direct access.
func (o *CatalogTree) Raw() []CatalogInfo {
	return o.objects
}

// Extension describes an installed DuckDB extension.
type Extension struct {
	Name        string `json:"extension_name"`
	Loaded      bool   `json:"loaded"`
	Installed   bool   `json:"installed"`
	Version     string `json:"extension_version"`
	Description string `json:"description"`
	InstallMode string `json:"install_mode"`
}

// Secret describes a stored DuckDB secret. Sensitive fields are redacted
// by DuckDB.
type Secret struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Provider string `json:"provider"`
	Scope    string `json:"scope"`
}

// Setting describes a DuckDB configuration setting.
type Setting struct {
	Name        string `json:"name"`
	Value       string `json:"value"`
	Description string `json:"description"`
	InputType   string `json:"input_type"`
	Scope       string `json:"scope"`
}

// DatabaseSize describes the on-disk size information for a database.
type DatabaseSize struct {
	DatabaseName string `json:"database_name"`
	DatabaseSize string `json:"database_size"`
	BlockSize    int64  `json:"block_size"`
	TotalBlocks  int64  `json:"total_blocks"`
	UsedBlocks   int64  `json:"used_blocks"`
	FreeBlocks   int64  `json:"free_blocks"`
	WALSize      string `json:"wal_size"`
	MemoryUsage  string `json:"memory_usage"`
	MemoryLimit  string `json:"memory_limit"`
}

// ColumnInfo describes a column as returned by DESCRIBE.
type ColumnInfo struct {
	Name       string `json:"column_name"`
	Type       string `json:"column_type"`
	Null       string `json:"null"`
	Key        string `json:"key"`
	Default    string `json:"default"`
	Extra      string `json:"extra"`
}

// TableInfo describes a table as returned by SHOW ALL TABLES.
type TableInfo struct {
	Database    string   `json:"database"`
	Schema      string   `json:"schema"`
	TableName   string   `json:"table_name"`
	ColumnNames []string `json:"column_names"`
	ColumnTypes []string `json:"column_types"`
	Temporary   bool     `json:"temporary"`
}

// quoteIdentifier quotes a DuckDB identifier to prevent SQL injection.
// It doubles any embedded double-quotes per SQL standard.
func quoteIdentifier(name string) string {
	quoted := make([]byte, 0, len(name)+2)
	quoted = append(quoted, '"')
	for i := 0; i < len(name); i++ {
		if name[i] == '"' {
			quoted = append(quoted, '"', '"')
		} else {
			quoted = append(quoted, name[i])
		}
	}
	quoted = append(quoted, '"')
	return string(quoted)
}

// quoteString returns a single-quoted SQL string literal, escaping any
// embedded single-quotes by doubling them. Use this for ATTACH paths
// and other string values (not identifiers).
func quoteString(s string) string {
	quoted := make([]byte, 0, len(s)+2)
	quoted = append(quoted, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			quoted = append(quoted, '\'', '\'')
		} else {
			quoted = append(quoted, s[i])
		}
	}
	quoted = append(quoted, '\'')
	return string(quoted)
}

// cloneStr copies a string so the returned value does not reference the
// underlying Arrow buffer memory. Arrow's ValueStr returns strings backed
// by Arrow buffers that become invalid once the RecordReader/Statement is
// closed. Any string that outlives a QueryResult must be cloned.
func cloneStr(s string) string {
	if s == "" {
		return ""
	}
	b := make([]byte, len(s))
	copy(b, s)
	return string(b)
}
