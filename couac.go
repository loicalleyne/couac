// Package couac provides a helpful wrapper around ADBC for DuckDB.
package couac

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/apache/arrow-adbc/go/adbc"
	drivermgr "github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	json "github.com/goccy/go-json"
)

type ObjectDepth int

const (
	ObjectDepthAll ObjectDepth = iota
	ObjectDepthCatalogs
	ObjectDepthDBSchemas
	ObjectDepthTables
	ObjectDepthColumns = ObjectDepthAll
)

type DuckDatabase = Quacker
type Connection = QuackCon

// Quacker represents a DuckDB database.
type Quacker struct {
	ctx context.Context
	drv adbc.Driver
	db  adbc.Database
	// duckdb database connections
	ducklings []*QuackCon
	// path to database file, in-memory if empty
	path       string
	driverPath string
}

// QuackCon represents a connection to a DuckDB database.
type QuackCon struct {
	parent   *Quacker
	conn     adbc.Connection
	catalog  *string
	dbSchema *string
}
type Statement = adbc.Statement

// Option configures a Quacker
type (
	Option func(config)
	config *Quacker
)

// WithPath option provides the location of the DuckDB file,
// if none provided, defaults to in-memory.
func WithPath(path string) Option {
	return func(cfg config) {
		cfg.path = path
	}
}

// WithDriverPath specifies the location of  libduckdb.so, if driver
// path is empty, defaults to /usr/local/lib.
func WithDriverPath(path string) Option {
	return func(cfg config) {
		cfg.driverPath = path
	}
}

// WithContext specifies the context for new database connections.
func WithContext(ctx context.Context) Option {
	return func(cfg config) {
		cfg.ctx = ctx
	}
}

func NewDB(opts ...Option) (*DuckDatabase, error) {
	return NewDuck(opts...)
}

// NewDuck opens a DuckDB database. WithPath option provides the location of
// the DuckDB file, if none provided, defaults to in-memory.
// The WithDriverPath specifies the location of libduckdb.so, if driver
// path is empty, defaults to /usr/local/lib.
func NewDuck(opts ...Option) (*Quacker, error) {
	var err error
	var dPath string
	coincoin := new(Quacker)
	for _, opt := range opts {
		opt(coincoin)
	}
	if coincoin.driverPath == "" {
		switch runtime.GOOS {
		case "darwin":
			dPath = "/usr/local/lib/libduckdb.so.dylib"
		case "linux":
			dPath = "/usr/local/lib/libduckdb.so"
		case "windows":
			h, _ := os.UserHomeDir()
			dPath = h + "\\Downloads\\libduckdb-windows-amd64\\duckdb.dll"
		default:
		}
	} else {
		dPath = coincoin.driverPath
	}
	if coincoin.ctx == nil {
		coincoin.ctx = context.TODO()
	}
	coincoin.drv = drivermgr.Driver{}
	dbOpts := make(map[string]string)
	// path to duckdb driver file
	dbOpts["driver"] = dPath
	dbOpts["entrypoint"] = "duckdb_adbc_init"
	// if path is empty, defaults to in-memory
	if coincoin.path != "" {
		dbOpts["path"] = coincoin.path
	}
	coincoin.db, err = coincoin.drv.NewDatabase(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("new database error: %v", err)
	}
	return coincoin, nil
}

// NewConnection returns a new connection to the database. It is best practice to
// close connections after use, however Quacker.Close() will also close any open connections
// before closing the database.
func (q *Quacker) NewConnection() (*QuackCon, error) {
	var err error
	qc := new(Connection)
	qc.conn, err = q.db.Open(q.ctx)
	if err != nil {
		return nil, fmt.Errorf("db open error: %v", err)
	}
	qc.parent = q
	q.ducklings = append(q.ducklings, qc)
	return qc, nil
}

// NewConnectionWithOpts returns a new connection to the database, specifying the
// catalog and schema. It is best practice toclose connections after use,
// however Quacker.Close() will also close any open connections before closing
// the database.
func (q *Quacker) NewConnectionWithOpts(catalog, schema string) (*QuackCon, error) {
	var err error
	qc := new(QuackCon)
	qc.catalog = &catalog
	qc.dbSchema = &schema
	qc.conn, err = q.db.Open(q.ctx)
	if err != nil {
		return nil, fmt.Errorf("db open error: %v", err)
	}
	qc.parent = q
	q.ducklings = append(q.ducklings, qc)
	return qc, nil
}

// ConnectionCount returns the number of open connections.
func (q *Quacker) ConnectionCount() int { return len(q.ducklings) }

// DefaultContext returns the default context of the database.
func (q *Quacker) DefaultContext() context.Context { return q.ctx }

// Close closes the database and releases any associated resources.
// It is important to do this to allow DuckDB to properly commit all WAL file
// changes before closing.
func (q *Quacker) Close() {
	for _, d := range q.ducklings {
		d.conn.Close()
	}
	q.db.Close()
}

// Path returns the path to the db file. If empty, the db is in-memory.
func (q *Quacker) Path() string { return q.path }

func (q *Quacker) removeConn(i int) {
	if len(q.ducklings) > 0 {
		q.ducklings[i] = q.ducklings[len(q.ducklings)-1]
		q.ducklings = q.ducklings[:len(q.ducklings)-1]
	}
}

// Catalog returns the connection's catalog if set. Nil represents default catalog.
func (q *QuackCon) Catalog() *string { return q.catalog }

// DBSchema returns the connnection's database schema if set. Nil represents default schema.
func (q *QuackCon) DBSchema() *string { return q.dbSchema }

// Close closes the connection to database and releases any associated resources.
// It is important to do this to allow DuckDB to properly commit all WAL file
// changes before closing.
func (q *QuackCon) Close() {
	for i, v := range q.parent.ducklings {
		if v == q {
			q.parent.removeConn(i)
			break
		}
	}
	q.parent = nil
	q.conn.Close()
}

// Exec executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (q *QuackCon) Exec(ctx context.Context, query string) (int64, error) {
	var u int64
	stmt, err := q.conn.NewStatement()
	if err != nil {
		return u, fmt.Errorf("new statement error: %v", err)
	}
	defer stmt.Close()
	err = stmt.SetSqlQuery(query)
	if err != nil {
		return u, fmt.Errorf("error setting sql query: %v", err)
	}
	u, err = stmt.ExecuteUpdate(ctx)
	return u, err
}

// GetObjects gets a hierarchical view of all catalogs, database schemas,
// tables, and columns.
//
// The result is an Arrow Dataset with the following schema:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	catalog_name								| utf8
//	catalog_db_schemas					| list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	db_schema_name							| utf8
//	db_schema_tables						|	list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	table_name									| utf8 not null
//	table_type									|	utf8 not null
//	table_columns								| list<COLUMN_SCHEMA>
//	table_constraints						| list<CONSTRAINT_SCHEMA>
//
// COLUMN_SCHEMA is a Struct with the fields:
//
//		Field Name 									| Field Type					| Comments
//		----------------------------|---------------------|---------
//		column_name									| utf8 not null				|
//		ordinal_position						| int32								| (1)
//		remarks											| utf8								| (2)
//		xdbc_data_type							| int16								| (3)
//		xdbc_type_name							| utf8								| (3)
//		xdbc_column_size						| int32								| (3)
//		xdbc_decimal_digits					| int16								| (3)
//		xdbc_num_prec_radix					| int16								| (3)
//		xdbc_nullable								| int16								| (3)
//		xdbc_column_def							| utf8								| (3)
//		xdbc_sql_data_type					| int16								| (3)
//		xdbc_datetime_sub						| int16								| (3)
//		xdbc_char_octet_length			| int32								| (3)
//		xdbc_is_nullable						| utf8								| (3)
//		xdbc_scope_catalog					| utf8								| (3)
//		xdbc_scope_schema						| utf8								| (3)
//		xdbc_scope_table						| utf8								| (3)
//		xdbc_is_autoincrement				| bool								| (3)
//		xdbc_is_generatedcolumn			| bool								| (3)
//
//	 1. The column's ordinal position in the table (starting from 1).
//	 2. Database-specific description of the column.
//	 3. Optional Value. Should be null if not supported by the driver.
//	    xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//	    in an agnostic manner.
//
// CONSTRAINT_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type					| Comments
//	----------------------------|---------------------|---------
//	constraint_name							| utf8								|
//	constraint_type							| utf8 not null				| (1)
//	constraint_column_names			| list<utf8> not null | (2)
//	constraint_column_usage			| list<USAGE_SCHEMA>	| (3)
//
// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
// 2. The columns on the current table that are constrained, in order.
// 3. For FOREIGN KEY only, the referenced table and columns.
//
// USAGE_SCHEMA is a Struct with fields:
//
//	Field Name									|	Field Type
//	----------------------------|----------------------------
//	fk_catalog									| utf8
//	fk_db_schema								| utf8
//	fk_table										| utf8 not null
//	fk_column_name							| utf8 not null

// DBObject represents the hierarchical structure of catalogs, schemas, tables, and columns.
type DBObject struct {
	CatalogName      string     `json:"catalog_name"`
	CatalogDBSchemas []DBSchema `json:"catalog_db_schemas"`
}

type DBSchema struct {
	DBSchemaName   string        `json:"db_schema_name"`
	DBSchemaTables []TableSchema `json:"db_schema_tables"`
}

type TableSchema struct {
	TableName        string             `json:"table_name"`
	TableType        string             `json:"table_type"`
	TableColumns     []ColumnSchema     `json:"table_columns"`
	TableConstraints []ConstraintSchema `json:"table_constraints"`
}

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

type ConstraintSchema struct {
	ConstraintName        string        `json:"constraint_name"`
	ConstraintType        string        `json:"constraint_type"`
	ConstraintColumnNames []string      `json:"constraint_column_names"`
	ConstraintColumnUsage []UsageSchema `json:"constraint_column_usage"`
}

type UsageSchema struct {
	FKCatalog    string `json:"fk_catalog"`
	FKDBSchema   string `json:"fk_db_schema"`
	FKTable      string `json:"fk_table"`
	FKColumnName string `json:"fk_column_name"`
}

func (q *QuackCon) GetObjects() ([]DBObject, error) {
	rr, err := q.conn.GetObjects(q.parent.ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	var objects []DBObject
	for rr.Next() {
		var ob []byte
		rec := rr.Record()
		ob, err = rec.MarshalJSON()
		if err != nil {
			return nil, err
		}
		fmt.Println(string(ob))
		err = json.Unmarshal(ob, &objects)
		if err != nil {
			return nil, err
		}
		break
	}
	// var tables []string
	// for _, obj := range objects {
	// 	for _, name := range obj.CatalogDBSchemas {
	// 		for _, table := range name.DBSchemaTables {
	// 			tables = append(tables, table.TableName)
	// 		}
	// 	}
	// }
	// fmt.Printf("tables: %v\n", tables)
	// fmt.Printf("objects: %v\n", objects)
	return objects, err
}

// GetTableSchema returns the Arrow schema of a DuckDB table. Pass nil for catalog and dbSchema
// to use the default catalog and database schema.
func (q *QuackCon) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return q.conn.GetTableSchema(ctx, catalog, dbSchema, tableName)
}

// IngestCreateAppend attempts to ingest an Arrow record into the DuckDB database, creating the table
// from the record's schema if it does not exist. It returns the number of rows affected if known, otherwise -1.
// Ingest mode switches between Create and Append since DuckDB does not currently support CreateAppend mode.
// DuckDB also does not support AutoCommit option.
func (q *QuackCon) IngestCreateAppend(ctx context.Context, destTable string, rec arrow.Record) (int64, error) {
	var u int64
	if destTable == "" {
		return u, fmt.Errorf("destination table name error")
	}
	if rec == nil {
		return u, fmt.Errorf("nil arrow record")
	}
	// If schema is non-nil the table is assumed to exist, otherwise table is not found.
	schema, _ := q.conn.GetTableSchema(ctx, q.catalog, q.dbSchema, destTable)

	stmt, err := q.conn.NewStatement()
	if err != nil {
		return u, fmt.Errorf("new statement error: %v", err)
	}
	defer stmt.Close()
	// If schema is non-nil the table is assumed to exist, Append ingest mode will be used;
	// otherwise the table will be created with Create ingest mode.
	if schema == nil {
		err = stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate)
		if err != nil {
			return u, fmt.Errorf("set option ingest mode create error: %v", err)
		}
	} else {
		err = stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend)
		if err != nil {
			return u, fmt.Errorf("set option ingest mode append error: %v", err)
		}
	}
	// Invalid Argument: Statement Set Option adbc.connection.autocommit is not yet accepted by DuckDB
	// err = stmt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled)
	// if err != nil {
	// 	return 0, fmt.Errorf("setoption autocommit error: %v", err)
	// }
	err = stmt.SetOption(adbc.OptionKeyIngestTargetTable, destTable)
	if err != nil {
		return u, fmt.Errorf("set option target table error: %v", err)
	}
	err = stmt.Bind(ctx, rec)
	if err != nil {
		return u, fmt.Errorf("statement binding arrow record error: %v", err)
	}
	u, err = stmt.ExecuteUpdate(ctx)
	if err != nil {
		return u, fmt.Errorf("execute update error: %w", err)
	}
	return u, nil
}

// NewStatement initializes a new statement object tied to an open connection.
// The caller must close the statement when done with it.
func (q *QuackCon) NewStatement() (Statement, error) {
	if q.conn == nil {
		return nil, fmt.Errorf("database connection is closed")
	}
	return q.conn.NewStatement()
}

// Query executes the query and returns a RecordReader for the results, the statement, and the number
// of rows affected if known, otherwise it will be -1. The statement should be closed once done with the
// RecordReader.
// Since ADBC 1.1.0: releasing the returned RecordReader without consuming it fully is equivalent to
// calling AdbcStatementCancel.
func (q *QuackCon) Query(ctx context.Context, query string) (array.RecordReader, adbc.Statement, int64, error) {
	var u int64
	stmt, err := q.conn.NewStatement()
	if err != nil {
		return nil, nil, u, fmt.Errorf("new statement error: %v", err)
	}
	err = stmt.SetSqlQuery(query)
	if err != nil {
		return nil, nil, u, fmt.Errorf("error setting sql query: %v", err)
	}
	rr, u, err := stmt.ExecuteQuery(ctx)
	return rr, stmt, u, err
}
