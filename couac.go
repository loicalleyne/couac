// Package couac provides a helpful wrapper around ADBC for DuckDB.
package couac

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	drivermgr "github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
)

type Quacker struct {
	ctx  context.Context
	drv  adbc.Driver
	db   adbc.Database
	conn adbc.Connection
}

// Close closes the connection and database. It is important to do this to allow DuckDB to
// properly commit all WAL file changes before closing.
func (q *Quacker) Close() {
	q.conn.Close()
	q.db.Close()
}

// IngestCreateAppend attempts to ingest an Arrow record into the DuckDB database, creating the table
// from the record's schema if it does not exist. It returns the number of rows affected if known, otherwise -1.
// Ingest mode switches between Create and Append since DuckDB does not currently support CreateAppend mode.
// DuckDB also does not support AutoCommit option.
func (q *Quacker) IngestCreateAppend(ctx context.Context, destTable string, rec arrow.Record) (int64, error) {
	var u int64
	if destTable == "" {
		return u, fmt.Errorf("destination table name error")
	}
	if rec == nil {
		return u, fmt.Errorf("nil arrow record")
	}

	schema, _ := q.conn.GetTableSchema(q.ctx, nil, nil, destTable)

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
	return u, err
}

// NewDuck opens a DuckDB database. The driverPath argument specifies the location of
// libduckdb.so, if driverPath is empty it will default to /usr/local/lib.
func NewDuck(path, driverPath string) (*Quacker, error) {
	var err error
	if driverPath == "" {
		driverPath = "/usr/local/lib/libduckdb.so"
	}
	couac := new(Quacker)
	couac.ctx = context.TODO()
	couac.drv = drivermgr.Driver{}
	couac.db, err = couac.drv.NewDatabase(map[string]string{
		"driver":     driverPath,
		"entrypoint": "duckdb_adbc_init",
		"path":       path,
	})
	if err != nil {
		return nil, fmt.Errorf("new database error: %v", err)
	}
	couac.conn, err = couac.db.Open(context.Background())
	if err != nil {
		return nil, fmt.Errorf("db open error: %v", err)
	}
	return couac, nil
}
