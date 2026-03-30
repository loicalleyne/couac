package couac

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	json "github.com/goccy/go-json"
)

// Objects retrieves a hierarchical view of database objects (catalogs,
// schemas, tables, columns) and returns a [CatalogTree] wrapper with typed
// navigation methods.
//
// Options control the depth of recursion and filtering:
//
//	objs, err := conn.Objects(ctx,
//	    couac.WithDepth(couac.ObjectDepthTables),
//	    couac.WithCatalogFilter("memory"),
//	    couac.WithTableTypes([]string{"TABLE"}),
//	)
//	if err != nil { ... }
//	for _, t := range objs.Tables("memory", "main") {
//	    fmt.Println(t.TableName)
//	}
//
// When databases are ATTACHed, each attached database appears as a
// separate catalog in the returned hierarchy.
//
// The result is an Arrow dataset with the following schema:
//
//	Field Name              | Field Type
//	------------------------|----------------------------
//	catalog_name            | utf8
//	catalog_db_schemas      | list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//	Field Name              | Field Type
//	------------------------|----------------------------
//	db_schema_name          | utf8
//	db_schema_tables        | list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//	Field Name              | Field Type
//	------------------------|----------------------------
//	table_name              | utf8 not null
//	table_type              | utf8 not null
//	table_columns           | list<COLUMN_SCHEMA>
//	table_constraints       | list<CONSTRAINT_SCHEMA>
//
// See [CatalogInfo], [SchemaInfo], [TableSchema], [ColumnSchema],
// [ConstraintSchema], and [UsageSchema] for the full field definitions.
func (q *Conn) Objects(ctx context.Context, opts ...ObjectsOption) (*CatalogTree, error) {
	if err := q.ensureConnOpen(); err != nil {
		return nil, err
	}
	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	cfg := &objectsConfig{
		depth: ObjectDepthAll,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	rr, err := q.conn.GetObjects(ctx, adbc.ObjectDepth(cfg.depth),
		cfg.catalog, cfg.dbSchema, cfg.tableName, cfg.columnName, cfg.tableTypes)
	if err != nil {
		return nil, fmt.Errorf("couac: get objects: %w", err)
	}
	defer rr.Release()

	var allObjects []CatalogInfo
	for rr.Next() {
		rec := rr.RecordBatch()
		ob, err := rec.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("couac: marshal objects: %w", err)
		}
		var batch []CatalogInfo
		if err := json.Unmarshal(ob, &batch); err != nil {
			return nil, fmt.Errorf("couac: unmarshal objects: %w", err)
		}
		allObjects = append(allObjects, batch...)
	}
	if err := rr.Err(); err != nil {
		return nil, fmt.Errorf("couac: read objects: %w", err)
	}

	return &CatalogTree{objects: allObjects}, nil
}

// GetObjects is a backward-compatible alias for [Conn.Objects].
//
// Deprecated: Use [Conn.Objects] instead.
func (q *Conn) GetObjects(ctx context.Context, opts ...ObjectsOption) (*CatalogTree, error) {
	return q.Objects(ctx, opts...)
}

// ObjectsMap is a convenience function that retrieves all database
// objects and returns them as a nested map:
// catalog name → schema name → []TableSchema.
//
// This is equivalent to calling Objects followed by [CatalogTree.Map].
func (q *Conn) ObjectsMap(ctx context.Context) (map[string]map[string][]TableSchema, error) {
	objs, err := q.Objects(ctx)
	if err != nil {
		return nil, err
	}
	return objs.Map(), nil
}

// GetObjectsMap is a backward-compatible alias for [Conn.ObjectsMap].
//
// Deprecated: Use [Conn.ObjectsMap] instead.
func (q *Conn) GetObjectsMap(ctx context.Context) (map[string]map[string][]TableSchema, error) {
	return q.ObjectsMap(ctx)
}

// TableSchema returns the Arrow schema of a DuckDB table using the
// connection's default catalog and schema.
func (q *Conn) TableSchema(ctx context.Context, tableName string) (*arrow.Schema, error) {
	return q.TableSchemaIn(ctx, q.catalog, q.dbSchema, tableName)
}

// TableSchemaIn returns the Arrow schema of a DuckDB table in the
// specified catalog and schema. Pass empty strings for catalog and
// dbSchema to use the defaults.
func (q *Conn) TableSchemaIn(ctx context.Context, catalog, dbSchema, tableName string) (*arrow.Schema, error) {
	if err := q.ensureConnOpen(); err != nil {
		return nil, err
	}
	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	var cp, sp *string
	if catalog != "" {
		cp = &catalog
	}
	if dbSchema != "" {
		sp = &dbSchema
	}
	s, err := q.conn.GetTableSchema(ctx, cp, sp, tableName)
	if err != nil {
		return nil, fmt.Errorf("couac: get table schema %q: %w", tableName, err)
	}
	return s, nil
}

// GetTableSchema is a backward-compatible alias for [Conn.TableSchemaIn].
// It accepts *string parameters for catalog and dbSchema.
//
// Deprecated: Use [Conn.TableSchema] or [Conn.TableSchemaIn] instead.
func (q *Conn) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	var c, s string
	if catalog != nil {
		c = *catalog
	}
	if dbSchema != nil {
		s = *dbSchema
	}
	return q.TableSchemaIn(ctx, c, s, tableName)
}

// TableTypes returns the table types available in the database
// (e.g. "BASE TABLE", "LOCAL TEMPORARY", "VIEW").
func (q *Conn) TableTypes(ctx context.Context) ([]string, error) {
	if err := q.ensureConnOpen(); err != nil {
		return nil, err
	}
	q.parent.mu.RLock()
	defer q.parent.mu.RUnlock()

	rr, err := q.conn.GetTableTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("couac: get table types: %w", err)
	}
	defer rr.Release()

	var types []string
	for rr.Next() {
		rec := rr.RecordBatch()
		col := rec.Column(0)
		for i := 0; i < col.Len(); i++ {
			types = append(types, cloneStr(col.ValueStr(i)))
		}
	}
	if err := rr.Err(); err != nil {
		return nil, fmt.Errorf("couac: read table types: %w", err)
	}
	return types, nil
}

// GetTableTypes is a backward-compatible alias for [Conn.TableTypes].
//
// Deprecated: Use [Conn.TableTypes] instead.
func (q *Conn) GetTableTypes(ctx context.Context) ([]string, error) {
	return q.TableTypes(ctx)
}
