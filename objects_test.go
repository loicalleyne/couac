package couac_test

import (
	"context"
	"testing"

	"github.com/loicalleyne/couac"
)

func TestObjects(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	// Create some tables
	_, err := conn.Exec(ctx, "CREATE TABLE obj_test1 (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "CREATE TABLE obj_test2 (name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	objs, err := conn.Objects(ctx)
	if err != nil {
		t.Fatal(err)
	}

	catalogs := objs.Catalogs()
	if len(catalogs) == 0 {
		t.Fatal("expected at least one catalog")
	}
}

func TestObjects_WithDepth(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE depth_test (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	// Catalogs only
	objs, err := conn.Objects(ctx, couac.WithDepth(couac.ObjectDepthCatalogs))
	if err != nil {
		t.Fatal(err)
	}
	if len(objs.Catalogs()) == 0 {
		t.Error("expected at least one catalog")
	}
}

func TestCatalogTree_FindTable(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE findme (id INT, val VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	objs, err := conn.Objects(ctx)
	if err != nil {
		t.Fatal(err)
	}

	catalog, schema, table, found := objs.FindTable("findme")
	if !found {
		t.Fatal("expected to find table 'findme'")
	}
	if table == nil {
		t.Fatal("expected non-nil table")
	}
	if table.TableName != "findme" {
		t.Errorf("expected table name 'findme', got %q", table.TableName)
	}
	if catalog == "" {
		t.Error("expected non-empty catalog")
	}
	if schema == "" {
		t.Error("expected non-empty schema")
	}
}

func TestCatalogTree_TableExists(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE exists_test (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	objs, err := conn.Objects(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Find the catalog/schema first
	catalog, schema, _, found := objs.FindTable("exists_test")
	if !found {
		t.Fatal("setup failed: table not found")
	}

	if !objs.TableExists(catalog, schema, "exists_test") {
		t.Error("expected table to exist")
	}
	if objs.TableExists(catalog, schema, "nonexistent") {
		t.Error("expected nonexistent table to not exist")
	}
}

func TestCatalogTree_Map(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE map_test (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	objs, err := conn.Objects(ctx)
	if err != nil {
		t.Fatal(err)
	}

	m := objs.Map()
	if len(m) == 0 {
		t.Fatal("expected non-empty map")
	}
	// At least one catalog should have at least one schema
	for _, schemas := range m {
		if len(schemas) > 0 {
			return // pass
		}
	}
	t.Error("expected at least one schema in the map")
}

func TestObjectsMap(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE map2_test (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	m, err := conn.ObjectsMap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) == 0 {
		t.Error("expected non-empty map")
	}
}

func TestTableSchema(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	_, err := conn.Exec(ctx, "CREATE TABLE schema_test (id INT, name VARCHAR, val DOUBLE)")
	if err != nil {
		t.Fatal(err)
	}

	s, err := conn.TableSchema(ctx, "schema_test")
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Fields()) != 3 {
		t.Errorf("expected 3 fields, got %d", len(s.Fields()))
	}
}

func TestTableTypes(t *testing.T) {
	_, conn := newTestConn(t)
	ctx := context.Background()

	// Create a table so at least "BASE TABLE" is present
	_, err := conn.Exec(ctx, "CREATE TABLE type_test (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	types, err := conn.TableTypes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("table types: %v", types)
	if len(types) == 0 {
		t.Error("expected at least one table type")
	}
}
