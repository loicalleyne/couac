package couac_test

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/couac"
)

func ExampleNewDuck() {
	// Open an in-memory DuckDB database with default driver resolution.
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	fmt.Println("database opened")
	// Output:
	// database opened
}

func ExampleNewDuck_fileBacked() {
	// Open a file-backed DuckDB database.
	db, err := couac.NewDuck(couac.WithPath("example.db"))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	fmt.Println("file-backed database opened")
	// Output:
	// file-backed database opened
}

func ExampleDB_Connect() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	fmt.Println("connections:", db.ConnectionCount())
	// Output:
	// connections: 1
}

func ExampleConn_Exec() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	// Create a table and insert data.
	_, err = conn.Exec(context.Background(), "CREATE TABLE greetings (msg VARCHAR)")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	n, err := conn.Exec(context.Background(), "INSERT INTO greetings VALUES ('hello'), ('world')")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("rows inserted:", n)
	// Output:
	// rows inserted: 2
}

func ExampleConn_Query() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE colors (name VARCHAR, hex VARCHAR)")
	conn.Exec(ctx, "INSERT INTO colors VALUES ('red', '#FF0000'), ('green', '#00FF00')")

	// Query returns a QueryResult with a streaming RecordReader.
	res, err := conn.Query(ctx, "SELECT name, hex FROM colors ORDER BY name")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer res.Close()

	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("%s = %s\n", rec.Column(0).ValueStr(i), rec.Column(1).ValueStr(i))
		}
	}
	// Output:
	// green = #00FF00
	// red = #FF0000
}

func ExampleConn_Ingest() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	// Build an Arrow record batch.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Ingest creates the table on first call, appends on subsequent calls.
	ctx := context.Background()
	n, err := conn.Ingest(ctx, "users", rec)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("ingested:", n, "rows")

	// Verify.
	res, err := conn.Query(ctx, "SELECT count(*) FROM users")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer res.Close()
	if res.Reader.Next() {
		fmt.Println("total:", res.Reader.RecordBatch().Column(0).ValueStr(0))
	}
	// Output:
	// ingested: 3 rows
	// total: 3
}

func ExampleConn_Objects() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE products (id INTEGER, name VARCHAR)")

	objs, err := conn.Objects(ctx, couac.WithDepth(couac.ObjectDepthTables))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("has catalogs:", len(objs.Catalogs()) > 0)
	for _, t := range objs.Tables("memory", "main") {
		if t.TableName == "products" {
			fmt.Println("found table:", t.TableName, "type:", t.TableType)
		}
	}
	// Output:
	// has catalogs: true
	// found table: products type: BASE TABLE
}

func ExampleCatalogTree_FindTable() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE orders (id INTEGER, total DECIMAL(10,2))")

	objs, err := conn.Objects(ctx)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	catalog, schema, table, found := objs.FindTable("orders")
	if found {
		fmt.Printf("found: %s.%s.%s (%d columns)\n",
			catalog, schema, table.TableName, len(table.TableColumns))
	}
	// Output:
	// found: memory.main.orders (2 columns)
}

func ExampleDB_WithTransaction() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	ctx := context.Background()

	// Set up tables via a regular connection.
	setup, _ := db.Connect()
	setup.Exec(ctx, "CREATE TABLE account (id INT, balance INT)")
	setup.Exec(ctx, "INSERT INTO account VALUES (1, 1000), (2, 500)")
	setup.Close()

	// Run a transfer inside a transaction.
	err = db.WithTransaction(ctx, func(tx *couac.Conn) error {
		_, err := tx.Exec(ctx, "UPDATE account SET balance = balance - 200 WHERE id = 1")
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "UPDATE account SET balance = balance + 200 WHERE id = 2")
		return err
	})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Verify the transfer.
	verify, _ := db.Connect()
	defer verify.Close()
	res, _ := verify.Query(ctx, "SELECT id, balance FROM account ORDER BY id")
	defer res.Close()
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			fmt.Printf("account %s: balance %s\n",
				rec.Column(0).ValueStr(i), rec.Column(1).ValueStr(i))
		}
	}
	// Output:
	// account 1: balance 800
	// account 2: balance 700
}

func ExampleConn_Describe() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE metrics (ts TIMESTAMP, value DOUBLE, tag VARCHAR)")

	cols, err := conn.Describe(ctx, "metrics")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	for _, c := range cols {
		fmt.Printf("%s: %s\n", c.Name, c.Type)
	}
	// Output:
	// ts: TIMESTAMP
	// value: DOUBLE
	// tag: VARCHAR
}

func ExampleConn_Version() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	v, err := conn.Version(context.Background())
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// Version string starts with "v"
	fmt.Println("has version:", v != "")
	// Output:
	// has version: true
}

func ExampleConn_Extensions() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	exts, err := conn.Extensions(context.Background())
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// DuckDB always has at least a few built-in extensions
	fmt.Println("has extensions:", len(exts) > 0)
	// Output:
	// has extensions: true
}

func ExampleConn_ShowTables() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE alpha (x INT)")
	conn.Exec(ctx, "CREATE TABLE beta (y INT)")

	tables, err := conn.ShowTables(ctx)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	for _, t := range tables {
		fmt.Println(t)
	}
	// Output:
	// alpha
	// beta
}

func ExampleConn_Explain() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE t (x INT)")

	plan, err := conn.Explain(ctx, "SELECT * FROM t WHERE x > 5")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// The plan contains a scan operator
	fmt.Println("has plan:", plan != "")
	// Output:
	// has plan: true
}

func ExampleDB_StdDB() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	// Set up data via the native API.
	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE stddb_demo (id INT, name VARCHAR)")
	conn.Exec(ctx, "INSERT INTO stddb_demo VALUES (1, 'Pierre'), (2, 'Jean'), (3, 'Jacques')")
	conn.Close()

	// Use database/sql for querying.
	stdDB := db.StdDB()
	defer stdDB.Close()

	var count int
	if err := stdDB.QueryRowContext(ctx, "SELECT count(*) FROM stddb_demo").Scan(&count); err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("count:", count)
	// Output:
	// count: 3
}

func ExampleDB_StdDB_nativeTypes() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE products (name VARCHAR, price DECIMAL(10,2), in_stock BOOLEAN)")
	conn.Exec(ctx, "INSERT INTO products VALUES ('Widget', 19.99, true), ('Gadget', 49.50, false)")
	conn.Close()

	// Values come back as native Go types, not strings.
	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT name, price, in_stock FROM products ORDER BY name")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var price couac.Decimal
		var inStock bool
		if err := rows.Scan(&name, &price, &inStock); err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("%s: $%s (in stock: %v)\n", name, price.String(), inStock)
	}
	// Output:
	// Gadget: $49.50 (in stock: false)
	// Widget: $19.99 (in stock: true)
}

func ExampleDecimal() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE amounts (v DECIMAL(18,4))")
	conn.Exec(ctx, "INSERT INTO amounts VALUES (123456.7890)")
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	var d couac.Decimal
	if err := stdDB.QueryRowContext(ctx, "SELECT v FROM amounts").Scan(&d); err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("String:", d.String())
	fmt.Println("Float64:", d.Float64())
	fmt.Println("BigFloat:", d.BigFloat().Text('f', 4))
	// Output:
	// String: 123456.7890
	// Float64: 123456.789
	// BigFloat: 123456.7890
}

func ExampleNullDecimal() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE nullable_amounts (v DECIMAL(10,2))")
	conn.Exec(ctx, "INSERT INTO nullable_amounts VALUES (42.00), (NULL)")
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT v FROM nullable_amounts ORDER BY v NULLS FIRST")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var nd couac.NullDecimal
		if err := rows.Scan(&nd); err != nil {
			fmt.Println("Error:", err)
			return
		}
		if nd.Valid {
			fmt.Println("value:", nd.Decimal.String())
		} else {
			fmt.Println("value: NULL")
		}
	}
	// Output:
	// value: NULL
	// value: 42.00
}

func ExampleDB_StdDB_parameterizedQuery() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE inventory (id INT, item VARCHAR, qty INT)")
	conn.Exec(ctx, "INSERT INTO inventory VALUES (1,'bolt',100),(2,'nut',250),(3,'screw',75)")
	conn.Close()

	// Use parameterized queries with ? placeholders.
	stdDB := db.StdDB()
	defer stdDB.Close()

	var item string
	err = stdDB.QueryRowContext(ctx,
		"SELECT item FROM inventory WHERE qty > ? ORDER BY qty LIMIT 1",
		int64(80),
	).Scan(&item)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("item:", item)

	// $N placeholders also work.
	var qty int
	err = stdDB.QueryRowContext(ctx,
		"SELECT qty FROM inventory WHERE item = $1",
		"nut",
	).Scan(&qty)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("qty:", qty)
	// Output:
	// item: bolt
	// qty: 250
}

func ExampleDB_StdDB_preparedStatement() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	ctx := context.Background()
	stdDB.ExecContext(ctx, "CREATE TABLE scores (player VARCHAR, score INT)")

	// Prepare a statement once, execute it multiple times with different parameters.
	stmt, err := stdDB.PrepareContext(ctx, "INSERT INTO scores VALUES (?, ?)")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer stmt.Close()

	for _, p := range []struct {
		name  string
		score int64
	}{
		{"Alice", 95},
		{"Bob", 87},
		{"Carol", 92},
	} {
		if _, err := stmt.ExecContext(ctx, p.name, p.score); err != nil {
			fmt.Println("Error:", err)
			return
		}
	}

	var count int
	stdDB.QueryRowContext(ctx, "SELECT count(*) FROM scores").Scan(&count)
	fmt.Println("players:", count)

	var topName string
	stdDB.QueryRowContext(ctx, "SELECT player FROM scores ORDER BY score DESC LIMIT 1").Scan(&topName)
	fmt.Println("top scorer:", topName)
	// Output:
	// players: 3
	// top scorer: Alice
}

func ExampleDB_StdDB_transaction() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	ctx := context.Background()
	stdDB.ExecContext(ctx, "CREATE TABLE ledger (acct VARCHAR, balance INT)")
	stdDB.ExecContext(ctx, "INSERT INTO ledger VALUES ('checking', 1000), ('savings', 500)")

	// Execute a transfer inside a transaction.
	tx, err := stdDB.BeginTx(ctx, nil)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	tx.ExecContext(ctx, "UPDATE ledger SET balance = balance - ? WHERE acct = ?", int64(200), "checking")
	tx.ExecContext(ctx, "UPDATE ledger SET balance = balance + ? WHERE acct = ?", int64(200), "savings")
	if err := tx.Commit(); err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Verify balances.
	rows, _ := stdDB.QueryContext(ctx, "SELECT acct, balance FROM ledger ORDER BY acct")
	defer rows.Close()
	for rows.Next() {
		var acct string
		var bal int
		rows.Scan(&acct, &bal)
		fmt.Printf("%s: %d\n", acct, bal)
	}
	// Output:
	// checking: 800
	// savings: 700
}

func ExampleConn_Prepare() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE kv (key VARCHAR, value INT)")

	// Prepare returns a raw ADBC Statement for Arrow-native parameter binding.
	stmt, err := conn.Prepare(ctx, "INSERT INTO kv VALUES (?, ?)")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer stmt.Close()

	// Build an Arrow record batch with the parameters.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "key", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.StringBuilder).Append("hello")
	bldr.Field(1).(*array.Int64Builder).Append(42)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind the Arrow record batch and execute.
	if err := stmt.Bind(ctx, rec); err != nil {
		fmt.Println("Error:", err)
		return
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("inserted:", n)
	// Output:
	// inserted: 1
}

func ExampleConn_IngestMerge() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()

	// First ingest: creates table with {id, name}.
	schema1 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	bldr1 := array.NewRecordBuilder(memory.DefaultAllocator, schema1)
	defer bldr1.Release()
	bldr1.Field(0).(*array.Int64Builder).Append(1)
	bldr1.Field(1).(*array.StringBuilder).Append("Alice")
	rec1 := bldr1.NewRecordBatch()
	defer rec1.Release()

	conn.Ingest(ctx, "evolving", rec1)

	// Second ingest: adds a new "email" column via UNION BY NAME.
	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
	}, nil)
	bldr2 := array.NewRecordBuilder(memory.DefaultAllocator, schema2)
	defer bldr2.Release()
	bldr2.Field(0).(*array.Int64Builder).Append(2)
	bldr2.Field(1).(*array.StringBuilder).Append("Bob")
	bldr2.Field(2).(*array.StringBuilder).Append("bob@example.com")
	rec2 := bldr2.NewRecordBatch()
	defer rec2.Release()

	n, err := conn.IngestMerge(ctx, "evolving", rec2)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("merged:", n, "rows")

	// The table now has 3 columns and 2 rows.
	res, _ := conn.Query(ctx, "SELECT * FROM evolving ORDER BY id")
	defer res.Close()
	fmt.Println("schema:", res.Reader.Schema())
	// Output:
	// merged: 1 rows
	// schema: schema:
	//   fields: 3
	//     - id: type=int64, nullable
	//     - name: type=utf8, nullable
	//     - email: type=utf8, nullable
}

func ExampleConn_QueryRaw() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE raw_demo (x INT)")
	conn.Exec(ctx, "INSERT INTO raw_demo VALUES (10), (20), (30)")

	// QueryRaw returns the reader, statement, and row count separately.
	rr, stmt, n, err := conn.QueryRaw(ctx, "SELECT * FROM raw_demo")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer rr.Release()
	defer stmt.Close()

	fmt.Println("rows affected:", n)
	for rr.Next() {
		rec := rr.RecordBatch()
		fmt.Println("batch rows:", rec.NumRows())
	}
	// Output:
	// rows affected: -1
	// batch rows: 3
}

func ExampleConn_ObjectsMap() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE map_demo (x INT)")

	// ObjectsMap returns a nested catalog→schema→tables map.
	m, err := conn.ObjectsMap(ctx)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// Check if the table exists in the map.
	if tables, ok := m["memory"]["main"]; ok {
		for _, t := range tables {
			if t.TableName == "map_demo" {
				fmt.Println("found:", t.TableName, "type:", t.TableType)
			}
		}
	}
	// Output:
	// found: map_demo type: BASE TABLE
}

func ExampleConn_TableSchema() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	conn.Exec(ctx, "CREATE TABLE typed (id BIGINT, name VARCHAR, active BOOLEAN)")

	schema, err := conn.TableSchema(ctx, "typed")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	for _, f := range schema.Fields() {
		fmt.Printf("%s: %s\n", f.Name, f.Type)
	}
	// Output:
	// id: int64
	// name: utf8
	// active: bool
}

func ExampleConn_Set() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	ctx := context.Background()

	// Set and read back a configuration value.
	if err := conn.Set(ctx, "threads", "2"); err != nil {
		fmt.Println("Error:", err)
		return
	}
	val, err := conn.Setting(ctx, "threads")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("threads:", val)
	// Output:
	// threads: 2
}

func ExampleConn_Databases() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	conn, err := db.Connect()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer conn.Close()

	dbs, err := conn.Databases(context.Background())
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// In-memory DuckDB always has at least the "memory" catalog.
	fmt.Println("has databases:", len(dbs) > 0)
	// Output:
	// has databases: true
}

func ExampleList() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// Scan a DuckDB list column into a couac.List.
	var l couac.List
	err = stdDB.QueryRowContext(ctx, "SELECT [10, 20, 30]::INTEGER[]").Scan(&l)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("values:", l.Values)
	fmt.Println("ints:", l.Ints())
	fmt.Println("floats:", l.Floats())
	fmt.Println("strings:", l.Strings())
	fmt.Println("json:", l.String())
	// Output:
	// values: [10 20 30]
	// ints: [10 20 30]
	// floats: [10 20 30]
	// strings: [10 20 30]
	// json: [10,20,30]
}

func ExampleStruct() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// Scan a DuckDB struct column into a couac.Struct.
	var s couac.Struct
	err = stdDB.QueryRowContext(ctx,
		"SELECT {'name': 'Alice', 'age': 30}::STRUCT(name VARCHAR, age INTEGER)",
	).Scan(&s)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	name, _ := s.Get("name")
	age, _ := s.Get("age")
	fmt.Println("name:", name)
	fmt.Println("age:", age)
	// Output:
	// name: Alice
	// age: 30
}

func ExampleMap() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// Scan a DuckDB map column into a couac.Map.
	var m couac.Map
	err = stdDB.QueryRowContext(ctx, "SELECT MAP {'a': 1, 'b': 2}").Scan(&m)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	a, _ := m.Get("a")
	b, _ := m.Get("b")
	fmt.Println("a:", a)
	fmt.Println("b:", b)
	// Output:
	// a: 1
	// b: 2
}

func ExampleNullList() {
	db, err := couac.NewDuck()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer db.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	stdDB.ExecContext(ctx, "CREATE TABLE nl (tags INTEGER[])")
	stdDB.ExecContext(ctx, "INSERT INTO nl VALUES ([1,2]), (NULL)")

	rows, _ := stdDB.QueryContext(ctx, "SELECT tags FROM nl ORDER BY tags NULLS LAST")
	defer rows.Close()

	for rows.Next() {
		var nl couac.NullList
		rows.Scan(&nl)
		fmt.Println("valid:", nl.Valid)
	}
	// Output:
	// valid: true
	// valid: false
}
