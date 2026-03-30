package couac_test

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/loicalleyne/couac"
)

func TestStdDB_QueryRow(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()

	_, err = conn.Exec(ctx, "CREATE TABLE sqldb_test (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_test VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatal(err)
	}

	stdDB := db.StdDB()
	defer stdDB.Close()

	var count int
	if err := stdDB.QueryRowContext(ctx, "SELECT count(*) FROM sqldb_test").Scan(&count); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2, got %d", count)
	}
}

func TestStdDB_Query(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()

	_, err = conn.Exec(ctx, "CREATE TABLE sqldb_query (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_query VALUES (1, 'x'), (2, 'y'), (3, 'z')")
	if err != nil {
		t.Fatal(err)
	}

	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT id, name FROM sqldb_query ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var ids []int
	var names []string
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(ids) != 3 {
		t.Errorf("expected 3 rows, got %d", len(ids))
	}
	if ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Errorf("unexpected ids: %v", ids)
	}
	if names[0] != "x" || names[1] != "y" || names[2] != "z" {
		t.Errorf("unexpected names: %v", names)
	}
}

func TestStdDB_Exec(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE sqldb_exec (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := stdDB.ExecContext(ctx, "INSERT INTO sqldb_exec VALUES (1), (2), (3)")
	if err != nil {
		t.Fatal(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("expected 3 rows affected, got %d", n)
	}
}

func TestStdDB_Tx(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE sqldb_tx (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := stdDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO sqldb_tx VALUES (1)")
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := stdDB.QueryRowContext(ctx, "SELECT count(*) FROM sqldb_tx").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}
}

func TestStdDB_TxRollback(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE sqldb_txrb (id INT)")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := stdDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO sqldb_txrb VALUES (1)")
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := stdDB.QueryRowContext(ctx, "SELECT count(*) FROM sqldb_txrb").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 after rollback, got %d", count)
	}
}

func TestStdDB_Ping(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()

	if err := stdDB.PingContext(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestStdDB_DriverOpen_Errors(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()

	// Verify that the placeholder driver can't be used directly
	drv := stdDB.Driver()
	_, err := drv.Open("")
	if err == nil {
		t.Fatal("expected error from Driver.Open()")
	}
}

// Ensure sql.DB is usable as a function parameter type
func useStdDB(_ *sql.DB) {}

func TestStdDB_TypeSafety(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()

	// This should compile — verifies StdDB returns *sql.DB
	useStdDB(stdDB)
}

func TestStdDB_NativeTypes(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_types (
		b    BOOLEAN,
		i8   TINYINT,
		i16  SMALLINT,
		i32  INTEGER,
		i64  BIGINT,
		u8   UTINYINT,
		u16  USMALLINT,
		u32  UINTEGER,
		u64  UBIGINT,
		f32  FLOAT,
		f64  DOUBLE,
		s    VARCHAR,
		bin  BLOB,
		ts   TIMESTAMP,
		d    DATE,
		dec  DECIMAL(10,2)
	)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.Exec(ctx, `INSERT INTO sqldb_types VALUES (
		true,
		42,
		1000,
		100000,
		9223372036854775807,
		255,
		65535,
		4294967295,
		18446744073709551615,
		3.14,
		2.718281828459045,
		'hello',
		'\xDEADBEEF'::BLOB,
		'2025-06-15 14:30:00'::TIMESTAMP,
		'2025-06-15'::DATE,
		12345.67
	)`)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	row := stdDB.QueryRowContext(ctx, "SELECT * FROM sqldb_types")

	var (
		b   any
		i8  any
		i16 any
		i32 any
		i64 any
		u8  any
		u16 any
		u32 any
		u64 any
		f32 any
		f64 any
		s   any
		bin any
		ts  any
		d   any
		dec any
	)
	if err := row.Scan(&b, &i8, &i16, &i32, &i64, &u8, &u16, &u32, &u64, &f32, &f64, &s, &bin, &ts, &d, &dec); err != nil {
		t.Fatal(err)
	}

	// Verify concrete Go types returned by the driver.
	assertType[bool](t, "b", b)
	assertType[int8](t, "i8", i8)
	assertType[int16](t, "i16", i16)
	assertType[int32](t, "i32", i32)
	assertType[int64](t, "i64", i64)
	assertType[uint8](t, "u8", u8)
	assertType[uint16](t, "u16", u16)
	assertType[uint32](t, "u32", u32)
	assertType[uint64](t, "u64", u64)
	assertType[float32](t, "f32", f32)
	assertType[float64](t, "f64", f64)
	assertType[string](t, "s", s)
	assertType[[]byte](t, "bin", bin)
	assertType[time.Time](t, "ts", ts)
	assertType[time.Time](t, "d", d)
	assertType[couac.Decimal](t, "dec", dec)

	// Verify values.
	if b != true {
		t.Errorf("b: expected true, got %v", b)
	}
	if i8 != int8(42) {
		t.Errorf("i8: expected 42, got %v", i8)
	}
	if i64 != int64(math.MaxInt64) {
		t.Errorf("i64: expected MaxInt64, got %v", i64)
	}
	if u8 != uint8(255) {
		t.Errorf("u8: expected 255, got %v", u8)
	}
	if u64 != uint64(math.MaxUint64) {
		t.Errorf("u64: expected MaxUint64, got %v", u64)
	}
	if s != "hello" {
		t.Errorf("s: expected hello, got %v", s)
	}

	// Decimal value and precision check.
	decVal := dec.(couac.Decimal)
	if decVal.String() != "12345.67" {
		t.Errorf("dec: expected 12345.67, got %s", decVal.String())
	}
	if decVal.Float64() != 12345.67 {
		t.Errorf("dec.Float64(): expected 12345.67, got %f", decVal.Float64())
	}
	bf := decVal.BigFloat()
	expected, _, _ := new(big.Float).SetPrec(256).Parse("12345.67", 10)
	if bf.Cmp(expected) != 0 {
		t.Errorf("dec.BigFloat(): expected %s, got %s", expected.Text('f', 10), bf.Text('f', 10))
	}
}

func TestStdDB_NullValues(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_nulls (
		i INTEGER,
		s VARCHAR,
		d DECIMAL(10,2)
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_nulls VALUES (NULL, NULL, NULL)")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	var i, s, d any
	if err := stdDB.QueryRowContext(ctx, "SELECT * FROM sqldb_nulls").Scan(&i, &s, &d); err != nil {
		t.Fatal(err)
	}
	if i != nil {
		t.Errorf("i: expected nil, got %v (%T)", i, i)
	}
	if s != nil {
		t.Errorf("s: expected nil, got %v (%T)", s, s)
	}
	if d != nil {
		t.Errorf("d: expected nil, got %v (%T)", d, d)
	}
}

func TestStdDB_NullDecimal(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, "CREATE TABLE sqldb_nulldec (price DECIMAL(10,2))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_nulldec VALUES (99.99), (NULL)")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT price FROM sqldb_nulldec ORDER BY price NULLS FIRST")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var results []couac.NullDecimal
	for rows.Next() {
		var nd couac.NullDecimal
		if err := rows.Scan(&nd); err != nil {
			t.Fatal(err)
		}
		results = append(results, nd)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}

	// NULL comes first in ORDER BY
	if results[0].Valid {
		t.Error("expected first row to be NULL")
	}
	if !results[1].Valid {
		t.Error("expected second row to be non-NULL")
	}
	if results[1].Decimal.String() != "99.99" {
		t.Errorf("expected 99.99, got %s", results[1].Decimal.String())
	}
}

func TestStdDB_ColumnTypes(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_coltypes (
		i  INTEGER,
		s  VARCHAR,
		b  BOOLEAN,
		f  DOUBLE,
		d  DECIMAL(18,4),
		ts TIMESTAMP
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_coltypes VALUES (1, 'x', true, 1.5, 100.0001, '2025-01-01 00:00:00')")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT * FROM sqldb_coltypes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	expectedNames := []string{"INTEGER", "VARCHAR", "BOOLEAN", "DOUBLE", "DECIMAL", "TIMESTAMP"}
	for idx, ct := range types {
		dbName := ct.DatabaseTypeName()
		if dbName != expectedNames[idx] {
			t.Errorf("column %d: expected DatabaseTypeName %q, got %q", idx, expectedNames[idx], dbName)
		}

		scanType := ct.ScanType()
		if scanType == nil {
			t.Errorf("column %d: ScanType() returned nil", idx)
		}

		nullable, ok := ct.Nullable()
		if !ok {
			t.Errorf("column %d: Nullable() not supported", idx)
		}
		_ = nullable
	}

	// Check decimal precision/scale.
	decCol := types[4]
	p, s, ok := decCol.DecimalSize()
	if !ok {
		t.Fatal("DecimalSize not reported for DECIMAL column")
	}
	if p != 18 || s != 4 {
		t.Errorf("DECIMAL: expected precision=18 scale=4, got precision=%d scale=%d", p, s)
	}
}

func TestStdDB_DecimalScan(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, "CREATE TABLE sqldb_decscan (v DECIMAL(10,2))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_decscan VALUES (42.50)")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	// Scan directly into *Decimal
	var d couac.Decimal
	if err := stdDB.QueryRowContext(ctx, "SELECT v FROM sqldb_decscan").Scan(&d); err != nil {
		t.Fatal(err)
	}
	if d.String() != "42.50" {
		t.Errorf("expected 42.50, got %s", d.String())
	}
	if d.Float64() != 42.5 {
		t.Errorf("expected 42.5, got %f", d.Float64())
	}
}

func assertType[T any](t *testing.T, name string, v any) {
	t.Helper()
	if _, ok := v.(T); !ok {
		t.Errorf("%s: expected type %T, got %T (value: %v)", name, *new(T), v, v)
	}
}

// --------------- Parameterized query tests ---------------

func TestStdDB_Param_QuestionMark(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE param_qmark (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stdDB.ExecContext(ctx, "INSERT INTO param_qmark VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	if err != nil {
		t.Fatal(err)
	}

	var name string
	err = stdDB.QueryRowContext(ctx, "SELECT name FROM param_qmark WHERE id = ?", int64(2)).Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "bob" {
		t.Errorf("expected bob, got %s", name)
	}
}

func TestStdDB_Param_DollarN(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE param_dollar (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stdDB.ExecContext(ctx, "INSERT INTO param_dollar VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	if err != nil {
		t.Fatal(err)
	}

	var name string
	err = stdDB.QueryRowContext(ctx, "SELECT name FROM param_dollar WHERE id = $1", int64(2)).Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "bob" {
		t.Errorf("expected bob, got %s", name)
	}
}

func TestStdDB_Param_MultipleArgs(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE param_multi (id INT, name VARCHAR, score DOUBLE)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stdDB.ExecContext(ctx, "INSERT INTO param_multi VALUES (1, 'alice', 9.5), (2, 'bob', 7.2), (3, 'carol', 8.8)")
	if err != nil {
		t.Fatal(err)
	}

	var name string
	err = stdDB.QueryRowContext(ctx, "SELECT name FROM param_multi WHERE id = ? AND score > ?", int64(1), 9.0).Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "alice" {
		t.Errorf("expected alice, got %s", name)
	}
}

func TestStdDB_Param_AllGoTypes(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// Use SELECT with parameterized values — no table needed.
	row := stdDB.QueryRowContext(ctx,
		"SELECT ?::BIGINT, ?::DOUBLE, ?::BOOLEAN, ?::VARCHAR, ?::BLOB, ?::TIMESTAMP",
		int64(42),
		3.14,
		true,
		"hello",
		[]byte{0xDE, 0xAD},
		time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC),
	)

	var (
		i64 int64
		f64 float64
		b   bool
		s   string
		bin []byte
		ts  time.Time
	)
	if err := row.Scan(&i64, &f64, &b, &s, &bin, &ts); err != nil {
		t.Fatal(err)
	}
	if i64 != 42 {
		t.Errorf("int64: expected 42, got %d", i64)
	}
	if f64 != 3.14 {
		t.Errorf("float64: expected 3.14, got %f", f64)
	}
	if !b {
		t.Error("bool: expected true")
	}
	if s != "hello" {
		t.Errorf("string: expected hello, got %s", s)
	}
	if len(bin) != 2 || bin[0] != 0xDE || bin[1] != 0xAD {
		t.Errorf("[]byte: expected [DE AD], got %x", bin)
	}
	// DuckDB stores TIMESTAMP without timezone; binding a time.Time with
	// UTC and reading it back produces the same wall-clock values.
	if ts.Year() != 2025 || ts.Month() != 6 || ts.Day() != 15 {
		t.Errorf("time.Time: expected 2025-06-15, got %v", ts)
	}
}

func TestStdDB_Param_NilValue(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE param_nil (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert a row with a NULL parameter.
	_, err = stdDB.ExecContext(ctx, "INSERT INTO param_nil VALUES (?, ?)", int64(1), nil)
	if err != nil {
		t.Fatal(err)
	}

	var id int
	var name sql.NullString
	if err := stdDB.QueryRowContext(ctx, "SELECT * FROM param_nil").Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Errorf("id: expected 1, got %d", id)
	}
	if name.Valid {
		t.Errorf("name: expected NULL, got %q", name.String)
	}
}

func TestStdDB_Param_ExecInsert(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE param_exec (id BIGINT, label VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := stdDB.ExecContext(ctx, "INSERT INTO param_exec VALUES (?, ?)", int64(10), "ten")
	if err != nil {
		t.Fatal(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 row affected, got %d", n)
	}

	// Verify the inserted data.
	var id int64
	var label string
	if err := stdDB.QueryRowContext(ctx, "SELECT * FROM param_exec WHERE id = ?", int64(10)).Scan(&id, &label); err != nil {
		t.Fatal(err)
	}
	if id != 10 || label != "ten" {
		t.Errorf("expected (10, ten), got (%d, %s)", id, label)
	}
}

func TestStdDB_Param_Decimal(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE param_dec (id INT, price DECIMAL(10,2))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stdDB.ExecContext(ctx, "INSERT INTO param_dec VALUES (1, 99.99), (2, 50.00), (3, 199.95)")
	if err != nil {
		t.Fatal(err)
	}

	// Query with a Decimal parameter.
	threshold := couac.Decimal{Width: 10, Scale: 2, Unscaled: big.NewInt(5000)} // 50.00
	var id int
	err = stdDB.QueryRowContext(ctx,
		"SELECT id FROM param_dec WHERE price > ? ORDER BY price LIMIT 1",
		threshold,
	).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Errorf("expected id 1, got %d", id)
	}
}

func TestStdDB_Param_NullDecimal(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// NullDecimal with Valid=false should bind as NULL.
	nd := couac.NullDecimal{Valid: false}
	row := stdDB.QueryRowContext(ctx, "SELECT ? IS NULL", nd)
	var isNull bool
	if err := row.Scan(&isNull); err != nil {
		t.Fatal(err)
	}
	if !isNull {
		t.Error("expected NullDecimal{Valid:false} to bind as NULL")
	}

	// NullDecimal with Valid=true should bind as the Decimal value.
	nd2 := couac.NullDecimal{
		Decimal: couac.Decimal{Width: 10, Scale: 2, Unscaled: big.NewInt(4250)},
		Valid:   true,
	}
	row = stdDB.QueryRowContext(ctx, "SELECT ?::DECIMAL(10,2)", nd2)
	var d couac.Decimal
	if err := row.Scan(&d); err != nil {
		t.Fatal(err)
	}
	if d.String() != "42.50" {
		t.Errorf("expected 42.50, got %s", d.String())
	}
}

func TestStdDB_Param_PreparedStmt(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE param_prep (id INT, val VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	// Use an explicit prepared statement with parameters.
	stmt, err := stdDB.PrepareContext(ctx, "INSERT INTO param_prep VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i := int64(1); i <= 3; i++ {
		_, err := stmt.ExecContext(ctx, i, fmt.Sprintf("v%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	var count int
	if err := stdDB.QueryRowContext(ctx, "SELECT count(*) FROM param_prep").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestStdDB_BeginTx_ReadOnly(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.ExecContext(ctx, "CREATE TABLE txro (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stdDB.ExecContext(ctx, "INSERT INTO txro VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := stdDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	// Read should work in a read-only transaction.
	var id int
	if err := tx.QueryRowContext(ctx, "SELECT id FROM txro").Scan(&id); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if id != 1 {
		t.Errorf("expected 1, got %d", id)
	}
	tx.Rollback()
}

func TestStdDB_BeginTx_UnsupportedIsolation(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	_, err := stdDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		t.Fatal("expected error for unsupported isolation level")
	}
}

// --------------- List / Struct / Map tests ---------------

func TestStdDB_List(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, "CREATE TABLE sqldb_list (id INT, tags VARCHAR[])")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_list VALUES (1, ['a', 'b', 'c']), (2, ['x'])")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	// Scan into List
	var l couac.List
	if err := stdDB.QueryRowContext(ctx, "SELECT tags FROM sqldb_list WHERE id = 1").Scan(&l); err != nil {
		t.Fatal(err)
	}
	if len(l.Values) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(l.Values))
	}
	strs := l.Strings()
	if strs[0] != "a" || strs[1] != "b" || strs[2] != "c" {
		t.Errorf("unexpected Strings: %v", strs)
	}

	// Scan into any → verify it's a List
	var raw any
	if err := stdDB.QueryRowContext(ctx, "SELECT tags FROM sqldb_list WHERE id = 2").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	assertType[couac.List](t, "list-any", raw)
}

func TestStdDB_List_Integers(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l couac.List
	if err := stdDB.QueryRowContext(ctx, "SELECT [10, 20, 30]::INTEGER[]").Scan(&l); err != nil {
		t.Fatal(err)
	}
	ints := l.Ints()
	if len(ints) != 3 || ints[0] != 10 || ints[1] != 20 || ints[2] != 30 {
		t.Errorf("unexpected Ints: %v", ints)
	}
	floats := l.Floats()
	if len(floats) != 3 || floats[0] != 10 || floats[1] != 20 || floats[2] != 30 {
		t.Errorf("unexpected Floats: %v", floats)
	}
}

func TestStdDB_List_Bools(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l couac.List
	if err := stdDB.QueryRowContext(ctx, "SELECT [true, false, true]::BOOLEAN[]").Scan(&l); err != nil {
		t.Fatal(err)
	}
	bools := l.Bools()
	if len(bools) != 3 || bools[0] != true || bools[1] != false || bools[2] != true {
		t.Errorf("unexpected Bools: %v", bools)
	}
}

func TestStdDB_List_JSON(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l couac.List
	if err := stdDB.QueryRowContext(ctx, "SELECT [1, 2, 3]::INTEGER[]").Scan(&l); err != nil {
		t.Fatal(err)
	}
	jl := l.JSON()
	for i, v := range jl.Values {
		s, ok := v.(string)
		if !ok {
			t.Errorf("JSON element %d: expected string, got %T", i, v)
		}
		_ = s
	}
	// MarshalJSON round-trip
	b, err := l.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var l2 couac.List
	if err := l2.Scan(string(b)); err != nil {
		t.Fatal(err)
	}
	if len(l2.Values) != 3 {
		t.Errorf("JSON round-trip: expected 3 elements, got %d", len(l2.Values))
	}
}

func TestStdDB_NullList(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, "CREATE TABLE sqldb_nulllist (tags INTEGER[])")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_nulllist VALUES ([1,2]), (NULL)")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT tags FROM sqldb_nulllist ORDER BY tags NULLS FIRST")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var results []couac.NullList
	for rows.Next() {
		var nl couac.NullList
		if err := rows.Scan(&nl); err != nil {
			t.Fatal(err)
		}
		results = append(results, nl)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
	if results[0].Valid {
		t.Error("expected first row to be NULL")
	}
	if !results[1].Valid {
		t.Fatal("expected second row to be non-NULL")
	}
	if len(results[1].List.Values) != 2 {
		t.Errorf("expected 2 elements, got %d", len(results[1].List.Values))
	}
}

func TestStdDB_Struct(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT {'name': 'Alice', 'age': 30}::STRUCT(name VARCHAR, age INTEGER)",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}

	name, ok := s.Get("name")
	if !ok || name != "Alice" {
		t.Errorf("name: expected Alice, got %v (ok=%v)", name, ok)
	}
	age, ok := s.Get("age")
	if !ok {
		t.Errorf("age: field not found")
	}
	assertType[int32](t, "age", age)
	if age.(int32) != 30 {
		t.Errorf("age: expected 30, got %v", age)
	}

	// MarshalJSON
	b, err := s.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	// Should contain both fields
	js := string(b)
	if len(js) < 10 {
		t.Errorf("unexpected JSON: %s", js)
	}
}

func TestStdDB_Struct_JSON(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT {'x': 1, 'y': 2}::STRUCT(x INTEGER, y INTEGER)",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}
	js := s.JSON()
	for k, v := range js.Fields {
		if _, ok := v.(string); !ok {
			t.Errorf("JSON field %q: expected string, got %T", k, v)
		}
	}
}

func TestStdDB_NullStruct(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_nullstruct (data STRUCT(a INT, b VARCHAR))`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_nullstruct VALUES ({'a': 1, 'b': 'hi'}), (NULL)")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT data FROM sqldb_nullstruct ORDER BY data NULLS LAST")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var results []couac.NullStruct
	for rows.Next() {
		var ns couac.NullStruct
		if err := rows.Scan(&ns); err != nil {
			t.Fatal(err)
		}
		results = append(results, ns)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
	if !results[0].Valid {
		t.Error("expected first row to be non-NULL")
	}
	if results[1].Valid {
		t.Error("expected second row to be NULL")
	}
	a, _ := results[0].Struct.Get("a")
	if a.(int32) != 1 {
		t.Errorf("a: expected 1, got %v", a)
	}
}

func TestStdDB_Map(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var m couac.Map
	if err := stdDB.QueryRowContext(ctx,
		"SELECT MAP {'key1': 10, 'key2': 20}",
	).Scan(&m); err != nil {
		t.Fatal(err)
	}

	v1, ok := m.Get("key1")
	if !ok {
		t.Fatal("key1 not found")
	}
	assertType[int32](t, "key1", v1)
	if v1.(int32) != 10 {
		t.Errorf("key1: expected 10, got %v", v1)
	}

	v2, ok := m.Get("key2")
	if !ok {
		t.Fatal("key2 not found")
	}
	if v2.(int32) != 20 {
		t.Errorf("key2: expected 20, got %v", v2)
	}
}

func TestStdDB_Map_JSON(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var m couac.Map
	if err := stdDB.QueryRowContext(ctx,
		"SELECT MAP {'a': 1, 'b': 2}",
	).Scan(&m); err != nil {
		t.Fatal(err)
	}
	jm := m.JSON()
	for k, v := range jm.Values {
		if _, ok := v.(string); !ok {
			t.Errorf("JSON key %q: expected string, got %T", k, v)
		}
	}
	keys := m.Keys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

func TestStdDB_NullMap(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_nullmap (id INT, data MAP(VARCHAR, INTEGER))`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_nullmap VALUES (1, MAP {'x': 1}), (2, NULL)")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT data FROM sqldb_nullmap ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var results []couac.NullMap
	for rows.Next() {
		var nm couac.NullMap
		if err := rows.Scan(&nm); err != nil {
			t.Fatal(err)
		}
		results = append(results, nm)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
	if !results[0].Valid {
		t.Error("expected first row to be non-NULL")
	}
	if results[1].Valid {
		t.Error("expected second row to be NULL")
	}
	v, ok := results[0].Map.Get("x")
	if !ok {
		t.Fatal("key 'x' not found in map")
	}
	if v.(int32) != 1 {
		t.Errorf("x: expected 1, got %v", v)
	}
}

func TestStdDB_NestedListOfStruct(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}]::STRUCT(a INTEGER, b VARCHAR)[]",
	).Scan(&l); err != nil {
		t.Fatal(err)
	}
	if len(l.Values) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(l.Values))
	}
	// Each element should be map[string]any
	m0, ok := l.Values[0].(map[string]any)
	if !ok {
		t.Fatalf("element 0: expected map[string]any, got %T", l.Values[0])
	}
	if m0["a"].(int32) != 1 {
		t.Errorf("element 0 a: expected 1, got %v", m0["a"])
	}
	if m0["b"] != "x" {
		t.Errorf("element 0 b: expected x, got %v", m0["b"])
	}
}

func TestStdDB_StructWithNestedList(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT {'name': 'Alice', 'scores': [90, 85, 95]}::STRUCT(name VARCHAR, scores INTEGER[])",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}
	name, _ := s.Get("name")
	if name != "Alice" {
		t.Errorf("name: expected Alice, got %v", name)
	}
	scores, ok := s.Get("scores")
	if !ok {
		t.Fatal("scores field not found")
	}
	scoreSlice, ok := scores.([]any)
	if !ok {
		t.Fatalf("scores: expected []any, got %T", scores)
	}
	if len(scoreSlice) != 3 {
		t.Errorf("scores: expected 3 elements, got %d", len(scoreSlice))
	}
}

func TestStdDB_ColumnTypes_Nested(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_nested_ct (
		l INTEGER[],
		s STRUCT(a INT, b VARCHAR),
		m MAP(VARCHAR, INTEGER)
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_nested_ct VALUES ([1], {'a':1,'b':'x'}, MAP{'k':1})")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	rows, err := stdDB.QueryContext(ctx, "SELECT * FROM sqldb_nested_ct")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	expectedDBNames := []string{"LIST", "STRUCT", "MAP"}
	for idx, ct := range types {
		if ct.DatabaseTypeName() != expectedDBNames[idx] {
			t.Errorf("column %d: expected %q, got %q", idx, expectedDBNames[idx], ct.DatabaseTypeName())
		}
		scanType := ct.ScanType()
		if scanType == nil {
			t.Errorf("column %d: ScanType() returned nil", idx)
		}
	}

	// Verify ScanType is the correct rich type
	if types[0].ScanType().Name() != "List" {
		t.Errorf("LIST ScanType: expected List, got %s", types[0].ScanType().Name())
	}
	if types[1].ScanType().Name() != "Struct" {
		t.Errorf("STRUCT ScanType: expected Struct, got %s", types[1].ScanType().Name())
	}
	if types[2].ScanType().Name() != "Map" {
		t.Errorf("MAP ScanType: expected Map, got %s", types[2].ScanType().Name())
	}
}

func TestStdDB_Param_ListAsJSON(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// Pass a List as a JSON parameter
	l := couac.List{Values: []any{int64(1), int64(2), int64(3)}}
	var result string
	if err := stdDB.QueryRowContext(ctx, "SELECT ?::VARCHAR", l).Scan(&result); err != nil {
		t.Fatal(err)
	}
	if result != "[1,2,3]" {
		t.Errorf("expected [1,2,3], got %s", result)
	}
}

func TestStdDB_Param_NullListAsNULL(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	nl := couac.NullList{Valid: false}
	var isNull bool
	if err := stdDB.QueryRowContext(ctx, "SELECT ? IS NULL", nl).Scan(&isNull); err != nil {
		t.Fatal(err)
	}
	if !isNull {
		t.Error("expected NullList{Valid:false} to bind as NULL")
	}
}

// --------------- Deep nesting (depth ≥ 2) with chaining ---------------

func TestStdDB_ListOfList(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var outer couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [[1, 2], [3, 4, 5]]::INTEGER[][]",
	).Scan(&outer); err != nil {
		t.Fatal(err)
	}
	if len(outer.Values) != 2 {
		t.Fatalf("expected 2 outer elements, got %d", len(outer.Values))
	}

	ints0 := outer.ListAt(0).Ints()
	if len(ints0) != 2 || ints0[0] != 1 || ints0[1] != 2 {
		t.Errorf("inner[0]: expected [1, 2], got %v", ints0)
	}

	ints1 := outer.ListAt(1).Ints()
	if len(ints1) != 3 || ints1[0] != 3 || ints1[1] != 4 || ints1[2] != 5 {
		t.Errorf("inner[1]: expected [3, 4, 5], got %v", ints1)
	}
}

func TestStdDB_StructOfStruct(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT {'name': 'Alice', 'address': {'city': 'Montreal', 'zip': '12345'}}::"+
			"STRUCT(name VARCHAR, address STRUCT(city VARCHAR, zip VARCHAR))",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}
	name, ok := s.Str("name")
	if !ok || name != "Alice" {
		t.Errorf("name: expected Alice, got %v", name)
	}

	// Chain into nested struct
	city, ok := s.Struct("address").Str("city")
	if !ok || city != "Montreal" {
		t.Errorf("city: expected Montreal, got %v", city)
	}
	zip, ok := s.Struct("address").Str("zip")
	if !ok || zip != "12345" {
		t.Errorf("zip: expected 12345, got %v", zip)
	}
}

func TestStdDB_StructContainingMap(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT {'label': 'test', 'attrs': MAP {'k1': 10, 'k2': 20}}::"+
			"STRUCT(label VARCHAR, attrs MAP(VARCHAR, INTEGER))",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}
	label, _ := s.Str("label")
	if label != "test" {
		t.Errorf("label: expected test, got %v", label)
	}

	// Chain: Struct → Map → Int32
	k1, ok := s.Map("attrs").Int32("k1")
	if !ok || k1 != 10 {
		t.Errorf("k1: expected 10, got %v", k1)
	}
	k2, ok := s.Map("attrs").Int32("k2")
	if !ok || k2 != 20 {
		t.Errorf("k2: expected 20, got %v", k2)
	}
}

func TestStdDB_MapWithNestedStructValues(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var m couac.Map
	if err := stdDB.QueryRowContext(ctx,
		"SELECT MAP {'alice': {'age': 30, 'active': true}, 'bob': {'age': 25, 'active': false}}::"+
			"MAP(VARCHAR, STRUCT(age INTEGER, active BOOLEAN))",
	).Scan(&m); err != nil {
		t.Fatal(err)
	}

	// Chain: Map → Struct → Int32/Bool
	age, ok := m.Struct("alice").Int32("age")
	if !ok || age != 30 {
		t.Errorf("alice.age: expected 30, got %v", age)
	}
	active, ok := m.Struct("alice").Bool("active")
	if !ok || active != true {
		t.Errorf("alice.active: expected true, got %v", active)
	}
	bobAge, ok := m.Struct("bob").Int32("age")
	if !ok || bobAge != 25 {
		t.Errorf("bob.age: expected 25, got %v", bobAge)
	}
}

func TestStdDB_MapWithNestedListValues(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var m couac.Map
	if err := stdDB.QueryRowContext(ctx,
		"SELECT MAP {'x': [1, 2], 'y': [3, 4, 5]}::MAP(VARCHAR, INTEGER[])",
	).Scan(&m); err != nil {
		t.Fatal(err)
	}

	// Chain: Map → List → Ints
	xInts := m.List("x").Ints()
	if len(xInts) != 2 || xInts[0] != 1 || xInts[1] != 2 {
		t.Errorf("x: expected [1, 2], got %v", xInts)
	}
	if len(m.List("y").Ints()) != 3 {
		t.Errorf("y: expected 3 elements, got %d", len(m.List("y").Ints()))
	}
}

func TestStdDB_ListOfMap(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [MAP {'a': 1}, MAP {'b': 2}]::MAP(VARCHAR, INTEGER)[]",
	).Scan(&l); err != nil {
		t.Fatal(err)
	}
	if len(l.Values) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(l.Values))
	}

	// Chain: List → Map → Int32
	a, ok := l.MapAt(0).Int32("a")
	if !ok || a != 1 {
		t.Errorf("m0.a: expected 1, got %v", a)
	}
	b, ok := l.MapAt(1).Int32("b")
	if !ok || b != 2 {
		t.Errorf("m1.b: expected 2, got %v", b)
	}
}

func TestStdDB_ListOfStructContainingList(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// Depth-3: LIST< STRUCT< ..., LIST<INT> > >
	var outer couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [{'name': 'a', 'vals': [1, 2]}, {'name': 'b', 'vals': [3]}]::"+
			"STRUCT(name VARCHAR, vals INTEGER[])[]",
	).Scan(&outer); err != nil {
		t.Fatal(err)
	}
	if len(outer.Values) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(outer.Values))
	}

	// Chain: List → Struct → Str / List → Ints
	name0, ok := outer.StructAt(0).Str("name")
	if !ok || name0 != "a" {
		t.Errorf("elem0.name: expected a, got %v", name0)
	}
	ints0 := outer.StructAt(0).List("vals").Ints()
	if len(ints0) != 2 || ints0[0] != 1 || ints0[1] != 2 {
		t.Errorf("elem0.vals: expected [1, 2], got %v", ints0)
	}

	ints1 := outer.StructAt(1).List("vals").Ints()
	if len(ints1) != 1 || ints1[0] != 3 {
		t.Errorf("elem1.vals: expected [3], got %v", ints1)
	}
}

func TestStdDB_StructOfStructOfList(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// Depth-3: STRUCT< ..., STRUCT< ..., LIST<INT> > >
	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT {'id': 1, 'child': {'label': 'deep', 'nums': [10, 20]}}::"+
			"STRUCT(id INTEGER, child STRUCT(label VARCHAR, nums INTEGER[]))",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}
	id, _ := s.Int32("id")
	if id != 1 {
		t.Errorf("id: expected 1, got %v", id)
	}

	// Chain: Struct → Struct → Str / List → Ints
	label, _ := s.Struct("child").Str("label")
	if label != "deep" {
		t.Errorf("child.label: expected deep, got %v", label)
	}
	intNums := s.Struct("child").List("nums").Ints()
	if len(intNums) != 2 || intNums[0] != 10 || intNums[1] != 20 {
		t.Errorf("child.nums: expected [10, 20], got %v", intNums)
	}
}

// --------------- Null values within nested children ---------------

func TestStdDB_ListWithNullElements(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [1, NULL, 3]::INTEGER[]",
	).Scan(&l); err != nil {
		t.Fatal(err)
	}
	if len(l.Values) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(l.Values))
	}
	v0, ok := l.Int32At(0)
	if !ok || v0 != 1 {
		t.Errorf("element 0: expected 1, got %v (ok=%v)", v0, ok)
	}
	// NULL element
	_, ok = l.Int32At(1)
	if ok {
		t.Errorf("element 1: expected ok=false for NULL")
	}
	v2, ok := l.Int32At(2)
	if !ok || v2 != 3 {
		t.Errorf("element 2: expected 3, got %v (ok=%v)", v2, ok)
	}
}

func TestStdDB_StructWithNullField(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_struct_nullfield (
		data STRUCT(name VARCHAR, score INTEGER))`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_struct_nullfield VALUES ({'name': 'Alice', 'score': NULL})")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT data FROM sqldb_struct_nullfield",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}
	name, ok := s.Str("name")
	if !ok || name != "Alice" {
		t.Errorf("name: expected Alice, got %v", name)
	}
	// NULL field
	_, ok = s.Int32("score")
	if ok {
		t.Errorf("score: expected ok=false for NULL field")
	}
}

func TestStdDB_ListOfStructWithNullFields(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [{'a': 1, 'b': 'x'}, {'a': NULL, 'b': 'y'}]::STRUCT(a INTEGER, b VARCHAR)[]",
	).Scan(&l); err != nil {
		t.Fatal(err)
	}
	if len(l.Values) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(l.Values))
	}

	// Chain: List → Struct → Int32
	a0, _ := l.StructAt(0).Int32("a")
	if a0 != 1 {
		t.Errorf("s0.a: expected 1, got %v", a0)
	}

	_, ok := l.StructAt(1).Int32("a")
	if ok {
		t.Errorf("s1.a: expected ok=false for NULL field")
	}
	b1, _ := l.StructAt(1).Str("b")
	if b1 != "y" {
		t.Errorf("s1.b: expected y, got %v", b1)
	}
}

func TestStdDB_StructWithNullNestedStruct(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_null_child (
		data STRUCT(name VARCHAR, child STRUCT(x INTEGER, y INTEGER)))`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_null_child VALUES ({'name': 'test', 'child': NULL})")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT data FROM sqldb_null_child",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}
	name, _ := s.Str("name")
	if name != "test" {
		t.Errorf("name: expected test, got %v", name)
	}

	// NULL nested struct: chaining through it yields zero/false
	_, ok := s.Struct("child").Int32("x")
	if ok {
		t.Errorf("child.x: expected ok=false for NULL nested struct chain")
	}
}

func TestStdDB_StructWithNullNestedList(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_null_list_field (
		data STRUCT(label VARCHAR, tags VARCHAR[]))`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_null_list_field VALUES ({'label': 'ok', 'tags': NULL})")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	var s couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT data FROM sqldb_null_list_field",
	).Scan(&s); err != nil {
		t.Fatal(err)
	}
	label, _ := s.Str("label")
	if label != "ok" {
		t.Errorf("label: expected ok, got %v", label)
	}

	// NULL nested list: chaining through it yields empty slice
	tags := s.List("tags").Strings()
	if len(tags) != 0 {
		t.Errorf("tags: expected empty, got %v", tags)
	}
}

func TestStdDB_ListOfListWithNulls(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var outer couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [[1, NULL], NULL, [3]]::INTEGER[][]",
	).Scan(&outer); err != nil {
		t.Fatal(err)
	}
	if len(outer.Values) != 3 {
		t.Fatalf("expected 3 outer elements, got %d", len(outer.Values))
	}

	// First inner list: [1, NULL]
	inner0 := outer.ListAt(0)
	if len(inner0.Values) != 2 || inner0.Values[0].(int32) != 1 || inner0.Values[1] != nil {
		t.Errorf("inner0: expected [1, nil], got %v", inner0.Values)
	}

	// Second element: NULL — chaining through yields empty
	nullInts := outer.ListAt(1).Ints()
	if len(nullInts) != 0 {
		t.Errorf("ListAt(1): expected empty ints from NULL, got %v", nullInts)
	}

	// Third inner list: [3]
	ints2 := outer.ListAt(2).Ints()
	if len(ints2) != 1 || ints2[0] != 3 {
		t.Errorf("inner2: expected [3], got %v", ints2)
	}
}

func TestStdDB_MapWithNullValues(t *testing.T) {
	db := newTestDB(t)
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = conn.Exec(ctx, `CREATE TABLE sqldb_map_nullvals (data MAP(VARCHAR, INTEGER))`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO sqldb_map_nullvals VALUES (MAP {'a': 1, 'b': NULL})")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	stdDB := db.StdDB()
	defer stdDB.Close()

	var m couac.Map
	if err := stdDB.QueryRowContext(ctx,
		"SELECT data FROM sqldb_map_nullvals",
	).Scan(&m); err != nil {
		t.Fatal(err)
	}
	a, ok := m.Int32("a")
	if !ok || a != 1 {
		t.Errorf("a: expected 1, got %v (ok=%v)", a, ok)
	}
	_, ok = m.Int32("b")
	if ok {
		t.Errorf("b: expected ok=false for NULL value")
	}
}

func TestStdDB_DeepNesting_JSON_MarshalRoundTrip(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	// Depth-3: LIST< STRUCT< name, child STRUCT< ..., LIST > > >
	var l couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [{'name': 'root', 'child': {'tag': 'deep', 'ids': [100, 200]}}]::"+
			"STRUCT(name VARCHAR, child STRUCT(tag VARCHAR, ids INTEGER[]))[]",
	).Scan(&l); err != nil {
		t.Fatal(err)
	}

	// Full chain: List → Struct → Struct → List → Ints
	intIDs := l.StructAt(0).Struct("child").List("ids").Ints()
	if len(intIDs) != 2 || intIDs[0] != 100 || intIDs[1] != 200 {
		t.Errorf("ids: expected [100, 200], got %v", intIDs)
	}

	// MarshalJSON round-trip
	b, err := l.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}
	var l2 couac.List
	if err := l2.Scan(string(b)); err != nil {
		t.Fatalf("JSON round-trip Scan failed: %v", err)
	}
	if len(l2.Values) != 1 {
		t.Errorf("round-trip: expected 1 element, got %d", len(l2.Values))
	}
}

// --------------- 6-level deep nesting tests ---------------

// TestStdDB_Depth6_ListOfListOfList tests 6 levels of nested LIST:
// INTEGER[][][][][][]
func TestStdDB_Depth6_ListOfListOfList(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l1 couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [[[[[[42]]]]]]::INTEGER[][][][][][]",
	).Scan(&l1); err != nil {
		t.Fatal(err)
	}

	// Chain ListAt all the way down
	ints := l1.ListAt(0).ListAt(0).ListAt(0).ListAt(0).ListAt(0).Ints()
	if len(ints) != 1 || ints[0] != 42 {
		t.Errorf("leaf: expected [42], got %v", ints)
	}
}

// TestStdDB_Depth6_NestedStructs tests 6 levels of nested STRUCT.
func TestStdDB_Depth6_NestedStructs(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var s1 couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT {'v': 1, 'c': {'v': 2, 'c': {'v': 3, 'c': {'v': 4, 'c': {'v': 5, 'c': {'v': 6}}}}}}::"+
			"STRUCT(v INTEGER, c STRUCT(v INTEGER, c STRUCT(v INTEGER, c STRUCT(v INTEGER, c STRUCT(v INTEGER, c STRUCT(v INTEGER))))))",
	).Scan(&s1); err != nil {
		t.Fatal(err)
	}

	// Walk 6 levels with chaining
	current := s1
	for level := int32(1); level <= 6; level++ {
		v, ok := current.Int32("v")
		if !ok || v != level {
			t.Errorf("level %d: expected v=%d, got %v (ok=%v)", level, level, v, ok)
		}
		if level < 6 {
			current = current.Struct("c")
		}
	}
}

// TestStdDB_Depth6_MixedNesting tests 6 levels with alternating types:
// LIST< STRUCT< list: LIST< MAP< key, STRUCT< val: LIST<INT> > > > > >
func TestStdDB_Depth6_MixedNesting(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l1 couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [{'label': 'top', 'items': [MAP {'k': {'val': [7, 8, 9]}}]}]::"+
			"STRUCT(label VARCHAR, items MAP(VARCHAR, STRUCT(val INTEGER[]))[])[]",
	).Scan(&l1); err != nil {
		t.Fatal(err)
	}

	label, ok := l1.StructAt(0).Str("label")
	if !ok || label != "top" {
		t.Errorf("label: expected top, got %v", label)
	}

	// Full chain: List → Struct → List → Map → Struct → List → Ints
	ints := l1.StructAt(0).List("items").MapAt(0).Struct("k").List("val").Ints()
	if len(ints) != 3 || ints[0] != 7 || ints[1] != 8 || ints[2] != 9 {
		t.Errorf("leaf: expected [7, 8, 9], got %v", ints)
	}
}

// TestStdDB_Depth6_NullsAtEveryLevel tests NULL propagation through 6
// levels of struct nesting — chaining through NULL yields zero/false.
func TestStdDB_Depth6_NullsAtEveryLevel(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var s1 couac.Struct
	if err := stdDB.QueryRowContext(ctx,
		"SELECT {'v': 1, 'c': {'v': 2, 'c': {'v': 3, 'c': {'v': 4, 'c': {'v': 5, 'c': NULL}}}}}::"+
			"STRUCT(v INTEGER, c STRUCT(v INTEGER, c STRUCT(v INTEGER, c STRUCT(v INTEGER, c STRUCT(v INTEGER, c STRUCT(v INTEGER))))))",
	).Scan(&s1); err != nil {
		t.Fatal(err)
	}

	current := s1
	for level := int32(1); level <= 5; level++ {
		v, ok := current.Int32("v")
		if !ok || v != level {
			t.Errorf("level %d: expected v=%d, got %v", level, level, v)
		}
		current = current.Struct("c")
	}
	// Level 6 is NULL — further chaining yields false
	_, ok := current.Int32("v")
	if ok {
		t.Errorf("level 6: expected ok=false for NULL struct chain")
	}
}

// TestStdDB_Depth6_ListWithNullsAtMultipleLevels tests a 4-level nested
// list with NULL values interspersed at different levels.
func TestStdDB_Depth6_ListWithNullsAtMultipleLevels(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var l1 couac.List
	if err := stdDB.QueryRowContext(ctx,
		"SELECT [[[[1, NULL]], NULL], NULL]::INTEGER[][][][]",
	).Scan(&l1); err != nil {
		t.Fatal(err)
	}
	if len(l1.Values) != 2 {
		t.Fatalf("l1: expected 2 elements, got %d", len(l1.Values))
	}

	// l1[1] is NULL — chaining yields empty
	nullInts := l1.ListAt(1).Ints()
	if len(nullInts) != 0 {
		t.Errorf("l1[1]: expected empty from NULL, got %v", nullInts)
	}

	// l1[0] → l2
	l2 := l1.ListAt(0)
	if len(l2.Values) != 2 {
		t.Fatalf("l2: expected 2 elements, got %d", len(l2.Values))
	}

	// l2[1] is NULL
	nullInts = l2.ListAt(1).Ints()
	if len(nullInts) != 0 {
		t.Errorf("l2[1]: expected empty from NULL, got %v", nullInts)
	}

	// Chain through to leaf: l2[0] → l3[0] → l4 = [1, NULL]
	l4 := l2.ListAt(0).ListAt(0)
	if len(l4.Values) != 2 {
		t.Fatalf("l4: expected 2 elements, got %d", len(l4.Values))
	}
	v0, ok := l4.Int32At(0)
	if !ok || v0 != 1 {
		t.Errorf("l4[0]: expected 1, got %v", v0)
	}
	_, ok = l4.Int32At(1)
	if ok {
		t.Errorf("l4[1]: expected ok=false for NULL")
	}
}

// TestStdDB_Depth6_MapNestedInListInStructInMap tests: MAP→STRUCT→LIST→MAP.
func TestStdDB_Depth6_MapNestedInListInStructInMap(t *testing.T) {
	db := newTestDB(t)
	stdDB := db.StdDB()
	defer stdDB.Close()
	ctx := context.Background()

	var m1 couac.Map
	if err := stdDB.QueryRowContext(ctx,
		"SELECT MAP {'root': {'tag': 'x', 'entries': [MAP {'a': 1, 'b': 2}]}}::"+
			"MAP(VARCHAR, STRUCT(tag VARCHAR, entries MAP(VARCHAR, INTEGER)[]))",
	).Scan(&m1); err != nil {
		t.Fatal(err)
	}

	// Chain: Map → Struct → Str
	tag, _ := m1.Struct("root").Str("tag")
	if tag != "x" {
		t.Errorf("tag: expected x, got %v", tag)
	}

	// Chain: Map → Struct → List → Map → Int32
	aVal, _ := m1.Struct("root").List("entries").MapAt(0).Int32("a")
	bVal, _ := m1.Struct("root").List("entries").MapAt(0).Int32("b")
	if aVal != 1 {
		t.Errorf("a: expected 1, got %v", aVal)
	}
	if bVal != 2 {
		t.Errorf("b: expected 2, got %v", bVal)
	}
}

// TestStdDB_Depth6_ScanNilPreservesZeroValue ensures that calling Scan(nil)
// on each wrapper type consistently zeroes it.
func TestStdDB_Depth6_ScanNilPreservesZeroValue(t *testing.T) {
	var l couac.List
	l.Values = []any{1, 2, 3}
	if err := l.Scan(nil); err != nil {
		t.Fatalf("List.Scan(nil): %v", err)
	}
	if l.Values != nil {
		t.Errorf("List.Scan(nil): expected nil Values, got %v", l.Values)
	}

	var s couac.Struct
	s.Fields = map[string]any{"x": 1}
	if err := s.Scan(nil); err != nil {
		t.Fatalf("Struct.Scan(nil): %v", err)
	}
	if s.Fields != nil {
		t.Errorf("Struct.Scan(nil): expected nil Fields, got %v", s.Fields)
	}

	var m couac.Map
	m.Values = map[string]any{"k": 1}
	if err := m.Scan(nil); err != nil {
		t.Fatalf("Map.Scan(nil): %v", err)
	}
	if m.Values != nil {
		t.Errorf("Map.Scan(nil): expected nil Values, got %v", m.Values)
	}
}

// TestStdDB_TypedGetters_OutOfRange verifies typed leaf methods return
// false for out-of-range indices and missing keys.
func TestStdDB_TypedGetters_OutOfRange(t *testing.T) {
	l := couac.List{Values: []any{int32(1)}}
	_, ok := l.Int32At(-1)
	if ok {
		t.Error("Int32At(-1): expected false")
	}
	_, ok = l.Int32At(99)
	if ok {
		t.Error("Int32At(99): expected false")
	}

	s := couac.Struct{Fields: map[string]any{"a": int32(1)}}
	_, ok = s.Int32("nonexistent")
	if ok {
		t.Error("Int32(nonexistent): expected false")
	}

	m := couac.Map{Values: map[string]any{"a": int32(1)}}
	_, ok = m.Int32("nonexistent")
	if ok {
		t.Error("Int32(nonexistent): expected false")
	}
}

// TestStdDB_TypedGetters_TypeMismatch verifies typed leaf methods return
// false when the stored type doesn't match.
func TestStdDB_TypedGetters_TypeMismatch(t *testing.T) {
	l := couac.List{Values: []any{"hello"}}
	_, ok := l.Int32At(0)
	if ok {
		t.Error("Int32At on string: expected false")
	}

	s := couac.Struct{Fields: map[string]any{"name": "Alice"}}
	_, ok = s.Int32("name")
	if ok {
		t.Error("Int32 on string: expected false")
	}

	m := couac.Map{Values: map[string]any{"k": "v"}}
	_, ok = m.Int32("k")
	if ok {
		t.Error("Int32 on string: expected false")
	}
}

// TestStdDB_ChainingThroughMissing verifies that chaining through a
// missing or NULL intermediate always yields zero/false at the end.
func TestStdDB_ChainingThroughMissing(t *testing.T) {
	s := couac.Struct{Fields: map[string]any{"name": "Alice"}}

	// Chain through nonexistent struct field
	_, ok := s.Struct("nope").Str("city")
	if ok {
		t.Error("chain through missing: expected false")
	}

	// Chain through nonexistent list field
	ints := s.List("nope").Ints()
	if len(ints) != 0 {
		t.Errorf("chain through missing list: expected empty, got %v", ints)
	}

	// Chain through nonexistent map field
	_, ok = s.Map("nope").Int32("key")
	if ok {
		t.Error("chain through missing map: expected false")
	}

	// Deep chain through all zeros
	_, ok = s.Struct("nope").Struct("also_nope").List("deep").MapAt(0).Int32("k")
	if ok {
		t.Error("deep chain through zeros: expected false")
	}
}
