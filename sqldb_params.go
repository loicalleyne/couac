package couac

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// namedValuesToRecord converts a slice of [driver.NamedValue] into a
// single-row [arrow.RecordBatch] suitable for [adbc.Statement.Bind].
//
// The mapping from Go types to Arrow types is:
//
//	int64      → Int64
//	float64    → Float64
//	bool       → Boolean
//	string     → String (Utf8)
//	[]byte     → Binary
//	time.Time  → Timestamp(µs, UTC)
//	Decimal    → Decimal128(Width, Scale)
//	nil        → Null
//
// DuckDB's ADBC driver only supports binding a single-row record batch
// for prepared-statement parameters.
func namedValuesToRecord(args []driver.NamedValue) (arrow.RecordBatch, error) {
	n := len(args)
	fields := make([]arrow.Field, n)
	builders := make([]array.Builder, n)
	alloc := memory.DefaultAllocator

	for i, nv := range args {
		name := fmt.Sprintf("p%d", nv.Ordinal)
		switch v := nv.Value.(type) {
		case nil:
			fields[i] = arrow.Field{Name: name, Type: arrow.Null, Nullable: true}
			b := array.NewNullBuilder(alloc)
			b.AppendNull()
			builders[i] = b

		case bool:
			fields[i] = arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean, Nullable: false}
			b := array.NewBooleanBuilder(alloc)
			b.Append(v)
			builders[i] = b

		case int64:
			fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: false}
			b := array.NewInt64Builder(alloc)
			b.Append(v)
			builders[i] = b

		case float64:
			fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: false}
			b := array.NewFloat64Builder(alloc)
			b.Append(v)
			builders[i] = b

		case string:
			fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: false}
			b := array.NewStringBuilder(alloc)
			b.Append(v)
			builders[i] = b

		case []byte:
			fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.Binary, Nullable: false}
			b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
			b.Append(v)
			builders[i] = b

		case time.Time:
			dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
			fields[i] = arrow.Field{Name: name, Type: dt, Nullable: false}
			b := array.NewTimestampBuilder(alloc, dt)
			b.Append(arrow.Timestamp(v.UTC().UnixMicro()))
			builders[i] = b

		case Decimal:
			dt := &arrow.Decimal128Type{Precision: int32(v.Width), Scale: int32(v.Scale)}
			fields[i] = arrow.Field{Name: name, Type: dt, Nullable: false}
			b := array.NewDecimal128Builder(alloc, dt)
			b.Append(decimal128FromBigInt(v.Unscaled))
			builders[i] = b

		default:
			// Release any builders already created.
			for j := range i {
				builders[j].Release()
			}
			return nil, fmt.Errorf("couac: unsupported parameter type %T at ordinal %d", nv.Value, nv.Ordinal)
		}
	}

	// Build arrays and construct the record.
	schema := arrow.NewSchema(fields, nil)
	cols := make([]arrow.Array, n)
	for i, b := range builders {
		cols[i] = b.NewArray()
		b.Release()
	}
	rec := array.NewRecordBatch(schema, cols, 1)
	// NewRecordBatch retains the columns, so we release our references.
	for _, c := range cols {
		c.Release()
	}
	return rec, nil
}

// decimal128FromBigInt converts a *big.Int to an Arrow Decimal128 value.
func decimal128FromBigInt(bi *big.Int) decimal128.Num {
	if bi == nil {
		return decimal128.New(0, 0)
	}
	return decimal128.FromBigInt(bi)
}

// bindArgs binds args to an ADBC statement if any arguments are provided.
// It builds a single-row Arrow record batch from the args and calls Bind.
// The record batch is released after binding.
func bindArgs(ctx context.Context, stmt adbcStatement, args []driver.NamedValue) error {
	if len(args) == 0 {
		return nil
	}
	rec, err := namedValuesToRecord(args)
	if err != nil {
		return err
	}
	defer rec.Release()
	return stmt.Bind(ctx, rec)
}

// adbcStatement is the subset of adbc.Statement used by bindArgs,
// allowing easier testing.
type adbcStatement interface {
	Bind(ctx context.Context, values arrow.RecordBatch) error
}
