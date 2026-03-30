package couac

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Decimal represents an arbitrary-precision decimal number.
// Width is the total number of digits, Scale is the number of
// digits after the decimal point, and Unscaled is the integer value
// before applying the scale (i.e. the actual value is Unscaled / 10^Scale).
//
// This mirrors the Decimal type used by duckdb-go for compatibility.
//
// Usage with [database/sql]:
//
//	var d couac.Decimal
//	row.Scan(&d)
//	precise := d.BigFloat()    // lossless *big.Float
//	approx  := d.Float64()     // lossy float64
//	text    := d.String()      // "123.45"
type Decimal struct {
	Width    uint8
	Scale    uint8
	Unscaled *big.Int
}

// BigFloat converts the Decimal to a [*big.Float] with full precision.
// This is lossless for all decimal values that DuckDB can produce.
//
// Usage with database/sql:
//
//	var d couac.Decimal
//	row.Scan(&d)
//	precise := d.BigFloat()
func (d Decimal) BigFloat() *big.Float {
	if d.Unscaled == nil {
		return new(big.Float)
	}
	bf := new(big.Float).SetPrec(256).SetInt(d.Unscaled)
	if d.Scale == 0 {
		return bf
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.Scale)), nil)
	return bf.Quo(bf, new(big.Float).SetPrec(256).SetInt(scale))
}

// Float64 converts the Decimal to a float64. This may lose precision
// for high-precision values; use [Decimal.BigFloat] when precision matters.
func (d Decimal) Float64() float64 {
	f, _ := d.BigFloat().Float64()
	return f
}

// Value implements [database/sql/driver.Valuer], allowing Decimal to be
// passed as a query parameter. It serializes as the fixed-point string
// representation (e.g. "123.45").
func (d Decimal) Value() (driver.Value, error) {
	return d.String(), nil
}

// String formats the Decimal as a fixed-point string with the correct
// number of decimal places (e.g. "123.45" for Value=12345, Scale=2).
func (d Decimal) String() string {
	if d.Unscaled == nil {
		return "0"
	}
	if d.Scale == 0 {
		return d.Unscaled.Text(10)
	}
	negative := d.Unscaled.Sign() < 0
	abs := new(big.Int).Abs(d.Unscaled)
	str := abs.Text(10)
	scale := int(d.Scale)
	// Pad with leading zeros if the string is shorter than the scale.
	for len(str) <= scale {
		str = "0" + str
	}
	result := str[:len(str)-scale] + "." + str[len(str)-scale:]
	if negative {
		result = "-" + result
	}
	return result
}

// Scan implements [sql.Scanner] for Decimal. It accepts the following source types:
//   - Decimal: direct assignment
//   - *big.Int: assigns Unscaled directly (Scale=0)
//   - int64, float64: converts to Decimal with Scale=0
//   - string, []byte: parses as a decimal string (e.g. "123.45")
//   - nil: zeroes the Decimal
func (d *Decimal) Scan(src any) error {
	if src == nil {
		d.Width = 0
		d.Scale = 0
		d.Unscaled = nil
		return nil
	}
	switch v := src.(type) {
	case Decimal:
		*d = v
	case *big.Int:
		d.Unscaled = new(big.Int).Set(v)
		d.Scale = 0
	case int64:
		d.Unscaled = big.NewInt(v)
		d.Scale = 0
	case float64:
		return d.scanString(fmt.Sprintf("%g", v))
	case string:
		return d.scanString(v)
	case []byte:
		return d.scanString(string(v))
	default:
		return fmt.Errorf("couac: cannot scan %T into Decimal", src)
	}
	return nil
}

// scanString parses a decimal string like "123.45" or "-0.007" into d.
func (d *Decimal) scanString(s string) error {
	// Use big.Float for exact parsing, then extract integer + scale.
	bf, _, err := new(big.Float).SetPrec(256).Parse(s, 10)
	if err != nil {
		return fmt.Errorf("couac: cannot parse %q as Decimal: %w", s, err)
	}
	// Determine scale from the string's decimal point position.
	scale := 0
	if dot := indexOf(s, '.'); dot >= 0 {
		scale = len(s) - dot - 1
	}
	// Multiply by 10^scale to get the unscaled integer.
	exp := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	unscaled, _ := new(big.Float).SetPrec(256).Mul(bf, new(big.Float).SetPrec(256).SetInt(exp)).Int(nil)
	d.Unscaled = unscaled
	d.Scale = uint8(scale)
	return nil
}

// indexOf returns the index of the first occurrence of b in s, or -1.
func indexOf(s string, b byte) int {
	for i := range len(s) {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// NullDecimal represents a [Decimal] that may be null. NullDecimal implements
// [sql.Scanner] and [driver.Valuer], following the same pattern as
// [sql.NullString], [sql.NullInt64], and [sql.NullFloat64].
//
// Usage with [database/sql]:
//
//	var nd couac.NullDecimal
//	err := row.Scan(&nd)
//	if nd.Valid {
//	    fmt.Println(nd.Decimal.BigFloat())
//	}
type NullDecimal struct {
	Decimal Decimal
	Valid   bool // Valid is true if Decimal is not NULL
}

// Scan implements [sql.Scanner]. It sets Valid to false for nil values
// and delegates to [Decimal.Scan] otherwise.
func (nd *NullDecimal) Scan(src any) error {
	if src == nil {
		nd.Decimal = Decimal{}
		nd.Valid = false
		return nil
	}
	nd.Valid = true
	return nd.Decimal.Scan(src)
}

// Value implements [driver.Valuer]. It returns nil when !Valid and
// delegates to [Decimal.Value] otherwise.
func (nd NullDecimal) Value() (driver.Value, error) {
	if !nd.Valid {
		return nil, nil
	}
	return nd.Decimal.Value()
}

// compile-time interface checks
var (
	_ sql.Scanner   = (*Decimal)(nil)
	_ driver.Valuer = Decimal{}
	_ sql.Scanner   = (*NullDecimal)(nil)
	_ driver.Valuer = NullDecimal{}
)

// arrowToDriverValue extracts a non-null value from an Arrow array column
// at the given row index and returns it as the closest native Go type.
//
// The mapping follows duckdb-go conventions: integer types keep their
// native width (int8–uint64), floats keep native width (float32/float64),
// temporal types return time.Time, decimal types return [Decimal], binary
// types return []byte, list types return [List], struct types return
// [Struct], map types return [Map], and union/interval types fall back
// to their Arrow JSON string representation.
func arrowToDriverValue(col arrow.Array, i int) driver.Value {
	switch col := col.(type) {
	// ---- Boolean ----
	case *array.Boolean:
		return col.Value(i)

	// ---- Signed integers ----
	case *array.Int8:
		return col.Value(i)
	case *array.Int16:
		return col.Value(i)
	case *array.Int32:
		return col.Value(i)
	case *array.Int64:
		return col.Value(i)

	// ---- Unsigned integers ----
	case *array.Uint8:
		return col.Value(i)
	case *array.Uint16:
		return col.Value(i)
	case *array.Uint32:
		return col.Value(i)
	case *array.Uint64:
		return col.Value(i)

	// ---- Floating-point ----
	case *array.Float16:
		return float32(col.Value(i).Float32())
	case *array.Float32:
		return col.Value(i)
	case *array.Float64:
		return col.Value(i)

	// ---- Strings ----
	case *array.String:
		return cloneStr(col.Value(i))
	case *array.LargeString:
		return cloneStr(col.Value(i))

	// ---- Binary ----
	case *array.Binary:
		return bytes.Clone(col.Value(i))
	case *array.LargeBinary:
		return bytes.Clone(col.Value(i))
	case *array.FixedSizeBinary:
		return bytes.Clone(col.Value(i))

	// ---- Temporal ----
	case *array.Timestamp:
		typ := col.DataType().(*arrow.TimestampType)
		return col.Value(i).ToTime(typ.Unit)
	case *array.Date32:
		return col.Value(i).ToTime()
	case *array.Date64:
		return col.Value(i).ToTime()
	case *array.Time32:
		typ := col.DataType().(*arrow.Time32Type)
		return col.Value(i).ToTime(typ.Unit)
	case *array.Time64:
		typ := col.DataType().(*arrow.Time64Type)
		return col.Value(i).ToTime(typ.Unit)

	// ---- Duration → int64 (raw value in the type's time unit) ----
	case *array.Duration:
		return int64(col.Value(i))

	// ---- Decimal → Decimal ----
	case *array.Decimal128:
		typ := col.DataType().(*arrow.Decimal128Type)
		return Decimal{
			Width:    uint8(typ.Precision),
			Scale:    uint8(typ.Scale),
			Unscaled: col.Value(i).BigInt(),
		}
	case *array.Decimal256:
		typ := col.DataType().(*arrow.Decimal256Type)
		return Decimal{
			Width:    uint8(typ.Precision),
			Scale:    uint8(typ.Scale),
			Unscaled: col.Value(i).BigInt(),
		}

	// ---- Dictionary (ENUM) → string ----
	case *array.Dictionary:
		return cloneStr(col.ValueStr(i))

	// ---- Null ----
	case *array.Null:
		return nil

	// ---- Nested / complex → rich types ----
	case *array.List:
		return List{Values: listToSlice(col, i)}
	case *array.LargeList:
		return List{Values: largeListToSlice(col, i)}
	case *array.FixedSizeList:
		return List{Values: fixedSizeListToSlice(col, i)}
	case *array.Struct:
		return Struct{Fields: structToGoMap(col, i)}
	case *array.Map:
		return Map{Values: mapToGoMap(col, i)}
	case *array.DenseUnion:
		return col.ValueStr(i)
	case *array.SparseUnion:
		return col.ValueStr(i)

	// ---- Intervals → string representation ----
	case *array.MonthInterval:
		return col.ValueStr(i)
	case *array.DayTimeInterval:
		return col.ValueStr(i)
	case *array.MonthDayNanoInterval:
		return col.ValueStr(i)

	default:
		// Extension types: unwrap to storage array.
		if ext, ok := col.(interface{ Storage() arrow.Array }); ok {
			return arrowToDriverValue(ext.Storage(), i)
		}
		return cloneStr(col.ValueStr(i))
	}
}

// arrowToReflectType returns the [reflect.Type] that [arrowToDriverValue]
// will produce for columns of the given Arrow DataType.
func arrowToReflectType(dt arrow.DataType) reflect.Type {
	switch dt.ID() {
	case arrow.BOOL:
		return reflect.TypeFor[bool]()
	case arrow.INT8:
		return reflect.TypeFor[int8]()
	case arrow.INT16:
		return reflect.TypeFor[int16]()
	case arrow.INT32:
		return reflect.TypeFor[int32]()
	case arrow.INT64:
		return reflect.TypeFor[int64]()
	case arrow.UINT8:
		return reflect.TypeFor[uint8]()
	case arrow.UINT16:
		return reflect.TypeFor[uint16]()
	case arrow.UINT32:
		return reflect.TypeFor[uint32]()
	case arrow.UINT64:
		return reflect.TypeFor[uint64]()
	case arrow.FLOAT16, arrow.FLOAT32:
		return reflect.TypeFor[float32]()
	case arrow.FLOAT64:
		return reflect.TypeFor[float64]()
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return reflect.TypeFor[Decimal]()
	case arrow.STRING, arrow.LARGE_STRING:
		return reflect.TypeFor[string]()
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		return reflect.TypeFor[[]byte]()
	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64:
		return reflect.TypeFor[time.Time]()
	case arrow.DURATION:
		return reflect.TypeFor[int64]()
	case arrow.DICTIONARY:
		return reflect.TypeFor[string]()
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return reflect.TypeFor[List]()
	case arrow.STRUCT:
		return reflect.TypeFor[Struct]()
	case arrow.MAP:
		return reflect.TypeFor[Map]()
	case arrow.NULL:
		return reflect.TypeFor[any]()
	default:
		return reflect.TypeFor[string]()
	}
}

// arrowToDatabaseTypeName returns a DuckDB-style type name string
// for the given Arrow DataType.
func arrowToDatabaseTypeName(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "UTINYINT"
	case arrow.UINT16:
		return "USMALLINT"
	case arrow.UINT32:
		return "UINTEGER"
	case arrow.UINT64:
		return "UBIGINT"
	case arrow.FLOAT16, arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return "DECIMAL"
	case arrow.STRING, arrow.LARGE_STRING:
		return "VARCHAR"
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		return "BLOB"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	case arrow.TIME32, arrow.TIME64:
		return "TIME"
	case arrow.DURATION:
		return "INTERVAL"
	case arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
		return "INTERVAL"
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return "LIST"
	case arrow.STRUCT:
		return "STRUCT"
	case arrow.MAP:
		return "MAP"
	case arrow.DENSE_UNION, arrow.SPARSE_UNION:
		return "UNION"
	case arrow.DICTIONARY:
		return "ENUM"
	case arrow.NULL:
		return "NULL"
	default:
		return dt.Name()
	}
}

// ---- driver.RowsColumnType* interfaces on sqlRows ----

// ColumnTypeScanType implements [driver.RowsColumnTypeScanType].
// It returns the [reflect.Type] of the Go value that [arrowToDriverValue]
// produces for the column at the given index.
func (r *sqlRows) ColumnTypeScanType(index int) reflect.Type {
	return arrowToReflectType(r.rr.Schema().Field(index).Type)
}

// ColumnTypeDatabaseTypeName implements [driver.RowsColumnTypeDatabaseTypeName].
// It returns a DuckDB-style type name (e.g. "INTEGER", "VARCHAR", "TIMESTAMP").
func (r *sqlRows) ColumnTypeDatabaseTypeName(index int) string {
	return arrowToDatabaseTypeName(r.rr.Schema().Field(index).Type)
}

// ColumnTypeNullable implements [driver.RowsColumnTypeNullable].
func (r *sqlRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.rr.Schema().Field(index).Nullable, true
}

// ColumnTypePrecisionScale implements [driver.RowsColumnTypePrecisionScale].
// It returns precision and scale for DECIMAL columns; ok is false for other types.
func (r *sqlRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	switch dt := r.rr.Schema().Field(index).Type.(type) {
	case *arrow.Decimal128Type:
		return int64(dt.Precision), int64(dt.Scale), true
	case *arrow.Decimal256Type:
		return int64(dt.Precision), int64(dt.Scale), true
	default:
		return 0, 0, false
	}
}
