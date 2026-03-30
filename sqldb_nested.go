package couac

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ---- List ----

// List represents a DuckDB LIST column value as a Go slice.
// Elements are Go-native types produced by the recursive Arrow-to-Go
// conversion: int8–uint64, float32, float64, bool, string, []byte,
// [Decimal], nested [List], [Struct], [Map], or nil.
//
// List also represents LargeList and FixedSizeList columns.
//
// List implements [sql.Scanner] and [driver.Valuer].
//
// Usage with [database/sql]:
//
//	var l couac.List
//	row.Scan(&l)
//	fmt.Println(l.Values)       // []any{1, 2, 3}
//	ints := l.Ints()            // []int64{1, 2, 3}
//	strs := l.Strings()         // []string{"1", "2", "3"}
//	j, _ := l.MarshalJSON()     // [1,2,3]
//
// Typed element access:
//
//	n, ok := l.Int32At(0)                  // leaf value
//	s, ok := l.StringAt(2)                 // leaf value
//	n, ok  = couac.Elem[int32](l, 0)       // generic (any leaf type)
//
// Chaining into nested types — navigation methods return a zero value
// on failure, so further calls safely return zero/false:
//
//	name, ok := outer.StructAt(0).Str("name")    // LIST<STRUCT<…>>
//	ints     := outer.ListAt(0).Ints()             // LIST<LIST<INT>>
//	v, ok    := outer.MapAt(0).Int32("key")        // LIST<MAP<…>>
type List struct {
	Values []any
}

// Ints returns the list elements as []int64. Elements that are not
// integer or float types are skipped. This is a convenience accessor
// for lists known to contain only integers (TINYINT through UBIGINT).
func (l List) Ints() []int64 {
	out := make([]int64, 0, len(l.Values))
	for _, v := range l.Values {
		switch n := v.(type) {
		case int8:
			out = append(out, int64(n))
		case int16:
			out = append(out, int64(n))
		case int32:
			out = append(out, int64(n))
		case int64:
			out = append(out, n)
		case uint8:
			out = append(out, int64(n))
		case uint16:
			out = append(out, int64(n))
		case uint32:
			out = append(out, int64(n))
		case uint64:
			out = append(out, int64(n))
		case float32:
			out = append(out, int64(n))
		case float64:
			out = append(out, int64(n))
		}
	}
	return out
}

// Floats returns the list elements as []float64. Elements that are not
// numeric types are skipped.
func (l List) Floats() []float64 {
	out := make([]float64, 0, len(l.Values))
	for _, v := range l.Values {
		switch n := v.(type) {
		case float32:
			out = append(out, float64(n))
		case float64:
			out = append(out, n)
		case int8:
			out = append(out, float64(n))
		case int16:
			out = append(out, float64(n))
		case int32:
			out = append(out, float64(n))
		case int64:
			out = append(out, float64(n))
		case uint8:
			out = append(out, float64(n))
		case uint16:
			out = append(out, float64(n))
		case uint32:
			out = append(out, float64(n))
		case uint64:
			out = append(out, float64(n))
		}
	}
	return out
}

// Strings returns the list elements as []string. Each element is
// formatted with [fmt.Sprint]. Nil elements become the empty string.
func (l List) Strings() []string {
	out := make([]string, len(l.Values))
	for i, v := range l.Values {
		if v == nil {
			out[i] = ""
		} else {
			out[i] = fmt.Sprint(v)
		}
	}
	return out
}

// Bools returns the list elements as []bool. Elements that are not
// bool are skipped.
func (l List) Bools() []bool {
	out := make([]bool, 0, len(l.Values))
	for _, v := range l.Values {
		if b, ok := v.(bool); ok {
			out = append(out, b)
		}
	}
	return out
}

// JSON returns a copy of the List with every element converted to its
// JSON string representation. This is useful when you want to process
// nested structures as JSON text rather than Go maps/slices.
func (l List) JSON() List {
	out := List{Values: make([]any, len(l.Values))}
	for i, v := range l.Values {
		if v == nil {
			out.Values[i] = nil
		} else {
			b, _ := json.Marshal(v)
			out.Values[i] = string(b)
		}
	}
	return out
}

// MarshalJSON implements [json.Marshaler].
func (l List) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.Values)
}

// String returns the JSON representation of the list.
func (l List) String() string {
	b, _ := l.MarshalJSON()
	return string(b)
}

// Value implements [driver.Valuer]. It serializes the list as a JSON string.
func (l List) Value() (driver.Value, error) {
	return l.String(), nil
}

// Scan implements [sql.Scanner] for List. It accepts:
//   - List: direct assignment
//   - []any: wraps directly
//   - string, []byte: parses as JSON array
//   - nil: zeroes the list
func (l *List) Scan(src any) error {
	if src == nil {
		l.Values = nil
		return nil
	}
	switch v := src.(type) {
	case List:
		*l = v
	case []any:
		l.Values = v
	case string:
		return json.Unmarshal([]byte(v), &l.Values)
	case []byte:
		return json.Unmarshal(v, &l.Values)
	default:
		return fmt.Errorf("couac: cannot scan %T into List", src)
	}
	return nil
}

// ---- NullList ----

// NullList represents a [List] that may be null, following the same
// pattern as [sql.NullString].
type NullList struct {
	List  List
	Valid bool // Valid is true if List is not NULL
}

// Scan implements [sql.Scanner].
func (nl *NullList) Scan(src any) error {
	if src == nil {
		nl.List = List{}
		nl.Valid = false
		return nil
	}
	nl.Valid = true
	return nl.List.Scan(src)
}

// Value implements [driver.Valuer].
func (nl NullList) Value() (driver.Value, error) {
	if !nl.Valid {
		return nil, nil
	}
	return nl.List.Value()
}

// ---- Struct ----

// Struct represents a DuckDB STRUCT column value as a Go map.
// Field names are the map keys; values are Go-native types produced
// by the recursive Arrow-to-Go conversion.
//
// Struct implements [sql.Scanner] and [driver.Valuer].
//
// Usage with [database/sql]:
//
//	var s couac.Struct
//	row.Scan(&s)
//	fmt.Println(s.Fields["name"]) // "Alice"
//	j, _ := s.MarshalJSON()       // {"name":"Alice","age":30}
//
// Typed field access:
//
//	name, ok := s.Str("name")                   // string field
//	age, ok  := s.Int32("age")                   // int32 field
//	name, ok  = couac.Field[string](s, "name")   // generic (any leaf type)
//
// Chaining into nested types — navigation methods return a zero value
// on failure, so further calls safely return zero/false:
//
//	city, ok := s.Struct("address").Str("city")  // STRUCT field → Struct
//	vals     := s.List("tags").Ints()              // LIST field → List
//	v, ok    := s.Map("attrs").Int32("k1")         // MAP field → Map
type Struct struct {
	Fields map[string]any
}

// Get returns the value for the given field name and whether the field
// exists in the struct. This is a convenience shorthand for
// s.Fields[name].
func (s Struct) Get(name string) (any, bool) {
	v, ok := s.Fields[name]
	return v, ok
}

// JSON returns a copy of the Struct with every field value converted
// to its JSON string representation. This is useful when you want to
// process nested structures as JSON text.
func (s Struct) JSON() Struct {
	out := Struct{Fields: make(map[string]any, len(s.Fields))}
	for k, v := range s.Fields {
		if v == nil {
			out.Fields[k] = nil
		} else {
			b, _ := json.Marshal(v)
			out.Fields[k] = string(b)
		}
	}
	return out
}

// MarshalJSON implements [json.Marshaler].
func (s Struct) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Fields)
}

// String returns the JSON representation of the struct.
func (s Struct) String() string {
	b, _ := s.MarshalJSON()
	return string(b)
}

// Value implements [driver.Valuer]. It serializes the struct as a JSON string.
func (s Struct) Value() (driver.Value, error) {
	return s.String(), nil
}

// Scan implements [sql.Scanner] for Struct. It accepts:
//   - Struct: direct assignment
//   - map[string]any: wraps directly
//   - string, []byte: parses as JSON object
//   - nil: zeroes the struct
func (s *Struct) Scan(src any) error {
	if src == nil {
		s.Fields = nil
		return nil
	}
	switch v := src.(type) {
	case Struct:
		*s = v
	case map[string]any:
		s.Fields = v
	case string:
		return json.Unmarshal([]byte(v), &s.Fields)
	case []byte:
		return json.Unmarshal(v, &s.Fields)
	default:
		return fmt.Errorf("couac: cannot scan %T into Struct", src)
	}
	return nil
}

// ---- NullStruct ----

// NullStruct represents a [Struct] that may be null, following the same
// pattern as [sql.NullString].
type NullStruct struct {
	Struct Struct
	Valid  bool // Valid is true if Struct is not NULL
}

// Scan implements [sql.Scanner].
func (ns *NullStruct) Scan(src any) error {
	if src == nil {
		ns.Struct = Struct{}
		ns.Valid = false
		return nil
	}
	ns.Valid = true
	return ns.Struct.Scan(src)
}

// Value implements [driver.Valuer].
func (ns NullStruct) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.Struct.Value()
}

// ---- Map ----

// Map represents a DuckDB MAP column value as a Go map.
// DuckDB MAP keys are typically VARCHAR; non-string keys are converted
// with [fmt.Sprint]. Values are Go-native types produced by the
// recursive Arrow-to-Go conversion.
//
// Map implements [sql.Scanner] and [driver.Valuer].
//
// Usage with [database/sql]:
//
//	var m couac.Map
//	row.Scan(&m)
//	fmt.Println(m.Values["key1"]) // 42
//	j, _ := m.MarshalJSON()       // {"key1":42,"key2":99}
//
// Typed value access:
//
//	count, ok := m.Int32("key1")                // int32 value
//	name, ok  := m.Str("key2")                  // string value
//	count, ok  = couac.Value[int32](m, "key1")  // generic (any leaf type)
//
// Chaining into nested types — navigation methods return a zero value
// on failure, so further calls safely return zero/false:
//
//	age, ok := m.Struct("alice").Int32("age")   // STRUCT value → Struct
//	nums    := m.List("nums").Ints()              // LIST value → List
//	v, ok   := m.Map("sub").Str("key")            // MAP value → Map
type Map struct {
	Values map[string]any
}

// Get returns the value for the given key and whether the key exists
// in the map. This is a convenience shorthand for m.Values[key].
func (m Map) Get(key string) (any, bool) {
	v, ok := m.Values[key]
	return v, ok
}

// Keys returns the map keys as a string slice.
func (m Map) Keys() []string {
	out := make([]string, 0, len(m.Values))
	for k := range m.Values {
		out = append(out, k)
	}
	return out
}

// JSON returns a copy of the Map with every value converted to its
// JSON string representation.
func (m Map) JSON() Map {
	out := Map{Values: make(map[string]any, len(m.Values))}
	for k, v := range m.Values {
		if v == nil {
			out.Values[k] = nil
		} else {
			b, _ := json.Marshal(v)
			out.Values[k] = string(b)
		}
	}
	return out
}

// MarshalJSON implements [json.Marshaler].
func (m Map) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Values)
}

// String returns the JSON representation of the map.
func (m Map) String() string {
	b, _ := m.MarshalJSON()
	return string(b)
}

// Value implements [driver.Valuer]. It serializes the map as a JSON string.
func (m Map) Value() (driver.Value, error) {
	return m.String(), nil
}

// Scan implements [sql.Scanner] for Map. It accepts:
//   - Map: direct assignment
//   - map[string]any: wraps directly
//   - string, []byte: parses as JSON object
//   - nil: zeroes the map
func (m *Map) Scan(src any) error {
	if src == nil {
		m.Values = nil
		return nil
	}
	switch v := src.(type) {
	case Map:
		*m = v
	case map[string]any:
		m.Values = v
	case string:
		return json.Unmarshal([]byte(v), &m.Values)
	case []byte:
		return json.Unmarshal(v, &m.Values)
	default:
		return fmt.Errorf("couac: cannot scan %T into Map", src)
	}
	return nil
}

// ---- NullMap ----

// NullMap represents a [Map] that may be null, following the same
// pattern as [sql.NullString].
type NullMap struct {
	Map   Map
	Valid bool // Valid is true if Map is not NULL
}

// Scan implements [sql.Scanner].
func (nm *NullMap) Scan(src any) error {
	if src == nil {
		nm.Map = Map{}
		nm.Valid = false
		return nil
	}
	nm.Valid = true
	return nm.Map.Scan(src)
}

// Value implements [driver.Valuer].
func (nm NullMap) Value() (driver.Value, error) {
	if !nm.Valid {
		return nil, nil
	}
	return nm.Map.Value()
}

// ---- compile-time interface checks ----

var (
	_ sql.Scanner    = (*List)(nil)
	_ driver.Valuer  = List{}
	_ json.Marshaler = List{}
	_ sql.Scanner    = (*NullList)(nil)
	_ driver.Valuer  = NullList{}

	_ sql.Scanner    = (*Struct)(nil)
	_ driver.Valuer  = Struct{}
	_ json.Marshaler = Struct{}
	_ sql.Scanner    = (*NullStruct)(nil)
	_ driver.Valuer  = NullStruct{}

	_ sql.Scanner    = (*Map)(nil)
	_ driver.Valuer  = Map{}
	_ json.Marshaler = Map{}
	_ sql.Scanner    = (*NullMap)(nil)
	_ driver.Valuer  = NullMap{}
)

// ---- Generic typed accessors ----

// Field returns the value of a [Struct] field as type T. If the field
// does not exist, is nil, or cannot be converted to T, the zero value
// and false are returned.
//
// For leaf types (int32, string, bool, etc.) a direct type assertion is
// used. For nested types ([List], [Struct], [Map]) the raw value
// ([]any or map[string]any) is automatically passed through Scan, so
// the full wrapper API is available on the result.
//
// Example:
//
//	name, ok := couac.Field[string](s, "name")
//	age, ok  := couac.Field[int32](s, "age")
//	addr, ok := couac.Field[couac.Struct](s, "address") // auto-Scans map[string]any
//	tags, ok := couac.Field[couac.List](s, "tags")       // auto-Scans []any
func Field[T any](s Struct, name string) (T, bool) {
	raw, ok := s.Fields[name]
	if !ok || raw == nil {
		var zero T
		return zero, false
	}
	return convert[T](raw)
}

// Value returns the value for a [Map] key as type T. If the key does
// not exist, is nil, or cannot be converted to T, the zero value and
// false are returned. The same Scan-fallback logic as [Field] applies
// for nested types.
//
// Example:
//
//	count, ok := couac.Value[int32](m, "key1")
//	inner, ok := couac.Value[couac.Struct](m, "alice") // auto-Scans
func Value[T any](m Map, key string) (T, bool) {
	raw, ok := m.Values[key]
	if !ok || raw == nil {
		var zero T
		return zero, false
	}
	return convert[T](raw)
}

// Elem returns the element at index i from a [List] as type T. If the
// index is out of range, the element is nil, or it cannot be converted
// to T, the zero value and false are returned. The same Scan-fallback
// logic as [Field] applies for nested types.
//
// Example:
//
//	n, ok := couac.Elem[int32](l, 0)
//	inner, ok := couac.Elem[couac.Struct](l, 0) // auto-Scans
func Elem[T any](l List, index int) (T, bool) {
	if index < 0 || index >= len(l.Values) {
		var zero T
		return zero, false
	}
	raw := l.Values[index]
	if raw == nil {
		var zero T
		return zero, false
	}
	return convert[T](raw)
}

// convert attempts to produce a T from a raw value. It first tries a
// direct type assertion. If that fails and *T implements [sql.Scanner]
// (as [List], [Struct], and [Map] do), it calls Scan to perform the
// conversion (e.g. map[string]any → Struct).
func convert[T any](raw any) (T, bool) {
	// Fast path: direct type assertion.
	if v, ok := raw.(T); ok {
		return v, true
	}
	// Slow path: try Scan for nested types (List, Struct, Map).
	var zero T
	if scanner, ok := any(&zero).(sql.Scanner); ok {
		if err := scanner.Scan(raw); err == nil {
			return zero, true
		}
	}
	return zero, false
}

// ---- Navigation convenience methods ----

// Struct returns the named field as a [Struct], enabling chaining.
// Returns a zero [Struct] if the field is missing, nil, or not a struct;
// further calls on the zero value safely return zero/false.
func (s Struct) Struct(name string) Struct {
	v, _ := Field[Struct](s, name)
	return v
}

// List returns the named field as a [List], enabling chaining.
// Returns a zero [List] if the field is missing, nil, or not a list;
// further calls on the zero value safely return zero/false.
func (s Struct) List(name string) List {
	v, _ := Field[List](s, name)
	return v
}

// Map returns the named field as a [Map], enabling chaining.
// Returns a zero [Map] if the field is missing, nil, or not a map;
// further calls on the zero value safely return zero/false.
func (s Struct) Map(name string) Map {
	v, _ := Field[Map](s, name)
	return v
}

// Struct returns the value for key as a [Struct], enabling chaining.
// Returns a zero [Struct] if the key is missing, nil, or not a struct;
// further calls on the zero value safely return zero/false.
func (m Map) Struct(key string) Struct {
	v, _ := Value[Struct](m, key)
	return v
}

// List returns the value for key as a [List], enabling chaining.
// Returns a zero [List] if the key is missing, nil, or not a list;
// further calls on the zero value safely return zero/false.
func (m Map) List(key string) List {
	v, _ := Value[List](m, key)
	return v
}

// Map returns the value for key as a [Map], enabling chaining.
// Returns a zero [Map] if the key is missing, nil, or not a map;
// further calls on the zero value safely return zero/false.
func (m Map) Map(key string) Map {
	v, _ := Value[Map](m, key)
	return v
}

// StructAt returns the element at index i as a [Struct], enabling chaining.
// Returns a zero [Struct] if i is out of range, nil, or not a struct;
// further calls on the zero value safely return zero/false.
func (l List) StructAt(i int) Struct {
	v, _ := Elem[Struct](l, i)
	return v
}

// ListAt returns the element at index i as a [List], enabling chaining.
// Returns a zero [List] if i is out of range, nil, or not a list;
// further calls on the zero value safely return zero/false.
func (l List) ListAt(i int) List {
	v, _ := Elem[List](l, i)
	return v
}

// MapAt returns the element at index i as a [Map], enabling chaining.
// Returns a zero [Map] if i is out of range, nil, or not a map;
// further calls on the zero value safely return zero/false.
func (l List) MapAt(i int) Map {
	v, _ := Elem[Map](l, i)
	return v
}

// ---- Typed leaf accessors (Struct) ----

// Int32 returns the named field as int32.
// Returns (0, false) if the field is missing, nil, or not int32.
func (s Struct) Int32(name string) (int32, bool) { return Field[int32](s, name) }

// Int64 returns the named field as int64.
// Returns (0, false) if the field is missing, nil, or not int64.
func (s Struct) Int64(name string) (int64, bool) { return Field[int64](s, name) }

// Float64 returns the named field as float64.
// Returns (0, false) if the field is missing, nil, or not float64.
func (s Struct) Float64(name string) (float64, bool) { return Field[float64](s, name) }

// Bool returns the named field as bool.
// Returns (false, false) if the field is missing, nil, or not bool.
func (s Struct) Bool(name string) (bool, bool) { return Field[bool](s, name) }

// Str returns the named field as string.
// Returns ("", false) if the field is missing, nil, or not a string.
// (Named Str to avoid collision with the String() string method.)
func (s Struct) Str(name string) (string, bool) { return Field[string](s, name) }

// ---- Typed leaf accessors (Map) ----

// Int32 returns the value for key as int32.
// Returns (0, false) if the key is missing, nil, or not int32.
func (m Map) Int32(key string) (int32, bool) { return Value[int32](m, key) }

// Int64 returns the value for key as int64.
// Returns (0, false) if the key is missing, nil, or not int64.
func (m Map) Int64(key string) (int64, bool) { return Value[int64](m, key) }

// Float64 returns the value for key as float64.
// Returns (0, false) if the key is missing, nil, or not float64.
func (m Map) Float64(key string) (float64, bool) { return Value[float64](m, key) }

// Bool returns the value for key as bool.
// Returns (false, false) if the key is missing, nil, or not bool.
func (m Map) Bool(key string) (bool, bool) { return Value[bool](m, key) }

// Str returns the value for key as string.
// Returns ("", false) if the key is missing, nil, or not a string.
// (Named Str to avoid collision with the String() string method.)
func (m Map) Str(key string) (string, bool) { return Value[string](m, key) }

// ---- Typed leaf accessors (List) ----

// Int32At returns the element at index i as int32.
// Returns (0, false) if i is out of range, nil, or not int32.
func (l List) Int32At(i int) (int32, bool) { return Elem[int32](l, i) }

// Int64At returns the element at index i as int64.
// Returns (0, false) if i is out of range, nil, or not int64.
func (l List) Int64At(i int) (int64, bool) { return Elem[int64](l, i) }

// Float64At returns the element at index i as float64.
// Returns (0, false) if i is out of range, nil, or not float64.
func (l List) Float64At(i int) (float64, bool) { return Elem[float64](l, i) }

// BoolAt returns the element at index i as bool.
// Returns (false, false) if i is out of range, nil, or not bool.
func (l List) BoolAt(i int) (bool, bool) { return Elem[bool](l, i) }

// StringAt returns the element at index i as string.
// Returns ("", false) if i is out of range, nil, or not a string.
func (l List) StringAt(i int) (string, bool) { return Elem[string](l, i) }

// ---- Arrow-to-Go recursive walker ----

// arrowToGoValue extracts a single value from an Arrow array at row index i
// and returns it as a Go-native type. For nested types (List, Struct, Map),
// it recurses into child arrays. Leaf types are delegated to
// [arrowToDriverValue].
//
// [json.Number] values produced by Arrow's GetOneForMarshal are
// normalized to their concrete Go numeric types.
func arrowToGoValue(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch c := col.(type) {
	// ---- Struct → map[string]any ----
	case *array.Struct:
		m := structToGoMap(c, i)
		return m

	// ---- List variants → []any ----
	case *array.List:
		return listToSlice(c, i)
	case *array.LargeList:
		return largeListToSlice(c, i)
	case *array.FixedSizeList:
		return fixedSizeListToSlice(c, i)

	// ---- Map → map[string]any ----
	case *array.Map:
		return mapToGoMap(c, i)

	// ---- Leaf types: delegate ----
	default:
		return arrowToDriverValue(col, i)
	}
}

// structToGoMap builds a map[string]any from an Arrow Struct at row i,
// recursing into child arrays with arrowToGoValue so nested types are
// fully converted to Go-native types (not json.RawMessage).
func structToGoMap(col *array.Struct, i int) map[string]any {
	st := col.DataType().(*arrow.StructType)
	m := make(map[string]any, st.NumFields())
	for fi := range st.NumFields() {
		child := col.Field(fi)
		if child.IsNull(i) {
			m[st.Field(fi).Name] = nil
		} else {
			m[st.Field(fi).Name] = arrowToGoValue(child, i)
		}
	}
	return m
}

// listToSlice extracts a single List element at row i as []any.
func listToSlice(col *array.List, i int) []any {
	beg, end := col.ValueOffsets(i)
	child := col.ListValues()
	out := make([]any, 0, end-beg)
	for j := int(beg); j < int(end); j++ {
		out = append(out, arrowToGoValue(child, j))
	}
	return out
}

// largeListToSlice extracts a single LargeList element at row i as []any.
func largeListToSlice(col *array.LargeList, i int) []any {
	beg, end := col.ValueOffsets(i)
	child := col.ListValues()
	out := make([]any, 0, end-beg)
	for j := int(beg); j < int(end); j++ {
		out = append(out, arrowToGoValue(child, j))
	}
	return out
}

// fixedSizeListToSlice extracts a single FixedSizeList element at row i as []any.
func fixedSizeListToSlice(col *array.FixedSizeList, i int) []any {
	size := int(col.DataType().(*arrow.FixedSizeListType).Len())
	child := col.ListValues()
	offset := i * size
	out := make([]any, size)
	for j := range size {
		out[j] = arrowToGoValue(child, offset+j)
	}
	return out
}

// mapToGoMap extracts a single Map element at row i as map[string]any.
// DuckDB MAP keys are typically VARCHAR; non-string keys are converted
// with [fmt.Sprint].
func mapToGoMap(col *array.Map, i int) map[string]any {
	beg, end := col.ValueOffsets(i)
	keys := col.Keys()
	items := col.Items()
	out := make(map[string]any, end-beg)
	for j := int(beg); j < int(end); j++ {
		var key string
		if sk, ok := keys.(*array.String); ok {
			key = cloneStr(sk.Value(j))
		} else {
			key = fmt.Sprint(arrowToGoValue(keys, j))
		}
		out[key] = arrowToGoValue(items, j)
	}
	return out
}
