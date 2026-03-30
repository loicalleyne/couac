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
//	jl   := l.JSON()            // List with all elements as JSON strings
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
//	js   := s.JSON()               // Struct with all values as JSON strings
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
//	jm   := m.JSON()               // Map with all values as JSON strings
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
