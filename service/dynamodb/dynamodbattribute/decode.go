package dynamodbattribute

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// An Unmarshaler is an interface to provide custom unmarshaling of
// AttributeValues. Use this to provide custom logic determining
// how AttributeValues should be unmarshaled.
type Unmarshaler interface {
	UnmarshalDynamoDBAttributeValue(*dynamodb.AttributeValue) error
}

// Unmarshal PLACE HOLDER
//
// The output value provided must be a non-nil pointer
func Unmarshal(av *dynamodb.AttributeValue, out interface{}) error {
	return NewDecoder().Decode(av, out)
}

// UnmarshalMap is an alias for Unmarshal which unmarshals from
// a map of AttributeValues.
//
// The output value provided must be a non-nil pointer
func UnmarshalMap(m map[string]*dynamodb.AttributeValue, out interface{}) error {
	return NewDecoder().Decode(&dynamodb.AttributeValue{M: m}, out)
}

// UnmarshalList is an alias for Unmarshal func which unmarshals
// a slice of AttributeValues.
//
// The output value provided must be a non-nil pointer
func UnmarshalList(l []*dynamodb.AttributeValue, out interface{}) error {
	return NewDecoder().Decode(&dynamodb.AttributeValue{L: l}, out)
}

// A Decoder provides unmarshaling AttributeValues to Go value types.
type Decoder struct {
	MarshalOptions
}

// NewDecoder creates a new Decoder with default configuration. Use
// the `opts` functional options to override the default configuration.
func NewDecoder(opts ...func(*Decoder)) *Decoder {
	d := &Decoder{MarshalOptions{
		SupportJSONTags: true,
	}}
	for _, o := range opts {
		o(d)
	}

	return d
}

// Decode will unmarshal an AttributeValue into a Go value type. An error
// will be return if the decoder is unable to unmarshal the AttributeValue
// to the provide Go value type.
//
// The output value provided must be a non-nil pointer
func (d *Decoder) Decode(av *dynamodb.AttributeValue, out interface{}, opts ...func(*Decoder)) error {
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Ptr || v.IsNil() || !v.IsValid() {
		return &InvalidUnmarshalError{Type: reflect.TypeOf(out)}
	}

	return d.decode(av, v, tag{})
}

var stringInterfaceMapType = reflect.TypeOf(map[string]interface{}(nil))
var byteSliceType = reflect.TypeOf([]byte(nil))
var byteSliceSlicetype = reflect.TypeOf([][]byte(nil))

func (d *Decoder) decode(av *dynamodb.AttributeValue, v reflect.Value, fieldTag tag) error {
	var u Unmarshaler
	if av == nil || av.NULL != nil {
		u, v = indirect(v, true)
		if u != nil {
			return u.UnmarshalDynamoDBAttributeValue(av)
		}
		return d.decodeNull(v)
	}

	u, v = indirect(v, false)
	if u != nil {
		return u.UnmarshalDynamoDBAttributeValue(av)
	}

	switch {
	case len(av.B) != 0:
		return d.decodeBinary(av.B, v)
	case av.BOOL != nil:
		return d.decodeBool(av.BOOL, v)
	case len(av.BS) != 0:
		return d.decodeBinarySet(av.BS, v)
	case len(av.L) != 0:
		return d.decodeList(av.L, v)
	case len(av.M) != 0:
		return d.decodeMap(av.M, v)
	case av.N != nil:
		return d.decodeNumber(av.N, v)
	case len(av.NS) != 0:
		return d.decodeNumberSet(av.NS, v)
	case av.S != nil:
		return d.decodeString(av.S, v, fieldTag)
	case len(av.SS) != 0:
		return d.decodeStringSet(av.SS, v)
	}

	return nil
}

func (d *Decoder) decodeBinary(b []byte, v reflect.Value) error {
	if v.Kind() == reflect.Interface {
		buf := make([]byte, len(b))
		copy(buf, b)
		v.Set(reflect.ValueOf(buf))
		return nil
	}

	switch v.Interface().(type) {
	case []byte:
		if v.IsNil() || v.Cap() < len(b) {
			v.Set(reflect.MakeSlice(byteSliceType, len(b), len(b)))
		} else if v.Len() != len(b) {
			v.SetLen(len(b))
		}
		copy(v.Interface().([]byte), b)
	default:
		if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			reflect.Copy(v, reflect.ValueOf(b))
			break
		}
		return &UnmarshalTypeError{Value: "binary", Type: v.Type()}
	}

	return nil
}

func (d *Decoder) decodeBool(b *bool, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Bool, reflect.Interface:
		v.Set(reflect.ValueOf(*b))
	default:
		return &UnmarshalTypeError{Value: "bool", Type: v.Type()}
	}

	return nil
}

func (d *Decoder) decodeBinarySet(bs [][]byte, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Slice:
		// Make room for the slice elements if needed
		if v.IsNil() || v.Cap() < len(bs) {
			// What about if ignoring nil/empty values?
			v.Set(reflect.MakeSlice(v.Type(), 0, len(bs)))
		}
	case reflect.Array:
		// Limited to capacity of existing array.
	case reflect.Interface:
		set := make([][]byte, len(bs))
		for i, b := range bs {
			if err := d.decodeBinary(b, reflect.ValueOf(&set[i]).Elem()); err != nil {
				return err
			}
		}
		v.Set(reflect.ValueOf(set))
		return nil
	default:
		return &UnmarshalTypeError{Value: "binary set", Type: v.Type()}
	}

	for i := 0; i < v.Cap() && i < len(bs); i++ {
		v.SetLen(i + 1)
		u, elem := indirect(v.Index(i), false)
		if u != nil {
			return u.UnmarshalDynamoDBAttributeValue(&dynamodb.AttributeValue{BS: bs})
		}
		if err := d.decodeBinary(bs[i], elem); err != nil {
			return err
		}
	}

	return nil
}

func (d *Decoder) decodeNumber(n *string, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Interface:
		i, err := d.decodeNumberToInterface(n)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(i))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(*n, 10, 64)
		if err != nil || v.OverflowInt(i) {
			// TODO better error for overflow
			return err
		}
		v.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i, err := strconv.ParseUint(*n, 10, 64)
		if err != nil || v.OverflowUint(i) {
			// TODO better error for overflow
			return err
		}
		v.SetUint(i)
	case reflect.Float32, reflect.Float64:
		i, err := strconv.ParseFloat(*n, 64)
		if err != nil || v.OverflowFloat(i) {
			// TODO better error for overflow
			return err
		}
		v.SetFloat(i)
	default:
		return &UnmarshalTypeError{Value: "number", Type: v.Type()}
	}

	return nil
}

func (d *Decoder) decodeNumberToInterface(n *string) (interface{}, error) {
	// Number is tricky b/c we don't know which numeric type to use. Here we
	// simply try the different types from most to least restrictive.
	if i, err := strconv.ParseInt(*n, 10, 64); err == nil {
		return int(i), nil
	}
	if u, err := strconv.ParseUint(*n, 10, 64); err == nil {
		return uint(u), nil
	}
	return strconv.ParseFloat(*n, 64)
}

func (d *Decoder) decodeNumberSet(ns []*string, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Slice:
		// Make room for the slice elements if needed
		if v.IsNil() || v.Cap() < len(ns) {
			// What about if ignoring nil/empty values?
			v.Set(reflect.MakeSlice(v.Type(), 0, len(ns)))
		}
	case reflect.Array:
		// Limited to capacity of existing array.
	case reflect.Interface:
		// Need to use generic []interface because the numbers may be
		// multiple types. e.g int, uint, float
		set := make([]interface{}, len(ns))
		for i, n := range ns {
			if err := d.decodeNumber(n, reflect.ValueOf(&set[i]).Elem()); err != nil {
				return err
			}
		}
		v.Set(reflect.ValueOf(set))
		return nil
	default:
		return &UnmarshalTypeError{Value: "number set", Type: v.Type()}
	}

	for i := 0; i < v.Cap() && i < len(ns); i++ {
		v.SetLen(i + 1)
		u, elem := indirect(v.Index(i), false)
		if u != nil {
			return u.UnmarshalDynamoDBAttributeValue(&dynamodb.AttributeValue{NS: ns})
		}
		if err := d.decodeNumber(ns[i], elem); err != nil {
			return err
		}
	}

	return nil
}

func (d *Decoder) decodeList(avList []*dynamodb.AttributeValue, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Slice:
		// Make room for the slice elements if needed
		if v.IsNil() || v.Cap() < len(avList) {
			// What about if ignoring nil/empty values?
			v.Set(reflect.MakeSlice(v.Type(), 0, len(avList)))
		}
	case reflect.Array:
		// Limited to capacity of existing array.
	case reflect.Interface:
		s := make([]interface{}, len(avList))
		for i, av := range avList {
			if err := d.decode(av, reflect.ValueOf(&s[i]).Elem(), tag{}); err != nil {
				return err
			}
		}
		v.Set(reflect.ValueOf(s))
		return nil
	default:
		return &UnmarshalTypeError{Value: "list", Type: v.Type()}
	}

	// If v is not a slice, array
	for i := 0; i < v.Cap() && i < len(avList); i++ {
		v.SetLen(i + 1)
		if err := d.decode(avList[i], v.Index(i), tag{}); err != nil {
			return err
		}
	}

	return nil
}

func (d *Decoder) decodeMap(avMap map[string]*dynamodb.AttributeValue, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Map:
		t := v.Type()
		if t.Key().Kind() != reflect.String {
			return &UnmarshalTypeError{Value: "map string key", Type: t.Key()}
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
	case reflect.Struct:
	case reflect.Interface:
		v.Set(reflect.MakeMap(stringInterfaceMapType))
		v = v.Elem()
	default:
		return &UnmarshalTypeError{Value: "map", Type: v.Type()}
	}

	if v.Kind() == reflect.Map {
		for k, av := range avMap {
			key := reflect.ValueOf(k)
			elem := v.MapIndex(key)
			if !elem.IsValid() || !elem.CanAddr() {
				elem = reflect.New(v.Type().Elem()).Elem()
			}
			if err := d.decode(av, elem, tag{}); err != nil {
				return err
			}
			v.SetMapIndex(key, elem)
		}
	} else if v.Kind() == reflect.Struct {
		fields := unionStructFields(v.Type(), d.MarshalOptions)
		for k, av := range avMap {
			if f, ok := fieldByName(fields, k); ok {
				fv := v.FieldByIndex(f.Index)
				if err := d.decode(av, fv, f.tag); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (d *Decoder) decodeNull(v reflect.Value) error {
	if v.IsValid() && v.CanSet() {
		v.Set(reflect.Zero(v.Type()))
	}

	return nil
}

func (d *Decoder) decodeString(s *string, v reflect.Value, fieldTag tag) error {
	if fieldTag.AsString {
		return d.decodeNumber(s, v)
	}

	switch v.Kind() {
	case reflect.String, reflect.Interface:
		v.Set(reflect.ValueOf(*s))
	default:
		return &UnmarshalTypeError{Value: "string", Type: v.Type()}
	}

	return nil
}

func (d *Decoder) decodeStringSet(ss []*string, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Slice:
		// Make room for the slice elements if needed
		if v.IsNil() || v.Cap() < len(ss) {
			v.Set(reflect.MakeSlice(v.Type(), 0, len(ss)))
		}
	case reflect.Array:
		// Limited to capacity of existing array.
	case reflect.Interface:
		set := make([]string, len(ss))
		for i, s := range ss {
			if err := d.decodeString(s, reflect.ValueOf(&set[i]).Elem(), tag{}); err != nil {
				return err
			}
		}
		v.Set(reflect.ValueOf(set))
		return nil
	default:
		return &UnmarshalTypeError{Value: "string set", Type: v.Type()}
	}

	for i := 0; i < v.Cap() && i < len(ss); i++ {
		v.SetLen(i + 1)
		u, elem := indirect(v.Index(i), false)
		if u != nil {
			return u.UnmarshalDynamoDBAttributeValue(&dynamodb.AttributeValue{SS: ss})
		}
		if err := d.decodeString(ss[i], elem, tag{}); err != nil {
			return err
		}
	}

	return nil
}

// indirect will walk a value's interface or pointer value types. Returning
// the final value or the value a unmarshaler is defined on.
//
// Based on the enoding/json type reflect value type indirection in Go Stdlib
// https://golang.org/src/encoding/json/decode.go indirect func.
func indirect(v reflect.Value, decodingNull bool) (Unmarshaler, reflect.Value) {
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}
	for {
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodingNull || e.Elem().Kind() == reflect.Ptr) {
				v = e
				continue
			}
		}
		if v.Kind() != reflect.Ptr {
			break
		}
		if v.Elem().Kind() != reflect.Ptr && decodingNull && v.CanSet() {
			break
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		if v.Type().NumMethod() > 0 {
			if u, ok := v.Interface().(Unmarshaler); ok {
				return u, reflect.Value{}
			}
		}
		v = v.Elem()
	}

	return nil, v
}

type emptyOrigError struct{}

func (e emptyOrigError) OrigErr() error {
	return nil
}

// An UnmarshalTypeError is an error type representing a error
// unmarshaling the AttributeValue's element to a Go value type.
// Includes details about the AttributeValue type and Go value type.
type UnmarshalTypeError struct {
	emptyOrigError
	Value string
	Type  reflect.Type
}

// Error returns the string representation of the error.
// satisfying the error interface
func (e *UnmarshalTypeError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code(), e.Message())
}

// Code returns the code of the error, satisfying the awserr.Error
// interface.
func (e *UnmarshalTypeError) Code() string {
	return "UnmarshalTypeError"
}

// Message returns the detailed message of the error, satisfying
// the awserr.Error interface.
func (e *UnmarshalTypeError) Message() string {
	return "cannot unmarshal " + e.Value + " into Go value of type " + e.Type.String()
}

// An InvalidUnmarshalError is an error type representing an invalid type
// encountered while unmarshaling a AttributeValue to a Go value type.
type InvalidUnmarshalError struct {
	emptyOrigError
	Type reflect.Type
}

// Error returns the string representation of the error.
// satisfying the error interface
func (e *InvalidUnmarshalError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code(), e.Message())
}

// Code returns the code of the error, satisfying the awserr.Error
// interface.
func (e *InvalidUnmarshalError) Code() string {
	return "InvalidUnmarshalError"
}

// Message returns the detailed message of the error, satisfying
// the awserr.Error interface.
func (e *InvalidUnmarshalError) Message() string {
	if e.Type == nil {
		return "cannot unmarshal to nil value"
	}
	if e.Type.Kind() != reflect.Ptr {
		return "cannot unmasrhal to non-pointer value, got " + e.Type.String()
	}
	return "cannot unmarshal to nil value, " + e.Type.String()
}
