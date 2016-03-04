package dynamodbattribute

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// A Marshaler is an interface to provide custom marshalling of Go value types
// to AttributeValues. Use this to provide custom logic determining how a
// Go Value type should be marshaled.
type Marshaler interface {
	MarshalDynamoDBAttributeValue(*dynamodb.AttributeValue) error
}

// Marshal PLACE HOLDER
func Marshal(in interface{}) (*dynamodb.AttributeValue, error) {
	return NewEncoder().Encode(in)
}

// MarshalMap is an alias for Marshal func which marshals Go value
// type to a map of AttributeValues.
func MarshalMap(in interface{}) (map[string]*dynamodb.AttributeValue, error) {
	av, err := NewEncoder().Encode(in)
	if err != nil || av == nil || av.M == nil {
		return map[string]*dynamodb.AttributeValue{}, err
	}

	return av.M, nil
}

// MarshalList is an alias for Marshal func which marshals Go value
// type to a slice of AttributeValues.
func MarshalList(in interface{}) ([]*dynamodb.AttributeValue, error) {
	av, err := NewEncoder().Encode(in)
	if err != nil || av == nil || av.L == nil {
		return []*dynamodb.AttributeValue{}, err
	}

	return av.L, nil
}

// A MarshalOptions is a collection of options shared between marshaling
// and unmarshaling
type MarshalOptions struct {
	// States that the encoding/json struct tags should be supported.
	// if a `dynamodbav` struct tag is also provided the encoding/json
	// tag will be ignored.
	//
	// Enabled by default.
	SupportJSONTags bool
}

// An Encoder provides marshaling Go value types to AttributeValues.
type Encoder struct {
	MarshalOptions

	// Empty strings, "", will be marked as NULL AttributeValue types.
	// Empty strings are not valid values for DynamoDB. Will not apply
	// to lists, sets, or maps. Use the struct tag `omitemptyelem`
	// to skip empty (zero) values in lists, sets and maps.
	//
	// Enabled by default.
	NullEmptyString bool
}

// NewEncoder creates a new Encoder with default configurtion. Use
// the `opts` functional options to override the default configuration.
func NewEncoder(opts ...func(*Encoder)) *Encoder {
	e := &Encoder{
		MarshalOptions: MarshalOptions{
			SupportJSONTags: true,
		},
		NullEmptyString: true,
	}
	for _, o := range opts {
		o(e)
	}

	return e
}

// Encode will marshal a Go value type to an AttributeValue. Returning
// the AttributeValue constructed or error.
func (e *Encoder) Encode(in interface{}) (*dynamodb.AttributeValue, error) {
	av := &dynamodb.AttributeValue{}
	if err := e.encode(av, reflect.ValueOf(in), tag{}); err != nil {
		return nil, err
	}

	return av, nil
}

func (e *Encoder) encode(av *dynamodb.AttributeValue, v reflect.Value, fieldTag tag) error {
	// Handle both pointers and interface conversion into types
	v = valueElem(v)

	if v.Kind() != reflect.Invalid {
		if used, err := tryMarshaler(av, v); used {
			return err
		}
	}

	if fieldTag.OmitEmpty && emptyValue(v) {
		encodeNull(av)
		return nil
	}

	switch v.Kind() {
	case reflect.Invalid:
		encodeNull(av)
	case reflect.Struct:
		return e.encodeStruct(av, v)
	case reflect.Map:
		return e.encodeMap(av, v, fieldTag)
	case reflect.Slice, reflect.Array:
		return e.encodeSlice(av, v, fieldTag)
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		// do nothing for unsupported types
	default:
		return e.encodeScalar(av, v, fieldTag)
	}

	return nil
}

func (e *Encoder) encodeStruct(av *dynamodb.AttributeValue, v reflect.Value) error {
	av.M = map[string]*dynamodb.AttributeValue{}
	fields := unionStructFields(v.Type(), e.MarshalOptions)
	for _, f := range fields {
		if f.Name == "" {
			return &InvalidMarshalError{msg: "map key cannot be empty"}
		}

		fv := v.FieldByIndex(f.Index)
		elem := &dynamodb.AttributeValue{}
		err := e.encode(elem, fv, f.tag)
		skip, err := keepOrOmitEmpty(f.OmitEmpty, elem, err)
		if err != nil {
			return err
		} else if skip {
			continue
		}

		av.M[f.Name] = elem
	}
	if len(av.M) == 0 {
		encodeNull(av)
	}

	return nil
}

func (e *Encoder) encodeMap(av *dynamodb.AttributeValue, v reflect.Value, fieldTag tag) error {
	av.M = map[string]*dynamodb.AttributeValue{}
	for _, key := range v.MapKeys() {
		keyName := fmt.Sprint(key.Interface())
		if keyName == "" {
			return &InvalidMarshalError{msg: "map key cannot be empty"}
		}

		elemVal := v.MapIndex(key)
		elem := &dynamodb.AttributeValue{}
		err := e.encode(elem, elemVal, tag{})
		skip, err := keepOrOmitEmpty(fieldTag.OmitEmptyElem, elem, err)
		if err != nil {
			return err
		} else if skip {
			continue
		}

		av.M[keyName] = elem
	}
	if len(av.M) == 0 {
		encodeNull(av)
	}

	return nil
}

func (e *Encoder) encodeSlice(av *dynamodb.AttributeValue, v reflect.Value, fieldTag tag) error {
	switch typed := v.Interface().(type) {
	case []byte:
		if len(typed) == 0 {
			encodeNull(av)
			return nil
		}
		av.B = append([]byte{}, typed...)
	default:
		var elemFn func(dynamodb.AttributeValue) error

		if fieldTag.AsBinSet || v.Type() == byteSliceSlicetype { // Binary Set
			av.BS = make([][]byte, 0, v.Len())
			elemFn = func(elem dynamodb.AttributeValue) error {
				if elem.B == nil {
					return &InvalidMarshalError{msg: "binary set must only contain non-nil byte slices"}
				}
				av.BS = append(av.BS, elem.B)
				return nil
			}
		} else if fieldTag.AsNumSet { // Number Set
			av.NS = make([]*string, 0, v.Len())
			elemFn = func(elem dynamodb.AttributeValue) error {
				if elem.N == nil {
					return &InvalidMarshalError{msg: "number set must only contain non-nil string numbers"}
				}
				av.NS = append(av.NS, elem.N)
				return nil
			}
		} else if fieldTag.AsStrSet { // String Set
			av.SS = make([]*string, 0, v.Len())
			elemFn = func(elem dynamodb.AttributeValue) error {
				if elem.S == nil {
					return &InvalidMarshalError{msg: "string set must only contain non-nil strings"}
				}
				av.SS = append(av.SS, elem.S)
				return nil
			}
		} else { // List
			av.L = make([]*dynamodb.AttributeValue, 0, v.Len())
			elemFn = func(elem dynamodb.AttributeValue) error {
				av.L = append(av.L, &elem)
				return nil
			}
		}

		if n, err := e.encodeList(v, fieldTag, elemFn); err != nil {
			return err
		} else if n == 0 {
			encodeNull(av)
		}
	}

	return nil
}

func (e *Encoder) encodeList(v reflect.Value, fieldTag tag, elemFn func(dynamodb.AttributeValue) error) (int, error) {
	count := 0
	for i := 0; i < v.Len(); i++ {
		elem := dynamodb.AttributeValue{}
		err := e.encode(&elem, v.Index(i), tag{OmitEmpty: fieldTag.OmitEmptyElem})
		skip, err := keepOrOmitEmpty(fieldTag.OmitEmptyElem, &elem, err)
		if err != nil {
			return 0, err
		} else if skip {
			continue
		}

		if err := elemFn(elem); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

func (e *Encoder) encodeScalar(av *dynamodb.AttributeValue, v reflect.Value, fieldTag tag) error {
	switch typed := v.Interface().(type) {
	case bool:
		av.BOOL = new(bool)
		*av.BOOL = typed
	case string:
		if err := e.encodeString(av, v); err != nil {
			return err
		}
	default:
		// Fallback to encoding numbers, will return invalid type if not supported
		if err := e.encodeNumber(av, v); err != nil {
			return err
		}
		if fieldTag.AsString && av.NULL == nil && av.N != nil {
			av.S = av.N
			av.N = nil
		}
	}

	return nil
}

func (e *Encoder) encodeNumber(av *dynamodb.AttributeValue, v reflect.Value) error {
	if used, err := tryMarshaler(av, v); used {
		return err
	}

	var out string
	switch typed := v.Interface().(type) {
	case int:
		out = encodeInt(int64(typed))
	case int8:
		out = encodeInt(int64(typed))
	case int16:
		out = encodeInt(int64(typed))
	case int32:
		out = encodeInt(int64(typed))
	case int64:
		out = encodeInt(typed)
	case uint:
		out = encodeUint(uint64(typed))
	case uint8:
		out = encodeUint(uint64(typed))
	case uint16:
		out = encodeUint(uint64(typed))
	case uint32:
		out = encodeUint(uint64(typed))
	case uint64:
		out = encodeUint(typed)
	case float32:
		out = encodeFloat(float64(typed))
	case float64:
		out = encodeFloat(typed)
	default:
		return &unsupportedMarshalTypeError{Type: v.Type()}
	}

	av.N = &out

	return nil
}

func (e *Encoder) encodeString(av *dynamodb.AttributeValue, v reflect.Value) error {
	if used, err := tryMarshaler(av, v); used {
		return err
	}

	switch typed := v.Interface().(type) {
	case string:
		if len(typed) == 0 && e.NullEmptyString {
			encodeNull(av)
		} else {
			av.S = &typed
		}
	default:
		return &unsupportedMarshalTypeError{Type: v.Type()}
	}

	return nil
}

func encodeInt(i int64) string {
	return strconv.FormatInt(i, 10)
}
func encodeUint(u uint64) string {
	return strconv.FormatUint(u, 10)
}
func encodeFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
func encodeNull(av *dynamodb.AttributeValue) {
	t := true
	*av = dynamodb.AttributeValue{NULL: &t}
}

func valueElem(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Interface, reflect.Ptr:
		for v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
	}

	return v
}

func emptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func tryMarshaler(av *dynamodb.AttributeValue, v reflect.Value) (bool, error) {
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}

	if v.Type().NumMethod() == 0 {
		return false, nil
	}

	if m, ok := v.Interface().(Marshaler); ok {
		return true, m.MarshalDynamoDBAttributeValue(av)
	}

	return false, nil
}

func keepOrOmitEmpty(omitEmpty bool, av *dynamodb.AttributeValue, err error) (bool, error) {
	if err != nil {
		if _, ok := err.(*unsupportedMarshalTypeError); ok {
			return true, nil
		}
		return false, err
	}

	if av.NULL != nil && omitEmpty {
		return true, nil
	}

	return false, nil
}

// An InvalidMarshalError is an error type representing an error
// occurring when marshaling a Go value type to an AttributeValue.
type InvalidMarshalError struct {
	emptyOrigError
	msg string
}

// Error returns the string representation of the error.
// satisfying the error interface
func (e *InvalidMarshalError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code(), e.Message())
}

// Code returns the code of the error, satisfying the awserr.Error
// interface.
func (e *InvalidMarshalError) Code() string {
	return "InvalidMarshalError"
}

// Message returns the detailed message of the error, satisfying
// the awserr.Error interface.
func (e *InvalidMarshalError) Message() string {
	return e.msg
}

// An unsupportedMarshalTypeError represents a Go value type
// which cannot be marshaled into an AttributeValue and should
// be skipped by the marshaler.
type unsupportedMarshalTypeError struct {
	emptyOrigError
	Type reflect.Type
}

// Error returns the string representation of the error.
// satisfying the error interface
func (e *unsupportedMarshalTypeError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code(), e.Message())
}

// Code returns the code of the error, satisfying the awserr.Error
// interface.
func (e *unsupportedMarshalTypeError) Code() string {
	return "unsupportedMarshalTypeError"
}

// Message returns the detailed message of the error, satisfying
// the awserr.Error interface.
func (e *unsupportedMarshalTypeError) Message() string {
	return "Go value type " + e.Type.String() + " is not supported"
}
