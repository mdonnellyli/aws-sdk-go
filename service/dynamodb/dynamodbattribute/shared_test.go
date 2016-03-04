package dynamodbattribute

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

type testBinarySetStruct struct {
	Binarys [][]byte `dynamodbav:",binaryset"`
}
type testNumberSetStruct struct {
	Numbers []int `dynamodbav:",numberset"`
}
type testStringSetStruct struct {
	Strings []string `dynamodbav:",stringset"`
}

type testIntAsStringStruct struct {
	Value int `dynamodbav:",string"`
}

type testOmitEmptyStruct struct {
	Value  string  `dynamodbav:",omitempty"`
	Value2 *string `dynamodbav:",omitempty"`
	Value3 int
}

var sharedTestCases = []struct {
	in               *dynamodb.AttributeValue
	actual, expected interface{}
	err              error
}{
	{ // Binary slice
		in:       &dynamodb.AttributeValue{B: []byte{48, 49}},
		actual:   &[]byte{},
		expected: []byte{48, 49},
	},
	{ // Binary slice
		in:       &dynamodb.AttributeValue{B: []byte{48, 49}},
		actual:   &[]byte{},
		expected: []byte{48, 49},
	},
	{ // Binary slice oversized
		in: &dynamodb.AttributeValue{B: []byte{48, 49}},
		actual: func() *[]byte {
			v := make([]byte, 0, 10)
			return &v
		}(),
		expected: []byte{48, 49},
	},
	{ // Binary slice pointer
		in: &dynamodb.AttributeValue{B: []byte{48, 49}},
		actual: func() **[]byte {
			v := make([]byte, 0, 10)
			v2 := &v
			return &v2
		}(),
		expected: []byte{48, 49},
	},
	{ // Bool
		in:       &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		actual:   new(bool),
		expected: true,
	},
	{ // List
		in: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{N: aws.String("123")},
		}},
		actual:   &[]int{},
		expected: []int{123},
	},
	{ // Map, interface
		in: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"abc": {N: aws.String("123")},
		}},
		actual:   &map[string]int{},
		expected: map[string]int{"abc": 123},
	},
	{ // Map, struct
		in: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"Abc": {N: aws.String("123")},
		}},
		actual:   &struct{ Abc int }{},
		expected: struct{ Abc int }{Abc: 123},
	},
	{ // Map, struct
		in: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"abc": {N: aws.String("123")},
		}},
		actual: &struct {
			Abc int `json:"abc" dynamodbav:"abc"`
		}{},
		expected: struct {
			Abc int `json:"abc" dynamodbav:"abc"`
		}{Abc: 123},
	},
	{ // Number, int
		in:       &dynamodb.AttributeValue{N: aws.String("123")},
		actual:   new(int),
		expected: 123,
	},
	{ // Number, Float
		in:       &dynamodb.AttributeValue{N: aws.String("123.1")},
		actual:   new(float64),
		expected: float64(123.1),
	},
	{ // Null
		in:       &dynamodb.AttributeValue{NULL: aws.Bool(true)},
		actual:   new(string),
		expected: "",
	},
	{ // Null ptr
		in:       &dynamodb.AttributeValue{NULL: aws.Bool(true)},
		actual:   new(*string),
		expected: nil,
	},
	{ // String
		in:       &dynamodb.AttributeValue{S: aws.String("abc")},
		actual:   new(string),
		expected: "abc",
	},
	{ // Binary Set
		in: &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"Binarys": &dynamodb.AttributeValue{BS: [][]byte{[]byte{48, 49}, []byte{50, 51}}},
			},
		},
		actual:   &testBinarySetStruct{},
		expected: testBinarySetStruct{Binarys: [][]byte{[]byte{48, 49}, []byte{50, 51}}},
	},
	{ // Number Set
		in: &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"Numbers": &dynamodb.AttributeValue{NS: []*string{aws.String("123"), aws.String("321")}},
			},
		},
		actual:   &testNumberSetStruct{},
		expected: testNumberSetStruct{Numbers: []int{123, 321}},
	},
	{ // String Set
		in: &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"Strings": &dynamodb.AttributeValue{SS: []*string{aws.String("abc"), aws.String("efg")}},
			},
		},
		actual:   &testStringSetStruct{},
		expected: testStringSetStruct{Strings: []string{"abc", "efg"}},
	},
	{ // Int value as string
		in: &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"Value": &dynamodb.AttributeValue{S: aws.String("123")},
			},
		},
		actual:   &testIntAsStringStruct{},
		expected: testIntAsStringStruct{Value: 123},
	},
	{ // Omitempty
		in: &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"Value3": &dynamodb.AttributeValue{N: aws.String("0")},
			},
		},
		actual:   &testOmitEmptyStruct{},
		expected: testOmitEmptyStruct{Value: "", Value2: nil, Value3: 0},
	},
}

var sharedListTestCases = []struct {
	in               []*dynamodb.AttributeValue
	actual, expected interface{}
	err              error
}{
	{
		in: []*dynamodb.AttributeValue{
			{B: []byte{48, 49}},
			{BOOL: aws.Bool(true)},
			{N: aws.String("123")},
			{S: aws.String("123")},
		},
		actual: func() *[]interface{} {
			v := []interface{}{}
			return &v
		}(),
		expected: []interface{}{[]byte{48, 49}, true, 123, "123"},
	},
	{
		in: []*dynamodb.AttributeValue{
			{N: aws.String("1")},
			{N: aws.String("2")},
			{N: aws.String("3")},
		},
		actual:   &[]interface{}{},
		expected: []interface{}{1, 2, 3},
	},
}

var sharedMapTestCases = []struct {
	in               map[string]*dynamodb.AttributeValue
	actual, expected interface{}
	err              error
}{
	{
		in: map[string]*dynamodb.AttributeValue{
			"B":    {B: []byte{48, 49}},
			"BOOL": {BOOL: aws.Bool(true)},
			"N":    {N: aws.String("123")},
			"S":    {S: aws.String("123")},
		},
		actual: &map[string]interface{}{},
		expected: map[string]interface{}{
			"B": []byte{48, 49}, "BOOL": true,
			"N": 123, "S": "123",
		},
	},
}

func assertConvertTest(t *testing.T, i int, actual, expected interface{}, err, expectedErr error) {
	i++
	if expectedErr != nil {
		if err != nil {
			assert.Equal(t, expectedErr, err, "case %d", i)
		} else {
			assert.Fail(t, "", "case %d, expected error, %v", i)
		}
	} else if err != nil {
		assert.Fail(t, "", "case %d, expect no error, got %v", i, err)
	} else {
		assert.Equal(t, ptrToValue(expected), ptrToValue(actual), "case %d", i)
	}
}

func ptrToValue(in interface{}) interface{} {
	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() {
		return nil
	}
	if v.Kind() == reflect.Ptr {
		return ptrToValue(v.Interface())
	}
	return v.Interface()
}
