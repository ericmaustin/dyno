package encoding

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type testSubStruct struct {
	SubString string
	SubInt    int
}

type testStruct struct {
	String             string
	Int                int
	IntNamed           int     `dyno:"named_int"`
	StringOmitZero     string  `dyno:",omitzero"`
	IntOmitZero        int     `dyno:",omitzero"`
	StringPtrOmitNil   *string `dyno:",omitnil"`
	IntPtrOmitNil      *int    `dyno:",omitnil"`
	IntPtrOmitZero     *int    `dyno:",omitzero"`
	SubStruct          testSubStruct
	SubStructPtr       *testSubStruct `dyno:",*"`
	SubStructOmitEmpty *testSubStruct `dyno:",omitempty"`
	SubStructOmitNil   *testSubStruct `dyno:",omitnil"`
	SubStructAppend    *testSubStruct `dyno:",*,append=append"`
	SubStructPrepend   *testSubStruct `dyno:",*,prepend=prepend"`
	SubStructJson      *testSubStruct `dyno:",json"`
	SubStringSkip      *testSubStruct `dyno:",-"`
	StringMap          map[string]string
	StringMapJson      map[string]string `dyno:",json"`
	EmbeddedMap        map[string]string `dyno:",*,prepend=prepend_"`
}

func getTestStruct() *testStruct {
	_int := 0

	return &testStruct{
		String:           "test",
		Int:              1,
		IntNamed:         2,
		StringOmitZero:   "",
		IntOmitZero:      0,
		StringPtrOmitNil: nil,
		IntPtrOmitNil:    &_int, // should not be omitted
		IntPtrOmitZero:   &_int, // should be omitted
		SubStruct: testSubStruct{
			SubString: "testSubStruct struct string",
			SubInt:    100,
		},
		SubStructPtr: &testSubStruct{
			SubString: "testSubStruct struct ptr string",
			SubInt:    200,
		},
		SubStructOmitEmpty: &testSubStruct{},
		SubStructOmitNil:   nil,
		SubStructAppend: &testSubStruct{
			SubString: "testSubStruct string append",
			SubInt:    300,
		},
		SubStructPrepend: &testSubStruct{
			SubString: "testSubStruct string prepend",
			SubInt:    400,
		},
		SubStructJson: &testSubStruct{
			SubString: "testSubStruct string json",
			SubInt:    500,
		},
		SubStringSkip: &testSubStruct{
			SubString: "testSubStruct string skip",
			SubInt:    600,
		},
		StringMap: map[string]string{
			"stringMap1": "a",
			"stringMap2": "b",
		},
		StringMapJson: map[string]string{
			"stringMap1": "a",
			"stringMap2": "b",
		},
		EmbeddedMap: map[string]string{
			"embededMap1": "a",
			"embededMap2": "b",
		},
	}
}

func TestStructToAttributeValueMap(t *testing.T) {

	s := getTestStruct()
	av, err := MarshalItem(s)
	fmt.Printf("err = %v", err)
	assert.NoError(t, err)
	fmt.Printf("av = %+v\n", av)

	fmt.Printf("isZero: %v type: %v, kind: %v, isZero: %v, isNil: %v", s.IntPtrOmitNil,
		reflect.ValueOf(s.IntPtrOmitNil).Type(),
		reflect.ValueOf(s.IntPtrOmitNil).Kind(),
		reflect.ValueOf(s.IntPtrOmitNil).IsZero(),
		reflect.ValueOf(s.IntPtrOmitNil).IsNil())

	_, ok := av["SubStructOmitEmpty"]
	assert.False(t, ok)
	_, ok = av["SubStructOmitNil"]
	assert.False(t, ok)
	_, ok = av["SubStringSkip"]
	assert.False(t, ok)
	_, ok = av["StringOmitZero"]
	assert.False(t, ok)
	_, ok = av["StringPtrOmitNil"]
	assert.False(t, ok)
	_, ok = av["IntPtrOmitZero"]
	assert.False(t, ok)

	assert.Equal(t, *av["prependSubInt"].N, fmt.Sprintf("%d", s.SubStructPrepend.SubInt))
	assert.Equal(t, *av["SubIntappend"].N, fmt.Sprintf("%d", s.SubStructAppend.SubInt))
	assert.Equal(t, *av["IntPtrOmitNil"].N, fmt.Sprintf("%d", *s.IntPtrOmitNil))
	assert.Equal(t, *av["StringMap"].M["stringMap1"].S, s.StringMap["stringMap1"])
	assert.Equal(t, *av["prepend_embededMap1"].S, s.EmbeddedMap["embededMap1"])
}

func TestMapToAttributeValueMap(t *testing.T) {
	stringMap := map[string]string{
		"a": "value 1",
		"b": "value 2",
	}

	avMap := make(map[string]*dynamodb.AttributeValue)

	err := addMapToRecord(reflect.ValueOf(stringMap), avMap, "", "")
	assert.NoError(t, err)

	assert.Equal(t, *avMap["a"].S, stringMap["a"])

	intMap := map[string]int{
		"a": 1,
		"b": 2,
	}

	err = addMapToRecord(reflect.ValueOf(intMap), avMap, "", "")
	assert.NoError(t, err)

	assert.Equal(t, *avMap["a"].N, fmt.Sprintf("%d", intMap["a"]))
}

func TestAddToAttributeValueMap(t *testing.T) {
	mapA := map[string]string{
		"one": "1",
		"two": "2"
	}
	mapB := map[string]string{
		"three": "3",
		"four":  "4",
	}

	avMap := map[string]*dynamodb.AttributeValue{}

	err := AddToRecord(avMap, mapA, mapB)
	assert.NoError(t, err)

	assert.Equal(t, mapA["one"], *avMap["one"].S)
	assert.Equal(t, mapB["three"], *avMap["three"].S)
}

func TestFieldNames(t *testing.T) {
	s := getTestStruct()
	names, err := FieldNames(s)
	assert.NoError(t, err)
	fmt.Printf("%v\n", names)
}
