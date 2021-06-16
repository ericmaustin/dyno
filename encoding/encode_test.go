package encoding

import (
	"fmt"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type testSubStruct struct {
	SubString string
	SubInt    int
}

type testStruct struct {
	String             string
	Int                int
	Time               time.Time
	TimeZero           time.Time `dynamodbav:",omitempty"`
	IntNamed           int       `dynamodbav:"named_int"`
	StringOmitZero     string    `dynamodbav:",omitzero"`
	IntOmitZero        int       `dynamodbav:",omitzero"`
	StringPtrOmitNil   *string   `dynamodbav:",omitnil"`
	IntPtrOmitNil      *int      `dynamodbav:",omitnil"`
	IntPtrOmitZero     *int      `dynamodbav:",omitzero"`
	SubStruct          testSubStruct
	SubStructPtr       *testSubStruct `dynamodbav:"*"`
	SubStructOmitEmpty *testSubStruct `dynamodbav:",omitempty"`
	SubStructOmitNil   *testSubStruct `dynamodbav:",omitnil"`
	SubStructAppend    *testSubStruct `dynamodbav:"*,append=append"`
	SubStructPrepend   *testSubStruct `dynamodbav:"*,prepend=prepend"`
	SubStructJSON      *testSubStruct `dynamodbav:",json"`
	SubStringSkip      *testSubStruct `dynamodbav:"-"`
	StringMap          map[string]string
	StringMapJSON      map[string]string `dynamodbav:",json"`
	EmbeddedMap        map[string]string `dynamodbav:"*,prepend=prepend_"`
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
		Time:             time.Now(),
		TimeZero:         time.Time{},
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
		SubStructJSON: &testSubStruct{
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
		StringMapJSON: map[string]string{
			"stringMap1": "a",
			"stringMap2": "b",
		},
		EmbeddedMap: map[string]string{
			"embededMap1": "a",
			"embededMap2": "b",
		},
	}
}

func MustBeInt(i int, err error) int {
	if err != nil {
		panic(err)
	}
	return i
}

func TestStructToAttributeValueMap(t *testing.T) {

	s := getTestStruct()
	av, err := MarshalMap(s)
	fmt.Printf("err = %v", err)
	assert.NoError(t, err)
	fmt.Printf("av = %+v\n", av)

	fmt.Printf("isZero: %v type: %v, kind: %v, isZero: %v, isNil: %v", s.IntPtrOmitNil,
		reflect.ValueOf(s.IntPtrOmitNil).Type(),
		reflect.ValueOf(s.IntPtrOmitNil).Kind(),
		reflect.ValueOf(s.IntPtrOmitNil).IsZero(),
		reflect.ValueOf(s.IntPtrOmitNil).IsNil())

	assert.Equal(t, av["prependSubInt"].(*ddb.AttributeValueMemberN).Value, fmt.Sprintf("%d", s.SubStructPrepend.SubInt))
	assert.Equal(t, av["SubIntappend"].(*ddb.AttributeValueMemberN).Value, fmt.Sprintf("%d", s.SubStructAppend.SubInt))
	assert.Equal(t, av["IntPtrOmitNil"].(*ddb.AttributeValueMemberN).Value, fmt.Sprintf("%d", *s.IntPtrOmitNil))
	// todo: wtf this is so long, is there a way to see if we can shorten a reference like this?
	assert.Equal(t, av["StringMap"].(*ddb.AttributeValueMemberM).Value["stringMap1"].(*ddb.AttributeValueMemberS).Value, s.StringMap["stringMap1"])
	assert.Equal(t, av["prepend_embededMap1"].(*ddb.AttributeValueMemberS).Value, s.EmbeddedMap["embededMap1"])
}

func TestMapToAttributeValueMap(t *testing.T) {
	stringMap := map[string]string{
		"a": "value 1",
		"b": "value 2",
	}

	avMap := make(map[string]ddb.AttributeValue)

	err := addMapToRecord(reflect.ValueOf(stringMap), avMap, "", "")
	assert.NoError(t, err)

	assert.Equal(t, avMap["a"].(*ddb.AttributeValueMemberS).Value, stringMap["a"])

	intMap := map[string]int{
		"a": 1,
		"b": 2,
	}

	err = addMapToRecord(reflect.ValueOf(intMap), avMap, "", "")
	assert.NoError(t, err)

	assert.Equal(t, avMap["a"].(*ddb.AttributeValueMemberN).Value, fmt.Sprintf("%d", intMap["a"]))
}

func TestFieldNames(t *testing.T) {
	s := getTestStruct()
	names, err := FieldNames(s)
	assert.NoError(t, err)
	fmt.Printf("%v\n", names)
}

func TestFieldNamesFromSlice(t *testing.T) {
	names, err := FieldNames([]string{
		"test1",
		"test2",
	})
	assert.NoError(t, err)
	fmt.Printf("%v\n", names)
}

func TestNameBuildersFromSingleString(t *testing.T) {
	names := NameBuilders("test1")
	fmt.Printf("%+v\n", names)
}

func TestNameBuildersFromStringSlice(t *testing.T) {
	names := NameBuilders([]string{
		"test1",
		"test2",
	})
	fmt.Printf("%+v\n", names)
}

type MarshalStruct struct {
	Foo *string
}

func (m *MarshalStruct) MarshalItem() (map[string]ddb.AttributeValue, error) {
	return map[string]ddb.AttributeValue{
		"Foo": &ddb.AttributeValueMemberS{Value: *m.Foo},
	}, nil
}

func TestItemMarshaller(t *testing.T) {
	v := "Bar"
	newFoo := &MarshalStruct{Foo: &v}
	av, err := MarshalMap(newFoo)
	assert.NoError(t, err)
	assert.Equal(t, av["Foo"].(*ddb.AttributeValueMemberS).Value, "Bar")
}
