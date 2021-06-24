package encoding

import (
	"fmt"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalRecord(t *testing.T) {

	s := getTestStruct()
	av, err := structToRecord(s)
	if err != nil {
		panic(err)
	}

	target := &testStruct{
		StringMap: map[string]string{
			"stringMap1": "",
			"stringMap2": "",
		},
		EmbeddedMap: map[string]string{
			"nestedMap1": "",
			"nestedMap2": "",
		},
	}

	err = UnmarshalMap(av, target)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, s.String, target.String)
	assert.Equal(t, s.Int, target.Int)
	fmt.Printf("target = %+v\n", target)
	fmt.Printf("nestedMap1 = %+v => %+v \n", s.EmbeddedMap["nestedMap1"], target.EmbeddedMap["nestedMap1"])
	fmt.Printf("prepend:\n%+v\n", target.SubStructPrepend)
	fmt.Printf("append:\n%+v\n", target.SubStructAppend)
	assert.Equal(t, s.SubStructPrepend.SubString, target.SubStructPrepend.SubString)
	assert.Equal(t, s.SubStructAppend.SubInt, target.SubStructAppend.SubInt)
	assert.Equal(t, s.SubStructJSON.SubString, target.SubStructJSON.SubString)
	assert.Equal(t, s.SubStructPtr.SubString, target.SubStructPtr.SubString)
	assert.Equal(t, s.SubStruct.SubString, target.SubStruct.SubString)
	assert.Equal(t, s.StringMapJSON["stringMap1"], target.StringMapJSON["stringMap1"])
	assert.Equal(t, s.StringMap["stringMap1"], target.StringMap["stringMap1"])
	assert.Equal(t, s.EmbeddedMap["nestedMap1"], target.EmbeddedMap["nestedMap1"])
}

func TestUnmarshalRecords(t *testing.T) {
	tm := time.Now()

	records, err := MarshalMaps([]*testStruct{
		{
			String:           "A",
			Time:             tm,
			Int:              1,
			IntNamed:         10,
			StringPtrOmitNil: nil,
			IntPtrOmitNil:    nil,
			IntPtrOmitZero:   nil,
			SubStruct: testSubStruct{
				SubString: "subA",
				SubInt:    100,
			},
		},
		{
			String:           "B",
			Int:              2,
			Time:             tm,
			IntNamed:         20,
			StringOmitZero:   "",
			IntOmitZero:      0,
			StringPtrOmitNil: nil,
			IntPtrOmitNil:    nil,
			IntPtrOmitZero:   nil,
			SubStruct: testSubStruct{
				SubString: "subB",
				SubInt:    200,
			},
		},
	})

	assert.NoError(t, err)
	assert.Len(t, records, 2)

	fmt.Printf("%+v\n", records[0])

	target := make([]*testStruct, 0)

	err = UnmarshalMaps(records, &target)
	assert.NoError(t, err)
	fmt.Printf("%s", target[0].Time)
	assert.True(t, tm.Equal(target[0].Time))
}

type UnMarshalStruct struct {
	Foo *string
}

func (u *UnMarshalStruct) UnmarshalAttributeValueMap(av map[string]ddb.AttributeValue) error {
	u.Foo = new(string)
	if v, ok := av["Foo"].(*ddb.AttributeValueMemberS); ok {
		*u.Foo = v.Value
		return nil
	}
	return fmt.Errorf("not a string value")
}

func TestItemUnmarshaller(t *testing.T) {

	av := map[string]ddb.AttributeValue{
		"Foo": &ddb.AttributeValueMemberS{Value: "Bar"},
	}

	newFoo := new(UnMarshalStruct)
	err := UnmarshalMap(av, newFoo)
	assert.NoError(t, err)
	assert.Equal(t, *newFoo.Foo, "Bar")
}
