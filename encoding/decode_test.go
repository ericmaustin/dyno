package encoding

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalRecord(t *testing.T) {

	s := getTestStruct()
	av, err := structToRecord(s)
	assert.NoError(t, err)

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

	err = UnmarshalItem(av, target)
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
	assert.Equal(t, s.SubStructJson.SubString, target.SubStructJson.SubString)
	assert.Equal(t, s.SubStructPtr.SubString, target.SubStructPtr.SubString)
	assert.Equal(t, s.SubStruct.SubString, target.SubStruct.SubString)
	assert.Equal(t, s.StringMapJson["stringMap1"], target.StringMapJson["stringMap1"])
	assert.Equal(t, s.StringMap["stringMap1"], target.StringMap["stringMap1"])
	assert.Equal(t, s.EmbeddedMap["nestedMap1"], target.EmbeddedMap["nestedMap1"])
}

func TestUnmarshalRecords(t *testing.T) {
	records, err := MarshalItems([]*testStruct{
		{
			String:           "A",
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

	fmt.Printf("%+v", records[0])

	target := make([]*testStruct, 0)

	err = UnmarshalItems(records, &target)
	assert.NoError(t, err)
}
