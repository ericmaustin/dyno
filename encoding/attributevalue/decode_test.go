package attributevalue

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntUnmarshaller(t *testing.T) {
	v := 0

	decoder := IntUnmarshaler(&v)

	av := &ddb.AttributeValueMemberN{Value: "2"}
	if err := decoder(av); err != nil {
		panic(err)
	}
	assert.Equal(t, 2, v)

	avMap := map[string]ddb.AttributeValue{
		"Foo": &ddb.AttributeValueMemberN{Value: "1"},
		"Bar": &ddb.AttributeValueMemberN{Value: "2"},
	}

	foo := 0
	bar := 0

	decoderMap := UnmarshallerMap(map[string]attributevalue.Unmarshaler{
		"Foo": IntUnmarshaler(&foo),
		"Bar": IntUnmarshaler(&bar),
	})

	if err := decoderMap.UnmarshalMap(avMap); err != nil {
		panic(err)
	}

	assert.Equal(t, 1, foo)
	assert.Equal(t, 2, bar)
}

func TestInt64Unmarshaller(t *testing.T) {
	v := int64(0)

	decoder := Int64Unmarshaler(&v)

	av := &ddb.AttributeValueMemberN{Value: "2"}
	if err := decoder(av); err != nil {
		panic(err)
	}
	assert.Equal(t, int64(2), v)

	avMap := map[string]ddb.AttributeValue{
		"Foo": &ddb.AttributeValueMemberN{Value: "1"},
		"Bar": &ddb.AttributeValueMemberN{Value: "2"},
	}

	foo := int64(0)
	bar := int64(0)

	decoderMap := UnmarshallerMap(map[string]attributevalue.Unmarshaler{
		"Foo": Int64Unmarshaler(&foo),
		"Bar": Int64Unmarshaler(&bar),
	})

	if err := decoderMap.UnmarshalMap(avMap); err != nil {
		panic(err)
	}

	assert.Equal(t, int64(1), foo)
	assert.Equal(t, int64(2), bar)
}

func TestStringUnmarshaller(t *testing.T) {
	v := ""

	decoder := StringUnmarshaler(&v)

	av := &ddb.AttributeValueMemberS{Value: "hello world"}
	if err := decoder(av); err != nil {
		panic(err)
	}
	assert.Equal(t, "hello world", v)

	avMap := map[string]ddb.AttributeValue{
		"Foo": &ddb.AttributeValueMemberS{Value: "hello"},
		"Bar": &ddb.AttributeValueMemberS{Value: "world"},
	}

	foo := ""
	bar := ""

	decoderMap := UnmarshallerMap(map[string]attributevalue.Unmarshaler{
		"Foo": StringUnmarshaler(&foo),
		"Bar": StringUnmarshaler(&bar),
	})

	if err := decoderMap.UnmarshalMap(avMap); err != nil {
		panic(err)
	}

	assert.Equal(t, "hello", foo)
	assert.Equal(t, "world", bar)
}

func TestFloat64Unmarshaller(t *testing.T) {
	v := float64(0)

	decoder := Float64Unmarshaler(&v)

	av := &ddb.AttributeValueMemberN{Value: "2"}
	if err := decoder(av); err != nil {
		panic(err)
	}
	assert.Equal(t, float64(2), v)

	avMap := map[string]ddb.AttributeValue{
		"Foo": &ddb.AttributeValueMemberN{Value: "1"},
		"Bar": &ddb.AttributeValueMemberN{Value: "2"},
	}

	foo := float64(0)
	bar := float64(0)

	decoderMap := UnmarshallerMap(map[string]attributevalue.Unmarshaler{
		"Foo": Float64Unmarshaler(&foo),
		"Bar": Float64Unmarshaler(&bar),
	})

	if err := decoderMap.UnmarshalMap(avMap); err != nil {
		panic(err)
	}

	assert.Equal(t, float64(1), foo)
	assert.Equal(t, float64(2), bar)
}
