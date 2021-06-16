package attributevalue

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"
)

// MarshalFunc represents a func that Marshals an AttributeValue
type MarshalFunc func() (ddb.AttributeValue, error)

func (e MarshalFunc) MarshalDynamoDBAttributeValue() (ddb.AttributeValue, error) {
	return e()
}

// MarshalMap represents a map of attributevalue.Marshal values
type MarshalMap map[string]attributevalue.Marshaler

// MarshalMap runs all the attributevalue.Marshaler in the MarshalMap and generates a map of AttributeValues
func (em MarshalMap) MarshalMap() (map[string]ddb.AttributeValue, error) {
	var err error
	m := make(map[string]ddb.AttributeValue, len(em))
	for k, v := range em {
		if m[k], err = v.MarshalDynamoDBAttributeValue(); err != nil {
			return nil, err
		}

	}
	return m, nil
}

// MarshalInt marshals an AttributeValue into the given value
func MarshalInt(v int) ddb.AttributeValue {
	return &ddb.AttributeValueMemberN{Value: strconv.Itoa(v)}
}

// IntMarshaler returns a MarshalFunc func that will generate an AttributeValue
func IntMarshaler(v int) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalInt(v), nil
	}
}

// MarshalInt64 marshals an AttributeValue into the given value
func MarshalInt64(v int64) ddb.AttributeValue {
	return &ddb.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

// Int64Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Int64Marshaler(v int64) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalInt64(v), nil
	}
}

// MarshalFloat32 marshals an AttributeValue into the given value
func MarshalFloat32(v float32) ddb.AttributeValue {
	return &ddb.AttributeValueMemberN{Value: strconv.FormatFloat(float64(v), 'g', -1, 32)}
}

// Float32Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Float32Marshaler(v float32) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalFloat32(v), nil
	}
}

// MarshalFloat64 marshals an AttributeValue into the given value
func MarshalFloat64(v float64) ddb.AttributeValue {
	return &ddb.AttributeValueMemberN{Value: strconv.FormatFloat(v, 'g', -1, 64)}
}

// Float64Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Float64Marshaler(v float64) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalFloat64(v), nil
	}
}

// MarshalBool marshals an AttributeValue into the given value
func MarshalBool(v bool) ddb.AttributeValue {
	return &ddb.AttributeValueMemberBOOL{Value: v}
}

// BoolMarshaler returns a MarshalFunc func that will generate an AttributeValue
func BoolMarshaler(v bool) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalBool(v), nil
	}
}

// MarshalString marshals an AttributeValue into the given value
func MarshalString(v string) ddb.AttributeValue {
	return &ddb.AttributeValueMemberS{Value: v}
}

// StringMarshaler returns a MarshalFunc func that will generate an AttributeValue
func StringMarshaler(v string) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalString(v), nil
	}
}

// MarshalBytes marshals an AttributeValue into the given value
func MarshalBytes(v []byte) ddb.AttributeValue {
	return &ddb.AttributeValueMemberB{Value: v}
}

// BytesMarshaler returns a MarshalFunc func that will generate an AttributeValue
func BytesMarshaler(v []byte) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalBytes(v), nil
	}
}

// MarshalJSON marshals an AttributeValue into the given value
func MarshalJSON(v interface{}) (ddb.AttributeValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &ddb.AttributeValueMemberS{Value: string(b)}, nil
}

// JSONMarshaler returns a MarshalFunc func that will generate an AttributeValue
func JSONMarshaler(v interface{}) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalJSON(v)
	}
}
