package encoding

import (
	"encoding/json"
	"errors"
	ddbav "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"
	"time"
)

// UnmarshalerFunc represents a func that unmarshals an AttributeValue
type UnmarshalerFunc func(ddb.AttributeValue) error

// UnmarshalDynamoDBAttributeValue implements attributevalue.Unmarshal values
func (d UnmarshalerFunc) UnmarshalDynamoDBAttributeValue(v ddb.AttributeValue) error {
	return d(v)
}

// ValueUnmarshalerMap represents a map of attributevalue.Unmarshaler
type ValueUnmarshalerMap map[string]ddbav.Unmarshaler

// UnmarshalAttributeValueMap runs all the attributevalue.Unmarshalers in the UnmarshalerFunc map
func (dm ValueUnmarshalerMap) UnmarshalAttributeValueMap(m map[string]ddb.AttributeValue) error {
	for key, decoder := range dm {
		if av, ok := m[key]; ok {
			if err := decoder.UnmarshalDynamoDBAttributeValue(av); err != nil {
				return err
			}
		}
	}

	return nil
}

// UnmarshalInt unmarshals an AttributeValue into the given value
func UnmarshalInt(av ddb.AttributeValue, v *int) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)
	if !ok {
		return errors.New("cannot decode AttributeValue to int")
	}

	i, err := strconv.Atoi(nv.Value)

	if err != nil {
		return err
	}

	*v = i

	return nil
}

// IntUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func IntUnmarshaler(v *int) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalInt(av, v)
	}
}

// UnmarshalInt64 unmarshals an AttributeValue into the given value
func UnmarshalInt64(av ddb.AttributeValue, v *int64) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)

	if !ok {
		return errors.New("cannot decode AttributeValue to int64")
	}

	i, err := strconv.ParseInt(nv.Value, 10, 64)

	if err != nil {
		return err
	}

	*v = i

	return nil
}

// Int64Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Int64Unmarshaler(v *int64) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalInt64(av, v)
	}
}

// UnmarshalFloat32 unmarshals an AttributeValue into the given value
func UnmarshalFloat32(av ddb.AttributeValue, v *float32) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)

	if !ok {
		return errors.New("cannot decode AttributeValue to int64")
	}

	i, err := strconv.ParseFloat(nv.Value, 32)

	if err != nil {
		return err
	}

	*v = float32(i)

	return nil
}

// Float32Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Float32Unmarshaler(v *float32) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalFloat32(av, v)
	}
}

// UnmarshalFloat64 unmarshals an AttributeValue into the given value
func UnmarshalFloat64(av ddb.AttributeValue, v *float64) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)

	if !ok {
		return errors.New("cannot decode AttributeValue to int64")
	}

	i, err := strconv.ParseFloat(nv.Value, 64)

	if err != nil {
		return err
	}

	*v = i

	return nil
}

// Float64Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Float64Unmarshaler(v *float64) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalFloat64(av, v)
	}
}

// UnmarshalBool unmarshals an AttributeValue into the given value
func UnmarshalBool(av ddb.AttributeValue, v *bool) error {
	bv, ok := av.(*ddb.AttributeValueMemberBOOL)
	if !ok {
		return errors.New("cannot decode AttributeValue to bool")
	}

	*v = bv.Value

	return nil
}

// BoolUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func BoolUnmarshaler(v *bool) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalBool(av, v)
	}
}

// UnmarshalString unmarshals an AttributeValue into the given value
func UnmarshalString(av ddb.AttributeValue, v *string) error {
	nv, ok := av.(*ddb.AttributeValueMemberS)

	if !ok {
		return errors.New("cannot decode AttributeValue to string")
	}

	*v = nv.Value

	return nil
}

// StringUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func StringUnmarshaler(v *string) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalString(av, v)
	}
}

// UnmarshalBytes unmarshals an AttributeValue into the given value
func UnmarshalBytes(av ddb.AttributeValue, v *[]byte) error {
	bv, ok := av.(*ddb.AttributeValueMemberB)
	if !ok {
		return errors.New("cannot decode AttributeValue to bytes")
	}

	*v = bv.Value

	return nil
}

// BytesUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func BytesUnmarshaler(v *[]byte) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalBytes(av, v)
	}
}

// UnmarshalJSON unmarshals an AttributeValue into the given value
func UnmarshalJSON(av ddb.AttributeValue, v interface{}) error {
	var jb []byte

	if bv, ok := av.(*ddb.AttributeValueMemberB); ok {
		jb = bv.Value
	} else if sv, ok := av.(*ddb.AttributeValueMemberS); ok {
		jb = []byte(sv.Value)
	} else {
		return errors.New("cannot decode AttributeValue to JSON")
	}

	return json.Unmarshal(jb, v)
}

// JSONUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func JSONUnmarshaler(v interface{}) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalJSON(av, v)
	}
}

// UnmarshalUnixNano unmarshals an AttributeValue into the given value
func UnmarshalUnixNano(av ddb.AttributeValue, v *time.Time) error {
	intV := int64(0)

	if err := UnmarshalInt64(av, &intV); err != nil {
		return err
	}

	t := time.Unix(0, intV)

	*v = t

	return nil
}

// UnixNanoUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func UnixNanoUnmarshaler(v *time.Time) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUnixNano(av, v)
	}
}

// UnmarshalUnix unmarshals an AttributeValue into the given value
func UnmarshalUnix(av ddb.AttributeValue, v *time.Time) error {
	intV := int64(0)

	if err := UnmarshalInt64(av, &intV); err != nil {
		return err
	}

	t := time.Unix(intV, 0)
	*v = t

	return nil
}

// UnixUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func UnixUnmarshaler(v *time.Time) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUnix(av, v)
	}
}
