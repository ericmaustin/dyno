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

// ValueUnmarshalMap represents a map of attributevalue.Unmarshaler
type ValueUnmarshalMap map[string]ddbav.Unmarshaler

// UnmarshalAttributeValueMap runs all the attributevalue.Unmarshalers in the UnmarshalerFunc map
func (dm ValueUnmarshalMap) UnmarshalAttributeValueMap(m map[string]ddb.AttributeValue) error {
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
	return UnmarshalIntPtr(av, &v)
}

// IntUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func IntUnmarshaler(v *int) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalInt(av, v)
	}
}

// UnmarshalIntPtr unmarshals an AttributeValue into the given value
func UnmarshalIntPtr(av ddb.AttributeValue, v **int) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)
	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to int")
	}

	i, err := strconv.Atoi(nv.Value)

	if err != nil {
		return err
	}

	if *v == nil {
		*v = new(int)
	}

	**v = i

	return nil
}

// IntUnmarshalerPtr returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func IntUnmarshalerPtr(v **int) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalIntPtr(av, v)
	}
}


// UnmarshalUint unmarshals an AttributeValue into the given value
func UnmarshalUint(av ddb.AttributeValue, v *uint) error {
	return UnmarshalUintPtr(av, &v)
}

// UintUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func UintUnmarshaler(v *uint) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUint(av, v)
	}
}

// UnmarshalUintPtr unmarshals an AttributeValue into the given value
func UnmarshalUintPtr(av ddb.AttributeValue, v **uint) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)
	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to int")
	}

	i, err := strconv.ParseUint(nv.Value, 10, 64)

	if err != nil {
		return err
	}

	if *v == nil {
		*v = new(uint)
	}

	**v = uint(i)

	return nil
}

// UintUnmarshalerPtr returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func UintUnmarshalerPtr(v **uint) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUintPtr(av, v)
	}
}

// UnmarshalInt32Ptr unmarshals an AttributeValue into the given value
func UnmarshalInt32Ptr(av ddb.AttributeValue, v **int32) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)
	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to int32")
	}

	i, err := strconv.ParseUint(nv.Value, 10, 32)

	if err != nil {
		return err
	}

	if *v == nil {
		*v = new(int32)
	}

	**v = int32(i)

	return nil
}


// UnmarshalInt32 unmarshals an AttributeValue into the given value
func UnmarshalInt32(av ddb.AttributeValue, v *int32) error {
	return UnmarshalInt32Ptr(av, &v)
}

// Int32PtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Int32PtrUnmarshaler(v **int32) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalInt32Ptr(av, v)
	}
}

// Int32Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Int32Unmarshaler(v *int32) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalInt32(av, v)
	}
}

// UnmarshalUint32Ptr unmarshals an AttributeValue into the given value
func UnmarshalUint32Ptr(av ddb.AttributeValue, v **uint32) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)
	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to int32")
	}

	i, err := strconv.ParseInt(nv.Value, 10, 32)

	if err != nil {
		return err
	}

	if *v == nil {
		*v = new(uint32)
	}

	**v = uint32(i)

	return nil
}


// UnmarshalUint32 unmarshals an AttributeValue into the given value
func UnmarshalUint32(av ddb.AttributeValue, v *uint32) error {
	return UnmarshalUint32Ptr(av, &v)
}

// Uint32PtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Uint32PtrUnmarshaler(v **uint32) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUint32Ptr(av, v)
	}
}

// Uint32Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Uint32Unmarshaler(v *uint32) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUint32(av, v)
	}
}

// UnmarshalInt64Ptr unmarshals an AttributeValue into the given value
func UnmarshalInt64Ptr(av ddb.AttributeValue, v **int64) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)
	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to int64")
	}

	i, err := strconv.ParseInt(nv.Value, 10, 64)

	if err != nil {
		return err
	}

	if *v == nil {
		*v = new(int64)
	}

	**v = i
	
	return nil
}

// UnmarshalInt64 unmarshals an AttributeValue into the given value
func UnmarshalInt64(av ddb.AttributeValue, v *int64) error {
	return UnmarshalInt64Ptr(av, &v)
}

// Int64PtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Int64PtrUnmarshaler(v **int64) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalInt64Ptr(av, v)
	}
}

// Int64Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Int64Unmarshaler(v *int64) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalInt64(av, v)
	}
}


// UnmarshalUint64Ptr unmarshals an AttributeValue into the given value
func UnmarshalUint64Ptr(av ddb.AttributeValue, v **uint64) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)
	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to int64")
	}

	i, err := strconv.ParseUint(nv.Value, 10, 64)

	if err != nil {
		return err
	}

	if *v == nil {
		*v = new(uint64)
	}

	**v = i

	return nil
}

// UnmarshalUint64 unmarshals an AttributeValue into the given value
func UnmarshalUint64(av ddb.AttributeValue, v *uint64) error {
	return UnmarshalUint64Ptr(av, &v)
}

// Uint64PtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Uint64PtrUnmarshaler(v **uint64) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUint64Ptr(av, v)
	}
}

// Uint64Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Uint64Unmarshaler(v *uint64) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUint64(av, v)
	}
}

// UnmarshalFloat32Ptr unmarshals an AttributeValue into the given value
func UnmarshalFloat32Ptr(av ddb.AttributeValue, v **float32) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)

	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to float32")
	}

	f, err := strconv.ParseFloat(nv.Value, 32)

	if err != nil {
		return err
	}

	if *v == nil {
		*v = new(float32)
	}

	**v = float32(f)

	return nil
}

// UnmarshalFloat32 unmarshals an AttributeValue into the given value
func UnmarshalFloat32(av ddb.AttributeValue, v *float32) error {
	return UnmarshalFloat32Ptr(av, &v)
}

// Float32Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Float32Unmarshaler(v *float32) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalFloat32(av, v)
	}
}

// Float32PtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Float32PtrUnmarshaler(v **float32) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalFloat32Ptr(av, v)
	}
}

// UnmarshalFloat64Ptr unmarshals an AttributeValue into the given value
func UnmarshalFloat64Ptr(av ddb.AttributeValue, v **float64) error {
	nv, ok := av.(*ddb.AttributeValueMemberN)

	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to float64")
	}

	f, err := strconv.ParseFloat(nv.Value, 64)

	if err != nil {
		return err
	}

	if *v == nil {
		*v = new(float64)
	}

	**v = f

	return nil
}

// UnmarshalFloat64 unmarshals an AttributeValue into the given value
func UnmarshalFloat64(av ddb.AttributeValue, v *float64) error {
	err := UnmarshalFloat64Ptr(av, &v)
	return err
}

// Float64Unmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Float64Unmarshaler(v *float64) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalFloat64Ptr(av, &v)
	}
}

// Float64PtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func Float64PtrUnmarshaler(v **float64) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalFloat64Ptr(av, v)
	}
}

// UnmarshalBoolPtr unmarshals an AttributeValue into the given value
func UnmarshalBoolPtr(av ddb.AttributeValue, v **bool) error {
	bv, ok := av.(*ddb.AttributeValueMemberBOOL)
	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to bool")
	}

	if *v == nil {
		*v = new(bool)
	}

	**v = bv.Value
	
	return nil
}

// UnmarshalBool unmarshals an AttributeValue into the given value
func UnmarshalBool(av ddb.AttributeValue, v *bool) error {
	return UnmarshalBoolPtr(av, &v)
}

// BoolUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func BoolUnmarshaler(v *bool) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalBool(av, v)
	}
}

// BoolPtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func BoolPtrUnmarshaler(v **bool) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalBoolPtr(av, v)
	}
}

// UnmarshalStringPtr unmarshals an AttributeValue into the given value
func UnmarshalStringPtr(av ddb.AttributeValue, v **string) error {
	sv, ok := av.(*ddb.AttributeValueMemberS)

	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to string")
	}

	if *v == nil {
		*v = new(string)
	}

	**v = sv.Value

	return nil
}

// UnmarshalString unmarshals an AttributeValue into the given value
func UnmarshalString(av ddb.AttributeValue, v *string) error {
	return UnmarshalStringPtr(av, &v)
}

// StringUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func StringUnmarshaler(v *string) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalString(av, v)
	}
}

// StringPtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func StringPtrUnmarshaler(v **string) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalStringPtr(av, v)
	}
}

// UnmarshalBytesPtr unmarshals an AttributeValue into the given value
func UnmarshalBytesPtr(av ddb.AttributeValue, v **[]byte) error {
	bv, ok := av.(*ddb.AttributeValueMemberB)
	if !ok {
		if avNull, ok := av.(*ddb.AttributeValueMemberNULL); ok && avNull.Value {
			// nil
			return nil
		}

		return errors.New("cannot decode AttributeValue to bytes")
	}
	
	if *v == nil {
		*v = new([]byte)
	}

	**v = bv.Value

	return nil
}

// UnmarshalBytes unmarshals an AttributeValue into the given value
func UnmarshalBytes(av ddb.AttributeValue, v *[]byte) error {
	return UnmarshalBytesPtr(av, &v)
}

// BytesUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func BytesUnmarshaler(v *[]byte) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalBytes(av, v)
	}
}

// BytesPtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func BytesPtrUnmarshaler(v **[]byte) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalBytesPtr(av, v)
	}
}

// UnmarshalJSON unmarshals an AttributeValue into the given value
func UnmarshalJSON(av ddb.AttributeValue, v interface{}) error {
	var jb []byte

	if bv, ok := av.(*ddb.AttributeValueMemberB); ok {
		jb = bv.Value
	} else if sv, ok := av.(*ddb.AttributeValueMemberS); ok {
		jb = []byte(sv.Value)
	} else if _, ok := av.(*ddb.AttributeValueMemberNULL); ok {
		jb = []byte{}
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

// UnmarshalUnixNanoPtr unmarshals an AttributeValue into the given value
func UnmarshalUnixNanoPtr(av ddb.AttributeValue, v **time.Time) error {
	intV := new(int64)

	if err := UnmarshalInt64Ptr(av, &intV); err != nil {
		return err
	}

	if *v == nil {
		*v = new(time.Time)
	}

	**v = time.Unix(0, *intV)

	return nil
}

// UnmarshalUnixNano unmarshals an AttributeValue into the given value
func UnmarshalUnixNano(av ddb.AttributeValue, v *time.Time) error {
	return UnmarshalUnixNanoPtr(av, &v)
}

// UnixNanoUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func UnixNanoUnmarshaler(v *time.Time) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUnixNano(av, v)
	}
}

// UnixNanoPtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func UnixNanoPtrUnmarshaler(v **time.Time) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUnixNanoPtr(av, v)
	}
}

// UnmarshalUnixPtr unmarshals an AttributeValue into the given value
func UnmarshalUnixPtr(av ddb.AttributeValue, v **time.Time) error {
	intV := new(int64)

	if err := UnmarshalInt64Ptr(av, &intV); err != nil {
		return err
	}

	if *v == nil {
		*v = new(time.Time)
	}

	**v = time.Unix(*intV, 0)

	return nil
}


// UnmarshalUnix unmarshals an AttributeValue into the given value
func UnmarshalUnix(av ddb.AttributeValue, v *time.Time) error {
	return UnmarshalUnixPtr(av, &v)
}

// UnixUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func UnixUnmarshaler(v *time.Time) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUnix(av, v)
	}
}

// UnixPtrUnmarshaler returns a UnmarshalerFunc func that will unmarshal an AttributeValue into the given ptr
func UnixPtrUnmarshaler(v **time.Time) UnmarshalerFunc {
	return func(av ddb.AttributeValue) error {
		return UnmarshalUnixPtr(av, v)
	}
}