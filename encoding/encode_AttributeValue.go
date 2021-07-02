package encoding

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"
	"time"
)

// NilMode represents the mode for how the marshaler will marshal a nil value
type NilMode int

const (
	// NilZero will marshal the value to the zero value of the scalar type if nil
	NilZero NilMode = iota
	// NilNull will marshal the value to a NULL value if nil
	NilNull
	// NilNil will return nil instead of an AttributeValue
	NilNil
)

// MarshalFunc represents a func that Marshals an AttributeValue
type MarshalFunc func() (ddb.AttributeValue, error)

func (e MarshalFunc) MarshalDynamoDBAttributeValue() (ddb.AttributeValue, error) {
	return e()
}

// ValueMarshalMap represents a map of attributevalue.Marshaler values
type ValueMarshalMap map[string]attributevalue.Marshaler

// MarshalMap runs all the attributevalue.Marshaler in the MarshalMap and generates a map of AttributeValues
func (em ValueMarshalMap) MarshalMap() (map[string]ddb.AttributeValue, error) {
	m := make(map[string]ddb.AttributeValue, len(em))

	for k, v := range em {
		av, err := v.MarshalDynamoDBAttributeValue()
		if err != nil {
			return nil, err
		}

		if av != nil {
			m[k] = av
		}
	}

	return m, nil
}

// MarshalToMap runs all the attributevalue.Marshaler in the MarshalMap and adds their values to the input
func (em ValueMarshalMap) MarshalToMap(input map[string]ddb.AttributeValue) error {
	for k, v := range em {
		av, err := v.MarshalDynamoDBAttributeValue()
		if err != nil {
			return err
		}

		if av != nil {
			input[k] = av
		}
	}

	return nil
}

func numericNil(mode NilMode) ddb.AttributeValue {
	switch mode {
	case NilZero:
		return &ddb.AttributeValueMemberN{Value: "0"}
	case NilNull:
		return &ddb.AttributeValueMemberNULL{Value: true}
	}
	// NilNil
	return nil
}

func stringNil(mode NilMode) ddb.AttributeValue {
	switch mode {
	case NilZero:
		return &ddb.AttributeValueMemberS{Value: ""}
	case NilNull:
		return &ddb.AttributeValueMemberNULL{Value: true}
	}
	// NilNil
	return nil
}

func boolNil(mode NilMode) ddb.AttributeValue {
	switch mode {
	case NilZero:
		return &ddb.AttributeValueMemberBOOL{Value: false}
	case NilNull:
		return &ddb.AttributeValueMemberNULL{Value: true}
	}
	// NilNil
	return nil
}

func bytesNil(mode NilMode) ddb.AttributeValue {
	switch mode {
	case NilZero:
		return &ddb.AttributeValueMemberB{Value: make([]byte, 0)}
	case NilNull:
		return &ddb.AttributeValueMemberNULL{Value: true}
	}
	// NilNil
	return nil
}

// MarshalInt marshals an AttributeValue into the given value
func MarshalInt(v *int, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.Itoa(*v)}
}

// IntMarshaler returns a MarshalFunc func that will generate an AttributeValue
func IntMarshaler(v *int, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalInt(v, mode), nil
	}
}

// MarshalUint marshals an AttributeValue into the given value
func MarshalUint(v *uint, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatUint(uint64(*v), 10)}
}

// UintMarshaler returns a MarshalFunc func that will generate an AttributeValue
func UintMarshaler(v *uint, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUint(v, mode), nil
	}
}

// MarshalInt32 marshals an AttributeValue into the given value
func MarshalInt32(v *int32, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatInt(int64(*v), 10)}
}

// Int32Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Int32Marshaler(v *int32, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalInt32(v, mode), nil
	}
}

// MarshalUint32 marshals an AttributeValue into the given value
func MarshalUint32(v *uint32, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatUint(uint64(*v), 10)}
}

// Uint32Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Uint32Marshaler(v *uint32, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUint32(v, mode), nil
	}
}

// MarshalInt64 marshals an AttributeValue into the given value
func MarshalInt64(v *int64, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatInt(*v, 10)}
}

// Int64Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Int64Marshaler(v *int64, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalInt64(v, mode), nil
	}
}

// MarshalUint64 marshals an AttributeValue into the given value
func MarshalUint64(v *uint64, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatUint(*v, 10)}
}

// Uint64Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Uint64Marshaler(v *uint64, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUint64(v, mode), nil
	}
}

// MarshalFloat32 marshals an AttributeValue into the given value
func MarshalFloat32(v *float32, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatFloat(float64(*v), 'g', -1, 32)}
}

// Float32Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Float32Marshaler(v *float32, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalFloat32(v, mode), nil
	}
}

// MarshalFloat64 marshals an AttributeValue into the given value
func MarshalFloat64(v *float64, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatFloat(*v, 'g', -1, 64)}
}

// Float64Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Float64Marshaler(v *float64, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalFloat64(v, mode), nil
	}
}

// MarshalBool marshals an AttributeValue into the given value
func MarshalBool(v *bool, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return boolNil(mode)
	}

	return &ddb.AttributeValueMemberBOOL{Value: *v}
}

// BoolMarshaler returns a MarshalFunc func that will generate an AttributeValue
func BoolMarshaler(v *bool, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalBool(v, mode), nil
	}
}

// MarshalString marshals an AttributeValue into the given value
func MarshalString(v *string, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return stringNil(mode)
	}

	return &ddb.AttributeValueMemberS{Value: *v}
}

// StringMarshaler returns a MarshalFunc func that will generate an AttributeValue
func StringMarshaler(v *string, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalString(v, mode), nil
	}
}

// MarshalBytes marshals an AttributeValue into the given value
func MarshalBytes(v []byte, mode NilMode) ddb.AttributeValue {
	if len(v) < 1 {
		return bytesNil(mode)
	}

	return &ddb.AttributeValueMemberB{Value: v}
}

// BytesMarshaler returns a MarshalFunc func that will generate an AttributeValue
func BytesMarshaler(v []byte, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalBytes(v, mode), nil
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

// MarshalUnixNano marshals an AttributeValue into the given value
func MarshalUnixNano(v *time.Time, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}

	intV := v.UnixNano()

	return MarshalInt64(&intV, mode)
}

// UnixNanoMarshaler returns a MarshalFunc func that will generate an AttributeValue
func UnixNanoMarshaler(v *time.Time, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUnixNano(v, mode), nil
	}
}

// MarshalUnix marshals an AttributeValue into the given value
func MarshalUnix(v *time.Time, mode NilMode) ddb.AttributeValue {
	if v == nil {
		return numericNil(mode)
	}
	intV := v.Unix()
	return MarshalInt64(&intV, mode)
}

// UnixMarshaler returns a MarshalFunc func that will generate an AttributeValue
func UnixMarshaler(v *time.Time, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUnix(v, mode), nil
	}
}
