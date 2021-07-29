package encoding

import (
	"encoding/json"
	"errors"
	"fmt"
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
	// NilError will return an error if the value is Nil
	NilError
)

var ErrNil = errors.New("value must not be nil when NilError flag is set")

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
			return nil, fmt.Errorf("error encoding field %s: %s", k, err)
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
			return fmt.Errorf("error encoding field %s: %s", k, err)
		}

		if av != nil {
			input[k] = av
		}
	}

	return nil
}

func numericNil(mode NilMode) (ddb.AttributeValue, error) {
	switch mode {
	case NilZero:
		return &ddb.AttributeValueMemberN{Value: "0"}, nil
	case NilNull:
		return &ddb.AttributeValueMemberNULL{Value: true}, nil
	case NilError:
		return nil, ErrNil
	}
	// NilNil
	return nil, nil
}

func stringNil(mode NilMode) (ddb.AttributeValue, error) {
	switch mode {
	case NilZero:
		return &ddb.AttributeValueMemberS{Value: ""}, nil
	case NilNull:
		return &ddb.AttributeValueMemberNULL{Value: true}, nil
	case NilError:
		return nil, ErrNil
	}
	// NilNil
	return nil, nil
}

func boolNil(mode NilMode) (ddb.AttributeValue, error) {
	switch mode {
	case NilZero:
		return &ddb.AttributeValueMemberBOOL{Value: false}, nil
	case NilNull:
		return &ddb.AttributeValueMemberNULL{Value: true}, nil
	case NilError:
		return nil, ErrNil
	}
	// NilNil
	return nil, nil
}

func bytesNil(mode NilMode) (ddb.AttributeValue, error) {
	switch mode {
	case NilZero:
		return &ddb.AttributeValueMemberB{Value: make([]byte, 0)}, nil
	case NilNull:
		return &ddb.AttributeValueMemberNULL{Value: true}, nil
	case NilError:
		return nil, ErrNil
	}
	// NilNil
	return nil, nil
}

// MarshalInt marshals an AttributeValue into the given value
func MarshalInt(v *int, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.Itoa(*v)}, nil
}

// IntMarshaler returns a MarshalFunc func that will generate an AttributeValue
func IntMarshaler(v *int, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalInt(v, mode)
	}
}

// MarshalUint marshals an AttributeValue into the given value
func MarshalUint(v *uint, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatUint(uint64(*v), 10)}, nil
}

// UintMarshaler returns a MarshalFunc func that will generate an AttributeValue
func UintMarshaler(v *uint, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUint(v, mode)
	}
}

// MarshalInt32 marshals an AttributeValue into the given value
func MarshalInt32(v *int32, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatInt(int64(*v), 10)}, nil
}

// Int32Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Int32Marshaler(v *int32, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalInt32(v, mode)
	}
}

// MarshalUint32 marshals an AttributeValue into the given value
func MarshalUint32(v *uint32, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatUint(uint64(*v), 10)}, nil
}

// Uint32Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Uint32Marshaler(v *uint32, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUint32(v, mode)
	}
}

// MarshalInt64 marshals an AttributeValue into the given value
func MarshalInt64(v *int64, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatInt(*v, 10)}, nil
}

// Int64Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Int64Marshaler(v *int64, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalInt64(v, mode)
	}
}

// MarshalUint64 marshals an AttributeValue into the given value
func MarshalUint64(v *uint64, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatUint(*v, 10)}, nil
}

// Uint64Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Uint64Marshaler(v *uint64, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUint64(v, mode)
	}
}

// MarshalFloat32 marshals an AttributeValue into the given value
func MarshalFloat32(v *float32, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatFloat(float64(*v), 'g', -1, 32)}, nil
}

// Float32Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Float32Marshaler(v *float32, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalFloat32(v, mode)
	}
}

// MarshalFloat64 marshals an AttributeValue into the given value
func MarshalFloat64(v *float64, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	return &ddb.AttributeValueMemberN{Value: strconv.FormatFloat(*v, 'g', -1, 64)}, nil
}

// Float64Marshaler returns a MarshalFunc func that will generate an AttributeValue
func Float64Marshaler(v *float64, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalFloat64(v, mode)
	}
}

// MarshalBool marshals an AttributeValue into the given value
func MarshalBool(v *bool, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return boolNil(mode)
	}

	return &ddb.AttributeValueMemberBOOL{Value: *v}, nil
}

// BoolMarshaler returns a MarshalFunc func that will generate an AttributeValue
func BoolMarshaler(v *bool, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalBool(v, mode)
	}
}

// MarshalString marshals an AttributeValue into the given value
func MarshalString(v *string, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return stringNil(mode)
	}

	return &ddb.AttributeValueMemberS{Value: *v}, nil
}

// StringMarshaler returns a MarshalFunc func that will generate an AttributeValue
func StringMarshaler(v *string, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalString(v, mode)
	}
}

// MarshalBytes marshals an AttributeValue into the given value
func MarshalBytes(v []byte, mode NilMode) (ddb.AttributeValue, error) {
	if len(v) < 1 {
		return bytesNil(mode)
	}

	return &ddb.AttributeValueMemberB{Value: v}, nil
}

// BytesMarshaler returns a MarshalFunc func that will generate an AttributeValue
func BytesMarshaler(v []byte, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalBytes(v, mode)
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
func MarshalUnixNano(v *time.Time, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	intV := v.UnixNano()

	return MarshalInt64(&intV, mode)
}

// UnixNanoMarshaler returns a MarshalFunc func that will generate an AttributeValue
func UnixNanoMarshaler(v *time.Time, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUnixNano(v, mode)
	}
}

// MarshalUnix marshals an AttributeValue into the given value
func MarshalUnix(v *time.Time, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	intV := v.Unix()

	return MarshalInt64(&intV, mode)
}

// UnixMarshaler returns a MarshalFunc func that will generate an AttributeValue
func UnixMarshaler(v *time.Time, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalUnix(v, mode)
	}
}

// MarshalDuration marshals an AttributeValue into the given value
func MarshalDuration(v *time.Duration, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return numericNil(mode)
	}

	intV := v.Nanoseconds()

	return MarshalInt64(&intV, mode)
}

// DurationMarshaler returns a MarshalFunc func that will generate an AttributeValue
func DurationMarshaler(v *time.Duration, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalDuration(v, mode)
	}
}

// MarshalStringSlice marshals an AttributeValue into the given value
func MarshalStringSlice(v *[]string, mode NilMode) (ddb.AttributeValue, error) {
	if v == nil {
		return stringNil(mode)
	}

	return &ddb.AttributeValueMemberSS{Value: *v}, nil
}

// StringSliceMarshaler returns a MarshalFunc func that will generate an AttributeValue
func StringSliceMarshaler(v *[]string, mode NilMode) MarshalFunc {
	return func() (ddb.AttributeValue, error) {
		return MarshalStringSlice(v, mode)
	}
}