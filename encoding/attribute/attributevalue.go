//Package attribute contains verbose encoding and decoding funcs for basic go types for dynamodb.AttributeValues
// to speed up the processing of these values vs. calling the marshalling functions
package attribute

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strconv"
	"time"
)

type AttributeType string

const (
	TypeNumber    = AttributeType("Number")
	TypeString    = AttributeType("DecodeString")
	TypeBOOL      = AttributeType("Boolean")
	TypeStringSet = AttributeType("DecodeString Set")
	TypeBinary    = AttributeType("Binary")
	TypeByteSet   = AttributeType("Byte Set")
	TypeList      = AttributeType("List")
	TypeMap       = AttributeType("Map")
	TypeNumberSet = AttributeType("Number Set")
	TypeNull      = AttributeType("Null")
)

//ErrIncorrectType represents an error during the decoding of AttributeValues when the incorrect attribute value
// type is encountered
type ErrIncorrectType struct {
	attributeType AttributeType
	expected      AttributeType
}

//Error implements error interface
func (e *ErrIncorrectType) Error() string {
	return fmt.Sprintf("attribute value is an incorrect type. expected: '%s' got:'%s'",
		e.expected, e.attributeType)
}

//NewErrIncorrectType creates a new ErrIncorrectType with the given AttributeType and the expected AttributeType
func NewErrIncorrectType(got AttributeType, expected AttributeType) *ErrIncorrectType {
	return &ErrIncorrectType{
		attributeType: got,
		expected:      expected,
	}
}

//NewNullValue create a new NULL dynamodb.AttributeValue
func NewNullValue() *dynamodb.AttributeValue {
	b := true
	return &dynamodb.AttributeValue{NULL: &b}
}

//GetAttributeType gets the AttributeType from the dynamodb.AttributeValue
func GetAttributeType(av *dynamodb.AttributeValue) AttributeType {
	switch true {
	case av.NULL != nil:
		return TypeNull
	case av.BOOL != nil:
		return TypeBOOL
	case av.N != nil:
		return TypeNumber
	case av.S != nil:
		return TypeString
	case av.M != nil:
		return TypeMap
	case av.B != nil:
		return TypeBinary
	case len(av.SS) > 0:
		return TypeStringSet
	case len(av.BS) > 0:
		return TypeByteSet
	case len(av.L) > 0:
		return TypeList
	case len(av.NS) > 0:
		return TypeNumberSet
	default:
		// av is null
		return TypeNull
	}
}

//type ValueHandler func(*dynamodb.AttributeValue)
//
//func HandleValue(valueMap map[string]*dynamodb.AttributeValue, key string, handler ValueHandler) {
//	if av, ok := valueMap[key]; ok {
//		handler(av)
//	}
//}
//
//func HandleValues(valueMap map[string]*dynamodb.AttributeValue, handlers map[string]ValueHandler) {
//	for key, handler := range handlers {
//		HandleValue(valueMap, key, handler)
//	}
//}

//AddValue is a helper func used to facilitate adding attribute values to attribute value maps
func AddValue(avMap map[string]*dynamodb.AttributeValue, key string, av *dynamodb.AttributeValue) {
	avMap[key] = av
}

//AddNonNullValue is a helper func used to facilitate adding attribute values to attribute value maps
// same as AddValue but NULL or nil dynamodb.AttributeValue are not added to the map
func AddNonNullValue(avMap map[string]*dynamodb.AttributeValue, key string, av *dynamodb.AttributeValue) {
	if av == nil || IsNilOrNULL(av) {
		return
	}
	AddValue(avMap, key, av)
}

//RemoveNullValues removes all NULL dynamodb.AttributeValue from a map dynamodb.AttributeValue
func RemoveNullValues(avMap map[string]*dynamodb.AttributeValue) {
	var rmKeys []string
	for k, v := range avMap {
		if IsNilOrNULL(v) {
			rmKeys = append(rmKeys, k)
		}
	}
	for _, k := range rmKeys {
		delete(avMap, k)
	}
}

//IsNilOrNULL returns true if the *dynamodb.AttributeValue is nil or is of the NULL type
func IsNilOrNULL(av *dynamodb.AttributeValue) bool {
	return av == nil || (av.NULL != nil && *av.NULL)
}

//EncodeString encodes a given *string as a dynamodb.AttributeValue
func EncodeString(str *string) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalString(str, av)
	return av
}

//DecodeString decodes a given dynamodb.AttributeValue to a *string
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not a string or NULL type
func DecodeString(av *dynamodb.AttributeValue) (*string, error) {
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if av.S == nil {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeString)
	}
	return av.S, nil
}

//MarshalString marshals the *string into the provided *dynamodb.AttributeValue
func MarshalString(str *string, av *dynamodb.AttributeValue) {
	if str == nil {
		return
	}
	av.S = str
}

//UnmarshalString unmarshals the *dynamodb.AttributeValue into the provided *string
func UnmarshalString(av *dynamodb.AttributeValue, str *string) error {
	_str, err := DecodeString(av)
	if err != nil || _str == nil {
		return err
	}
	*str = *_str
	return nil
}

//EncodeInt64 encodes a given *int64 as a dynamodb.AttributeValue
func EncodeInt64(i *int64) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalInt64(i, av)
	return av
}

//DecodeInt64 decodes a given dynamodb.AttributeValue to a *int64
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not a Number or NULL type
func DecodeInt64(av *dynamodb.AttributeValue) (*int64, error) {
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if av.N == nil {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeNumber)
	}
	i, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return nil, err
	}
	return &i, nil
}

//MarshalInt64 marshals the *int64 into the provided *dynamodb.AttributeValue
func MarshalInt64(i *int64, av *dynamodb.AttributeValue) {
	if i == nil {
		return
	}
	str := strconv.FormatInt(*i, 10)
	av.N = &str
}

//UnmarshalInt64 unmarshals the *dynamodb.AttributeValue into the provided *int64
func UnmarshalInt64(av *dynamodb.AttributeValue, i *int64) error {
	_i, err := DecodeInt64(av)
	if err != nil || _i == nil {
		return err
	}
	*i = *_i
	return nil
}

//EncodeInt encodes a given *int as a dynamodb.AttributeValue
func EncodeInt(i *int) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalInt(i, av)
	return av
}

//DecodeInt decodes a given dynamodb.AttributeValue to a *int,
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not a Number or NULL type
func DecodeInt(av *dynamodb.AttributeValue) (*int, error) {
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if av.N == nil {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeNumber)
	}
	i, err := strconv.Atoi(*av.N)
	if err != nil {
		return nil, err
	}
	return &i, nil
}

//MarshalInt marshals the *int into the provided *dynamodb.AttributeValue
func MarshalInt(i *int, av *dynamodb.AttributeValue) {
	if i == nil {
		return
	}
	str := strconv.Itoa(*i)
	av.N = &str
}

//UnmarshalInt unmarshals the *dynamodb.AttributeValue into the provided *int
func UnmarshalInt(av *dynamodb.AttributeValue, i *int) error {
	_i, err := DecodeInt(av)
	if err != nil || _i == nil {
		return err
	}
	*i = *_i
	return nil
}

//EncodeFloat encodes a given *float64 as a dynamodb.AttributeValue
func EncodeFloat(f *float64) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalFloat(f, av)
	return av
}

//DecodeFloat decodes a given dynamodb.AttributeValue to a *float64,
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not a Number or NULL type
func DecodeFloat(av *dynamodb.AttributeValue) (*float64, error) {
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if av.N == nil {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeNumber)
	}
	i, err := strconv.ParseFloat(*av.N, 64)
	if err != nil {
		return nil, err
	}
	return &i, nil
}

//MarshalFloat marshals the *float64 into the provided *dynamodb.AttributeValue
func MarshalFloat(f *float64, av *dynamodb.AttributeValue) {
	if f == nil {
		return
	}
	str := strconv.FormatFloat(*f, 'f', -1, 64)
	av.N = &str
}

//UnmarshalFloat unmarshals the *dynamodb.AttributeValue into the provided *float64
func UnmarshalFloat(av *dynamodb.AttributeValue, f *float64) error {
	_f, err := DecodeFloat(av)
	if err != nil || _f == nil {
		return err
	}
	*f = *_f
	return nil
}

//EncodeUnixNano encodes a given *time.Time as a dynamodb.AttributeValue
func EncodeUnixNano(t *time.Time) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalUnixNano(t, av)
	return av
}

//DecodeUnixNano decodes a given dynamodb.AttributeValue to a *time.Time from a UNIX Nanosecond timestamp value,
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not a Number or NULL type
func DecodeUnixNano(av *dynamodb.AttributeValue) (*time.Time, error) {
	i, err := DecodeInt64(av)
	if err != nil || i == nil {
		return nil, err
	}
	t := time.Unix(0, *i)
	return &t, nil
}

//MarshalUnixNano marshals the *time.Time into the provided *dynamodb.AttributeValue
func MarshalUnixNano(t *time.Time, av *dynamodb.AttributeValue) {
	if t == nil {
		return
	}
	str := strconv.FormatInt(t.UnixNano(), 10)
	av.N = &str
}

//UnmarshalUnixNano unmarshals the *dynamodb.AttributeValue into the provided *time.Time
func UnmarshalUnixNano(av *dynamodb.AttributeValue, t *time.Time) error {
	_t, err := DecodeUnixNano(av)
	if err != nil || _t == nil {
		return err
	}
	*t = *_t
	return nil
}

//EncodeUnix encodes a given *time.Time as a dynamodb.AttributeValue
func EncodeUnix(t *time.Time) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalUnix(t, av)
	return av
}

//DecodeUnix decodes a given dynamodb.AttributeValue to a *time.Time from a UNIX timestamp value,
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not a Number or NULL type
func DecodeUnix(av *dynamodb.AttributeValue) (*time.Time, error) {
	i, err := DecodeInt64(av)
	if err != nil || i == nil {
		return nil, err
	}
	t := time.Unix(*i, 0)
	return &t, nil
}

//MarshalUnix marshals the *time.Time into the provided *dynamodb.AttributeValue
func MarshalUnix(t *time.Time, av *dynamodb.AttributeValue) {
	if t == nil {
		return
	}
	str := strconv.FormatInt(t.Unix(), 10)
	av.N = &str
}

//EncodeTimeFormat encodes a given *time.Time as a dynamodb.AttributeValue with a given format
func EncodeTimeFormat(t *time.Time, format string) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalTimeFormat(t, format, av)
	return av
}

//UnmarshalUnix unmarshals the *dynamodb.AttributeValue into the provided *time.Time
func UnmarshalUnix(av *dynamodb.AttributeValue, t *time.Time) error {
	_t, err := DecodeUnix(av)
	if err != nil || _t == nil {
		return err
	}
	*t = *_t
	return nil
}

//DecodeTimeFormat decodes a given dynamodb.AttributeValue to a *time.Time with a given layout,
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not a String or NULL type
func DecodeTimeFormat(av *dynamodb.AttributeValue, layout string) (*time.Time, error) {
	str, err := DecodeString(av)
	if err != nil || str == nil {
		return nil, err
	}
	t, err := time.Parse(layout, *str)
	return &t, err
}

//MarshalTimeFormat marshals the *time.Time into the provided *dynamodb.AttributeValue using the provided time layout
func MarshalTimeFormat(t *time.Time, layout string, av *dynamodb.AttributeValue) {
	if t == nil {
		return
	}
	str := t.Format(layout)
	av.S = &str
}

//UnmarshalTimeFormat unmarshals the *dynamodb.AttributeValue into the provided *time.Time using the provided time layout
func UnmarshalTimeFormat(av *dynamodb.AttributeValue, layout string, t *time.Time) error {
	_t, err := DecodeTimeFormat(av, layout)
	if err != nil || _t == nil {
		return err
	}
	*t = *_t
	return nil
}

//EncodeBytes encodes a given []byte as a dynamodb.AttributeValue
func EncodeBytes(b []byte) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalBytes(b, av)
	return av
}

//DecodeBytes decodes a given dynamodb.AttributeValue to []byte
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not Binary or NULL type
func DecodeBytes(av *dynamodb.AttributeValue) ([]byte, error) {
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if av.B == nil {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeBinary)
	}
	return av.B, nil
}

//MarshalBytes marshals the []byte into the provided *dynamodb.AttributeValue
func MarshalBytes(b []byte, av *dynamodb.AttributeValue) {
	if b == nil {
		return
	}
	av.B = b
}

//UnmarshalBytes unmarshals the *dynamodb.AttributeValue into the provided *[]byte
func UnmarshalBytes(av *dynamodb.AttributeValue, b *[]byte) error {
	_b, err := DecodeBytes(av)
	if err != nil || _b == nil {
		return err
	}
	*b = _b
	return nil
}

//EncodeStringSlice encodes a given []string as a dynamodb.AttributeValue
func EncodeStringSlice(ss []string) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalStringSlice(ss, av)
	return av
}

//DecodeStringSlice decodes a given dynamodb.AttributeValue to []*string
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not StringSet or NULL type
func DecodeStringSlice(av *dynamodb.AttributeValue) ([]string, error) {
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if len(av.SS) < 1 {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeStringSet)
	}
	out := make([]string, len(av.SS))
	for i := range av.SS {
		if av.SS[i] == nil {
			continue
		}
		out[i] = *av.SS[i]
	}
	return out, nil
}

//MarshalStringSlice marshals the []*string into the provided *dynamodb.AttributeValue
func MarshalStringSlice(ss []string, av *dynamodb.AttributeValue) {
	if ss == nil {
		return
	}
	av.SS = make([]*string, len(ss))
	for i := range ss {
		av.SS[i] = &ss[i]
	}
}

//UnmarshalStringSlice unmarshals the *dynamodb.AttributeValue into the provided *[]*string
func UnmarshalStringSlice(av *dynamodb.AttributeValue, ss *[]string) error {
	_ss, err := DecodeStringSlice(av)
	if err != nil || _ss == nil {
		return err
	}
	*ss = _ss
	return nil
}

//EncodeBool encodes a given *bool as a dynamodb.AttributeValue
func EncodeBool(b *bool) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalBool(b, av)
	return av
}

//DecodeBool decodes a given dynamodb.AttributeValue to *bool
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not StringSet or NULL type
func DecodeBool(av *dynamodb.AttributeValue) (*bool, error) {
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if av.BOOL == nil {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeBOOL)
	}
	return av.BOOL, nil
}

//MarshalBool marshals the *bool into the provided *dynamodb.AttributeValue
func MarshalBool(b *bool, av *dynamodb.AttributeValue) {
	if b == nil {
		return
	}
	av.BOOL = b
}

//UnmarshalBool unmarshals the *dynamodb.AttributeValue into the provided *bool
func UnmarshalBool(av *dynamodb.AttributeValue, b *bool) error {
	_b, err := DecodeBool(av)
	if err != nil || _b == nil {
		return err
	}
	*b = *_b
	return nil
}

//EncodeFloatSlice encodes a given []float64 as a dynamodb.AttributeValue
func EncodeFloatSlice(fs []float64) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalFloatSlice(fs, av)
	return av
}

//DecodeFloatSlice decodes a given dynamodb.AttributeValue to []float64
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not StringSet or NULL type
func DecodeFloatSlice(av *dynamodb.AttributeValue) ([]float64, error) {
	var err error
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if len(av.NS) < 1 {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeNumberSet)
	}
	out := make([]float64, len(av.NS))
	for i, ns := range av.NS {
		if ns == nil || len(*ns) == 0 {
			// skip nil or empty
			continue
		}
		out[i], err = strconv.ParseFloat(*ns, 64)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

//MarshalFloatSlice marshals the []float64 into the provided *dynamodb.AttributeValue
func MarshalFloatSlice(fs []float64, av *dynamodb.AttributeValue) {
	if len(fs) < 1 {
		return
	}
	av.NS = make([]*string, len(fs))
	for i, f := range fs {
		s := strconv.FormatFloat(f, 'f', -1, 64)
		av.NS[i] = &s
	}
}

//UnmarshalFloatSlice unmarshals the *dynamodb.AttributeValue into the provided *[]float64
func UnmarshalFloatSlice(av *dynamodb.AttributeValue, fs *[]float64) error {
	_fs, err := DecodeFloatSlice(av)
	if err != nil || _fs == nil {
		return err
	}
	*fs = _fs
	return nil
}

//EncodeIntSlice encodes a given []int as a dynamodb.AttributeValue
func EncodeIntSlice(is []int) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalIntSlice(is, av)
	return av
}

//DecodeIntSlice decodes a given dynamodb.AttributeValue to []int
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not StringSet or NULL type
func DecodeIntSlice(av *dynamodb.AttributeValue) ([]int, error) {
	var err error
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if len(av.NS) < 1 {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeNumberSet)
	}
	out := make([]int, len(av.NS))
	for i, ns := range av.NS {
		if ns == nil || len(*ns) == 0 {
			// skip nil or empty
			continue
		}
		out[i], err = strconv.Atoi(*ns)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

//MarshalIntSlice marshals the []int into the provided *dynamodb.AttributeValue
func MarshalIntSlice(is []int, av *dynamodb.AttributeValue) {
	if len(is) < 1 {
		return
	}
	av.NS = make([]*string, len(is))
	for i, in := range is {
		s := strconv.Itoa(in)
		av.NS[i] = &s
	}
}

//UnmarshalIntSlice unmarshals the *dynamodb.AttributeValue into the provided *[]int
func UnmarshalIntSlice(av *dynamodb.AttributeValue, is *[]int) error {
	_is, err := DecodeIntSlice(av)
	if err != nil || _is == nil {
		return err
	}
	*is = _is
	return nil
}

//EncodeInt64Slice encodes a given []int as a dynamodb.AttributeValue
func EncodeInt64Slice(is []int64) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalInt64Slice(is, av)
	return av
}

//DecodeInt64Slice decodes a given dynamodb.AttributeValue to []int64
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not StringSet or NULL type
func DecodeInt64Slice(av *dynamodb.AttributeValue) ([]int64, error) {
	var err error
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if len(av.NS) < 1 {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeNumberSet)
	}
	out := make([]int64, len(av.NS))
	for i, ns := range av.NS {
		if ns == nil || len(*ns) == 0 {
			// skip nil or empty
			continue
		}
		out[i], err = strconv.ParseInt(*ns, 10, 64)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

//MarshalInt64Slice marshals the []int64 into the provided *dynamodb.AttributeValue
func MarshalInt64Slice(is []int64, av *dynamodb.AttributeValue) {
	if len(is) < 1 {
		return
	}
	av.NS = make([]*string, len(is))
	for i, in := range is {
		s := strconv.FormatInt(in, 10)
		av.NS[i] = &s
	}
}

//UnmarshalInt64Slice unmarshals the *dynamodb.AttributeValue into the provided *[]int64
func UnmarshalInt64Slice(av *dynamodb.AttributeValue, is *[]int64) error {
	_is, err := DecodeInt64Slice(av)
	if err != nil || _is == nil {
		return err
	}
	*is = _is
	return nil
}

//EncodeByteSlice encodes a given [][]byte as a dynamodb.AttributeValue
func EncodeByteSlice(bs [][]byte) *dynamodb.AttributeValue {
	av := new(dynamodb.AttributeValue)
	MarshalByteSlice(bs, av)
	return av
}

//DecodeByteSlice decodes a given dynamodb.AttributeValue to [][]byte
// ErrIncorrectType is returned if the dynamodb.AttributeValue is not StringSet or NULL type
func DecodeByteSlice(av *dynamodb.AttributeValue) ([][]byte, error) {
	if IsNilOrNULL(av) {
		// return the null string
		return nil, nil
	}
	if len(av.BS) < 1 {
		return nil, NewErrIncorrectType(GetAttributeType(av), TypeByteSet)
	}
	return av.BS, nil
}

//MarshalByteSlice marshals the []int64 into the provided *dynamodb.AttributeValue
func MarshalByteSlice(bs [][]byte, av *dynamodb.AttributeValue) {
	if len(bs) < 1 {
		return
	}
	av.BS = bs
}

//UnmarshalByteSlice unmarshals the *dynamodb.AttributeValue into the provided *[]int64
func UnmarshalByteSlice(av *dynamodb.AttributeValue, bs *[][]byte) error {
	_bs, err := DecodeByteSlice(av)
	if err != nil || _bs == nil {
		return err
	}
	*bs = _bs
	return nil
}
