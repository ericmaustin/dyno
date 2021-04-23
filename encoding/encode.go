package encoding

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/ericmaustin/dyno"
)

// MarshalItems marshals an input slice into a slice of attribute value maps
// panics if the input's kind is not a slice
func MarshalItems(input interface{}) ([]map[string]*dynamodb.AttributeValue, error) {
	records := make([]map[string]*dynamodb.AttributeValue, 0)
	if err := AppendItems(&records, input); err != nil {
		return nil, err
	}
	return records, nil
}

// MustMarshalItems marshals an input slice into a slice of attribute value maps
// panics on error
func MustMarshalItems(input interface{}) []map[string]*dynamodb.AttributeValue {
	records := make([]map[string]*dynamodb.AttributeValue, 0)
	if err := AppendItems(&records, input); err != nil {
		panic(err)
	}
	return records
}

// AppendItems encodes a given input and appends these items to the given slice of items
func AppendItems(items *[]map[string]*dynamodb.AttributeValue, input interface{}) error {
	rv := indirect(reflect.ValueOf(input), false)

	if rv.Kind() != reflect.Slice {
		return &dyno.Error{
			Code:    dyno.ErrEncodingEmbeddedBadKind,
			Message: fmt.Sprintf("cannot marshal non-slice kind: %v", rv.Kind()),
		}
	}

	for i := 0; i < rv.Len(); i++ {
		rec, err := marshalValueToRecord(rv.Index(i))
		if err != nil {
			return err
		}
		*items = append(*items, rec)
	}
	return nil
}

// MarshalItem marshals a given input that is a map or a struct into an attribute value map
func MarshalItem(input interface{}) (map[string]*dynamodb.AttributeValue, error) {
	return marshalValueToRecord(reflect.ValueOf(input))
}

// MustMarshalItem marshals a given input that is a map or a struct into an attribute value map
// panics on error
func MustMarshalItem(input interface{}) map[string]*dynamodb.AttributeValue {
	itemMap, err := MarshalItem(input)
	if err != nil {
		panic(err)
	}
	return itemMap
}

func marshalValueToRecord(rv reflect.Value) (map[string]*dynamodb.AttributeValue, error) {
	avMap := map[string]*dynamodb.AttributeValue{}
	if err := addValuesToRecord(avMap, rv); err != nil {
		return nil, err
	}
	return avMap, nil
}

// AddToRecord adds given inputs that is a map or a struct to a attribute value map
func AddToRecord(rec map[string]*dynamodb.AttributeValue, inputs ...interface{}) error {
	for _, in := range inputs {
		if err := addValuesToRecord(rec, reflect.ValueOf(in)); err != nil {
			return err
		}
	}
	return nil
}

func addValuesToRecord(item map[string]*dynamodb.AttributeValue, rv reflect.Value) error {
	rv = indirect(rv, false)
	switch rv.Kind() {
	case reflect.Struct:
		if err := addStructToRecord(rv, item, "", ""); err != nil {
			return err
		}
	case reflect.Map:
		// if the map is already a map of dynamodb attribute values, add them as is
		if rv.Type() == reflect.TypeOf(item) {
			for _, key := range rv.MapKeys() {
				item[key.Interface().(string)] = rv.MapIndex(key).Interface().(*dynamodb.AttributeValue)
			}
			return nil
		}
		iFaceMap, err := mapToIfaceMap(rv, "", "")
		if err != nil {
			return err
		}
		if err := addMapToAv(iFaceMap, item); err != nil {
			return err
		}
	default:
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("input must be a struct or map, input is a %v", rv.Kind()),
		}
	}
	return nil
}

func structToRecord(input interface{}) (map[string]*dynamodb.AttributeValue, error) {
	av := map[string]*dynamodb.AttributeValue{}
	err := addStructToRecord(indirect(reflect.ValueOf(input), false), av, "", "")
	if err != nil {
		return nil, err
	}
	return av, nil
}

// addMapToRecord adds any map indexed by a string to the given attribute value map
func addMapToRecord(rv reflect.Value, item map[string]*dynamodb.AttributeValue, prepend, append string) error {
	// if the map is already a map of dynamodb attribute values, add them as is
	if rv.Type() == reflect.TypeOf(item) {
		for k, v := range rv.Interface().(map[string]*dynamodb.AttributeValue) {
			item[k] = v
		}
		return nil
	}
	iFaceMap, err := mapToIfaceMap(rv, prepend, append)
	if err != nil {
		return err
	}
	return addMapToAv(iFaceMap, item)
}

func mapToIfaceMap(rv reflect.Value, prepend, append string) (map[string]interface{}, error) {
	if rv.Kind() != reflect.Map {
		return nil, &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("input map is not a map. input is a %v", rv.Kind()),
		}
	}

	iter := rv.MapRange()
	iFaceMap := map[string]interface{}{}

	for iter.Next() {
		key := indirect(iter.Key(), false)
		iFaceMap[prepend+ToString(key.Interface())+append] = iter.Value().Interface()
	}
	return iFaceMap, nil
}

func marshalAv(rv reflect.Value, omitZero, omitNil, toJSON bool) (*dynamodb.AttributeValue, error) {

	if shouldOmit(rv, omitZero, omitNil) {
		return nil, nil
	}

	rv = indirect(rv, false)

	if toJSON {
		// we're encoding to json to marshal value with json marshaller and set value to string
		av := &dynamodb.AttributeValue{}
		jsonBytes, err := json.Marshal(rv.Interface())
		if err != nil {
			return nil, err
		}
		av.S = aws.String(string(jsonBytes))
		return av, nil
	}

	return dynamodbattribute.Marshal(rv.Interface())
}

func addStructToRecord(rv reflect.Value, item map[string]*dynamodb.AttributeValue, prepend, append string) error {

	var (
		av  *dynamodb.AttributeValue
		err error
	)

	typ := rv.Type()

	for i := 0; i < rv.NumField(); i++ {
		// gets us the struct field
		ft := typ.Field(i)
		fc := parseTag(ft.Tag.Get(FieldStructTagName))

		// skip unexported or explicitly ignored fields
		if len(ft.PkgPath) != 0 || fc.Skip || shouldOmit(rv, fc.OmitZero, fc.OmitNil) {
			continue
		}

		val := rv.Field(i)

		if fc.Embed {
			// treat this object as a embedded struct or map
			fv := indirect(val, false)
			switch fv.Kind() {
			case reflect.Struct:
				if err := addStructToRecord(fv, item, fc.Prepend, fc.Append); err != nil {
					return &dyno.Error{
						Code: dyno.ErrEncodingEmbeddedStructMarshalFailed,
						Message: fmt.Sprintf("addStructToRecord on embedded struct field '%s' failed with error: %v",
							ft.Name, err),
					}
				}
			case reflect.Map:
				if fc.OmitNil && fv.IsNil() {
					continue
				}
				if fc.OmitZero && fv.IsZero() {
					continue
				}
				if err := addMapToRecord(fv, item, fc.Prepend, fc.Append); err != nil {
					return &dyno.Error{
						Code: dyno.ErrEncodingEmbeddedMapMarshalFailed,
						Message: fmt.Sprintf("addStructToRecord on embedded map field '%s' failed with error: %v",
							ft.Name, err),
					}
				}
			default:
				return &dyno.Error{
					Code:    dyno.ErrEncodingEmbeddedBadKind,
					Message: fmt.Sprintf("embedded kind %v is not suported", fv.Kind()),
				}
			}
			continue
		}
		// marshal value as normal
		av, err = marshalAv(val, fc.OmitZero, fc.OmitNil, fc.JSON)
		if err != nil {
			return err
		}
		if av != nil {
			fn := fieldName(fc.Name, ft.Name, prepend, append)
			item[fn] = av
		}
	}
	return nil
}

func shouldOmit(rv reflect.Value, omitZero, omitNil bool) bool {
	if omitZero {
		omitNil = true
	}
	return (omitNil && reflectValueIsNil(rv)) || (omitZero && reflectValueIsZero(rv))
}

func fieldName(name, fieldName, prepend, append string) string {
	if len(name) < 1 {
		name = fieldName
	}
	return prepend + name + append
}

func addMapToAv(input map[string]interface{}, item map[string]*dynamodb.AttributeValue) error {
	for name, val := range input {
		_av, err := marshalAv(reflect.ValueOf(val), false, false, false)
		if err != nil {
			return err
		}
		item[name] = _av
	}
	return nil
}

// Based on the enoding/JSON type reflect value type indirection in GoExec Stdlib
// https://golang.org/src/encoding/json/decode.go Indirect func.
func indirect(rv reflect.Value, decodingNil bool) reflect.Value {
	v0 := rv
	haveAddr := false

	if rv.Kind() != reflect.Ptr && rv.Type().Name() != "" && rv.CanAddr() {
		haveAddr = true
		rv = rv.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if rv.Kind() == reflect.Interface && !rv.IsNil() {
			e := rv.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodingNil || e.Elem().Kind() == reflect.Ptr) {
				haveAddr = false
				rv = e
				continue
			}
		}
		if rv.Kind() != reflect.Ptr {
			break
		}
		if decodingNil && rv.CanSet() {
			break
		}
		// Prevent infinite loop if rv is an interface pointing to its own address:
		//     var rv interface{}
		//     rv = &rv
		if rv.Elem().Kind() == reflect.Interface && rv.Elem().Elem() == rv {
			rv = rv.Elem()
			break
		}
		if rv.IsNil() {
			rv.Set(reflect.New(rv.Type().Elem()))
		}

		if haveAddr {
			rv = v0 // restore original value after round-trip Value.Addr().Elem()
			haveAddr = false
		} else {
			rv = rv.Elem()
		}
	}

	return rv
}

func reflectValueIsNil(rv reflect.Value) bool {
	for {
		if (rv.Kind() == reflect.Interface ||
			rv.Kind() == reflect.Ptr ||
			rv.Kind() == reflect.Map ||
			rv.Kind() == reflect.Slice) && rv.IsNil() {
			return true
		}
		if rv.Kind() != reflect.Ptr ||
			rv.Kind() != reflect.Interface ||
			rv.Kind() != reflect.Slice ||
			rv.Kind() != reflect.Map {
			break
		}
		rv = rv.Elem()
	}

	return false
}

func reflectValueIsZero(v reflect.Value) bool {
	ind := indirect(v, false)
	return reflect.DeepEqual(ind.Interface(), reflect.Zero(ind.Type()).Interface())
}

// FieldNames returns a string slice with the field names for the given input
func FieldNames(input interface{}) (names []string, err error) {
	names = make([]string, 0)
	if inputStrSlice, ok := input.([]string); ok {
		for _, n := range inputStrSlice {
			names = append(names, n)
			return
		}
	}
	names, err = appendFieldNames(names, reflect.ValueOf(input))
	return
}

func appendFieldNames(names []string, rv reflect.Value) (out []string, err error) {
	rv = indirect(rv, false)
	switch rv.Kind() {
	case reflect.Struct:
		out, err = appendStructFieldNames(names, rv, "", "")
		if err != nil {
			return nil, err
		}
	case reflect.Map:
		// if the map is already a map of dynamodb attribute values, add them as is
		out = appendMapFieldNames(names, rv, "", "")
	default:
		return nil, &dyno.Error{
			Code:    dyno.ErrEncodingEmbeddedBadKind,
			Message: fmt.Sprintf("input must be a struct or map, input is a %v", rv.Kind()),
		}
	}
	return
}

func appendStructFieldNames(names []string, rv reflect.Value, prependStr, appendStr string) ([]string, error) {
	var (
		err      error
		subNames []string
	)

	typ := rv.Type()

	for i := 0; i < rv.NumField(); i++ {
		// gets us the struct field
		ft := typ.Field(i)
		fc := parseTag(ft.Tag.Get(FieldStructTagName))
		if fc.Skip {
			// field is ignored
			continue
		}

		val := rv.Field(i)

		if fc.Embed {
			fv := indirect(val, false)
			subNames = []string{}

			switch fv.Kind() {
			case reflect.Struct:
				// treat this object as a embedded struct
				subNames, err = appendStructFieldNames(subNames, fv, fc.Prepend, fc.Append)
				if err != nil {
					return nil, &dyno.Error{
						Code: dyno.ErrEncodingEmbeddedStructMarshalFailed,
						Message: fmt.Sprintf("addStructFieldNames on embedded field '%s' failed with error: %v",
							ft.Name, err),
					}
				}
			case reflect.Map:
				subNames = appendMapFieldNames(subNames, fv, fc.Prepend, fc.Append)
			default:
				return nil, &dyno.Error{
					Code:    dyno.ErrEncodingEmbeddedBadKind,
					Message: fmt.Sprintf("embedded kind %v is not suported", fv.Kind()),
				}
			}

			names = append(names, subNames...)
			continue
		}
		names = append(names, fieldName(fc.Name, ft.Name, prependStr, appendStr))
	}
	return names, nil
}

func appendMapFieldNames(names []string, rv reflect.Value, appendStr, prependStr string) []string {
	iter := rv.MapRange()
	for iter.Next() {
		key := indirect(iter.Key(), false)
		keyStr := ToString(key.Interface())
		names = append(names, appendStr+keyStr+prependStr)
	}
	return names
}
