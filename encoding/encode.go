package encoding

import (
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v2"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// MapMarshaler allows more control over encoding types to attribute value maps
type MapMarshaler interface {
	MarshalAttributeValueMap(map[string]types.AttributeValue) error
}

var mapMarshallerReflectType = reflect.TypeOf((*MapMarshaler)(nil)).Elem()

// MarshalMaps marshals an input slice into a slice of attribute value maps
// panics if the input's kind is not a slice
func MarshalMaps(input interface{}) ([]map[string]types.AttributeValue, error) {
	var records []map[string]types.AttributeValue

	if avMapSlice, ok := input.([]map[string]types.AttributeValue); ok {
		// input is already a slice of attribute value maps
		return avMapSlice, nil
	}

	if err := AppendItems(&records, input); err != nil {
		return nil, err
	}

	return records, nil
}

// MustMarshalMaps marshals an input slice into a slice of attribute value maps
// panics on error
func MustMarshalMaps(input interface{}) []map[string]types.AttributeValue {
	var records []map[string]types.AttributeValue
	if err := AppendItems(&records, input); err != nil {
		panic(err)
	}

	return records
}

// AppendItems encodes a given input and appends these items to the given slice of items
func AppendItems(items *[]map[string]types.AttributeValue, input interface{}) error {
	rv := Indirect(reflect.ValueOf(input), false)

	if rv.Kind() != reflect.Slice {
		return errors.New("reflect value is not a slice")
	}

	// if item marshaller then marshal
	if rv.Type().Elem().Implements(mapMarshallerReflectType) {
		for i := 0; i < rv.Len(); i++ {
			avMap := make(map[string]types.AttributeValue)
			if err := rv.Index(i).Interface().(MapMarshaler).MarshalAttributeValueMap(avMap); err != nil {
				return err
			}

			*items = append(*items, avMap)
		}

		return nil
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

// MarshalMap marshals a given input that is a map or a struct into an attribute value map
func MarshalMap(input interface{}) (map[string]types.AttributeValue, error) {
	if avMap, ok := input.(map[string]types.AttributeValue); ok {
		return avMap, nil
	}

	if marshaller, ok := input.(MapMarshaler); ok {
		avMap := make(map[string]types.AttributeValue)

		return avMap, marshaller.MarshalAttributeValueMap(avMap)
	}

	return marshalValueToRecord(reflect.ValueOf(input))
}

// MustMarshalMap marshals a given input that is a map or a struct into an attribute value map
// panics on error
func MustMarshalMap(input interface{}) map[string]types.AttributeValue {
	itemMap, err := MarshalMap(input)

	if err != nil {
		panic(err)
	}

	return itemMap
}

func marshalValueToRecord(rv reflect.Value) (map[string]types.AttributeValue, error) {
	avMap := make(map[string]types.AttributeValue)

	if err := addValuesToRecord(avMap, rv); err != nil {
		return nil, err
	}

	return avMap, nil
}

func addValuesToRecord(item map[string]types.AttributeValue, rv reflect.Value) error {
	var (
		err      error
		iFaceMap map[string]interface{}
	)

	rv = Indirect(rv, false)

	switch rv.Kind() {
	case reflect.Struct:
		if err := addStructToRecord(rv, item, "", ""); err != nil {
			return err
		}
	case reflect.Map:
		// if the map is already a map of dynamodb attribute values, add them as is
		if rv.Type() == reflect.TypeOf(item) {
			for _, key := range rv.MapKeys() {
				item[key.Interface().(string)] = rv.MapIndex(key).Interface().(types.AttributeValue)
			}
			return nil
		}

		if iFaceMap, err = mapToIfaceMap(rv, "", ""); err != nil {
			return err
		}

		if err = addMapToAv(iFaceMap, item); err != nil {
			return err
		}
	default:
		return errors.New("reflect value is not a Struct or Map")
	}

	return nil
}

func structToRecord(input interface{}) (map[string]types.AttributeValue, error) {
	av := make(map[string]types.AttributeValue)
	if err := addStructToRecord(Indirect(reflect.ValueOf(input), false), av, "", ""); err != nil {
		return nil, err
	}

	return av, nil
}

// addMapToRecord adds any map indexed by a string to the given attribute value map
func addMapToRecord(rv reflect.Value, item map[string]types.AttributeValue, prepend, append string) error {
	// if the map is already a map of dynamodb attribute values, add them as is
	if rv.Type() == reflect.TypeOf(item) {
		for k, v := range rv.Interface().(map[string]types.AttributeValue) {
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
		return nil, errors.New("reflect value is not Map")
	}

	iFaceMap := map[string]interface{}{}

	iter := rv.MapRange()
	for iter.Next() {
		key := Indirect(iter.Key(), false)
		iFaceMap[prepend+ToString(key.Interface())+append] = iter.Value().Interface()
	}

	return iFaceMap, nil
}

func marshalAv(rv reflect.Value, config *fieldConfig) (types.AttributeValue, error) {
	switch config.MarshalAs {
	case MarshalAsJSON:
		// we're encoding to json to marshal value with json marshaller and set value to string
		jsonBytes, err := json.Marshal(rv.Interface())
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberS{Value: string(jsonBytes)}, nil
	case MarshalAsYAML:
		yamlBytes, err := yaml.Marshal(rv.Interface())
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberS{Value: string(yamlBytes)}, nil
	}

	encoder := attributevalue.NewEncoder()

	return encoder.Encode(rv.Interface())
}

func addStructToRecord(rv reflect.Value, item map[string]types.AttributeValue, prepend, append string) error {

	var (
		av  types.AttributeValue
		err error
		fc  *fieldConfig
		ft  reflect.StructField
	)

	typ := rv.Type()

	for i := 0; i < rv.NumField(); i++ {
		// gets us the struct field
		ft = typ.Field(i)
		if fc, err = parseTag(ft.Tag.Get(FieldStructTagName)); err != nil {
			return err
		}

		// skip unexported or explicitly ignored fields
		if len(ft.PkgPath) != 0 || fc.Skip {
			continue
		}

		val := rv.Field(i)

		if fc.Embed {
			// treat this object as a embedded struct or map
			fv := Indirect(val, false)
			switch fv.Kind() {
			case reflect.Struct:
				if err = addStructToRecord(fv, item, fc.Prepend, fc.Append); err != nil {
					return err
				}
			case reflect.Map:
				if err = addMapToRecord(fv, item, fc.Prepend, fc.Append); err != nil {
					return err
				}
			default:
				return errors.New("embedded value is not a Struct or Map")
			}
			continue
		}

		// marshal value as normal
		if av, err = marshalAv(val, fc); err != nil {
			return err
		}

		if av != nil {
			fn := fieldName(fc.Name, ft.Name, prepend, append)
			item[fn] = av
		}
	}

	return nil
}

func fieldName(name, fieldName, prepend, append string) string {
	if len(name) < 1 {
		name = fieldName
	}
	return prepend + name + append
}

func addMapToAv(input map[string]interface{}, item map[string]types.AttributeValue) error {
	for name, val := range input {
		_av, err := marshalAv(reflect.ValueOf(val), new(fieldConfig))
		if err != nil {
			return err
		}
		item[name] = _av
	}

	return nil
}

// Indirect is Based on the enoding/JSON type reflect value type indirection in GoExec Stdlib
// https://golang.org/src/encoding/json/decode.go Indirect func.
func Indirect(rv reflect.Value, decodingNil bool) reflect.Value {
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

// IndirectType Based on the enoding/JSON type reflect value type indirection in GoExec Stdlib except
// used for reflect.Type
// https://golang.org/src/encoding/json/decode.go Indirect func.
func IndirectType(rt reflect.Type) (reflect.Type, int) {
	v0 := rt
	haveAddr := false
	steps := 0

	if rt.Kind() != reflect.Ptr && rt.Name() != "" {
		haveAddr = true
	}

	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if rt.Kind() == reflect.Interface {
			e := rt.Elem()
			if e.Kind() == reflect.Ptr && e.Elem().Kind() == reflect.Ptr {
				haveAddr = false
				rt = e
				steps++
				continue
			}
		}
		if rt.Kind() != reflect.Ptr {
			break
		}
		if rt.Elem().Kind() == reflect.Interface && rt.Elem().Elem() == rt {
			rt = rt.Elem()
			steps++
			break
		}
		if haveAddr {
			rt = v0 // restore original value after round-trip Value.Addr().Elem()
			haveAddr = false
		} else {
			rt = rt.Elem()
			steps++
		}
	}

	return rt, steps
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
	rv = Indirect(rv, false)

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
		return nil, errors.New("reflect value is not a Map or Struct")
	}

	return
}

func appendStructFieldNames(names []string, rv reflect.Value, prependStr, appendStr string) ([]string, error) {
	var (
		err      error
		subNames []string
		fc       *fieldConfig
		ft       reflect.StructField
	)

	typ := rv.Type()

	for i := 0; i < rv.NumField(); i++ {
		// gets us the struct field
		ft = typ.Field(i)
		if fc, err = parseTag(ft.Tag.Get(FieldStructTagName)); err != nil {
			return nil, err
		}
		if fc.Skip {
			// field is ignored
			continue
		}

		val := rv.Field(i)

		if fc.Embed {
			fv := Indirect(val, false)
			subNames = []string{}

			switch fv.Kind() {
			case reflect.Struct:
				// treat this object as a embedded struct
				subNames, err = appendStructFieldNames(subNames, fv, fc.Prepend, fc.Append)
				if err != nil {
					return nil, err
				}
			case reflect.Map:
				subNames = appendMapFieldNames(subNames, fv, fc.Prepend, fc.Append)
			default:
				return nil, errors.New("embedded reflect value is not a Struct or Map")
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
		key := Indirect(iter.Key(), false)
		keyStr := ToString(key.Interface())
		names = append(names, appendStr+keyStr+prependStr)
	}

	return names
}
