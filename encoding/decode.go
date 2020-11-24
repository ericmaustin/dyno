package encoding

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/ericmaustin/dyno"
)

// UnmarshalItems decodes a slice of items into the given input that must be a ptr to a slice
func UnmarshalItems(items []map[string]*dynamodb.AttributeValue, input interface{}) error {
	rv := reflect.ValueOf(input)

	if rv.Kind() != reflect.Ptr {
		return &dyno.Error{
			Code: dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("cannot unmarshal to non-ptr kind: %v. Must be a ptr to a slice",
				rv.Kind()),
		}
	}

	sliceVal := rv.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return &dyno.Error{
			Code: dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("cannot unmarshal to a ptr to a %v. Must be a ptr to a slice",
				sliceVal.Kind()),
		}
	}

	for _, item := range items {
		// create a new value from the slice's element type
		var target reflect.Value
		if sliceVal.Type().Elem().Kind() == reflect.Ptr {
			target = reflect.New(sliceVal.Type().Elem()).Elem()
		} else {
			target = reflect.New(sliceVal.Type().Elem())
		}
		target = reflect.New(indirect(target, false).Type())
		if err := unmarshalItemToValue(item, target); err != nil {
			return err
		}
		sliceVal.Set(reflect.Append(sliceVal, target))
	}

	return nil
}

// UnmarshalItem unmarshals a map of dynamodb attribute values into given input struct or map
// if the item is a map, the map must have the keys already set in the map
func UnmarshalItem(item map[string]*dynamodb.AttributeValue, input interface{}) error {
	return unmarshalItemToValue(item, reflect.ValueOf(input))
}

func unmarshalItemToValue(rec map[string]*dynamodb.AttributeValue, rv reflect.Value) error {
	if rv.Kind() != reflect.Ptr {
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("input is not a ptr, it is a %v", rv.Kind()),
		}
	}
	switch rv.Elem().Kind() {
	case reflect.Struct:
		if err := unmarshalItemToStruct(rec, rv, "", ""); err != nil {
			return err
		}
	case reflect.Map:
		if err := unmarshalItemToEmbededMap(rec, rv, "", ""); err != nil {
			return err
		}
	default:
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("cannot unmarshal input kind %v", rv.Elem().Kind()),
		}
	}
	return nil
}

func unmarshalItemToStruct(item map[string]*dynamodb.AttributeValue, rv reflect.Value, prepend, append string) error {
	if rv.Kind() != reflect.Ptr {
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("input kind is not a ptr, it is a %v", rv.Kind()),
		}
	}
	if rv.Elem().Kind() != reflect.Struct {
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("input struct is not a struct, it is a %v", rv.Elem().Kind()),
		}
	}
	if item == nil {
		return nil
	}

	typ := rv.Elem().Type()

	for i := 0; i < rv.Elem().NumField(); i++ {

		// gets us the struct field
		ft := typ.Field(i)
		fc := parseTag(ft.Tag.Get(FieldStructTagName))
		if fc.Skip {
			// field is ignored
			continue
		}

		fn := fieldName(fc.Name, ft.Name, prepend, append)

		if fc.Embed {
			// treat this object as a embedded struct
			fv := indirect(rv.Elem().Field(i), false)

			switch fv.Kind() {
			case reflect.Struct:
				targetVal := reflect.New(fv.Type())
				if err := unmarshalItemToStruct(item, targetVal, fc.Prepend, fc.Append); err != nil {
					return &dyno.Error{
						Code: dyno.ErrEncodingEmbeddedStructUnmarshalFailed,
						Message: fmt.Sprintf("could not process embedded struct field '%s' failed with error: %v",
							ft.Name, err),
					}
				}
				fv.Set(indirect(targetVal, false))
			case reflect.Map:
				if err := unmarshalItemToEmbededMap(item, fv, fc.Prepend, fc.Append); err != nil {
					return &dyno.Error{
						Code: dyno.ErrEncodingEmbeddedMapUnmarshalFailed,
						Message: fmt.Sprintf("could not process embedded map field '%s' failed with error: %v",
							ft.Name, err),
					}
				}
			default:
				return &dyno.Error{
					Code:    dyno.ErrEncodingEmbeddedBadKind,
					Message: fmt.Sprintf("cannot process embedded value with kind '%v'", fv.Kind()),
				}
			}

			continue
		}

		if _, ok := item[fn]; !ok {
			// skip if field doesn't exist
			continue
		}

		av := item[fn]

		if err := unmarshalItem(av, rv.Elem().Field(i), fc.Json); err != nil {
			return err
		}
	}
	return nil
}

// unmarshalItemToMap rv must be a map and must have index keys set that match the avMap
func unmarshalItemToMap(item map[string]*dynamodb.AttributeValue, rv reflect.Value) error {
	rv = indirect(rv, false)
	if rv.Kind() != reflect.Map {
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("input map is not a map, it is a %v", rv.Kind()),
		}
	}
	if item == nil {
		return nil
	}

	if rv.IsNil() {
		newMap := reflect.MakeMapWithSize(rv.Type(), 0)
		rv.Set(newMap)
	}

	if rv.Type().Key().Kind() != reflect.String {
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("map keys are not a string, map keys are %v", rv.Type().Key().Kind()),
		}
	}

	for fn, av := range item {
		target := reflect.New(rv.Type().Elem())
		if err := unmarshalItem(av, target, false); err != nil {
			return err
		}
		rv.SetMapIndex(reflect.ValueOf(fn), target.Elem())
	}
	return nil
}

// unmarshalItemToEmbededMap rv must be a map and must have index keys set that match the avMap
func unmarshalItemToEmbededMap(item map[string]*dynamodb.AttributeValue, rv reflect.Value, prepend, append string) error {

	if rv.Kind() != reflect.Map {
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("input map is not a map, it is a %v", rv.Kind()),
		}
	}
	if item == nil {
		return nil
	}

	if rv.Type().Key().Kind() != reflect.String {
		return &dyno.Error{
			Code:    dyno.ErrEncodingBadKind,
			Message: fmt.Sprintf("map keys are not a string, map keys are %v", rv.Type().Key().Kind()),
		}
	}

	for _, key := range rv.MapKeys() {
		keyStr := prepend + ToString(indirect(key, false)) + append

		if _, ok := item[keyStr]; ok {
			target := reflect.New(rv.Type().Elem())
			if err := unmarshalItem(item[keyStr], target, false); err != nil {
				return err
			}
			rv.SetMapIndex(key, target.Elem())
		}
	}

	return nil
}

func unmarshalItem(item *dynamodb.AttributeValue, rv reflect.Value, fromJson bool) error {
	if item.NULL != nil && *item.NULL {
		// ignore nil values
		return nil
	}

	rv = indirect(rv, false)

	target := reflect.New(rv.Type())

	if fromJson {
		if item.S == nil || len(*item.S) < 1 {
			// string is empty, no json data to unmarshal
			return nil
		}
		if err := json.Unmarshal([]byte(*item.S), target.Interface()); err != nil {
			return err
		}
		rv.Set(indirect(target, false))
		return nil
	}

	switch rv.Kind() {
	case reflect.Struct:
		if item.M == nil || len(item.M) < 1 {
			// nothing to do, possibly wrong value type?
			return nil
		}
		if err := unmarshalItemToStruct(item.M, target, "", ""); err != nil {
			return err
		}
		// set the value
		rv.Set(indirect(target, false))
	case reflect.Map:
		if item.M == nil || len(item.M) < 1 {
			// nothing to do, possibly wrong value type?
			return nil
		}
		if err := unmarshalItemToMap(item.M, target); err != nil {
			return err
		}
		// set the value
		rv.Set(indirect(target, false))
	default:
		// unmarshal as normal
		//targetIface := target.Interface()
		if err := dynamodbattribute.Unmarshal(item, target.Interface()); err != nil {
			return err
		}
		rv.Set(indirect(target, false))
	}
	return nil
}
