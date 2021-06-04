package encoding

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

//ItemUnmarshaller allows more control over decoding structs from attribute value maps
type ItemUnmarshaller interface {
	UnmarshalItem(avMap map[string]*dynamodb.AttributeValue) error
}

var itemUnmarshallerReflectType = reflect.TypeOf((*ItemUnmarshaller)(nil)).Elem()

// UnmarshalItems decodes a slice of items into the given input that must be a ptr to a slice
func UnmarshalItems(items []map[string]*dynamodb.AttributeValue, input interface{}) error {
	if len(items) < 1 {
		return nil
	}

	rv := reflect.ValueOf(input)

	if rv.Kind() != reflect.Ptr {
		return errors.New("reflect value is not a Ptr")
	}

	sliceVal := rv.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return errors.New("reflect value is not a Slice")
	}

	// check if unmarshaller
	if sliceVal.Type().Elem().Implements(itemUnmarshallerReflectType) {
		for _, item := range items {
			indirectType, steps := IndirectType(sliceVal.Type().Elem())
			// create a new value from the slice's element type
			target := reflect.New(indirectType)
			// run the unmarshaller
			if err := target.Interface().(ItemUnmarshaller).UnmarshalItem(item); err != nil {
				return err
			}
			for i := 0; i < steps-1; i++ {
				//add ptrs till we go back to the same number of ptrs as target should have
				newTarget := reflect.New(target.Type())
				newTarget.Elem().Set(target)
				target = newTarget
			}
			sliceVal.Set(reflect.Append(sliceVal, target))
		}
		return nil
	}

	// check if slice of ptrs
	if sliceVal.Type().Elem().Kind() == reflect.Ptr {
		for _, item := range items {
			// create a new value from the slice's element type
			target := reflect.New(sliceVal.Type().Elem()).Elem()
			target = reflect.New(Indirect(target, false).Type())
			if err := unmarshalItemToValue(item, target); err != nil {
				return err
			}
			sliceVal.Set(reflect.Append(sliceVal, target))
		}
		return nil
	}

	// slice of non-ptrs
	for _, item := range items {
		// create a new value from the slice's element type
		target := reflect.New(sliceVal.Type().Elem())
		target = reflect.New(Indirect(target, false).Type())
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
	if unmarshaller, ok := input.(ItemUnmarshaller); ok {
		return unmarshaller.UnmarshalItem(item)
	}
	return unmarshalItemToValue(item, reflect.ValueOf(input))
}

func unmarshalItemToValue(rec map[string]*dynamodb.AttributeValue, rv reflect.Value) error {
	if rv.Kind() != reflect.Ptr {
		return errors.New("reflect value is not a Ptr")
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
		return errors.New("reflect value is not a Struct or Map")
	}
	return nil
}

func unmarshalItemToStruct(item map[string]*dynamodb.AttributeValue, rv reflect.Value, prepend, append string) error {
	if rv.Kind() != reflect.Ptr {
		return errors.New("reflect value is not a Ptr")
	}
	if rv.Elem().Kind() != reflect.Struct {
		return errors.New("reflect value is not a Struct")
	}
	if item == nil {
		return nil
	}

	typ := rv.Elem().Type()

	for i := 0; i < rv.Elem().NumField(); i++ {

		// gets us the struct field
		ft := typ.Field(i)
		fc := parseTag(ft.Tag.Get(FieldStructTagName))

		// skip unexported or explicitly ignored fields
		if len(ft.PkgPath) != 0 || fc.Skip {
			continue
		}

		fn := fieldName(fc.Name, ft.Name, prepend, append)

		if fc.Embed {
			// treat this object as a embedded struct
			fv := Indirect(rv.Elem().Field(i), false)

			switch fv.Kind() {
			case reflect.Struct:
				targetVal := reflect.New(fv.Type())
				if err := unmarshalItemToStruct(item, targetVal, fc.Prepend, fc.Append); err != nil {
					return err
				}
				fv.Set(Indirect(targetVal, false))
			case reflect.Map:
				if err := unmarshalItemToEmbededMap(item, fv, fc.Prepend, fc.Append); err != nil {
					return err
				}
			default:
				return errors.New("reflect value is not a Struct or Map")
			}

			continue
		}

		if _, ok := item[fn]; !ok {
			// skip if field doesn't exist
			continue
		}

		av := item[fn]

		if err := unmarshalItem(av, rv.Elem().Field(i), fc.JSON); err != nil {
			return err
		}
	}
	return nil
}

// unmarshalItemToEmbededMap rv must be a map and must have index keys set that match the avMap
func unmarshalItemToEmbededMap(item map[string]*dynamodb.AttributeValue, rv reflect.Value, prepend, append string) error {

	if rv.Kind() != reflect.Map {
		return errors.New("reflect value is not a Map")
	}
	if item == nil {
		return nil
	}

	if rv.Type().Key().Kind() != reflect.String {
		return errors.New("reflect value map key is not a String")
	}

	for _, key := range rv.MapKeys() {
		keyStr := prepend + ToString(Indirect(key, false)) + append

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

func unmarshalItem(item *dynamodb.AttributeValue, rv reflect.Value, fromJSON bool) error {
	if item.NULL != nil && *item.NULL {
		// ignore nil values
		return nil
	}

	rv = Indirect(rv, false)

	target := reflect.New(rv.Type())

	if fromJSON {
		if item.S == nil || len(*item.S) < 1 {
			// string is empty, no json data to unmarshal
			return nil
		}
		if err := json.Unmarshal([]byte(*item.S), target.Interface()); err != nil {
			return err
		}
		rv.Set(Indirect(target, false))
		return nil
	}

	if err := dynamodbattribute.Unmarshal(item, target.Interface()); err != nil {
		return err
	}
	rv.Set(Indirect(target, false))
	return nil
}
