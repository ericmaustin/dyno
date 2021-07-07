package encoding

import (
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v2"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// MapUnmarshaller allows more control over decoding structs from attribute value maps
type MapUnmarshaller interface {
	UnmarshalAttributeValueMap(avMap map[string]ddb.AttributeValue) error
}

var itemUnmarshallerReflectType = reflect.TypeOf((*MapUnmarshaller)(nil)).Elem()

// UnmarshalMaps decodes a slice of AttributeValue maps into the given input that must be a ptr to a slice
func UnmarshalMaps(items []map[string]ddb.AttributeValue, input interface{}) error {
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
			if err := target.Interface().(MapUnmarshaller).UnmarshalAttributeValueMap(item); err != nil {
				return err
			}

			for i := 0; i < steps-1; i++ {
				// add ptrs till we go back to the same number of ptrs as target should have
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

			if err := unmarshalMapToValue(item, target); err != nil {
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

		if err := unmarshalMapToValue(item, target); err != nil {
			return err
		}

		sliceVal.Set(reflect.Append(sliceVal, target))
	}

	return nil
}

// UnmarshalMap unmarshals a map of dynamodb attribute values into given input struct or map
// if the item is a map, the map must have the keys already set in the map
func UnmarshalMap(item map[string]ddb.AttributeValue, input interface{}) error {
	if unmarshaller, ok := input.(MapUnmarshaller); ok {
		return unmarshaller.UnmarshalAttributeValueMap(item)
	}

	return unmarshalMapToValue(item, reflect.ValueOf(input))
}

func unmarshalMapToValue(rec map[string]ddb.AttributeValue, rv reflect.Value) error {
	if rv.Kind() != reflect.Ptr || rv.IsNil() || !rv.IsValid() {
		return errors.New("reflect value is not valid")
	}

	switch rv.Elem().Kind() {
	case reflect.Struct:
		if err := unmarshalMapToStruct(rec, rv, "", ""); err != nil {
			return err
		}
	case reflect.Map:
		if err := unmarshalMapToEmbeddedMap(rec, rv, "", ""); err != nil {
			return err
		}
	default:
		return errors.New("reflect value is not a Struct or Map")
	}

	return nil
}

func unmarshalMapToStruct(item map[string]ddb.AttributeValue, rv reflect.Value, prepend, append string) error {
	if rv.Kind() != reflect.Ptr {
		return errors.New("reflect value is not a Ptr")
	}

	if rv.Elem().Kind() != reflect.Struct {
		return errors.New("reflect value is not a Struct")
	}

	if item == nil {
		return nil
	}

	var (
		ft  reflect.StructField
		fc  *fieldConfig
		err error
	)

	typ := rv.Elem().Type()

	for i := 0; i < rv.Elem().NumField(); i++ {
		// gets us the struct field
		ft = typ.Field(i)
		if fc, err = parseTag(ft.Tag.Get(FieldStructTagName)); err != nil {
			return err
		}

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
				if err = unmarshalMapToStruct(item, targetVal, fc.Prepend, fc.Append); err != nil {
					return err
				}

				fv.Set(Indirect(targetVal, false))

			case reflect.Map:
				if err = unmarshalMapToEmbeddedMap(item, fv, fc.Prepend, fc.Append); err != nil {
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

		if err = unmarshalMap(av, rv.Elem().Field(i), fc); err != nil {
			return err
		}
	}

	return nil
}

// unmarshalMapToEmbeddedMap rv must be a map and must have index keys set that match the avMap
func unmarshalMapToEmbeddedMap(item map[string]ddb.AttributeValue, rv reflect.Value, prepend, append string) error {

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
			if err := unmarshalMap(item[keyStr], target, new(fieldConfig)); err != nil {
				return err
			}

			rv.SetMapIndex(key, target.Elem())
		}
	}

	return nil
}

func unmarshalMap(item ddb.AttributeValue, rv reflect.Value, conf *fieldConfig) error {
	if item == nil {
		return nil
	}

	if _, ok := item.(*ddb.AttributeValueMemberNULL); ok {
		// skip null types
		return nil
	}

	rv = Indirect(rv, false)

	target := reflect.New(rv.Type())

	switch conf.MarshalAs {
	case MarshalAsJSON:
		sMember, ok := item.(*ddb.AttributeValueMemberS)
		if !ok || len(sMember.Value) < 1 {
			// string is empty, no json data to unmarshal
			return nil
		}

		if err := json.Unmarshal([]byte(sMember.Value), target.Interface()); err != nil {
			return err
		}

		rv.Set(Indirect(target, false))

		return nil

	case MarshalAsYAML:
		sMember, ok := item.(*ddb.AttributeValueMemberS)
		if !ok || len(sMember.Value) < 1 {
			// string is empty, no json data to unmarshal
			return nil
		}

		if err := yaml.Unmarshal([]byte(sMember.Value), target.Interface()); err != nil {
			return err
		}

		rv.Set(Indirect(target, false))

		return nil
	}

	if err := attributevalue.Unmarshal(item, target.Interface()); err != nil {
		return err
	}

	rv.Set(Indirect(target, false))

	return nil
}
