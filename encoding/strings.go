package encoding

import (
	"fmt"
	"reflect"
)

// ToString gets the base string value of given string
// if input is a string or string ptr then the string is returned
// if the input implements fmt.Stringer, then .String() is called
// panics otherwise
func ToString(input interface{}) string {
	switch v := input.(type) {
	case string:
		return v
	case *string:
		return *v
	case fmt.Stringer:
		return input.(fmt.Stringer).String()
	}
	panic(fmt.Errorf("not a string or does not implement fmt.Stringer"))
}

// ToStringSlice converts an arbitrary input into a slice of strings
// panics if input is not a slice or does not contain elements that can be converted to a string
func ToStringSlice(input interface{}) []string {
	rv := Indirect(reflect.ValueOf(input), false)

	if rv.Kind() != reflect.Slice {
		panic(fmt.Errorf("cannot covert non-slice kind: %v", rv.Kind()))
	}

	out := make([]string, rv.Len())

	for i := 0; i < rv.Len(); i++ {
		out[i] = ToString(rv.Index(i).Interface())
	}
	return out
}
