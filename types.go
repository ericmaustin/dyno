package dyno

import (
	"reflect"
	"time"
)

// BoolPtr returns a ptr for the provided bool
func BoolPtr(b bool) *bool {
	return &b
}

// StringPtr returns a ptr for the provided string
func StringPtr(s string) *string {
	return &s
}

// TimePtr returns a ptr for the provided time
func TimePtr(t time.Time) *time.Time {
	return &t
}

// DurationPtr returns a ptr for the provided duration
func DurationPtr(dur time.Duration) *time.Duration {
	return &dur
}

// IntPtr returns a ptr for the provided int
func IntPtr(i int) *int {
	return &i
}

// Int64Ptr returns a ptr for the provided int64
func Int64Ptr(i int64) *int64 {
	return &i
}

// IsEmptyInterface checks if interface is empty
func IsEmptyInterface(x interface{}) bool {
	if x == nil {
		return true
	}
	return reflect.DeepEqual(x, reflect.Zero(reflect.TypeOf(x)).Interface())
}

// InterfaceValue returns the underlying value of the interface.
// The second returned value is True if interface was a ptr otherwise it's false
func InterfaceValue(v interface{}) interface{} {

	// first check if nil, if it is nil return nil
	if v == nil {
		return nil
	}

	val := reflect.ValueOf(v)

	if val.Type().Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		return val.Elem().Interface()
	}

	return val.Interface()
}
