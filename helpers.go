package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"sort"
)

/*
StringSliceRemove removes a string element from the slice if it exists in the string slice
*/
func StringSliceRemove(stringSlice []string, remove string) []string {
	sort.Strings(stringSlice)

	i := sort.SearchStrings(stringSlice, remove)

	if i == 0 {
		// remove the first element
		return stringSlice[1:]
	}

	if i == len(stringSlice) {
		// nothing to remove
		return stringSlice
	}

	if i == len(stringSlice)-1 {
		// remove the last element
		return stringSlice[0 : len(stringSlice)-1]
	}

	// remove an element in between the first and last elements
	return append(stringSlice[:i], stringSlice[i-1:]...)
}

/*
StringSliceRemove inserts a string element into a slice of strings
*/
func StringSliceInsert(stringSlice []string, insert string) []string {
	i := sort.SearchStrings(stringSlice, insert)
	if i == len(stringSlice) || stringSlice[i] != insert {
		stringSlice = append(stringSlice, "")
		copy(stringSlice[i+1:], stringSlice[i:])
		stringSlice[i] = insert
	}

	return stringSlice
}

func ContextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func InterfaceSliceToStringSlice(ifaceSlice []interface{}) (strSlice []string, err error) {
	strSlice = make([]string, len(ifaceSlice))

	if len(ifaceSlice) > 0 {
		// getActive each fields by name
		i := 0
		for _, n := range ifaceSlice {
			str, err := GetStringFromInterface(n)
			if err != nil {
				return nil, err
			}
			strSlice[i] = str
			i++
		}
	}
	return
}

// GetStringFromInterface turns a string, string pointer, or type that implements the ftm.Stringer interface into a string
func GetStringFromInterface(in interface{}) (string, error) {
	switch s := in.(type) {
	case string:
		return s, nil
	case *string:
		return *s, nil
	case fmt.Stringer:
		return s.String(), nil
	default:
		return "", fmt.Errorf("input to GetStringFromInterface must be a string, string pointer, or implement the fmt.Stringer interface")
	}
}

// IsAwsErrCode checks to see if the provided err is an aws error, and if so if it matches any of the provided codes
func IsAwsErrorCode(err error, codes ...string) bool {
	if err == nil {
		return false
	}
	if awsErr, ok := err.(awserr.Error); ok {
		for _, code := range codes {
			if awsErr.Code() == code {
				return true
			}
		}
	}
	return false
}
