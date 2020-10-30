package dyno

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

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

// IsAwsErrorCode checks to see if the provided err is an aws error, and if so if it matches any of the provided codes
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
