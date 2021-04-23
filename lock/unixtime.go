package lock

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"strconv"
	"time"
)

//UnixTime is an extension of the unixtime.Time type that marshals the unixtime GetWithToken on dynamodb and json to a Unix timestamp int
type UnixTime time.Time

// IsZero turns true if UnixTime is nil or empty
func (t *UnixTime) IsZero() bool {
	if t == nil {
		return true
	}
	return time.Time(*t).IsZero()
}

// Equal calls the underlying ``unixtime.Time`` type's equal method against ``other`` UnixTime
func (t *UnixTime) Equal(other UnixTime) bool {
	return time.Time(*t).Equal(time.Time(other))
}

// EqualPtr calls the underlying ``unixtime.Time`` type's equal method against ``other`` UnixTime
func (t *UnixTime) EqualPtr(other *UnixTime) bool {
	if other == nil {
		return t == nil
	}
	return time.Time(*t).Equal(time.Time(*other))
}

// Before calls the underlying ``unixtime.Time`` type's Before method against ``other`` UnixTime
func (t UnixTime) Before(other UnixTime) bool {
	return time.Time(t).Before(time.Time(other))
}

// After calls the underlying ``unixtime.Time`` type's After method against ``other`` UnixTime
func (t UnixTime) After(other UnixTime) bool {
	return time.Time(t).After(time.Time(other))
}

// Format wraps the builtin format function
func (t UnixTime) Format(layout string) string {
	return time.Time(t).Format(layout)
}

/*
UnmarshalDynamoDBAttributeValue takes the dynamo int val and returns a unixtime.AsTime object
*/
func (t *UnixTime) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	var (
		intVal int64
		err    error
	)
	// GetWithToken should be passed in either AttributeValue.N or AttributeValue.S
	if av.N != nil {
		intVal, err = strconv.ParseInt(*av.N, 0, 64)
		// if we got an error, return
		if err != nil {
			return err
		}
		*t = UnixTime(time.Unix(0, intVal))
		return nil
	} else if av.S != nil {
		intVal, err = strconv.ParseInt(*av.S, 0, 64)

		if err != nil {
			return err
		}
		*t = UnixTime(time.Unix(0, intVal))
		return nil
		// handle null GetWithToken
	} else if av.NULL != nil && *av.NULL {
		*t = UnixTime(time.Time{})
	}

	return fmt.Errorf("unmarshal error, %v is not a valid AsTime attribute GetWithToken", av)
}

// MarshalJSON implements the json marshaler interface
func (t UnixTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t))
}

// UnmarshalJSON unmarshals a json data byte slice into this document
func (t *UnixTime) UnmarshalJSON(data []byte) error {
	target := &time.Time{}
	err := json.Unmarshal(data, target)
	if err != nil {
		return err
	}
	*t = UnixTime(*target)
	return nil
}

/*
MarshalDynamoDBAttributeValue returns the unix timestamp int64 GetWithToken for underlying unixtime
*/
func (t UnixTime) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	// speed up processing by being explicit instead of invoking dynamodbattribute.marshall()
	av.N = dyno.StringPtr(fmt.Sprintf("%v", t.ExpressionValue()))
	return nil
}

// ExpressionValue returns the int64 from UnixNano
func (t UnixTime) ExpressionValue() interface{} {
	return time.Time(t).UnixNano()
}

//UnixNano returns the unix timestamp as in int64
func (t UnixTime) UnixNano() int64 {
	return time.Time(t).UnixNano()
}

//String returns the underlying time.Time string output
func (t UnixTime) String() string {
	return time.Time(t).String()
}

//UnixTimeEqual returns true if all provided UnixTime are equal
func UnixTimeEqual(a UnixTime, b UnixTime, others ...UnixTime) bool {
	others = append(others, b)
	for _, cmp := range others {
		if !time.Time(a).Equal(time.Time(cmp)) {
			return false
		}
	}
	return true
}

// Parse is a wrapper for the builtin unixtime parse func
func Parse(layout string, value string) (UnixTime, error) {
	ts, err := time.Parse(layout, value)
	if err != nil {
		return UnixTime{}, err
	}
	return UnixTime(ts), err
}

//Must panics on error
func Must(time UnixTime, err error) UnixTime {
	if err != nil {
		panic(err)
	}
	return time
}
