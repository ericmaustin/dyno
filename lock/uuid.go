package lock

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
)

// UUID wraps uuid and provides custom dynamodb and json marshaling
type UUID uuid.UUID

//NewUUID creates a new uuid
func NewUUID() UUID {
	uid := UUID(uuid.New())
	return uid
}

//IsZero returns true if this uuid is nil or 0
func (u *UUID) IsZero() bool {
	return u == nil || u.Equal(UUID{})
}

// Equal compares whether the underlying values of the ``other`` UUID is equal to this UUID
func (u UUID) Equal(other UUID, others ...UUID) bool {
	return UUIDEqual(u, other, others...)
}

// ParseUUID takes a string and creates a UUID
func ParseUUID(in string) (UUID, error) {
	parsedUUID, err := uuid.Parse(in)
	if err != nil {
		// return empty uuid and the error
		return UUID{}, err
	}
	u := UUID(parsedUUID)
	return u, nil
}

//String returns the uuid as a string
func (u UUID) String() string {
	return uuid.UUID(u).String()
}

// UnmarshalDynamoDBAttributeValue for UUID attempts to parse raw as a string GetWithToken with uuid.Parse
func (u *UUID) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if av.NULL != nil && *av.NULL {
		return nil
	}
	// av is a String
	if av.S == nil {
		// av has invalid value
		return fmt.Errorf("unmarshall Error, %v is not a valid UUID attribute GetWithToken", av)
	}
	parsedUUID, err := ParseUUID(*av.S)
	if err != nil {
		return err
	}
	*u = parsedUUID
	return nil
}

// MarshalDynamoDBAttributeValue for UUID convers the underlying UUID to a string GetWithToken
func (u UUID) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	str := u.String()
	av.S = &str
	return nil
}

// MarshalJSON outputs the string GetWithToken for the uuid
func (u UUID) MarshalJSON() ([]byte, error) {
	return json.Marshal(uuid.UUID(u).String())
}

// UnmarshalJSON converts the string GetWithToken back into a uuid
func (u *UUID) UnmarshalJSON(b []byte) error {
	str := ""
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	id, err := uuid.Parse(str)
	if err != nil {
		return err
	}
	*u = UUID(id)
	return nil
}

//UUIDEqual returns true if the provided UUIDS are all equal
func UUIDEqual(a UUID, b UUID, others ...UUID) bool {
	others = append(others, b)
	for _, cmp := range others {
		if cmp.String() != a.String() {
			return false
		}
	}
	return true
}
