package encoding

import (
	"fmt"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntMarshaller(t *testing.T) {
	v := 0

	encoder := IntMarshaler(&v, NilNull)

	av, err := encoder.MarshalDynamoDBAttributeValue()

	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%d", v), av.(*ddb.AttributeValueMemberN).Value)

	var vPtr *int

	encoder = IntMarshaler(vPtr, NilZero)
	av, err = encoder.MarshalDynamoDBAttributeValue()

	assert.NoError(t, err)
	assert.Equal(t, "0", av.(*ddb.AttributeValueMemberN).Value)

	encoder = IntMarshaler(vPtr, NilError)
	av, err = encoder.MarshalDynamoDBAttributeValue()
	assert.Error(t, err)

	fmt.Println("encoder error:", err)
}
