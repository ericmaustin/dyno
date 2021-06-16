package util

import (
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCopyAttributeValue(t *testing.T) {
	av1 := &ddb.AttributeValueMemberS{Value: "hello world"}
	newAv := CopyAttributeValue(av1)
	assert.Equal(t, av1.Value, newAv.(*ddb.AttributeValueMemberS).Value)
}
