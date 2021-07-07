package hash

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestAvHash(t *testing.T) {
	av1 := &types.AttributeValueMemberS{
		Value: "foo",
	}

	av2 := &types.AttributeValueMemberSS{
		Value: []string{
			"foo",
			"bar",
		},
	}

	av3 := &types.AttributeValueMemberM{
		Value: map[string]types.AttributeValue{
			"B": av1,
			"A": av2,
		},
	}

	buf1 := NewBuffer()
	buf1.WriteAttributeValue(av1)

	buf2 := NewBuffer()
	buf2.WriteAttributeValue(av2)

	buf3 := NewBuffer()
	buf3.WriteAttributeValue(av3)

	fmt.Println(buf1.String())
	fmt.Println(buf2.String())
	fmt.Println(buf3.String())
}

// todo: query, scan, etc. tests