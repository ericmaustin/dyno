package hash

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/operation"
	"testing"
)

func TestAvHash(t *testing.T) {
	av1 := &dynamodb.AttributeValue{
		S:    dyno.StringPtr("foo"),
	}

	av2 := &dynamodb.AttributeValue{
		SS:    []*string{
			dyno.StringPtr("foo"),
			dyno.StringPtr("bar"),
		},
	}

	av3 := &dynamodb.AttributeValue{
		M:    map[string]*dynamodb.AttributeValue{
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

func TestQueryInputHash(t *testing.T) {
	qb := operation.NewQueryBuilder()
	qb.AddFilter(condition.And(
		condition.Between("foo", 1, 2),
		condition.Equal("bar", 3),
	))
	qb.AddProjectionNames([]string{"foo", "bar"})
	qb.AddKeyCondition(condition.KeyEqual("baz", 4))

	input := qb.Build()

	buf := NewBuffer()
	buf.WriteQueryInput(input)
	fmt.Println(buf.String())
	fmt.Println(buf.SHA256String())
}

