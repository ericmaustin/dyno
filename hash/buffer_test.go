package hash

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
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

func TestQueryInputHash(t *testing.T) {
	qb := dyno.NewQueryBuilder(nil)
	qb.SetTableName("testTable")
	qb.AddFilter(condition.And(
		condition.Between("foo", 1, 2),
		condition.Equal("bar", 3),
	))
	qb.AddProjectionNames([]string{"foo", "bar"}...)
	qb.AddKeyCondition(condition.KeyEqual("baz", 4))

	input, err := qb.Build()
	if err != nil {
		panic(err)
	}

	buf := NewBuffer()
	buf.WriteQueryInput(input)
	fmt.Println(buf.String())
	fmt.Println(buf.SHA256String())
}
