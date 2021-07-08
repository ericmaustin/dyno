package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/stretchr/testify/suite"
	"testing"
)

// PoolTestSuite used to test pool operations
type ClientTestSuite struct {
	suite.Suite
	table               *Table
	client              *Client
	testItems           []*TestItem
	testMarshalledItems []*TestItemMarshaller
}

func (s *ClientTestSuite) SetupTest() {
	s.client = CreateTestClient()

	fmt.Println("creating test table")

	s.table = CreateTestTable(s.client)

	s.testItems = GetTestItems(10)
	s.testMarshalledItems = GetMarshalledTestRecords(10)
}

func (s *ClientTestSuite) TearDownSuite() {
	fmt.Println("deleting test table", s.table.Name())

	if _, err := s.table.Delete().Invoke(context.Background(), s.client.DynamoDB()).Await(); err != nil {
		panic(err)
	}

	if _, err := s.client.TableNotExistsWaiter(context.Background(), s.table.DescribeTableInput()).Await(); err != nil {
		panic(err)
	}

	fmt.Println("finished deleting test table", s.table.Name())
}

func (s *ClientTestSuite) TestBatchWriteItemWithMiddleWare() {
	items := encoding.MustMarshalMaps(s.testItems)

	var cached []*dynamodb.BatchWriteItemOutput

	cacheMW := BatchWriteItemAllMiddleWareFunc(func(next BatchWriteItemAllHandler) BatchWriteItemAllHandler {
		return BatchWriteItemAllHandlerFunc(func(ctx *BatchWriteItemAllContext, output *BatchWriteItemAllOutput) {
			if cached != nil {
				fmt.Println("cached!")
				output.Set(cached, nil)
				return
			}
			fmt.Println("not cached!")
			next.HandleBatchWriteItemAll(ctx, output)
			out, err := output.Get()
			if err != nil {
				panic(err)
			}
			fmt.Println("saving batch write to cache")
			cached = out
		})
	})

	op := s.table.BatchPut(items, cacheMW)
	s.Equal(PromisePending, op.GetState())
	op.Invoke(context.Background(), s.client.DynamoDB())
	s.Equal(PromiseWaiting, op.GetState())
	s.Greater(int64(op.Duration()), int64(0))
	out, err := op.Await()

	if err != nil {
		panic(err)
	}

	s.Equal(PromiseReady, op.GetState())

	s.NotNil(out)
	s.Greater(len(cached), 0)
	s.Equal(len(cached), len(out))
	// second call, should be cached
	out, err = s.table.BatchPut(items, cacheMW).Invoke(context.Background(), s.client.DynamoDB()).Await()
	if err != nil {
		panic(err)
	}

	s.NotNil(out)
	s.Greater(len(cached), 0)
	s.Equal(len(cached), len(out))
}

func (s *ClientTestSuite) TestScanWithMiddleware() {
	var cached []*dynamodb.ScanOutput

	cacheMW := ScanAllMiddleWareFunc(func(next ScanAllHandler) ScanAllHandler {
		return ScanAllHandlerFunc(func(ctx *ScanAllContext, output *ScanAllOutput) {
			if cached != nil {
				fmt.Println("cached!")
				output.Set(cached, nil)
				return
			}
			fmt.Println("not cached!")
			next.HandleScanAll(ctx, output)
			out, err := output.Get()
			if err != nil {
				panic(err)
			}
			fmt.Println("saving scan output to cache")
			cached = out
		})
	})

	s.Nil(cached)

	// first call, should not be cached
	input, err := s.table.ScanBuilder().
		AddProjectionNames("id", "timestamp").
		AddFilter(condition.GreaterThan("timestamp", 0)).
		SetTableName(s.table.Name()).
		Build()

	if err != nil {
		panic(err)
	}

	out, err := NewScanAll(input, cacheMW).Invoke(context.Background(), s.client.DynamoDB()).Await()
	if err != nil {
		panic(err)
	}

	s.NotNil(out)
	s.NotNil(cached)

	// second call, should be cached
	out, err = NewScanAll(input, cacheMW).Invoke(context.Background(), s.client.DynamoDB()).Await()
	if err != nil {
		panic(err)
	}

	s.NotNil(out)
	s.NotNil(cached)
	fmt.Println("CACHE RESULT:", MustYamlString(cached))
}

func (s *ClientTestSuite) TestQuery() {
	id1 := s.testItems[0].ID

	// first call, should not be cached
	input, err := s.table.QueryBuilder().
		AddProjectionNames("id", "timestamp").
		AddKeyEquals("id", id1).
		SetTableName(s.table.Name()).
		Build()

	if err != nil {
		panic(err)
	}

	out, err := NewQuery(input).Invoke(context.Background(), s.client.DynamoDB()).Await()
	if err != nil {
		panic(err)
	}

	s.NotNil(out)

	s.Equal(out.Items[0]["id"].(*ddb.AttributeValueMemberS).Value, id1)

	fmt.Println(MustYamlString(out))
}

func TestClientSuite(t *testing.T) {
	// not using suite.Run() as we want to control the order
	s := new(ClientTestSuite)
	s.SetT(t)

	defer func() {
		err := recover()
		// dont panic yet, we need to tear down the suite
		s.TearDownSuite()

		if err != nil {
			// ok, now you can panic
			panic(err)
		}
	}()

	// run tests in correct order...
	s.SetupTest()

	s.Run("write ops", func() {
		s.TestBatchWriteItemWithMiddleWare()
	})
	s.Run("read ops", func() {
		s.TestQuery()
		s.TestScanWithMiddleware()
	})
}

// todo: add more tests
