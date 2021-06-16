package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

//PoolTestSuite used to test pool operations
type PoolTestSuite struct {
	suite.Suite
	pool                *Pool
	table               *Table
	db                  *Client
	testItems           []*TestItem
	testMarshalledItems []*TestItemMarshaller
}

func (suite *PoolTestSuite) SetupTest() {
	suite.db = CreateTestSession()
	fmt.Println("creating test table")
	suite.table = CreateTestTable(suite.db)
	fmt.Println("test table created:", suite.table.String())
	suite.testItems = GetTestItems(10)
	suite.testMarshalledItems = GetMarshalledTestRecords(10)
	fmt.Println("test table created:", suite.table.String())
	suite.pool = NewPool(context.Background(), suite.db, 3)
}

func getLongRunningExecutionFunc() ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		time.Sleep(time.Second * 10)
		return nil, nil
	}
}

func (suite *PoolTestSuite) TearDownSuite() {
	fmt.Println("deleting test table", suite.table.Name())
	if err := suite.table.Delete(suite.db); err != nil {
		panic(err)
	}
	if err := suite.db.WaitUntilTableNotExists(suite.table.DescribeTableInput()).Await(); err != nil {
		panic(err)
	}
	fmt.Println("finished deleting test table", suite.table.Name())
}

func (suite *PoolTestSuite) TestPoolConcurrency() {
	promises := make(map[int]*Promise)
	for i := 0; i < 10; i++ {
		promises[i] = suite.pool.Do(getLongRunningExecutionFunc())
		suite.LessOrEqual(suite.pool.ActiveCount(), suite.pool.Limit())
		fmt.Println("pool active tasks", suite.pool.ActiveCount())
	}
	for i, p := range promises {
		if _, err := p.Await(); err != nil {
			fmt.Println("promise", i, "done")
		}
	}
}

func (suite *PoolTestSuite) TestPutItems() {
	for _, item := range suite.testItems {
		avMap, err := encoding.MarshalMap(item)
		if err != nil {
			panic(err)
		}
		fmt.Println("putting item:\n", avMap)
		out, err := suite.pool.PutItem(
			NewPutItemInput().
				SetTableName(suite.table.Name()).
				SetItem(avMap),
		).Await()
		if err != nil {
			panic(err)
		}
		fmt.Println("successfully put item. output:\n", out.String())
	}

	for _, item := range suite.testMarshalledItems {
		avMap, err := encoding.MarshalMap(item)
		if err != nil {
			panic(err)
		}
		fmt.Println("putting marshalled item:\n", avMap)
		out, err := suite.pool.PutItem(
			NewPutItemInput().
				SetTableName(suite.table.Name()).
				SetItem(avMap),
		).Await()
		if err != nil {
			panic(err)
		}
		fmt.Println("successfully put marshalled item. output:\n", out.String())
	}
}

func (suite *PoolTestSuite) TestScan() {
	scan := NewScanInput().SetTableName(suite.table.Name())

	fmt.Println("running scan:", scan.String())
	out, err := suite.pool.Scan(scan).Await()
	if err != nil {
		panic(err)
	}
	fmt.Println("scan result:", out.String())
	suite.Equal(len(out.Items), len(suite.testItems)+len(suite.testMarshalledItems))

	// test scan with unmarshalling
	scan = NewScanInput().SetTableName(suite.table.Name())
	var target []*TestItemMarshaller
	scan.SetOutputCallback(func(ctx context.Context, output *dynamodb.ScanOutput) error {
		if len(output.Items) > 0 {
			for _, item := range output.Items {
				itemTarget := new(TestItemMarshaller)
				if err := encoding.UnmarshalMap(item, itemTarget); err != nil {
					return err
				}
				target = append(target, itemTarget)
			}
		}
		return nil
	})
	fmt.Println("running scan with unmarshalling:", scan.String())
	_, err = suite.pool.Scan(scan).Await()
	if err != nil {
		panic(err)
	}
	fmt.Println("scan results:", awsutil.Prettify(target))
	suite.Equal(len(target), len(suite.testItems)+len(suite.testMarshalledItems))

}

func TestPoolSuite(t *testing.T) {
	// not using suite.Run() as we want to control the order
	s := new(PoolTestSuite)
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
	s.Run("concurrency", func() {
		s.TestPoolConcurrency()
	})
	s.Run("write ops", func() {
		s.TestPutItems()
	})
	s.Run("read ops", func() {
		s.TestScan()
	})
}
