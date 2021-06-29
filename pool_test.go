package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
	"time"
)

// PoolTestSuite used to test pool operations
type PoolTestSuite struct {
	suite.Suite
	pool                *Pool
	table               *Table
	db                  *Client
	testItems           []*TestItem
	testMarshalledItems []*TestItemMarshaller
}

func (suite *PoolTestSuite) SetupTest() {
	suite.db = CreateTestClient()

	fmt.Println("creating test table")

	suite.table = CreateTestTable(suite.db)

	suite.testItems = GetTestItems(10)
	suite.testMarshalledItems = GetMarshalledTestRecords(10)

	suite.pool = NewPool(context.Background(), suite.db.DynamoDB(), 3)
}

func getLongRunningExecutionFunc(id int, wg *sync.WaitGroup) OperationF {
	return func(_ context.Context, db *dynamodb.Client) {
		defer wg.Done()

		fmt.Printf("op [%d] started\n", id)
		time.Sleep(time.Second * 1)
		fmt.Printf("op [%d] is done\n", id)
	}
}

func (suite *PoolTestSuite) TearDownSuite() {
	fmt.Println("deleting test table", suite.table.Name())

	if _, err := suite.table.Delete().Invoke(context.Background(), suite.db.DynamoDB()).Await(); err != nil {
		panic(err)
	}

	if _, err := suite.db.TableNotExistsWaiter(context.Background(), suite.table.DescribeTableInput()).Await(); err != nil {
		panic(err)
	}

	fmt.Println("finished deleting test table", suite.table.Name())
}

func (suite *PoolTestSuite) TestPoolConcurrency() {
	ops := make([]Operation, 10)

	var wg sync.WaitGroup

	cnt := 10

	wg.Add(cnt)

	for i := 0; i < cnt; i++ {
		ops[i] = getLongRunningExecutionFunc(i, &wg)
	}

	for _, op := range ops {
		if err := suite.pool.Do(op); err != nil {
			panic(err)
		}

		assert.LessOrEqual(suite.T(), suite.pool.ActiveCount(), uint64(3))
	}

	// wait for all operations to be done
	wg.Wait()
}

func (suite *PoolTestSuite) TestPutItems() {
	for _, item := range suite.testItems {
		avMap, err := encoding.MarshalMap(item)

		if err != nil {
			panic(err)
		}

		fmt.Println("putting item:", MustYamlString(avMap))

		op := NewPutItem(NewPutItemInput(suite.table.TableName, avMap))

		if err = suite.pool.Do(op); err != nil {
			panic(err)
		}

		out, err := op.Await()
		if err != nil {
			panic(err)
		}

		fmt.Println("successfully put item. output:\n", MustYamlString(out))
	}

	for _, item := range suite.testMarshalledItems {
		avMap, err := encoding.MarshalMap(item)

		if err != nil {
			panic(err)
		}

		fmt.Println("putting marshalled item:", MustYamlString(avMap))

		op := NewPutItem(NewPutItemInput(suite.table.TableName, avMap))

		if err = suite.pool.Do(op); err != nil {
			panic(err)
		}

		out, err := op.Await()

		if err != nil {
			panic(err)
		}

		fmt.Println("successfully put marshalled item. output:\n", MustYamlString(out))
	}
}
//
//func (suite *PoolTestSuite) TestScan() {
//	scan := NewScanInput(suite.table.TableName)
//
//	fmt.Println("running scan:", MustYamlString(scan))
//
//	out, err := suite.pool.Scan(scan).Await()
//
//	if err != nil {
//		panic(err)
//	}
//
//	fmt.Println("scan result:\n", MustYamlString(out))
//
//	suite.Equal(len(out.Items), len(suite.testItems)+len(suite.testMarshalledItems))
//
//	// test scan with unmarshalling
//	scan = NewScanInput(suite.table.TableName)
//
//	var target []*TestItemMarshaller
//
//	cb := ScanOutputCallbackF(func(ctx context.Context, output *dynamodb.ScanOutput) error {
//		if len(output.Items) > 0 {
//			for _, item := range output.Items {
//				itemTarget := new(TestItemMarshaller)
//				if err := encoding.unmarshalMap(item, itemTarget); err != nil {
//					return err
//				}
//				target = append(target, itemTarget)
//			}
//		}
//		return nil
//	})
//
//	fmt.Println("running scan with unmarshalling:", MustYamlString(scan))
//
//	if out, err = suite.pool.Scan(scan, ScanWithOutputCallback(cb)).Await(); err != nil {
//		panic(err)
//	}
//
//	fmt.Println("scan results:\n", MustYamlString(out))
//
//	suite.Equal(len(target), len(suite.testItems)+len(suite.testMarshalledItems))
//}

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
	//s.Run("read ops", func() {
	//	s.TestScan()
	//})
}
