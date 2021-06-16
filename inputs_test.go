package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/stretchr/testify/suite"
	"testing"
)

type InputBuildersTestSuite struct {
	suite.Suite
	table               *Table
	sess                *Client
	testItems           []*TestItem
	testMarshalledItems []*TestItemMarshaller
}

func (suite *InputBuildersTestSuite) SetupTest() {
	suite.sess = CreateTestSession()
	fmt.Println("creating test table")
	suite.table = CreateTestTable(suite.sess)
	fmt.Println("test table created:", suite.table.String())
	suite.testItems = GetTestItems(10)
	suite.testMarshalledItems = GetMarshalledTestRecords(10)
}

func (suite *InputBuildersTestSuite) TearDownSuite() {
	fmt.Println("deleting test table", suite.table.Name())
	if err := suite.table.Delete(suite.sess); err != nil {
		panic(err)
	}
	if err := suite.sess.WaitUntilTableNotExists(suite.table.DescribeTableInput()).Await(); err != nil {
		panic(err)
	}
	fmt.Println("finished deleting test table", suite.table.Name())
}

func (suite *InputBuildersTestSuite) TestPutItems() {
	for _, item := range suite.testItems {
		avMap, err := encoding.MarshalMap(item)
		if err != nil {
			panic(err)
		}
		fmt.Println("putting item:\n", avMap)
		out, err := suite.table.PutItem(suite.sess, avMap).Await()
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
		out, err := suite.table.PutItem(suite.sess, avMap).Await()
		if err != nil {
			panic(err)
		}
		fmt.Println("successfully put marshalled item. output:\n", out.String())
	}
}

func (suite *InputBuildersTestSuite) TestScan() {
	scan := NewScanInput().SetTableName(suite.table.Name())

	fmt.Println("running scan:", scan.String())
	out, err := suite.sess.Scan(scan).Await()
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
	_, err = suite.sess.Scan(scan).Await()
	if err != nil {
		panic(err)
	}
	fmt.Println("scan results:", awsutil.Prettify(target))
	suite.Equal(len(target), len(suite.testItems)+len(suite.testMarshalledItems))
}

func TestInputBuildersSuite(t *testing.T) {
	// not using suite.Run() as we want to control the order
	s := new(InputBuildersTestSuite)
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
	s.SetupTest()
	s.Run("write ops", func() {
		s.TestPutItems()
	})
	s.Run("read ops", func() {
		s.TestScan()
	})
}
