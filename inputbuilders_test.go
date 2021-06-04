package dyno

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/stretchr/testify/suite"
	"testing"
)

type InputBuildersTestSuite struct {
	suite.Suite
	table               *Table
	sess                *Session
	testItems           []*TestItem
	testMarshalledItems []*MarshalledTestItem
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
	err := <-suite.table.Delete(suite.sess.Request())
	if err != nil {
		panic(err)
	}
	fmt.Println("finished deleting test table", suite.table.Name())
}

func (suite *InputBuildersTestSuite) TestPutItems() {
	for _, item := range suite.testItems {
		avMap, err := encoding.MarshalItem(item)
		if err != nil {
			panic(err)
		}
		fmt.Println("putting item:\n", avMap)
		out, err := suite.table.PutItem(suite.sess.Request(), avMap)
		if err != nil {
			panic(err)
		}
		fmt.Println("successfully put item. output:\n", out.String())
	}

	for _, item := range suite.testMarshalledItems {
		avMap, err := encoding.MarshalItem(item)
		if err != nil {
			panic(err)
		}
		fmt.Println("putting marshalled item:\n", avMap)
		out, err := suite.table.PutItem(suite.sess.Request(), avMap)
		if err != nil {
			panic(err)
		}
		fmt.Println("successfully put marshalled item. output:\n", out.String())
	}
}

func (suite *InputBuildersTestSuite) TestScan() {
	scan := NewScanBuilder(suite.table.TableName)
	input, err := scan.Build()
	if err != nil {
		panic(err)
	}
	fmt.Println("running scan:", input.String())
	out, err := suite.sess.Request().Scan(input, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("scan result:", out.String())
	suite.Equal(len(out.Items), len(suite.testItems)+len(suite.testMarshalledItems))

	// test scan with unmarshalling
	scan = NewScanBuilder(suite.table.TableName)
	input, err = scan.Build()
	if err != nil {
		panic(err)
	}
	fmt.Println("running scan with unmarshalling:", input.String())
	var target []*MarshalledTestItem
	_, err = suite.sess.Request().Scan(input, ScanUnmarshaller(&target))
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
