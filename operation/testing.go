package operation

import (
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/logging"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	//"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/table"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/sirupsen/logrus"
	"time"
)

type testEmbeddedItem struct {
	SubID    int
	SubField string
}

type testItem struct {
	ID        string            `dyno:"id"`
	TestField string            `dyno:"test_field"`
	Embedded  *testEmbeddedItem `dyno:",*"`
}

const testTableName = "__operation_testing"

func createTestSession() *dyno.Session {
	log := logging.New()
	log.SetLevel(logrus.DebugLevel)

	// create the session
	awsSess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: dyno.StringPtr("us-east-1"),
		},
		Profile:           "mt2_dev",
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		panic(err)
	}
	/* get a session */
	sess := dyno.New(awsSess).
		SetMaxTimeout(time.Minute).
		SetLogger(log)

	return sess
}

func createTestTable(sess *dyno.Session) {
	// set up the table
	tblInput := &dynamodb.CreateTableInput{
		TableName:   dyno.StringPtr(testTableName),
		BillingMode: dyno.StringPtr("PAY_PER_REQUEST"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: dyno.StringPtr("id"),
				AttributeType: dyno.StringPtr("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: dyno.StringPtr("id"),
				KeyType:       dyno.StringPtr("HASH"),
			},
		},
	}

	err := CreateTable(tblInput).SetWait(true).Execute(sess.Request()).Error()
	if err != nil {
		panic(err)
	}
	return
}

func destroytestTable(sess *dyno.Session) {
	err := DeleteTable(testTableName).
		Execute(sess.Request()).
		Error()

	if err != nil {
		panic(err)
	}

	// wait for table to be deleted
	err = WaitForTableDeletion(sess.Request(), testTableName, nil)
	if err != nil {
		panic(err)
	}
}

func putTestKeys(items []*testItem) []map[string]string {
	out := make([]map[string]string, len(items))
	for i, item := range items {
		out[i] = map[string]string{
			"id": item.ID,
		}
	}
	return out
}

func putTestRecords(sess *dyno.Session) []*testItem {
	testRecords := []*testItem{
		{
			ID:        "A",
			TestField: "A field",
			Embedded: &testEmbeddedItem{
				SubID:    1,
				SubField: "SubA",
			},
		},
		{
			ID:        "B",
			TestField: "B field",
			Embedded: &testEmbeddedItem{
				SubID:    2,
				SubField: "SubB",
			},
		},
		{
			ID:        "C",
			TestField: "C field",
			Embedded: &testEmbeddedItem{
				SubID:    3,
				SubField: "SubC",
			},
		},
		{
			ID:        "D",
			TestField: "D field",
			Embedded: &testEmbeddedItem{
				SubID:    4,
				SubField: "SubD",
			},
		},
		{
			ID:        "E",
			TestField: "E field",
			Embedded: &testEmbeddedItem{
				SubID:    5,
				SubField: "SubE",
			},
		},
	}

	batchWriteInput := NewBatchWriteBuilder(nil).AddPuts(testTableName, testRecords).Input()
	err := BatchWrite(batchWriteInput).SetConcurrency(5).Execute(sess.Request()).Error()
	if err != nil {
		panic(err)
	}
	return testRecords
}
