package operation

import (
	"fmt"
	"github.com/ericmaustin/dyno"
	"github.com/stretchr/testify/suite"
	"testing"
)

type DeleteTestSuite struct {
	suite.Suite
	sess *dyno.Session
}

func (d *DeleteTestSuite) SetupSuite() {
	d.sess = createTestSession()
	createTestTable(d.sess)
	putTestRecords(d.sess)
}

func (d *DeleteTestSuite) TearDownSuite() {
	destroytestTable(d.sess)
}

func (d *DeleteTestSuite) TestDelete() {
	key := struct {
		ID string `dyno:"id"`
	}{
		"A",
	}

	// scan for records with no conditions
	deleteInput := NewDeleteBuilder(nil).
		SetTable(testTableName).
		SetKey(key).
		Input()

	fmt.Printf("delete input\n%+v\n", deleteInput)

	deleteOutput, err := Delete(deleteInput).
		Execute(d.sess.Request()).
		OutputError()

	d.NoError(err)
	d.NotNil(deleteOutput)

	fmt.Printf("delete output:\n%+v\n", deleteOutput)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestDeleteTestSuite(t *testing.T) {
	suite.Run(t, new(DeleteTestSuite))
}
