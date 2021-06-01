package operation

import (
	"fmt"
	"github.com/ericmaustin/dyno"
	testing2 "github.com/ericmaustin/dyno/internal/dynotest"
	"github.com/stretchr/testify/suite"
	"testing"
)

type DeleteTestSuite struct {
	suite.Suite
	sess *dyno.Session
}

func (d *DeleteTestSuite) SetupSuite() {
	d.sess = testing2.createTestSession()
	testing2.createTestTable(d.sess)
	testing2.putTestRecords(d.sess)
}

func (d *DeleteTestSuite) TearDownSuite() {
	testing2.destroytestTable(d.sess)
}

func (d *DeleteTestSuite) TestDelete() {
	key := struct {
		ID string `dyno:"id"`
	}{
		"A",
	}

	// scan for records with no conditions
	deleteInput := NewDeleteBuilder().
		SetTable(testing2.getTestTableName()).
		SetKey(key).
		Build()

	fmt.Printf("delete input\n%+v\n", deleteInput)

	deleteOutput, err := Delete(deleteInput).
		Execute(d.sess.Request()).
		OutputError()

	d.NoError(err)
	d.NotNil(deleteOutput)

	fmt.Printf("delete output:\n%+v\n", deleteOutput)
}

// In order for 'go test' to Execute this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestDeleteTestSuite(t *testing.T) {
	suite.Run(t, new(DeleteTestSuite))
}
