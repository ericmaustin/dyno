package dyno

import (
	"fmt"
	"testing"
)

func TestCreateNewTableDeleteTable(t *testing.T) {
	sess := CreateTestSession()
	tbl := CreateTestTable(sess)
	sess.WaitUntilTableExists(NewDescribeTableInput(DescribeTableWithTableName(tbl.Name())))
	// delete the table
	err := tbl.Delete(sess)
	if err != nil {
		panic(err)
	}
	fmt.Println("table", tbl.Name(), "has been deleted")
}
