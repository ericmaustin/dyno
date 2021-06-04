package dyno

import (
	"fmt"
	"testing"
)

func TestCreateNewTableDeleteTable(t *testing.T) {
	sess := CreateTestSession()
	tbl := CreateTestTable(sess)
	// delete the table
	err := <-tbl.Delete(sess.Request())
	if err != nil {
		panic(err)
	}
	fmt.Println("table", tbl.Name(), "has been deleted")
}
