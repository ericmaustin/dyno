package dyno

import (
	"fmt"
	"testing"
)

func TestCreateNewTableDeleteTable(t *testing.T) {
	client := CreateTestClient()
	tbl := CreateTestTable(client)

	// delete the table
	_, err := client.DeleteTable(tbl.DeleteInput()).Await()
	if err != nil {
		panic(err)
	}

	fmt.Println("table", tbl.Name(), "has been deleted")
}
