package dyno

import (
	"context"
	"fmt"
	"testing"
)

func TestCreateNewTableDeleteTable(t *testing.T) {
	client := CreateTestClient()
	tbl := CreateTestTable(client)
	// delete the table
	_, err := client.DeleteTable(context.Background(), tbl.DeleteInput())
	if err != nil {
		panic(err)
	}

	fmt.Println("table", tbl.Name(), "has been deleted")
}
