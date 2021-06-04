package dyno

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding"
	"sync"
)

//GetItemHandler handles the dynamodb.ScanOutput from a Request and returns an error
type GetItemHandler func(*dynamodb.GetItemOutput) error

//GetItemUnmarshaller returns a new GetItemHandler that unmarshalls the dynamodb.GetItemOutput into the given target
func GetItemUnmarshaller(target interface{}) GetItemHandler {
	return func(output *dynamodb.GetItemOutput) error {
		return encoding.UnmarshalItem(output.Item, target)
	}
}

//ScanHandler handles the dynamodb.ScanOutput from a Request and returns an error
type ScanHandler func(*dynamodb.ScanOutput) error

//ScanUnmarshaller returns a new ScanHandler that unmarshalls the dynamodb.ScanOutput into the given target
func ScanUnmarshaller(target interface{}) ScanHandler {
	return func(output *dynamodb.ScanOutput) error {
		return encoding.UnmarshalItems(output.Items, target)
	}
}

//ScanSafeUnmarshaller returns a new ScanHandler that unmarshalls the dynamodb.ScanOutput into the given target
// and uses a mutex for safe multithreaded unmarshalling
func ScanSafeUnmarshaller(target interface{}, mu *sync.Mutex) ScanHandler {
	return func(output *dynamodb.ScanOutput) error {
		mu.Lock()
		defer mu.Unlock()
		return encoding.UnmarshalItems(output.Items, target)
	}
}

//QueryHandler handles the dynamodb.QueryOutput from a Request and returns an error
type QueryHandler func(output *dynamodb.QueryOutput) error

//QueryUnmarshaller returns a new QueryHandler that unmarshalls the dynamodb.QueryOutput into the given target
func QueryUnmarshaller(target interface{}) QueryHandler {
	return func(output *dynamodb.QueryOutput) error {
		return encoding.UnmarshalItems(output.Items, target)
	}
}

//QuerySafeUnmarshaller returns a new QueryHandler that unmarshalls the dynamodb.QueryOutput into the given target
// and uses a mutex for safe multithreaded unmarshalling
func QuerySafeUnmarshaller(target interface{}, mu *sync.Mutex) QueryHandler {
	return func(output *dynamodb.QueryOutput) error {
		mu.Lock()
		defer mu.Unlock()
		return encoding.UnmarshalItems(output.Items, target)
	}
}

//BatchGetItemHandler handles the dynamodb.BatchGetItemOutput from a Request and returns an error
type BatchGetItemHandler func(*dynamodb.BatchGetItemOutput) error

//BatchGetItemUnmarshaller returns a new BatchGetItemHandler that unmarshalls the dynamodb.BatchGetItemOutput into the given target
func BatchGetItemUnmarshaller(target interface{}) BatchGetItemHandler {
	return func(output *dynamodb.BatchGetItemOutput) error {
		for _, resp := range output.Responses {
			if err := encoding.UnmarshalItems(resp, target); err != nil {
				return err
			}
		}
		return nil
	}
}

//BatchGetItemSafeUnmarshaller returns a new BatchGetItemHandler that unmarshalls the dynamodb.BatchGetItemOutput into the given target
// and uses a mutex for safe multithreaded unmarshalling
func BatchGetItemSafeUnmarshaller(target interface{}, mu *sync.Mutex) BatchGetItemHandler {
	return func(output *dynamodb.BatchGetItemOutput) error {
		mu.Lock()
		defer mu.Unlock()
		for _, resp := range output.Responses {
			if err := encoding.UnmarshalItems(resp, target); err != nil {
				return err
			}
		}
		return nil
	}
}

//CreateTableHandler handles the dynamodb.CreateTableOutput from a Request and returns an error
type CreateTableHandler func(*dynamodb.CreateTableOutput) error

//CreateGlobalTableHandler handles the dynamodb.CreateGlobalTableOutput from a Request and returns an error
type CreateGlobalTableHandler func(output *dynamodb.CreateGlobalTableOutput) error

//DescribeTableHandler handles the dynamodb.DescribeTableOutput from a Request and returns an error
type DescribeTableHandler func(output *dynamodb.DescribeTableOutput) error

//DeleteTableHandler handles the dynamodb.DeleteTableOutput from a Request and returns an error
type DeleteTableHandler func(output *dynamodb.DeleteTableOutput) error

//CreateBackupHandler handles the dynamodb.CreateBackupOutput from a Request and returns an error
type CreateBackupHandler func(output *dynamodb.CreateBackupOutput) error

//DescribeBackupHandler handles the dynamodb.DescribeBackupOutput from a Request and returns an error
type DescribeBackupHandler func(output *dynamodb.DescribeBackupOutput) error

//PutItemHandler handles the dynamodb.PutItemOutput from a Request and returns an error
type PutItemHandler func(output *dynamodb.PutItemOutput) error

//UpdateItemHandler handles the dynamodb.UpdateItemOutput from a Request and returns an error
type UpdateItemHandler func(output *dynamodb.UpdateItemOutput) error

//DeleteItemHandler handles the dynamodb.DeleteItemOutput from a Request and returns an error
type DeleteItemHandler func(output *dynamodb.DeleteItemOutput) error
