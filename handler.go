package dyno

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding"
	"sync"
)

//GetItemHandler handles the dynamodb.ScanOutput from a request and returns an error
type GetItemHandler func(*dynamodb.GetItemOutput) error

//GetItemUnmarshaller returns a new GetItemHandler that unmarshalls the dynamodb.GetItemOutput into the given target
func GetItemUnmarshaller(target interface{}) GetItemHandler {
	return func(output *dynamodb.GetItemOutput) error {
		return encoding.UnmarshalItem(output.Item, target)
	}
}

//ScanHandler handles the dynamodb.ScanOutput from a request and returns an error
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

//BatchGetItemHandler handles the dynamodb.BatchGetItemOutput from a request and returns an error
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