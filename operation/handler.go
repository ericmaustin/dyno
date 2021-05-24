package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding"
	"sync"
)

// ItemHandler represents an item handler function to use with operations that return single outputs
type ItemHandler func(map[string]*dynamodb.AttributeValue) error

// LoadOne should be used for simple unmarshalling of dynamodb outputs
func LoadOne(target interface{}) ItemHandler {
	return func(rec map[string]*dynamodb.AttributeValue) error {
		if err := encoding.UnmarshalItem(rec, target); err != nil {
			return err
		}
		return nil
	}
}

// LoadOneIntoSlice used for loading a single attribute map into a slice of items
func LoadOneIntoSlice(target interface{}, mu *sync.Mutex) ItemHandler {
	return func(rec map[string]*dynamodb.AttributeValue) error {
		if mu != nil {
			mu.Lock()
			defer mu.Unlock()
		}
		if err := encoding.UnmarshalItems([]map[string]*dynamodb.AttributeValue{rec}, target); err != nil {
			return err
		}
		return nil
	}
}

// ItemSliceHandler represents an item handler function to be used with operations that return a batch or set of outputs
type ItemSliceHandler func([]map[string]*dynamodb.AttributeValue) error

// LoadSlice should be used for simple unmarshalling of dynamodb item slices
func LoadSlice(target interface{}, mu *sync.Mutex) ItemSliceHandler {
	return func(rec []map[string]*dynamodb.AttributeValue) error {
		if mu != nil {
			mu.Lock()
			defer mu.Unlock()
		}
		if err := encoding.UnmarshalItems(rec, target); err != nil {
			return err
		}
		return nil
	}
}

// LoadAttributeValues should be used for simple unmarshalling of dynamodb item slices
// into their native dynamodb attribute values types
func LoadAttributeValues(target []map[string]*dynamodb.AttributeValue, mu *sync.Mutex) ItemSliceHandler {
	return func(rec []map[string]*dynamodb.AttributeValue) error {
		mu.Lock()
		defer mu.Unlock()
		target = append(target, rec...)
		return nil
	}
}