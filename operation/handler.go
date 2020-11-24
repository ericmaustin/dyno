package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding"
)

// ItemHandler represents an item handler function to use with operations that return single outputs
type ItemHandler func(map[string]*dynamodb.AttributeValue) error

// ItemUnmarshaler should be used for simple unmarshalling of dynamodb outputs
func ItemUnmarshaler(target interface{}) ItemHandler {
	return func(rec map[string]*dynamodb.AttributeValue) error {
		if err := encoding.UnmarshalItem(rec, target); err != nil {
			return err
		}
		return nil
	}
}

// ItemSliceHandler represents an item handler function to be used with operations that return a batch or set of outputs
type ItemSliceHandler func([]map[string]*dynamodb.AttributeValue) error

// ItemSliceUnmarshaler should be used for simple unmarshalling of dynamodb item batches
func ItemSliceUnmarshaler(target interface{}) ItemSliceHandler {
	return func(rec []map[string]*dynamodb.AttributeValue) error {
		if err := encoding.UnmarshalItems(rec, target); err != nil {
			return err
		}
		return nil
	}
}
