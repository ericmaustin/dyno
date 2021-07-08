package dyno

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/encoding"
	"sync"
)

// UnmarshalMiddleWare used for unmarshalling the result of get requests
type UnmarshalMiddleWare struct {
	Unmarshal      func(m map[string]types.AttributeValue) error
	UnmarshalSlice func(ms []map[string]types.AttributeValue) error
}

// BatchGetItemMiddleWare implements the BatchGetItemMiddleWare interface
func (mw *UnmarshalMiddleWare) BatchGetItemMiddleWare(next BatchGetItemHandler) BatchGetItemHandler {
	return BatchGetItemHandlerFunc(func(ctx *BatchGetItemContext, output *BatchGetItemOutput) {
		next.HandleBatchGetItem(ctx, output)
		out, err := output.Get()
		if err != nil {
			return
		}

		for _, avs := range out.Responses {
			if err = mw.UnmarshalSlice(avs); err != nil {
				output.Set(nil, err)
				return
			}
		}
	})
}

// GetItemMiddleWare implements the GetItemMiddleWare interface
func (mw *UnmarshalMiddleWare) GetItemMiddleWare(next GetItemHandler) GetItemHandler {
	return GetItemHandlerFunc(func(ctx *GetItemContext, output *GetItemOutput) {
		next.HandleGetItem(ctx, output)
		out, err := output.Get()
		if err != nil {
			return
		}

		if err = mw.Unmarshal(out.Item); err != nil {
			output.Set(nil, err)
			return
		}
	})
}

// QueryMiddleWare implements the QueryMiddleWare interface
func (mw *UnmarshalMiddleWare) QueryMiddleWare(next QueryHandler) QueryHandler {
	return QueryHandlerFunc(func(ctx *QueryContext, output *QueryOutput) {
		next.HandleQuery(ctx, output)
		out, err := output.Get()
		if err != nil {
			return
		}

		if len(out.Items) > 0 {
			if err = mw.UnmarshalSlice(out.Items); err != nil {
				output.Set(nil, err)
				return
			}
		}
	})
}

// ScanMiddleWare implements the ScanMiddleWare interface
func (mw *UnmarshalMiddleWare) ScanMiddleWare(next ScanHandler) ScanHandler {
	return ScanHandlerFunc(func(ctx *ScanContext, output *ScanOutput) {
		next.HandleScan(ctx, output)
		out, err := output.Get()
		if err != nil {
			return
		}

		if len(out.Items) > 0 {
			if err = mw.UnmarshalSlice(out.Items); err != nil {
				output.Set(nil, err)
				return
			}
		}
	})
}

// NewUnmarshaler creates a new Unmarshaler with a given target interface{}
func NewUnmarshaler(target interface{}) *UnmarshalMiddleWare {

	mu := &sync.Mutex{}

	return &UnmarshalMiddleWare{
		Unmarshal: func(av map[string]types.AttributeValue) error {
			mu.Lock()
			defer mu.Unlock()

			return encoding.UnmarshalMap(av, target)
		},
		UnmarshalSlice: func(avs []map[string]types.AttributeValue) error {
			mu.Lock()
			defer mu.Unlock()

			return encoding.UnmarshalMaps(avs, target)
		},
	}
}
