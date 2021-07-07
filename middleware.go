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

// BatchGetItemAllMiddleWare implements the BatchGetItemAllMiddleWare interface
func (mw *UnmarshalMiddleWare) BatchGetItemAllMiddleWare(next BatchGetItemAllHandler) BatchGetItemAllHandler {
	return BatchGetItemAllHandlerFunc(func(ctx *BatchGetItemAllContext, output *BatchGetItemAllOutput) {
		next.HandleBatchGetItemAll(ctx, output)
		outs, err := output.Get()
		if err != nil {
			return
		}
		for _, out := range outs {
			if len(out.Responses) > 0 {
				for _, avs := range out.Responses {
					if err = mw.UnmarshalSlice(avs); err != nil {
						output.Set(nil, err)
						return
					}
				}
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

// QueryAllMiddleWare implements the QueryAllMiddleWare interface
func (mw *UnmarshalMiddleWare) QueryAllMiddleWare(next QueryAllHandler) QueryAllHandler {
	return QueryAllHandlerFunc(func(ctx *QueryAllContext, output *QueryAllOutput) {
		next.HandleQueryAll(ctx, output)
		outs, err := output.Get()
		if err != nil {
			return
		}
		for _, out := range outs {
			if len(out.Items) > 0 {
				if err = mw.UnmarshalSlice(out.Items); err != nil {
					output.Set(nil, err)
					return
				}
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

// ScanAllMiddleWare implements the ScanAllMiddleWare interface
func (mw *UnmarshalMiddleWare) ScanAllMiddleWare(next ScanAllHandler) ScanAllHandler {
	return ScanAllHandlerFunc(func(ctx *ScanAllContext, output *ScanAllOutput) {
		next.HandleScanAll(ctx, output)
		outs, err := output.Get()
		if err != nil {
			return
		}
		for _, out := range outs {
			if len(out.Items) > 0 {
				if err = mw.UnmarshalSlice(out.Items); err != nil {
					output.Set(nil, err)
					return
				}
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
