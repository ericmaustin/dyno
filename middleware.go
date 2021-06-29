package dyno

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/encoding"
	"sync"
)

// NewUnmarshaler creates a new Unmarshaler with a given target interface{}
func NewUnmarshaler(target interface{}) *Unmarshaler {
	return &Unmarshaler{
		target: target,
	}
}

// Unmarshaler is used to unmarshal the result of a get, scan, or query operation to a given target interface
type Unmarshaler struct {
	target interface{}
	mu sync.Mutex
}

// unmarshalMap unmarshals a single attribute value item
func (mw *Unmarshaler) unmarshalMap(av map[string]types.AttributeValue) error {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	return encoding.UnmarshalMap(av, mw.target)
}

// unmarshalMaps unmarshals a slice of attribute value maps
func (mw *Unmarshaler) unmarshalMaps(avs []map[string]types.AttributeValue) error {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	
	return encoding.UnmarshalMaps(avs, mw.target)
}

// BatchGetItemAllMiddleWare implements the BatchGetItemAllMiddleWare interface
func (mw *Unmarshaler) BatchGetItemAllMiddleWare(next BatchGetItemAllHandler) BatchGetItemAllHandler {
	return BatchGetItemAllHandlerFunc(func(ctx *BatchGetItemAllContext, output *BatchGetItemAllOutput) {
		next.HandleBatchGetItemAll(ctx, output)
		outs, err := output.Get()
		if err != nil {
			return
		}
		for _, out := range outs {
			if len(out.Responses) > 0 {
				for _, avs := range out.Responses {
					if err = mw.unmarshalMaps(avs); err != nil {
						output.Set(nil, err)
					}
				}
			}
		}
	})
}

// GetItemMiddleWare implements the GetItemMiddleWare interface
func (mw *Unmarshaler) GetItemMiddleWare(next GetItemHandler) GetItemHandler {
	return GetItemHandlerFunc(func(ctx *GetItemContext, output *GetItemOutput) {
		next.HandleGetItem(ctx, output)
		out, err := output.Get()
		if err != nil {
			return
		}
		if err = mw.unmarshalMap(out.Item); err != nil {
			output.Set(nil, err)
		}
	})
}

// QueryMiddleWare implements the QueryMiddleWare interface
func (mw *Unmarshaler) QueryMiddleWare(next QueryHandler) QueryHandler {
	return QueryHandlerFunc(func(ctx *QueryContext, output *QueryOutput) {
		next.HandleQuery(ctx, output)
		out, err := output.Get()
		if err != nil {
			return
		}
		if len(out.Items) > 0 {
			if err = mw.unmarshalMaps(out.Items); err != nil {
				output.Set(nil, err)
			}
		}
	})
}

// QueryAllMiddleWare implements the QueryAllMiddleWare interface
func (mw *Unmarshaler) QueryAllMiddleWare(next QueryAllHandler) QueryAllHandler {
	return QueryAllHandlerFunc(func(ctx *QueryAllContext, output *QueryAllOutput) {
		next.HandleQueryAll(ctx, output)
		outs, err := output.Get()
		if err != nil {
			return
		}
		for _, out := range outs {
			if len(out.Items) > 0 {
				if err = mw.unmarshalMaps(out.Items); err != nil {
					output.Set(nil, err)
				}
			}
		}
	})
}

// ScanMiddleWare implements the ScanMiddleWare interface
func (mw *Unmarshaler) ScanMiddleWare(next ScanHandler) ScanHandler {
	return ScanHandlerFunc(func(ctx *ScanContext, output *ScanOutput) {
		next.HandleScan(ctx, output)
		out, err := output.Get()
		if err != nil {
			return
		}
		if len(out.Items) > 0 {
			if err = mw.unmarshalMaps(out.Items); err != nil {
				output.Set(nil, err)
			}
		}
	})
}

// ScanAllMiddleWare implements the ScanAllMiddleWare interface
func (mw *Unmarshaler) ScanAllMiddleWare(next ScanAllHandler) ScanAllHandler {
	return ScanAllHandlerFunc(func(ctx *ScanAllContext, output *ScanAllOutput) {
		next.HandleScanAll(ctx, output)
		outs, err := output.Get()
		if err != nil {
			return
		}
		for _, out := range outs {
			if len(out.Items) > 0 {
				if err = mw.unmarshalMaps(out.Items); err != nil {
					output.Set(nil, err)
				}
			}
		}
	})
}


