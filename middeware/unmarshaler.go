package middeware

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno"
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
func (mw *Unmarshaler) BatchGetItemAllMiddleWare(next dyno.BatchGetItemAllHandler) dyno.BatchGetItemAllHandler {
	return dyno.BatchGetItemAllHandlerFunc(func(ctx *dyno.BatchGetItemAllContext, output *dyno.BatchGetItemAllOutput) {
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
func (mw *Unmarshaler) GetItemMiddleWare(next dyno.GetItemHandler) dyno.GetItemHandler {
	return dyno.GetItemHandlerFunc(func(ctx *dyno.GetItemContext, output *dyno.GetItemOutput) {
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
func (mw *Unmarshaler) QueryMiddleWare(next dyno.QueryHandler) dyno.QueryHandler {
	return dyno.QueryHandlerFunc(func(ctx *dyno.QueryContext, output *dyno.QueryOutput) {
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
func (mw *Unmarshaler) QueryAllMiddleWare(next dyno.QueryAllHandler) dyno.QueryAllHandler {
	return dyno.QueryAllHandlerFunc(func(ctx *dyno.QueryAllContext, output *dyno.QueryAllOutput) {
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
func (mw *Unmarshaler) ScanMiddleWare(next dyno.ScanHandler) dyno.ScanHandler {
	return dyno.ScanHandlerFunc(func(ctx *dyno.ScanContext, output *dyno.ScanOutput) {
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
func (mw *Unmarshaler) ScanAllMiddleWare(next dyno.ScanAllHandler) dyno.ScanAllHandler {
	return dyno.ScanAllHandlerFunc(func(ctx *dyno.ScanAllContext, output *dyno.ScanAllOutput) {
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


