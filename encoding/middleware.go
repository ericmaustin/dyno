package encoding

import (
	"github.com/ericmaustin/dyno"
)

type UnmarshalMiddleware struct {
	target interface{}
}

func (mw UnmarshalMiddleware) BatchGetItemAllMiddleWare(next dyno.BatchGetItemAllHandler) dyno.BatchGetItemAllHandler {
	return dyno.BatchGetItemAllHandlerFunc(func(ctx *dyno.BatchGetItemAllContext, promise *dyno.BatchGetItemAllPromise) {
		next.HandleBatchGetItemAll(ctx, promise)
		outs, err := promise.GetResponse()
		if err != nil {
			return
		}
		for _, out := range outs {
			if len(out.Responses) > 0 {
				for _, responses := range out.Responses {
					for _, av := range responses {
						if err = UnmarshalMap(av, mw.target); err != nil {

						}
					}
				}
			}
		}
	})
}



