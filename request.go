package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ericmaustin/dyno/util"
	"time"
)

//Execute executes a given Executable with provided context.Context and dynamodb.DynamoDB client and returns a *Promise
func Execute(ctx context.Context, ddbClient *ddb.Client, exe Executable) *Promise {
	promise := NewPromise(ctx)
	go execute(ctx, ddbClient, exe, promise)
	return promise
}

//execute executes a given Executable with provided context.Context and dynamodb.DynamoDB client
func execute(ctx context.Context, ddbClient *ddb.Client, exe Executable, promise *Promise) {
	promise.SetResponse(exe.DynoExecute(ctx, ddbClient))
}

// ExecutionFunction is a function that can be executed by a Request
type ExecutionFunction func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error)

//DynoExecute implements the Executable interface
func (e ExecutionFunction) DynoExecute(ctx context.Context, db *ddb.Client) (interface{}, error) {
	return e(ctx, db)
}

//Executable should be implemented for types that can be executed in a Request
type Executable interface {
	DynoExecute(ctx context.Context, db *ddb.Client) (interface{}, error)
}

//ScanFunc returns an ExecutionFunction that executes a Scan api call
func ScanFunc(input *ScanInput) ExecutionFunction {
	return func(ctx context.Context, client dynamodb.ScanAPIClient) (interface{}, error) {
		var (
			out *ddb.ScanOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.ScanInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = client.Scan(ctx, input.ddbInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.ScanOutputCallback(ctx, out)
		}
		return out, err
	}
}

//ScanAllFunc returns an ExecutionFunction that executes a Scan api call
// and keeps executing until there are no more LastEvaluatedKey left
func ScanAllFunc(input *ScanInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			outs []*ddb.ScanOutput
			out  *ddb.ScanOutput
			err  error
		)
		//copy the scan so we're not mutating the original
		ddbInput := util.CopyScan(input.ddbInput)
		for {
			if input.InputCallback != nil {
				if out, err = input.InputCallback.ScanInputCallback(ctx, ddbInput); out != nil || err != nil {
					if out != nil {
						outs = append(outs, out)
					}
					return outs, err
				}
			}
			if out, err = ddbClient.Scan(ctx, ddbInput); err != nil {
				return nil, err
			}
			if input.OutputCallback != nil {
				if err = input.OutputCallback.ScanOutputCallback(ctx, out); err != nil {
					return nil, err
				}
			}
			outs = append(outs, out)
			if out.LastEvaluatedKey == nil {
				// no more work
				break
			}
			ddbInput.ExclusiveStartKey = out.LastEvaluatedKey
		}
		return outs, err
	}
}

//QueryFunc returns an ExecutionFunction that executes a Query api call
func QueryFunc(input *QueryInput) ExecutionFunction {
	return func(ctx context.Context, db *ddb.Client) (interface{}, error) {
		var (
			out *ddb.QueryOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.QueryInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.Query(ctx, input.ddbInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.QueryOutputCallback(ctx, out)
		}
		return out, err
	}
}

//QueryAllFunc returns an ExecutionFunction that executes a Query api call
// and keeps executing until there are no more LastEvaluatedKey left
func QueryAllFunc(input *QueryInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			outs []*ddb.QueryOutput
			out  *ddb.QueryOutput
			err  error
		)
		// copy the query so we're not mutating the original
		ddbInput := util.CopyQuery(input.ddbInput)
		for {
			if input.InputCallback != nil {
				if out, err = input.InputCallback.QueryInputCallback(ctx, ddbInput); out != nil || err != nil {
					if out != nil {
						outs = append(outs, out)
					}
					return outs, err
				}
			}
			if out, err = ddbClient.Query(ctx, ddbInput); err != nil {
				return nil, err
			}
			if input.OutputCallback != nil {
				if err = input.OutputCallback.QueryOutputCallback(ctx, out); err != nil {
					return nil, err
				}
			}
			outs = append(outs, out)
			if out.LastEvaluatedKey == nil {
				// no more work
				break
			}
			ddbInput.ExclusiveStartKey = out.LastEvaluatedKey
		}
		return outs, err
	}
}

//PutItemFunc returns an ExecutionFunction that executes a PutItem api call
func PutItemFunc(input *PutItemInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.PutItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.PutItemInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.PutItem(ctx, input.ddbInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.PutItemOutputCallback(ctx, out)
		}
		return out, err
	}
}

//GetItemFunc returns an ExecutionFunction that executes a PutItem api call
func GetItemFunc(input *GetItemInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.GetItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.GetItemInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.GetItem(ctx, input.ddbInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.GetItemOutputCallback(ctx, out)
		}
		return out, err
	}
}

//UpdateItemFunc returns an ExecutionFunction that executes an UpdateItem api call
func UpdateItemFunc(input *UpdateItemInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.UpdateItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.UpdateItemInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.UpdateItem(ctx, input.ddbInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.UpdateItemOutputCallback(ctx, out)
		}
		return out, err
	}
}

//DeleteItemFunc returns an ExecutionFunction that executes an DeleteItem api call
func DeleteItemFunc(input *DeleteItemInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.DeleteItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.DeleteItemInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.DeleteItem(ctx, input.ddbInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.DeleteItemOutputCallback(ctx, out)
		}
		return out, err
	}
}

//BatchGetItemFunc returns an ExecutionFunction that executes a BatchGetItem api call
func BatchGetItemFunc(input *BatchGetItemInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.BatchGetItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.BatchGetItemInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}

		if out, err = ddbClient.BatchGetItem(ctx, input.ddbInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.BatchGetItemOutputCallback(ctx, out)
		}
		return out, err
	}
}

//BatchGetItemAllFunc returns an ExecutionFunction that executes a Query api call
// and keeps executing until there are no more LastEvaluatedKey left
func BatchGetItemAllFunc(input *BatchGetItemInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			outs []*ddb.BatchGetItemOutput
			out  *ddb.BatchGetItemOutput
			err  error
		)
		ddbInput := util.CopyBatchGetItemInput(input.ddbInput)
		for {
			if input.InputCallback != nil {
				if out, err = input.InputCallback.BatchGetItemInputCallback(ctx, ddbInput); out != nil || err != nil {
					if out != nil {
						outs = append(outs, out)
					}
					return outs, err
				}
			}

			if out, err = ddbClient.BatchGetItem(ctx, ddbInput); err != nil {
				return nil, err
			}
			if input.OutputCallback != nil {
				if err = input.OutputCallback.BatchGetItemOutputCallback(ctx, out); err != nil {
					return nil, err
				}
			}
			outs = append(outs, out)
			if out.UnprocessedKeys == nil {
				// no more work
				break
			}
			ddbInput.RequestItems = out.UnprocessedKeys
		}
		return outs, err
	}
}

//BatchWriteItemFunc returns an ExecutionFunction that
func BatchWriteItemFunc(input *BatchWriteItemInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.BatchWriteItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.BatchWriteItemInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.BatchWriteItem(ctx, input.ddbInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.BatchWriteItemOutputCallback(ctx, out)
		}
		return out, err
	}
}

//BatchWriteItemAllFunc returns an ExecutionFunction that runs a BatchWriteItemInput api call
func BatchWriteItemAllFunc(input *BatchWriteItemInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			outs []*ddb.BatchWriteItemOutput
			out  *ddb.BatchWriteItemOutput
			err  error
		)
		ddbInput := util.CopyBatchWriteItemInput(input.ddbInput)
		for {
			if input.InputCallback != nil {
				if out, err = input.InputCallback.BatchWriteItemInputCallback(ctx, ddbInput); out != nil || err != nil {
					if out != nil {
						outs = append(outs, out)
					}
					return outs, err
				}
			}
			if out, err = ddbClient.BatchWriteItem(ctx, ddbInput); err != nil {
				return nil, err
			}

			if input.OutputCallback != nil {
				if err = input.OutputCallback.BatchWriteItemOutputCallback(ctx, out); err != nil {
					return nil, err
				}
			}
			outs = append(outs, out)
			if len(out.UnprocessedItems) < 1 {
				break
			}
			ddbInput.RequestItems = out.UnprocessedItems
		}
		return outs, err
	}
}

//CreateTableFunc returns an ExecutionFunction that creates a table
func CreateTableFunc(input *CreateTableInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.CreateTableOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.CreateTableInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.CreateTable(ctx, input.ddbInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.CreateTableOutputCallback(ctx, nil)
		}
		return out, err
	}
}

//CreateBackupFunc returns an ExecutionFunction that creates a table backup
func CreateBackupFunc(input *CreateBackupInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.CreateBackupOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.CreateBackupInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.CreateBackup(ctx, input.ddbInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.CreateBackupOutputCallback(ctx, nil)
		}
		return out, err
	}
}

//DescribeTableFunc returns an ExecutionFunction that runs a DescribeTable api call
func DescribeTableFunc(input *DescribeTableInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.DescribeTableOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.DescribeTableInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.DescribeTable(ctx, input.ddbInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.DescribeTableOutputCallback(ctx, nil)
		}
		return out, err
	}
}

//DeleteTableFunc returns an ExecutionFunction that runs a DeleteTableFunc api call
func DeleteTableFunc(input *DeleteTableInput) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		var (
			out *ddb.DeleteTableOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback.DeleteTableInputCallback(ctx, input.ddbInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = ddbClient.DeleteTable(ctx, input.ddbInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback.DeleteTableOutputCallback(ctx, nil)
		}
		return out, err
	}
}

//WaitUntilTableNotExistsFunc returns an ExecutionFunction that waits for a table to no longer exist
func WaitUntilTableNotExistsFunc(input *DescribeTableInput, timeout time.Duration) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		return nil, dynamodb.NewTableNotExistsWaiter(ddbClient).Wait(ctx, input.ddbInput, timeout)
	}
}

//WaitUntilTableExistsFunc returns an ExecutionFunction that waits for a table to be ready
func WaitUntilTableExistsFunc(input *DescribeTableInput, timeout time.Duration) ExecutionFunction {
	return func(ctx context.Context, ddbClient *ddb.Client) (interface{}, error) {
		return nil, dynamodb.NewTableExistsWaiter(ddbClient).Wait(ctx, input.ddbInput, timeout)
	}
}
//
////WaitUntilBackupExists returns an ExecutionFunction that waits for a backup to be completed
//func WaitUntilBackupExists(input *DescribeBackupInput) ExecutionFunction {
//	return func(ctx context.Context, db *ddb.DynamoDB) (interface{}, error) {
//		//var out *dynamodb.DescribeTableOutput
//		w := request.Waiter{
//			Name:        "WaitUntilTableExistsFunc",
//			MaxAttempts: 360,
//			Delay:       request.ConstantWaiterDelay(time.Second),
//			Acceptors: []request.WaiterAcceptor{
//				{
//					State:    request.SuccessWaiterState,
//					Matcher:  request.PathWaiterMatch,
//					Argument: "BackupDescription.BackupStatus",
//					Expected: ddb.BackupStatusAvailable,
//				},
//				{
//					State:    request.RetryWaiterState,
//					Matcher:  request.ErrorWaiterMatch,
//					Expected: ddb.ErrCodeResourceNotFoundException,
//				},
//			},
//			Logger: db.Config.Logger,
//			NewRequest: func(opts []request.Option) (*request.Request, error) {
//				var inCpy *ddb.DescribeBackupInput
//				if input != nil {
//					tmp := *input.DescribeBackupInput
//					inCpy = &tmp
//				}
//				req, _ := db.DescribeBackupRequest(inCpy)
//				req.SetContext(ctx)
//				req.ApplyOptions(opts...)
//				return req, nil
//			},
//		}
//		return nil, w.WaitWithContext(ctx)
//	}
//}
