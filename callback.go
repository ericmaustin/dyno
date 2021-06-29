package dyno

//
//import (
//	"context"
//	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
//)
//
//type BatchGetItemInputCallback interface {
//	BatchGetItemInputCallback(context.Context, *ddb.BatchGetItemInput) (*ddb.BatchGetItemOutput, error)
//}
//
//type BatchGetItemInputCallbackFunc func(context.Context, *ddb.BatchGetItemInput) (*ddb.BatchGetItemOutput, error)
//
//func (cb BatchGetItemInputCallbackFunc) BatchGetItemInputCallback(ctx context.Context, input *ddb.BatchGetItemInput) (*ddb.BatchGetItemOutput, error) {
//	return cb(ctx, input)
//}
//
//type BatchGetItemOutputCallback interface {
//	BatchGetItemOutputCallback(context.Context, *ddb.BatchGetItemOutput) error
//}
//
//type BatchGetItemOutputCallbackFunc func(context.Context, *ddb.BatchGetItemOutput) error
//
//func (cb BatchGetItemOutputCallbackFunc) BatchGetItemOutputCallback(ctx context.Context, input *ddb.BatchGetItemOutput) error {
//	return cb(ctx, input)
//}
//
//type CreateTableInputCallback interface {
//	CreateTableInputCallback(context.Context, *ddb.CreateTableInput) (*ddb.CreateTableOutput, error)
//}
//
//type CreateTableInputCallbackF func(context.Context, *ddb.CreateTableInput) (*ddb.CreateTableOutput, error)
//
//func (cb CreateTableInputCallbackF) CreateTableInputCallback(ctx context.Context, input *ddb.CreateTableInput) (*ddb.CreateTableOutput, error) {
//	return cb(ctx, input)
//}
//
//type CreateTableOutputCallback interface {
//	CreateTableOutputCallback(context.Context, *ddb.CreateTableOutput) error
//}
//
//type CreateTableOutputCallbackF func(context.Context, *ddb.CreateTableOutput) error
//
//func (cb CreateTableOutputCallbackF) CreateTableOutputCallback(ctx context.Context, input *ddb.CreateTableOutput) error {
//	return cb(ctx, input)
//}
//
//func (cb GetItemOutputCallbackFunc) GetItemOutputCallback(ctx context.Context, input *ddb.GetItemOutput) error {
//	return cb(ctx, input)
//}
//
//type DeleteTableInputCallback interface {
//	DeleteTableInputCallback(context.Context, *ddb.DeleteTableInput) (*ddb.DeleteTableOutput, error)
//}
//
//type DeleteTableInputCallbackFunc interface {
//	DeleteTableInputCallbackFunc(context.Context, *ddb.DeleteTableInput) (*ddb.DeleteTableOutput, error)
//}
//
//type DeleteTableInputCallbackFuncFunc func(context.Context, *ddb.DeleteTableInput) (*ddb.DeleteTableOutput, error)
//
//func (cb DeleteTableInputCallbackFuncFunc) DeleteTableInputCallbackFunc(ctx context.Context, input *ddb.DeleteTableInput) (*ddb.DeleteTableOutput, error) {
//	return cb(ctx, input)
//}
//
//type DeleteTableOutputCallback interface {
//	DeleteTableOutputCallback(context.Context, *ddb.DeleteTableOutput) error
//}
//
//type DeleteTableOutputCallbackFunc func(context.Context, *ddb.DeleteTableOutput) error
//
//func (cb DeleteTableOutputCallbackFunc) DeleteTableOutputCallback(ctx context.Context, input *ddb.DeleteTableOutput) error {
//	return cb(ctx, input)
//}
//
//type GetItemHandleInputCallback interface {
//	GetItemHandleInputCallback(context.Context, *ddb.GetItemInput) (*ddb.GetItemOutput, error)
//}
//
//type GetItemHandleInputCallbackFunc func(context.Context, *ddb.GetItemInput) (*ddb.GetItemOutput, error)
//
//func (cb GetItemHandleInputCallbackFunc) GetItemHandleInputCallback(ctx context.Context, input *ddb.GetItemInput) (*ddb.GetItemOutput, error) {
//	return cb(ctx, input)
//}
//
//type GetItemHandleOutputCallback interface {
//	GetItemHandleOutputCallback(context.Context, *ddb.GetItemOutput) error
//}
//
//type GetItemHandleOutputCallbackFunc func(context.Context, *ddb.GetItemOutput) error
//
//func (cb GetItemHandleOutputCallbackFunc) GetItemHandleOutputCallback(ctx context.Context, input *ddb.GetItemOutput) error {
//	return cb(ctx, input)
//}
//
//
//type QueryInputCallback interface {
//	QueryInputCallback(context.Context, *ddb.QueryInput) (*ddb.QueryOutput, error)
//}
//
//type QueryInputCallbackFunc func(context.Context, *ddb.QueryInput) (*ddb.QueryOutput, error)
//
//func (cb QueryInputCallbackFunc) QueryInputCallback(ctx context.Context, input *ddb.QueryInput) (*ddb.QueryOutput, error) {
//	return cb(ctx, input)
//}
//
//type QueryOutputCallback interface {
//	QueryOutputCallback(context.Context, *ddb.QueryOutput) error
//}
//
//type QueryOutputCallbackF func(context.Context, *ddb.QueryOutput) error
//
//func (cb QueryOutputCallbackF) QueryOutputCallback(ctx context.Context, input *ddb.QueryOutput) error {
//	return cb(ctx, input)
//}
//
//
//type CreateGlobalTableOutputCallback interface {
//	CreateGlobalTableOutputCallback(context.Context, *ddb.CreateGlobalTableOutput) error
//}
//
//type CreateGlobalTableOutputCallbackFunc func(context.Context, *ddb.CreateGlobalTableOutput) error
//
//func (cb CreateGlobalTableOutputCallbackFunc) CreateGlobalTableOutputCallback(ctx context.Context, input *ddb.CreateGlobalTableOutput) error {
//	return cb(ctx, input)
//}
//
//type DescribeTableInputCallback interface {
//	DescribeTableInputCallback(context.Context, *ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error)
//}
//
//type DescribeTableInputCallbackF func(context.Context, *ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error)
//
//func (cb DescribeTableInputCallbackF) DescribeTableInputCallback(ctx context.Context, input *ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error) {
//	return cb(ctx, input)
//}
//
//type DescribeTableOutputCallback interface {
//	DescribeTableOutputCallback(context.Context, *ddb.DescribeTableOutput) error
//}
//
//type DescribeTableOutputCallbackF func(context.Context, *ddb.DescribeTableOutput) error
//
//func (cb DescribeTableOutputCallbackF) DescribeTableOutputCallback(ctx context.Context, input *ddb.DescribeTableOutput) error {
//	return cb(ctx, input)
//}
//
//type RestoreTableFromBackupInputCallback interface {
//	RestoreTableFromBackupInputCallback(context.Context, *ddb.RestoreTableFromBackupInput) (*ddb.RestoreTableFromBackupOutput, error)
//}
//
//type RestoreTableFromBackupInputCallbackFunc func(context.Context, *ddb.RestoreTableFromBackupInput) (*ddb.RestoreTableFromBackupOutput, error)
//
//func (cb RestoreTableFromBackupInputCallbackFunc) RestoreTableFromBackupInputCallback(ctx context.Context, input *ddb.RestoreTableFromBackupInput) (*ddb.RestoreTableFromBackupOutput, error) {
//	return cb(ctx, input)
//}
//
//type RestoreTableFromBackupOutputCallback interface {
//	RestoreTableFromBackupOutputCallback(context.Context, *ddb.RestoreTableFromBackupOutput) error
//}
//
//type RestoreTableFromBackupOutputCallbackFunc func(context.Context, *ddb.RestoreTableFromBackupOutput) error
//
//func (cb RestoreTableFromBackupOutputCallbackFunc) RestoreTableFromBackupOutputCallback(ctx context.Context, input *ddb.RestoreTableFromBackupOutput) error {
//	return cb(ctx, input)
//}
//
//type BatchWriteItemInputCallback interface {
//	BatchWriteItemInputCallback(context.Context, *ddb.BatchWriteItemInput) (*ddb.BatchWriteItemOutput, error)
//}
//
//type BatchWriteItemInputCallbackFunc func(context.Context, *ddb.BatchWriteItemInput) (*ddb.BatchWriteItemOutput, error)
//
//func (cb BatchWriteItemInputCallbackFunc) BatchWriteItemInputCallback(ctx context.Context, input *ddb.BatchWriteItemInput) (*ddb.BatchWriteItemOutput, error) {
//	return cb(ctx, input)
//}
//
//type BatchWriteItemOutputCallback interface {
//	BatchWriteItemOutputCallback(context.Context, *ddb.BatchWriteItemOutput) error
//}
//
//type BatchWriteItemOutputCallbackFunc func(context.Context, *ddb.BatchWriteItemOutput) error
//
//func (cb BatchWriteItemOutputCallbackFunc) BatchWriteItemOutputCallback(ctx context.Context, input *ddb.BatchWriteItemOutput) error {
//	return cb(ctx, input)
//}
//
//type CreateBackupInputCallback interface {
//	CreateBackupInputCallback(context.Context, *ddb.CreateBackupInput) (*ddb.CreateBackupOutput, error)
//}
//
//type CreateBackupInputCallbackFunc func(context.Context, *ddb.CreateBackupInput) (*ddb.CreateBackupOutput, error)
//
//func (cb CreateBackupInputCallbackFunc) CreateBackupInputCallback(ctx context.Context, input *ddb.CreateBackupInput) (*ddb.CreateBackupOutput, error) {
//	return cb(ctx, input)
//}
//
//type CreateBackupOutputCallback interface {
//	CreateBackupOutputCallback(context.Context, *ddb.CreateBackupOutput) error
//}
//
//type CreateBackupOutputCallbackFunc func(context.Context, *ddb.CreateBackupOutput) error
//
//func (cb CreateBackupOutputCallbackFunc) CreateBackupOutputCallback(ctx context.Context, input *ddb.CreateBackupOutput) error {
//	return cb(ctx, input)
//}
//
//type DescribeBackupInputCallback interface {
//	DescribeBackupInputCallback(context.Context, *ddb.DescribeBackupInput) (*ddb.DescribeBackupOutput, error)
//}
//
//type DescribeBackupInputCallbackFunc func(context.Context, *ddb.DescribeBackupInput) (*ddb.DescribeBackupOutput, error)
//
//func (cb DescribeBackupInputCallbackFunc) DescribeBackupInputCallback(ctx context.Context, input *ddb.DescribeBackupInput) (*ddb.DescribeBackupOutput, error) {
//	return cb(ctx, input)
//}
//
//type DescribeBackupOutputCallback interface {
//	DescribeBackupOutputCallback(context.Context, *ddb.DescribeBackupOutput) error
//}
//
//type DescribeBackupOutputCallbackFunc func(context.Context, *ddb.DescribeBackupOutput) error
//
//func (cb DescribeBackupOutputCallbackFunc) DescribeBackupOutputCallback(ctx context.Context, input *ddb.DescribeBackupOutput) error {
//	return cb(ctx, input)
//}
//
//type ListTablesInputCallback interface {
//	ListTablesInputCallback(context.Context, *ddb.ListTablesInput) (*ddb.ListTablesOutput, error)
//}
//
//type ListTablesInputCallbackFunc func(context.Context, *ddb.ListTablesInput) (*ddb.ListTablesOutput, error)
//
//func (cb ListTablesInputCallbackFunc) ListTablesInputCallback(ctx context.Context, input *ddb.ListTablesInput) (*ddb.ListTablesOutput, error) {
//	return cb(ctx, input)
//}
//
//type ListTablesOutputCallback interface {
//	ListTablesOutputCallback(context.Context, *ddb.ListTablesOutput) error
//}
//
//type ListTablesOutputCallbackFunc func(context.Context, *ddb.ListTablesOutput) error
//
//func (cb ListTablesOutputCallbackFunc) ListTablesOutputCallback(ctx context.Context, input *ddb.ListTablesOutput) error {
//	return cb(ctx, input)
//}
