DYNO
====
[![Build Status](https://travis-ci.com/ericmaustin/dyno.svg?branch=master)](https://travis-ci.com/ericmaustin/dyno)
[![Coverage Status](https://coveralls.io/repos/github/ericmaustin/dyno/badge.svg?branch=master)](https://coveralls.io/github/ericmaustin/dyno?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ericmaustin/dyno)](https://goreportcard.com/report/github.com/ericmaustin/dyno)

dyno is an AWS dynomodb API extension library

## Features:

### Input Builders 

Build inputs for API operations with input builders allowing more straight-forward building
of more complicated operations. Example:
```go
builder := NewScanBuilder().SetTableName("TableName")
builder.AddFilter(condition.And(
    condition.Equal("Bar", "Hello World"),
    condition.Between("Foo", 0, 2),
))
builder.AddProjectionNames("ID", "TimeStamp")
input, err := builder.Build()
```

### Operations as Promises

All `DynamoDB` API calls (e.g. `DynamoDB.Query()`) return an `Operation` type that will execute the API call in
a go routine and will wait and return values by calling `Operation.Await`.
Example:
```go
scanInput := NewScanInput().SetTableName("MyTable")
scanOp := session.Scan(scanInput)  // here we are using a ``Session`` type
scanOutput, err := scanOp.Await()
```

Each dynamodb API operation has its own `Operation` type

### Middleware

All `DynamoDB` API calls have their own middleware interface that can be used to wrap API calls.
Example:
```go

// Note: ScanAll does the same thing as Scan except it will keep running Scan operations until no results are left
// to be returned

var cached *dynamodb.ScanOutput

testCacheMiddelWare := ScanMiddleWareFunc(func(next ScanHandler) ScanHandler {
    return ScanHandlerFunc(func(ctx *ScanContext, output *ScanOutput) {
        if cached != nil {
            output.Set(cached, nil)
            return
        }
        
        next.HandleScan(ctx, output)
        out, err := output.Get()
        if err != nil {
            panic(err)
        }
        cached = out
    })
})

// create a NewScanBuilder, input is nil as we do not have an existing ScanInput
input, err := NewScanBuilder(nil).SetTableName("MyTable").Build()
if err != nil {
    panic(err)
}

// NewScanAll creates a Scan operation that repeats scan api calls until all values are returned
scan := NewScanAll(input, testCacheMiddelWare)
scanOperation := scan.Invoke(context.ToDo(), dynamoDBClient)
out, err := scanOperation.Await()
```

### Encoding

More control over struct encoding with extensions to the `dynamodbav` struct tags including:
- embedding (flattening) other structs or maps within structs with struct tags: `dynamodbav:"*"`
- prepended or appended key values to embedded structs: `dynamodbav:"*,prepend=Foo,append=Bar"`
- enable json conversion with struct tags: `dynamodbav:",json"`

`MapMarshaler` and `MapUnmarshaler` interfaces can be implemented to directly control
how a type will be marshalled or unmarshalled into a DynamoDB record with type `map[string]AttributeValue`.

Call encoder with `encoding.MarshalItem(myItem)` to marshal a value to a DynamoDB record or marshal
a slice of DynamoDB record all at once items with `encoding.MarshalMaps([]myItem{...})`

### Pool

Use the `Pool` type to limit parallel executions, or to batch process api calls in a controlled way.
Example:
```go
pool := NewPool(context.Background(), dynamoDBClient, 10)

var scanOperations []*Scan

for _, scan := range []*dynamodb.ScanInput{...} {
    scanOperations := pool.Scan(scanInput)
    scanOperations = append(scanPromises, scanPromise)
}

for _, promise := range scanOperations {
	out, err := promise.Await()
	...
}

```

### Distributed Locks

The `Lock` module provides distributed locking functionality for dynamodb records:
```go
lock := lock.MustAcquire(tableName, itemKey, db,
	lock.OptHeartbeatFrequency(time.Millisecond*200),
    lock.OptTimeout(time.Second),
    lock.OptLeaseDuration(time.Second))

// release
err = lock.Release()
```
