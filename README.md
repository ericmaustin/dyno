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
builder := NewScanBuilder().SetTableName(tableName)
builder.AddFilter(condition.And(
    condition.Between("Foo", 0, 2),
    condition.Equal("Bar", "Hello World"),
))
builder.AddProjectionNames("ID", "TimeStamp")
builder.Build()
```

### Promises

All `DynamoDB` API calls (e.g. `DynamoDB.Query()`) return a `Promise` type that will execute the API call in
a go routine and will wait and return values by calling `Promise.Await`.
Example:
```go
scanInput := NewScanInput().SetTableName(suite.table.Name())
scanPromise := client.Scan(scanInput)
scanOutput, err := scanPromise.Await()
```

Each API operation has its own `Promise` type that will return the correct type with `Await()`

### Middleware

All `DynamoDB` API calls have their own middleware interface that can be used to wrap API calls.
Example:
```go

// Note: ScanAll does the same thing as Scan except it will keep running Scan operations until no results are left
// to be returned

var cached []*dynamodb.ScanOutput

testCacheMiddelWare := ScanAllMiddleWareFunc(func(next ScanAllHandler) ScanAllHandler {
    return ScanAllHandlerFunc(func(ctx *ScanAllContext, output *ScanAllOutput) {
        if cached != nil {
            output.Set(cached, nil)
            return
        }
        next.HandleScanAll(ctx, output)
        out, err := output.Get()
        if err != nil {
            panic(err)
        }
        cached = out
    })
})

input, err := NewScanBuilder(nil).SetTableName(s.table.Name()).Build()
if err != nil {
    panic(err)
}

scan := NewScanAll(input, testCacheMiddelWare)
scanPromise := scan.Invoke(context.Background(), s.client.DynamoDB())
out, err := scanPromise.Await()
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

var scanPromises []*Scan

for _, scan := range []*dynamodb.ScanInput{...} {
    scanPromise := pool.Scan(scanInput)
    scanPromises = append(scanPromises, scanPromise)
}

for _, promise := range ScanPromises {
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

---

NOTE: This module is incomplete and in active development
