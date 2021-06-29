DYNO
====
[![Build Status](https://travis-ci.com/ericmaustin/dyno.svg?branch=master)](https://travis-ci.com/ericmaustin/dyno)
[![Coverage Status](https://coveralls.io/repos/github/ericmaustin/dyno/badge.svg?branch=master)](https://coveralls.io/github/ericmaustin/dyno?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ericmaustin/dyno)](https://goreportcard.com/report/github.com/ericmaustin/dyno)

dyno is an AWS dynomodb API extension library

##Features:

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
scanPromise := db.Scan(scanInput)
scanOutput, err := scanPromise.Await()
```

Each API operation has its own `Promise` type that will return the correct type with `Await()`

### Encoding

More control over struct encoding with `dyno` struct tags including:
- embedding (flattening) other structs or maps within structs with struct tags: `dyno:"*"`
- prepended or appended key values to embedded structs: `dyno:"*,prepend=Foo,append=Bar"`
- enable json conversion with struct tags: `dyno:",json"`
- omit zero values with struct tags: `dyno:",omitzero"`
- omit nil values with struct tags: `dyno:",omitnil`
- omit nil or zero values with struct tags: `dyno:",omitempty`

`ItemMarshaller` and `ItemUnmarshaller` interfaces can be implemented to directly control
how a type will be marshalled or unmarshalled into a DynamoDB record with type `map[string]*dynamodb.AttributeValue`.

Call encoder with `encoding.MarshalItem(myItem)` to marshal a value to a DynamoDB record and apply
dyno struct tags and `ItemMarshaller` interface. 

Or marshal a slice of DynamoDB record all at once items with `encoding.MarshalItems([]myItem{...})`

### Pool

Use the `Pool` type to limit parallel executions
```go
pool := NewPool(context.Background(), db, 10)
scanPromise := pool.Scan(scanInput)
scanOutput, err := scanPromise.Await()
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