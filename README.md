# DYNO

dyno is an AWS dynomodb API extension library with built in retry-on-failure, session management, and extensive type
encoding features

Features:
- All operations will retry on failure with exponential backoff
- ORM-like table modeling with the table module: 
    - Create, modify, delete, backup, and restore tables all in code.
- More control over struct encoding with "dyno" struct tags including:
    - embedding (or flattening) other structs or maps
    - prepending or appending strings to embedded structs or map attribute names
    - automatic json encoding struct fields automatically
- Batch execution of any mix of multiple operations with any number of given worker go routines
- Operation builders to facilitate quickly building and executing table operations
    - Single line of code can be used for most operations
    - Handlers for operations that return records 
- Atomic locking on any dynamodb record with the lock module


#### todo: finish documentation on each module