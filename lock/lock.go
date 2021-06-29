package lock

import (
	"context"
	"errors"
	"fmt"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/segmentio/ksuid"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
)

const (
	// LeaseFieldName is the name of the fields that specifies the lock lease duration for a lock
	LeaseFieldName = "dynoLockLeaseDuration"
	// VersionFieldName is the name of the fields that specifies the version of the record used for locking
	VersionFieldName = "dynoLockVersion"
	// ExpiresFieldName is the name of the fields that specifies when the lock will expire
	ExpiresFieldName = "dynoLockExpires"
)

const (
	//DefaultTimeout is the default timeout for the lock
	DefaultTimeout = time.Second
	//DefaultLeaseDuration is the default lock lease duration
	DefaultLeaseDuration = time.Millisecond * 500
	//DefaultHeartbeatFrequency is the default hertbean freq
	DefaultHeartbeatFrequency = time.Millisecond * 100
)

var (
	//ErrLockFailedToAcquire returned by Lock.Acquire if lock could not be acquired
	ErrLockFailedToAcquire = errors.New("lock failed to be acquired")
	//ErrLockFailedLeaseRenewal returned by Lock if lock lease could not be renewed
	ErrLockFailedLeaseRenewal = errors.New("lock failed to renew its lease")
	//ErrCodeConditionalCheckFailedException returned when lock fails a conditional check
	ErrCodeConditionalCheckFailedException = errors.New("lock failed on conditional check")
)

type (
	stopHeartBeatAck struct {
		err error
	}

	// Lock is a representation of a lock on a specific record in a table
	Lock struct {
		DB                     *dyno.Client
		Key                    map[string]types.AttributeValue // the item that is being locked
		TableName              string
		SessionID              *string       // this lock sessionId
		HasLock                bool          // whether or not we have a lock
		LockTimeout            time.Duration // the lock timeout
		LockHeartbeatFrequency time.Duration // the lock heartbeat frequency. We will update the lock expiration with every heartbeat
		LeaseDuration          time.Duration // the lease duration or how much time we will lease the lock for. This must be greater than or equal to heartbeat freq.
		currentLeaseExpires    time.Time     // the expiration time of the current lease
		stopHeartBeatAckChan   chan *stopHeartBeatAck
		Context                context.Context    // context of the lock, should be released on cancel
		cancel                 context.CancelFunc // cancel func that will release the lock
		mu                     sync.Mutex
	}
)

/*
Acquire the lock by attempting to update release the lock.
 will wait for a given time duration to acquire the lock
*/
func (dl *Lock) Acquire() (err error) {
	var (
		updateInput *dynamodb.UpdateItemInput
	)
	// create a ticker for the lock request
	ticker := time.NewTicker(100 * time.Millisecond)
	timer := time.NewTimer(dl.LockTimeout)

	defer func() {
		ticker.Stop()
		timer.Stop()
	}()

	timeNow := time.Now()
	// create a new sessionID this is the ID that we will check for when we upsert
	sessionID := ksuid.New().String()
	// set the value of the current lease expiration to now + lease duration
	dl.currentLeaseExpires = timeNow.Add(dl.LeaseDuration)

	updateInput, err = dl.getUpdateBuilder().
		// return all new values
		Set(LeaseFieldName, dl.LeaseDuration).
		// set the lock expiration to = lock expiration time
		Set(ExpiresFieldName, dl.currentLeaseExpires.UnixNano()).
		// set the lock version to the sessionId
		Set(VersionFieldName, sessionID).
		AddCondition(condition.Or(
			condition.NotExists(ExpiresFieldName),
			// MapLock Expiration must be < Now
			condition.LessThan(ExpiresFieldName, timeNow.UnixNano()),
			condition.Equal(ExpiresFieldName, 0))).Build()

	if err != nil {
		return err
	}

	ctx, done := context.WithTimeout(dl.Context, dl.LockHeartbeatFrequency)
	defer done()
	// loop until we acquire the lock or timeout is hit
	for {
		select {
		case <-timer.C:
			return &dyno.Error{Code: dyno.ErrLockTimeout}
		case <-ticker.C:

			updateOutput, err := dl.DB.UpdateItem(ctx, updateInput)
			if err != nil {
				var conditionalCheckError *types.ConditionalCheckFailedException
				if errors.As(err, &conditionalCheckError) {
					// conditional check failed... another process/routine holds the lock
					continue // keep looping
				} else {
					select {
					case <-dl.Context.Done():
						// outer context cancelled, return now
						return ErrLockFailedToAcquire
					default:
						// don't block
					}
					// not an aws error, return it
					return err
				}
			}
			// parse the output
			// if returned includes the lock version fields name key, then it's safe to assume
			//  update went through
			if _, ok := updateOutput.Attributes[VersionFieldName]; ok {
				// double check against expected ID string
				if updateOutput.Attributes[VersionFieldName].(*types.AttributeValueMemberS).Value == sessionID {
					// if sessionID is the current session id then we've got the lock
					// start the heartbeat check
					dl.HasLock = true
					dl.SessionID = &sessionID
					dl.StartHeartbeat()
					return nil
				}
			}
		} // select
	} // for loop
}

// Release the lock
func (dl *Lock) Release() error {
	// cancel the context
	dl.cancel()
	// wait for ack from heartbeat before returning
	ack := <-dl.stopHeartBeatAckChan
	return ack.err
}

// MustRelease attempts to release the lock and panics on error
func (dl *Lock) MustRelease() {
	err := dl.Release()
	if err != nil {
		panic(err)
	}
}

// StartHeartbeat starts a go routine that will update the lease no the lock even ``freq``
func (dl *Lock) StartHeartbeat() {

	go func() {
		// create a new Ticker to tick every heartbeat freq interval
		ticker := time.NewTicker(dl.LockHeartbeatFrequency)
		// create the heartbeat ack channel
		dl.stopHeartBeatAckChan = make(chan *stopHeartBeatAck)
		defer func() {
			var ack stopHeartBeatAck
			dl.clear()

			// stop ticker when func exits
			ticker.Stop()
			// lock has been released
			dl.HasLock = false
			dl.SessionID = nil

			if r := recover(); r != nil {
				ack = stopHeartBeatAck{ErrLockFailedLeaseRenewal}
			} else {
				ack = stopHeartBeatAck{}
			}
			// pass the ack signal through the channel to be picked up by the Release() call
			dl.stopHeartBeatAckChan <- &ack
			// close the channel
			close(dl.stopHeartBeatAckChan)
		}()

		for {
			select {
			case <-dl.Context.Done():
				// clear the lock and return
				return
			// create the update Expression to set the fields for our lock fields
			case <-ticker.C:
				dl.renew()
			}
		}
	}()
}

func (dl *Lock) getUpdateBuilder() *dyno.UpdateItemBuilder {
	return dyno.NewUpdateItemBuilder(nil).
		SetTableName(dl.TableName).
		SetKey(dl.Key).
		SetReturnValues(types.ReturnValueUpdatedNew)
}

// renew renews the lease for this lock
func (dl *Lock) renew() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.currentLeaseExpires = time.Now().Add(dl.LeaseDuration)

	updateInput, err := dl.getUpdateBuilder().
		AddCondition(condition.Equal(VersionFieldName, *dl.SessionID)).
		Set(ExpiresFieldName, dl.currentLeaseExpires.UnixNano()).
		Build()

	if err != nil {
		panic(err)
	}

	ctx, done := context.WithTimeout(dl.Context, dl.LeaseDuration)
	defer done()

	output, err := dl.DB.UpdateItem(ctx, updateInput)

	select {
	case <-dl.Context.Done():
		return
	default:
		// keep going
	}

	// panic if we got an error trying to update the record.
	if err != nil {
		panic(fmt.Errorf("got error when attempting to update %s lock '%s' lease duration. Error: %v",
			dl.TableName, *dl.SessionID, err))
	}
	if _, ok := output.Attributes[ExpiresFieldName]; !ok {
		panic(ErrLockFailedLeaseRenewal)
	}
}

// clear removes the lock info in dynamodb
func (dl *Lock) clear() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	updateInput, err := dyno.NewUpdateItemBuilder(nil).
		SetTableName(dl.TableName).
		SetKey(dl.Key).
		SetReturnValues(types.ReturnValueUpdatedNew).
		Set(ExpiresFieldName, 0).
		AddCondition(condition.Equal(VersionFieldName, *dl.SessionID)).
		Build()

	if _, err = dl.DB.UpdateItem(context.Background(), updateInput); err != nil {
		var conditionalCheckErr *types.ConditionalCheckFailedException
		if !errors.As(err, &conditionalCheckErr) {
			panic(ErrCodeConditionalCheckFailedException)
		}
	}
}

//Opts is used as the input for the Acquire func
//Required:
//	Document: the record that will be locked
//	KeyValues: the key fields for the record that will be locked
//Optional:
//	Timeout: the timeout as DurationNano for the lock
//	HeartbeatFreq: the freq of the heartbeat to renew the lock lease
//	LeaseDuration: the duration of the lease as a DurationNano
type Opts struct {
	Table              *dyno.Table `validate:"required"`
	Item               interface{} `validate:"required"`
	Timeout            time.Duration
	HeartbeatFrequency time.Duration
	LeaseDuration      time.Duration
	Context            context.Context
}

//Opt is an option
type Opt func(*Opts)

//OptTimeout sets the timeout for the lock
func OptTimeout(timeout time.Duration) Opt {
	return func(o *Opts) {
		o.Timeout = timeout
	}
}

//OptHeartbeatFrequency sets the heartbeat frequency for the lock
func OptHeartbeatFrequency(freq time.Duration) Opt {
	return func(o *Opts) {
		o.HeartbeatFrequency = freq
	}
}

//OptLeaseDuration sets the lease duration
func OptLeaseDuration(dur time.Duration) Opt {
	return func(o *Opts) {
		o.LeaseDuration = dur
	}
}

//OptContext sets the lock's context
func OptContext(ctx context.Context) Opt {
	return func(o *Opts) {
		o.Context = ctx
	}
}

/*
Acquire acquires a lock for a given table with a given map of key fields
*/
func Acquire(tableName string, itemKey interface{}, db *dyno.Client, opts ...Opt) (lock *Lock, err error) {

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	options := &Opts{}

	for _, o := range opts {
		o(options)
	}

	// marshal the item
	itemMap, err := encoding.MarshalMap(itemKey)
	if err != nil {
		return nil, err
	}

	if options.Context == nil {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithCancel(options.Context)
	}

	// create default lock
	lock = &Lock{
		DB:                     db,
		Key:                    itemMap,
		TableName:              tableName,
		HasLock:                false,
		LockTimeout:            DefaultTimeout,
		LeaseDuration:          DefaultLeaseDuration,
		LockHeartbeatFrequency: DefaultHeartbeatFrequency,
		Context:                ctx,
		cancel:                 cancel,
	}

	// apply options to lock object
	if options.Timeout > 0 {
		lock.LockTimeout = options.Timeout
	}

	if options.LeaseDuration > 0 {
		lock.LeaseDuration = options.LeaseDuration
	}

	if options.HeartbeatFrequency > 0 {
		lock.LockHeartbeatFrequency = options.HeartbeatFrequency
	}

	// if we have an error return it with no lock
	if err = lock.Acquire(); err != nil {
		return nil, err
	}
	// return the lock
	return lock, nil
}

// MustAcquire acquires a lock or panics
func MustAcquire(tableName string, itemKey interface{}, sess *dyno.Client, opts ...Opt) *Lock {
	lock, err := Acquire(tableName, itemKey, sess, opts...)
	if err != nil {
		panic(err)
	}
	return lock
}
