package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/operation"
	"github.com/ericmaustin/dyno/table"
	"github.com/google/uuid"
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

type (
	stopHeartBeatAck struct {
		err error
	}

	// Lock is a representation of a lock on a specific record in a table
	Lock struct {
		Session                *dyno.Session
		Item                   interface{}   // the item that is being locked
		Table                  *table.Table  // the table that this lock belongs to
		SessionID              *uuid.UUID    // this lock sessionId
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

	// create a ticker for the lock request
	ticker := time.NewTicker(100 * time.Millisecond)
	timer := time.NewTimer(dl.LockTimeout)

	defer func() {
		ticker.Stop()
		timer.Stop()
	}()

	timeNow := time.Now()
	// create a new sessionID this is the ID that we will check for when we upsert
	sessionID := uuid.New()
	// set the value of the current lease expiration to now + lease duration
	dl.currentLeaseExpires = timeNow.Add(dl.LeaseDuration)

	dyKey := dl.Table.ExtractKey(dl.Item)

	updateInput := operation.NewUpdateItemBuilder().
		// Set the table name
		SetTable(dl.Table.Name()).
		// set the key
		SetKey(dyKey).
		// return all new values
		SetReturnValues(operation.UpdateReturnUpdatedNew).
		Set(LeaseFieldName, dl.LeaseDuration).
		// set the lock expiration to = lock expiration time
		Set(ExpiresFieldName, UnixTime(dl.currentLeaseExpires)).
		// set the lock version to the sessionId
		Set(VersionFieldName, UUID(sessionID)).
		// create the condition update Expression to make sure another process has not
		//  already locked this record.
		//  We can lock the record if the lock Expiration < Now or lock Expiration is Nil
		AddCondition(condition.Or(
			condition.NotExists(ExpiresFieldName),
			// MapLock Expiration must be < Now
			condition.LessThan(ExpiresFieldName, UnixTime(timeNow)),
			condition.Equal(ExpiresFieldName, UnixTime(time.Time{})))).Build()

	// loop until we acquire the lock or timeout is hit
	for {
		select {
		case <-timer.C:
			return &dyno.Error{Code: dyno.ErrLockTimeout}
		case <-ticker.C:

			req := dl.Session.RequestWithTimeout(dl.LockHeartbeatFrequency)

			updateOutput, err := req.UpdateItem(updateInput)

			if err != nil {
				if dyno.IsAwsErrorCode(err, dynamodb.ErrCodeConditionalCheckFailedException) {
					// conditional check failed... another process/routine holds the lock
					continue // keep looping
				} else {
					select {
					case <-dl.Context.Done():
						// outer context cancelled, return now
						return &dyno.Error{
							Code:    dyno.ErrLockFailedToAcquire,
							Message: "context cancelled before lock was acquired",
						}
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
				if *updateOutput.Attributes[VersionFieldName].S == sessionID.String() {
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

			// stop ticker when func exits
			ticker.Stop()
			// lock has been released
			dl.HasLock = false
			dl.SessionID = nil

			if r := recover(); r != nil {
				switch r.(type) {
				case error:
					ack = stopHeartBeatAck{&dyno.Error{
						Code:    dyno.ErrLockFailedLeaseRenewal,
						Message: fmt.Sprintf("%v", r),
					}}
				default:
					ack = stopHeartBeatAck{&dyno.Error{
						Code:    dyno.ErrLockFailedLeaseRenewal,
						Message: fmt.Sprintf("%v", r),
					}}
				}
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
				dl.clear()
				return
			// create the update Expression to set the fields for our lock fields
			case <-ticker.C:
				dl.renew()
			}
		}
	}()
}

// renew renews the lease for this lock
func (dl *Lock) renew() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dyKey := dl.Table.ExtractKey(dl.Item)

	// extend the lease by now + lease duration
	dl.currentLeaseExpires = time.Now().Add(dl.LeaseDuration)

	updateInput := operation.NewUpdateItemBuilder().
		SetTable(dl.Table.Name()).
		SetKey(dyKey).
		SetReturnValues(operation.UpdateReturnUpdatedNew).
		Set(ExpiresFieldName, UnixTime(dl.currentLeaseExpires)).
		AddCondition(condition.Equal(VersionFieldName, UUID(*dl.SessionID))).
		Build()

	req := dl.Session.RequestWithTimeout(time.Minute)

	output, err := req.UpdateItem(updateInput)

	select {
	case <-dl.Context.Done():
		return
	default:
		// keep going
	}

	// panic if we got an error trying to update the record.
	if err != nil {
		panic(fmt.Errorf("got error when attempting to update %s lock '%s' lease duration. Error: %v",
			dl.Table.Name(), *dl.SessionID, err))
	}
	if _, ok := output.Attributes[ExpiresFieldName]; !ok {
		panic(&dyno.Error{Code: dyno.ErrLockFailedLeaseRenewal})
	}
}

// clear removes the lock info in dynamodb
func (dl *Lock) clear() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	// lease expires updated to "zero" time
	dl.currentLeaseExpires = time.Time{}

	dyKey := dl.Table.ExtractKey(dl.Item)

	updateInput := operation.NewUpdateItemBuilder().
		SetTable(dl.Table.Name()).
		SetKey(dyKey).
		SetReturnValues(operation.UpdateReturnUpdatedNew).
		Set(ExpiresFieldName, UnixTime(dl.currentLeaseExpires)).
		AddCondition(condition.Equal(VersionFieldName, UUID(*dl.SessionID))).
		Build()

	_, err := dl.Session.RequestWithTimeout(time.Minute).UpdateItem(updateInput)

	// panic if we got an error trying to update the record.
	if err != nil {
		if !dyno.IsAwsErrorCode(err, dynamodb.ErrCodeConditionalCheckFailedException) {
			panic(fmt.Errorf("got error when attempting to clear lock '%s' session: %v",
				err, *dl.SessionID))
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
	Table              *table.Table `validate:"required"`
	Item               interface{}  `validate:"required"`
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
func Acquire(tbl *table.Table, item interface{}, sess *dyno.Session, opts ...Opt) (lock *Lock, err error) {

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	options := &Opts{}

	for _, o := range opts {
		o(options)
	}

	if options.Context == nil {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithCancel(options.Context)
	}

	// create default lock
	lock = &Lock{
		Session:                sess,
		Item:                   item,
		Table:                  tbl,
		HasLock:                false,
		LockTimeout:            DefaultTimeout,
		LeaseDuration:          DefaultLeaseDuration,
		LockHeartbeatFrequency: DefaultHeartbeatFrequency,
		Context:                ctx,
		cancel:                 cancel,
		mu:                     sync.Mutex{},
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
func MustAcquire(tbl *table.Table, item interface{}, sess *dyno.Session, opts ...Opt) *Lock {
	lock, err := Acquire(tbl, item, sess, opts...)
	if err != nil {
		panic(err)
	}
	return lock
}
