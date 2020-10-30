package operation

import (
	"fmt"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/timer"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strings"
	"time"
)

const (
	// StatusCreating set as table status when table is being created
	tableStatusCreating = "CREATING"
	// StatusDeleting set as table status when table is being deleted
	tableStatusDeleting = "DELETING"
	// StatisActive set as table status when table is active
	tableStatisActive = "ACTIVE"
	// StatusUpdating set as table status when table is being updated
	tableStatusUpdating = "UPDATING"
	// StatusArchiving The table is being archived. Operations are not allowed until archival is complete.
	tableStatusArchiving = "ARCHIVING"
	// StatusArchived set when a table has been saved as a flat file and deleted
	tableStatusArchived = "ARCHIVED"
	// INACCESSIBLE_ENCRYPTION_CREDENTIALS
	tableStatusInaccessibleEncryptionCrednetials = "INACCESSIBLE_ENCRYPTION_CREDENTIALS"
)

func WaitForTableReady(req *dyno.Request, tableName string, timeout *time.Duration) (out *dynamodb.DescribeTableOutput, err error) {

	sleeper := timer.NewSleeper(time.Millisecond * 100).
		WithContext(req.Ctx()).
		WithAddRandom(time.Millisecond * 100)

	if timeout != nil {
		sleeper.WithTimeout(*timeout)
	} else {
		deadline, ok := req.Ctx().Deadline()
		if ok {
			sleeper.WithTimeout(time.Until(deadline))
		}
	}

	// loop until context cancelled or table is active
	for {
		// getActive the description res
		descRes, resErr := DescribeTable(tableName).
			Execute(req).
			OutputError()
		// set the error if there was one
		if resErr != nil {
			if !dyno.IsAwsErrorCode(resErr, dynamodb.ErrCodeResourceNotFoundException) {
				err = resErr
				return
			}
		} else if descRes.Table != nil {
			switch strings.ToUpper(*descRes.Table.TableStatus) {
			case tableStatisActive:
				return descRes, nil
			case tableStatusDeleting,
				tableStatusArchiving,
				tableStatusInaccessibleEncryptionCrednetials,
				tableStatusArchived:
				return nil, &dyno.Error{
					Code: dyno.ErrTableIsInInvalidState,
					Message: fmt.Sprintf("table '%s' is records an invalid state: '%v'",
						tableName,
						*descRes.Table.TableStatus),
				}
			}
		}

		err = <-sleeper.Sleep()
		if err != nil {
			return
		}
	} // end of for loop
}

// WaitForTableArchive waits for table to be archived, when done the DescribeTableOutput is returned
func WaitForTableArchive(req *dyno.Request, tableName string, timeout *time.Duration) (out *dynamodb.DescribeTableOutput, err error) {

	sleeper := timer.NewSleeper(time.Millisecond * 100).
		WithContext(req.Ctx()).
		WithAddRandom(time.Millisecond * 100)

	if timeout != nil {
		sleeper.WithTimeout(*timeout)
	}

	// loop until context cancelled or table is active
	for {
		// getActive the description res
		out, err = DescribeTable(tableName).
			Execute(req).
			OutputError()
		// set the error if there was one
		if err != nil {
			return
		} else if out.Table != nil {
			switch strings.ToUpper(*out.Table.TableStatus) {
			case tableStatusArchived:
				return
			case tableStatusDeleting,
				tableStatusInaccessibleEncryptionCrednetials,
				tableStatusCreating:
				err = &dyno.Error{
					Code: dyno.ErrTableIsInInvalidState,
					Message: fmt.Sprintf("table '%s' is records an invalid state: '%v'",
						tableName, *out.Table.TableStatus),
				}
				return
			}
		}
		// sleep some more
		err = <-sleeper.Sleep()
		if err != nil {
			return
		}
	} // end of for loop
}

func WaitForTableDeletion(req *dyno.Request, tableName string, timeout *time.Duration) (err error) {

	sleeper := timer.NewSleeper(time.Millisecond * 100).
		WithContext(req.Ctx()).
		WithAddRandom(time.Millisecond * 100)

	if timeout != nil {
		sleeper.WithTimeout(*timeout)
	} else {
		deadline, ok := req.Ctx().Deadline()
		if ok {
			sleeper.WithTimeout(time.Until(deadline))
		}
	}

	// loop until context cancelled or table is active
	for {
		// getActive the description res
		descRes, resErr := DescribeTable(tableName).
			Execute(req).
			OutputError()

		// set the error if there was one
		if resErr != nil && dyno.IsAwsErrorCode(resErr, dynamodb.ErrCodeResourceNotFoundException) {
			// this error means successful deletion
			return nil
		}

		if descRes.Table != nil {
			switch strings.ToUpper(*descRes.Table.TableStatus) {
			case tableStatusArchived,
				tableStatusCreating,
				tableStatusArchiving,
				tableStatusUpdating:
				// table is records an unexpected state
				return &dyno.Error{
					Code: dyno.ErrTableIsInInvalidState,
					Message: fmt.Sprintf("table '%s' is records an invalid state: '%v'",
						tableName, *descRes.Table.TableStatus),
				}
			}
		}

		err = <-sleeper.Sleep()
		if err != nil {
			return
		}
	} // end of for loop
}

type WaitForBackupInput struct {
	BackupArn string
	Request   *dyno.Request
	Timeout   time.Duration
}

// WaitForBackupCompletion waits for backup to be completed
func WaitForBackupCompletion(req *dyno.Request, backupArn string, timeout *time.Duration) (out *dynamodb.DescribeBackupOutput, err error) {

	sleeper := timer.NewSleeper(time.Millisecond * 100).
		WithContext(req.Ctx()).
		WithAddRandom(time.Millisecond * 100)

	if timeout != nil {
		sleeper.WithTimeout(*timeout)
	} else {
		deadline, ok := req.Ctx().Deadline()
		if ok {
			sleeper.WithTimeout(time.Until(deadline))
		}
	}

	// loop until context cancelled or table is active
	for {
		// getActive the description res
		out, err = DescribeBackup(backupArn).
			Execute(req).
			OutputError()

		if err != nil {
			if !dyno.IsAwsErrorCode(err, dynamodb.ErrCodeBackupNotFoundException) {
				// unexpected error
				return
			}
		} else {
			switch strings.ToUpper(*out.BackupDescription.BackupDetails.BackupStatus) {
			case "ACTIVE":
				// backup completed
				return
			case "DELETED":
				// backup invalid
				err = &dyno.Error{
					Code:    dyno.ErrBackupInInvalidState,
					Message: fmt.Sprintf("%v backup was already deleted", backupArn),
				}
				return
			}
		}

		err = <-sleeper.Sleep()
		if err != nil {
			return
		}
	} // end of for loop
}
