package timerange

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/stretchr/testify/assert"
)

func TestTimeRange(t *testing.T) {

	now := time.Now()

	tr := NewTimeRangeFromStart(now, time.Hour, true)
	fmt.Printf("TimeRange: start = %v\tEnd = %v\n", tr.Start, tr.End)
	trNext := tr.Next(1)
	fmt.Printf("NEXT TimeRange: start = %v\tEnd = %v\n", trNext.Start, trNext.End)

	assert.True(t, trNext.Start.After(tr.Start))
	trPrev := trNext.Prev(1)
	assert.True(t, trPrev.Start == tr.Start)

	trNoRound := NewTimeRangeFromStart(now, time.Hour, false)
	fmt.Printf("TimeRange: start = %v\tEnd = %v\n\tPeriodStart = %v\n",
		trNoRound.Start, trNoRound.End, trNoRound.PeriodStart)

	ts, err := NewTimeSeriesBetween(now,
		now.Add(time.Hour*1000),
		time.Hour, false, false)

	assert.NoError(t, err)

	fmt.Printf("TimeSeries start = %v End = %v\n", ts.Start, ts.End)

	assert.True(t, ts.Periods[0].Start == ts.Start)
	assert.True(t, ts.Periods[len(ts.Periods)-1].End == ts.End)

	findthisTimeTime := now.Add(time.Hour * time.Duration(4))
	found := ts.FindTimeRange(findthisTimeTime)

	assert.NotNil(t, found)

	fmt.Printf("AsTime to find = %v\n", findthisTimeTime)
	fmt.Printf("TimeSeries Found start = %v End = %v\n", found.Start, found.End)

	assert.True(t,
		(findthisTimeTime.After(found.Start) || findthisTimeTime == found.Start) &&
			(findthisTimeTime.Before(found.End) || findthisTimeTime == found.End))

	// TEST JSON
	jTr, err := json.Marshal(tr)
	fmt.Printf("Marshalled JSON AsTime Range:\n%s", jTr)
	assert.NoError(t, err)

	trUnmarshalled := &TimeRange{}

	err = json.Unmarshal(jTr, trUnmarshalled)
	assert.NoError(t, err)

	fmt.Printf("JSON Unmarshalled TR start = %v End = %v\n", trUnmarshalled.Start,
		trUnmarshalled.End)

	jTs, err := json.Marshal(ts)
	assert.NoError(t, err)

	tsUnmarshalled := &TimeSeries{}
	err = json.Unmarshal(jTs, tsUnmarshalled)
	assert.NoError(t, err)

	// TEST DYNAMODB ATTRIBUTE
	dTr, err := dynamodbattribute.Marshal(tr)
	assert.NoError(t, err)
	fmt.Printf("Marshalled DynamoDB AsTime Range:\n%v", dTr)

	dTrUnmarshalled := &TimeRange{}
	err = dynamodbattribute.Unmarshal(dTr, dTrUnmarshalled)
	assert.NoError(t, err)

	fmt.Printf("DYNAMODB Unmarshalled TR start = %v End = %v\n",
		dTrUnmarshalled.Start, dTrUnmarshalled.End)
}
