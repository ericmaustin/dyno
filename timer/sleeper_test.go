package timer

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSleeper(t *testing.T) {

	sleepFor := time.Millisecond
	now := time.Now()
	sleeper := NewSleeper(sleepFor).
		WithTimeout(time.Millisecond * 3)

	<-sleeper.Sleep()
	<-sleeper.Sleep()
	<-sleeper.Sleep()

	totalDuration := time.Since(now)
	assert.GreaterOrEqual(t, totalDuration.Nanoseconds(), (sleepFor * 3).Nanoseconds())

	err := <-sleeper.Sleep()

	fmt.Printf("error: %v\n", err)
	assert.NotNil(t, err)

	err = <-sleeper.Sleep()

	fmt.Printf("error 2: %v\n", err)
	assert.NotNil(t, err)
}

func TestExponentialSleeperSleeper(t *testing.T) {

	sleepFor := time.Millisecond
	now := time.Now()
	sleeper := NewExponentialSleeper(sleepFor).
		WithTimeout(time.Millisecond * 8)
	sleepStart := now
	<-sleeper.Sleep()
	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(2, 0))).Nanoseconds())
	sleepStart = time.Now()
	<-sleeper.Sleep()
	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(2, 1))).Nanoseconds())
	sleepStart = time.Now()
	<-sleeper.Sleep()
	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(2, 2))).Nanoseconds())

	err := <-sleeper.Sleep()

	fmt.Printf("error: %v\n", err)
	assert.NotNil(t, err)

	err = <-sleeper.Sleep()

	fmt.Printf("error 2: %v\n", err)
	assert.NotNil(t, err)
}
