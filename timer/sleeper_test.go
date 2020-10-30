package timer

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSleeper(t *testing.T) {

	sleepFor := time.Second
	now := time.Now()
	sleeper := NewSleeper(sleepFor)

	<-sleeper.Sleep()
	<-sleeper.Sleep()
	<-sleeper.Sleep()
	<-sleeper.Sleep()
	<-sleeper.Sleep()

	totalDuration := time.Since(now)
	assert.GreaterOrEqual(t, totalDuration.Nanoseconds(), (sleepFor * 5).Nanoseconds())

	err := <-sleeper.Sleep()

	fmt.Printf("error: %v\n", err)
	assert.NotNil(t, err)

	err = <-sleeper.Sleep()

	fmt.Printf("error 2: %v\n", err)
	assert.NotNil(t, err)
}

func TestExponentialSleeperSleeper(t *testing.T) {

	sleepFor := time.Second
	now := time.Now()
	sleeper := NewExponentialSleeper(sleepFor)
	sleepStart := now
	<-sleeper.Sleep()
	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(2, 0))).Nanoseconds())
	sleepStart = time.Now()
	<-sleeper.Sleep()
	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(2, 1))).Nanoseconds())
	sleepStart = time.Now()
	<-sleeper.Sleep()
	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(2, 2))).Nanoseconds())
	sleepStart = time.Now()
	<-sleeper.Sleep()
	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(2, 3))).Nanoseconds())
	sleepStart = time.Now()
	<-sleeper.Sleep()
	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(2, 4))).Nanoseconds())
	totalDuration := time.Since(now)
	assert.GreaterOrEqual(t, totalDuration.Nanoseconds(), (sleepFor * 31).Nanoseconds()) // should be > 31 seconds

	err := <-sleeper.Sleep()

	fmt.Printf("error: %v\n", err)
	assert.NotNil(t, err)

	err = <-sleeper.Sleep()

	fmt.Printf("error 2: %v\n", err)
	assert.NotNil(t, err)
}
