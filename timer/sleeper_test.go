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
	sleeper := NewSleeper(sleepFor)

	<-sleeper.Sleep()
	<-sleeper.Sleep()
	<-sleeper.Sleep()

	totalDuration := time.Since(now)
	assert.GreaterOrEqual(t, totalDuration.Nanoseconds(), (sleepFor * 3).Nanoseconds())
}

func TestExponentialSleeperSleeper(t *testing.T) {

	sleepFor := time.Millisecond
	now := time.Now()
	alpha := float64(2)
	sleeper := NewExponentialSleeper(sleepFor, alpha)
	sleepStart := now

	<-sleeper.Sleep()

	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(alpha, 0))).Nanoseconds())
	sleepStart = time.Now()

	<-sleeper.Sleep()

	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(alpha, 1))).Nanoseconds())
	sleepStart = time.Now()

	<-sleeper.Sleep()

	assert.GreaterOrEqual(t, time.Since(sleepStart).Nanoseconds(), (sleepFor * time.Duration(math.Pow(alpha, 2))).Nanoseconds())

	err := <-sleeper.Sleep()

	fmt.Printf("error: %v\n", err)
}
