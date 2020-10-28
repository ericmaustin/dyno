package timer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTiming(t *testing.T) {
	timer := NewTimer()
	time.Sleep(time.Second)
	timer.Stop()
	_ = timer.Start()
	time.Sleep(time.Second)
	timer.Stop()

	assert.GreaterOrEqual(t, int64(timer.Duration()), int64(time.Second*time.Duration(2)))
	fmt.Printf("Timer duration = %v", timer.Duration())
}
