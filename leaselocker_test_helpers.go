package leaselocker

import (
	"time"

	"k8s.io/utils/clock"
)

type testClock struct {
	currentTime time.Time
}

func (c *testClock) Now() time.Time {
	return c.currentTime
}

func (c *testClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

// Sleep implements clock.Clock's Sleep method.
func (c *testClock) Sleep(d time.Duration) {
	// No-op for test clock
}

// After implements clock.Clock's After method.
func (c *testClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- c.currentTime.Add(d)
	return ch
}

// NewTimer implements clock.Clock's NewTimer method.
func (c *testClock) NewTimer(d time.Duration) clock.Timer {
	return &testTimer{
		cTime: c.currentTime.Add(d),
	}
}

// Tick implements clock.Clock's Tick method.
func (c *testClock) Tick(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- c.currentTime.Add(d)
	return ch
}

type testTimer struct {
	cTime time.Time
}

func (t *testTimer) C() <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- t.cTime
	return ch
}

func (t *testTimer) Stop() bool {
	return true
}

func (t *testTimer) Reset(d time.Duration) bool {
	return true
}
