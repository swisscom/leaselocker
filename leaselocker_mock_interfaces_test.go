/*
Copyright 2025 Swisscom (Schweiz) AG.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leaselocker

import (
	"context"
	"errors"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

type mockLock struct {
	identity   string
	record     LockRecord
	mutex      sync.Mutex
	failGet    bool
	failUpdate bool
}

func (m *mockLock) Identity() string {
	return m.identity
}

func (m *mockLock) Get(ctx context.Context) (*LockRecord, []byte, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.failGet {
		return &LockRecord{}, nil, errors.New("failed to get lock")
	}
	return &m.record, nil, nil
}

func (m *mockLock) Update(ctx context.Context, record LockRecord) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.failUpdate {
		return errors.New("failed to update lock")
	}
	m.record = record
	return nil
}

func (m *mockLock) RecordEvent(msg string) {}

func (m *mockLock) Describe() string {
	return "MockLock"
}

func (m *mockLock) Create(ctx context.Context, record LockRecord) error {
	return nil
}

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
