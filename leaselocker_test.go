package leaselocker

import (
	"context"
	"errors"
	"sync"
	"testing"

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

func Test_getLeaseHolder(t *testing.T) {
	tests := []struct {
		name       string
		observed   LockRecord
		expectedID string
	}{
		{"valid holder", LockRecord{HolderIdentity: "holder1"}, "holder1"},
		{"empty holder", LockRecord{HolderIdentity: ""}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locker := &LeaseLocker{
				observedRecord: tt.observed,
			}
			if got := locker.getLeaseHolder(); got != tt.expectedID {
				t.Errorf("getLeaseHolder() = %v, want %v", got, tt.expectedID)
			}
		})
	}
}

func Test_holdsLock(t *testing.T) {
	tests := []struct {
		name     string
		observed LockRecord
		identity string
		expected bool
	}{
		{"holds lock", LockRecord{HolderIdentity: "holder1"}, "holder1", true},
		{"does not hold lock", LockRecord{HolderIdentity: "holder1"}, "another-holder", false},
		{"empty identity", LockRecord{HolderIdentity: ""}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockLock{identity: tt.identity}
			locker := &LeaseLocker{
				config:         Config{Lock: mock},
				observedRecord: tt.observed,
			}
			if got := locker.holdsLock(); got != tt.expected {
				t.Errorf("holdsLock() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func Test_fetchLockRecord(t *testing.T) {
	tests := []struct {
		name        string
		mockFail    bool
		expectedErr bool
	}{
		// {"success case", false, false},
		{"failure in Get", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockLock{failGet: tt.mockFail}
			locker := &LeaseLocker{
				config: Config{Lock: mock},
			}
			err := locker.fetchLockRecord(context.TODO())

			if (err != nil) != tt.expectedErr {
				t.Errorf("fetchLockRecord() error = %v, want %v", err != nil, tt.expectedErr)
			}
		})
	}
}

func Test_release(t *testing.T) {
	tests := []struct {
		name      string
		mockFail  bool
		holdsLock bool
		expected  bool
	}{
		{"success case", false, true, true},
		{"failure in Update", true, true, false},
		{"does not hold lock", false, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockLock{failUpdate: tt.mockFail}
			locker := &LeaseLocker{
				config: Config{Lock: mock},
				clock:  clock.RealClock{},
			}

			if tt.holdsLock {
				locker.observedRecord.HolderIdentity = mock.Identity()
			}

			if got := locker.release(); got != tt.expected {
				t.Errorf("release() = %v, want %v", got, tt.expected)
			}
		})
	}
}

//func Test_isLeaseValid(t *testing.T) {
//	now := time.Now()
//
//	tests := []struct {
//		name     string
//		record   LockRecord
//		timeNow  time.Time
//		expected bool
//	}{
//		{
//			name: "valid lease",
//			record: LockRecord{
//				HolderIdentity:       "holder1",
//				RenewTime:            metav1.NewTime(now.Add(-10 * time.Second)),
//				LeaseDurationSeconds: 20,
//			},
//			timeNow:  now,
//			expected: true,
//		},
//		{"expired lease", LockRecord{RenewTime: metav1.NewTime(now.Add(-30 * time.Second)), LeaseDurationSeconds: 20}, now, false},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			locker := &LeaseLocker{}
//			locker.setObservedRecord(&tt.record)
//			if got := locker.isLeaseValid(tt.timeNow); got != tt.expected {
//				t.Errorf("isLeaseValid() = %v, want %v", got, tt.expected)
//			}
//		})
//	}
//}
