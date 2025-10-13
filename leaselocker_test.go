package leaselocker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func Test_tryAcquireOrRenew(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(*LeaseLocker, *mockLock)
		expectedResult bool
		checkState     func(*testing.T, *LeaseLocker, *mockLock)
	}{
		{
			name: "acquire new lock when none exists",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.failGet = false
				m.record = LockRecord{}
			},
			expectedResult: true,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if m.record.HolderIdentity != l.config.Lock.Identity() {
					t.Errorf("Expected holder identity to be %s, got %s", l.config.Lock.Identity(), m.record.HolderIdentity)
				}
			},
		},
		{
			name: "fail to get lock record",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.failGet = true
			},
			expectedResult: false,
			checkState:     func(t *testing.T, l *LeaseLocker, m *mockLock) {},
		},
		{
			name: "fail to update existing lock",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.failGet = false
				m.failUpdate = true
				m.record = LockRecord{
					HolderIdentity: "other-holder",
					RenewTime:      metav1.NewTime(time.Now().Add(-2 * time.Hour)),
				}
			},
			expectedResult: false,
			checkState:     func(t *testing.T, l *LeaseLocker, m *mockLock) {},
		},
		{
			name: "successfully renew own lock",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity:   m.identity,
					RenewTime:        metav1.NewTime(time.Now()),
					LeaseTransitions: 1,
				}
				l.setObservedRecord(&m.record)
			},
			expectedResult: true,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if m.record.LeaseTransitions != 1 {
					t.Errorf("Expected lease transitions to remain 1, got %d", m.record.LeaseTransitions)
				}
			},
		},
		{
			name: "acquire expired lock",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity:   "other-holder",
					RenewTime:        metav1.NewTime(time.Now().Add(-2 * time.Hour)),
					LeaseTransitions: 1,
				}
			},
			expectedResult: true,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if m.record.LeaseTransitions != 2 {
					t.Errorf("Expected lease transitions to be 2, got %d", m.record.LeaseTransitions)
				}
			},
		},
		{
			name: "fail to acquire valid lock held by other",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity:       "other-holder",
					RenewTime:            metav1.NewTime(time.Now()),
					LeaseDurationSeconds: 60,
				}
			},
			expectedResult: false,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if m.record.HolderIdentity != "other-holder" {
					t.Errorf("Expected holder to remain other-holder, got %s", m.record.HolderIdentity)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLock := &mockLock{
				identity: "holder1",
			}

			locker := &LeaseLocker{
				config: Config{
					Lock:          mockLock,
					LeaseDuration: 60 * time.Second,
				},
				clock: clock.RealClock{},
			}

			tt.setup(locker, mockLock)

			result := locker.tryAcquireOrRenew(context.Background())

			if result != tt.expectedResult {
				t.Errorf("tryAcquireOrRenew() = %v, want %v", result, tt.expectedResult)
			}

			tt.checkState(t, locker, mockLock)
		})
	}
}

func TestLeaseLocker_Check(t *testing.T) {
	tests := []struct {
		name                string
		holdsLock           bool
		observedTime        time.Time
		leaseDuration       time.Duration
		maxTolerableExpired time.Duration
		currentTime         time.Time
		wantErr             bool
	}{
		{
			name:                "within tolerable lease expiration",
			holdsLock:           true,
			observedTime:        time.Now(),
			leaseDuration:       10 * time.Second,
			maxTolerableExpired: 5 * time.Second,
			currentTime:         time.Now().Add(14 * time.Second),
			wantErr:             false,
		},
		{
			name:                "exceeds tolerable lease expiration",
			holdsLock:           true,
			observedTime:        time.Now(),
			leaseDuration:       10 * time.Second,
			maxTolerableExpired: 5 * time.Second,
			currentTime:         time.Now().Add(16 * time.Second),
			wantErr:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClock := &testClock{
				currentTime: tt.currentTime,
			}

			l := &LeaseLocker{
				config: Config{
					LeaseDuration: tt.leaseDuration,
					Name:          "test-lease",
				},
				clock:        mockClock,
				observedTime: tt.observedTime,
			}

			if tt.holdsLock {
				l.observedRecord.HolderIdentity = "test-holder"
				l.config.Lock = &mockLock{identity: "test-holder"}
			}

			err := l.Check(tt.maxTolerableExpired)
			if (err != nil) != tt.wantErr {
				t.Errorf("Check() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_isLeaseValid(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		record   LockRecord
		timeNow  time.Time
		expected bool
	}{
		{
			name: "valid lease",
			record: LockRecord{
				HolderIdentity:       "holder1",
				RenewTime:            metav1.NewTime(now.Add(-10 * time.Second)),
				LeaseDurationSeconds: 20,
			},
			timeNow:  now,
			expected: true,
		},
		{
			name: "expired lease",
			record: LockRecord{
				HolderIdentity:       "",
				RenewTime:            metav1.NewTime(now.Add(-30 * time.Second)),
				LeaseDurationSeconds: 20,
			},
			timeNow:  now,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locker := &LeaseLocker{
				observedRecord: tt.record,
			}
			if got := locker.isLeaseValid(tt.timeNow); got != tt.expected {
				t.Errorf("isLeaseValid() = %v, want %v", got, tt.expected)
			}
		})
	}
}
