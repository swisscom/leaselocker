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
	"net/url"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"
)

func TestNewLeaseLocker(t *testing.T) {
	// We intentionally don't check for more than the url.Error type here because otherwise we'd need to add the Kubernetes API mocks
	restConfig := &rest.Config{}
	namespacedName := types.NamespacedName{Namespace: "default", Name: "test-lease"}
	identity := "test-holder"

	_, err := NewLeaseLocker(restConfig, namespacedName, identity)
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		t.Errorf("NewLeaseLocker() unexpected error: %v", err)
	}
}

func TestNewLeaseLockerWithConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		wantErr     bool
		checkLocker func(*testing.T, *LeaseLocker)
	}{
		{
			name: "valid config",
			config: Config{
				Lock:          &mockLock{identity: "test-holder"},
				LeaseDuration: 60 * time.Second,
				Name:          "test-lease",
				RenewDeadline: 10 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
			wantErr: false,
			checkLocker: func(t *testing.T, l *LeaseLocker) {
				if l.config.Lock == nil {
					t.Error("Lock not set")
				}
				if l.config.LeaseDuration != 60*time.Second {
					t.Error("Incorrect lease duration")
				}
				if l.config.Name != "test-lease" {
					t.Error("Incorrect name")
				}
			},
		},
		{
			name: "missing lock",
			config: Config{
				LeaseDuration: 60 * time.Second,
				Name:          "test-lease",
				RenewDeadline: 10 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "missing lease duration",
			config: Config{
				Lock:          &mockLock{identity: "test-holder"},
				Name:          "test-lease",
				RenewDeadline: 10 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "negative lease duration",
			config: Config{
				Lock:          &mockLock{identity: "test-holder"},
				LeaseDuration: -60 * time.Second,
				Name:          "test-lease",
				RenewDeadline: 10 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "lease duration is zero",
			config: Config{
				Lock:          &mockLock{identity: "test-holder"},
				LeaseDuration: 0 * time.Second,
				Name:          "test-lease",
				RenewDeadline: 10 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "negative renew deadline",
			config: Config{
				Lock:          &mockLock{identity: "test-holder"},
				LeaseDuration: 60 * time.Second,
				Name:          "test-lease",
				RenewDeadline: -10 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "renew deadline is zero",
			config: Config{
				Lock:          &mockLock{identity: "test-holder"},
				LeaseDuration: 60 * time.Second,
				Name:          "test-lease",
				RenewDeadline: 0 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "renew deadline less than retry period",
			config: Config{
				Lock:          &mockLock{identity: "test-holder"},
				LeaseDuration: 60 * time.Second,
				Name:          "test-lease",
				RenewDeadline: 1 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locker, err := newLeaseLockerWithConfig(tt.config)

			if (err != nil) != tt.wantErr {
				t.Errorf("newLeaseLockerWithConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkLocker != nil {
				tt.checkLocker(t, locker)
			}
		})
	}
}

func TestLeaseLocker_getLeaseHolder(t *testing.T) {
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

func TestLeaseLocker_holdsLock(t *testing.T) {
	tests := []struct {
		name         string
		observed     LockRecord
		identity     string
		wantHoldLock bool
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
			if got := locker.holdsLock(); got != tt.wantHoldLock {
				t.Errorf("holdsLock() = %v, want %v", got, tt.wantHoldLock)
			}
		})
	}
}

func TestLeaseLocker_fetchLockRecord(t *testing.T) {
	tests := []struct {
		name     string
		mockFail bool
		wantErr  bool
	}{
		{"success case", false, false},
		{"failure in Get", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockLock{failGet: tt.mockFail}
			locker := &LeaseLocker{
				config: Config{Lock: mock},
				clock:  clock.RealClock{},
			}
			err := locker.fetchLockRecord(context.TODO())

			if (err != nil) != tt.wantErr {
				t.Errorf("fetchLockRecord() error = %v, want %v", err != nil, tt.wantErr)
			}
		})
	}
}

func TestLeaseLocker_release(t *testing.T) {
	tests := []struct {
		name            string
		mockFail        bool
		holdsLock       bool
		wantReleaseLock bool
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

			if got := locker.release(); got != tt.wantReleaseLock {
				t.Errorf("release() = %v, want %v", got, tt.wantReleaseLock)
			}
		})
	}
}

func TestLeaseLocker_TryLock(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*LeaseLocker, *mockLock)
		wantTryLock bool
		checkState  func(*testing.T, *LeaseLocker, *mockLock)
	}{
		{
			name: "acquire lock successfully",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{}
			},
			wantTryLock: true,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if !l.holdsLock() {
					t.Error("Expected to hold lock after successful acquisition")
				}
			},
		},
		{
			name: "fail to acquire locked resource",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity:       "other-holder",
					RenewTime:            metav1.NewTime(time.Now()),
					LeaseDurationSeconds: 60,
				}
			},
			wantTryLock: false,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if l.holdsLock() {
					t.Error("Should not hold lock when acquisition fails")
				}
			},
		},
		{
			name: "acquire expired lock",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity:       "other-holder",
					RenewTime:            metav1.NewTime(time.Now().Add(-2 * time.Hour)),
					LeaseDurationSeconds: 60,
				}
			},
			wantTryLock: true,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if !l.holdsLock() {
					t.Error("Expected to hold lock after acquiring expired lock")
				}
			},
		},
		{
			name: "fail when Get fails",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.failGet = true
			},
			wantTryLock: false,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if l.holdsLock() {
					t.Error("Should not hold lock when Get fails")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLock := &mockLock{
				identity: "test-holder",
			}
			locker := &LeaseLocker{
				config: Config{
					Lock:          mockLock,
					LeaseDuration: 60 * time.Second,
				},
				clock: clock.RealClock{},
			}

			tt.setup(locker, mockLock)

			// Todo(jstudler): Fix hang in TryLock
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			result := locker.TryLock(ctx)

			if result != tt.wantTryLock {
				t.Errorf("TryLock() = %v, want %v", result, tt.wantTryLock)
			}

			tt.checkState(t, locker, mockLock)
		})
	}
}

func TestLeaseLocker_Unlock(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*LeaseLocker, *mockLock)
		wantUnlock bool
		checkState func(*testing.T, *LeaseLocker, *mockLock)
	}{
		{
			name: "successfully unlock held lock",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity: m.identity,
				}
				l.observedRecord = m.record
			},
			wantUnlock: true,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if l.holdsLock() {
					t.Error("Should not hold lock after unlock")
				}
				if m.record.HolderIdentity != "" {
					t.Error("Lock record should be cleared")
				}
			},
		},
		{
			name: "unlock when lock not held",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity: "other-holder",
				}
				l.observedRecord = m.record
			},
			wantUnlock: true,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if l.holdsLock() {
					t.Error("Should not hold lock")
				}
				if m.record.HolderIdentity != "other-holder" {
					t.Error("Should not modify other holder's lock")
				}
			},
		},
		{
			name: "unlock fails when update fails",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.failUpdate = true
				m.record = LockRecord{
					HolderIdentity: m.identity,
				}
				l.observedRecord = m.record
			},
			wantUnlock: false,
			checkState: func(t *testing.T, l *LeaseLocker, m *mockLock) {
				if !l.holdsLock() {
					t.Error("Should still hold lock after failed unlock")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLock := &mockLock{
				identity: "test-holder",
			}
			locker := &LeaseLocker{
				config: Config{
					Lock: mockLock,
				},
				clock:         clock.RealClock{},
				lockCtx:       context.Background(),
				lockCtxCancel: func() {},
			}
			if tt.setup != nil {
				tt.setup(locker, mockLock)
			}
			result := locker.Unlock()
			if result != tt.wantUnlock {
				t.Errorf("Unlock() = %v, want %v", result, tt.wantUnlock)
			}

			if tt.checkState != nil {
				tt.checkState(t, locker, mockLock)
			}
		})
	}
}

func TestLeaseLocker_acquire(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*LeaseLocker, *mockLock)
		wantAcquire bool
	}{
		{
			name: "acquire lock on first try",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{}
			},
			wantAcquire: true,
		},
		{
			name: "acquire lock after retries",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity:       "other-holder",
					RenewTime:            metav1.NewTime(time.Now()),
					LeaseDurationSeconds: 1,
				}
			},
			wantAcquire: true,
		},
		{
			name: "fail to acquire after max retries",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.record = LockRecord{
					HolderIdentity:       "other-holder",
					RenewTime:            metav1.NewTime(time.Now()),
					LeaseDurationSeconds: 60,
				}
			},
			wantAcquire: false,
		},
		{
			name: "fail when Get fails",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.failGet = true
			},
			wantAcquire: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLock := &mockLock{
				identity: "test-holder",
			}
			locker := &LeaseLocker{
				config: Config{
					Lock:          mockLock,
					LeaseDuration: 60 * time.Second,
				},
				clock: clock.RealClock{},
			}

			if tt.setup != nil {
				tt.setup(locker, mockLock)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			result := locker.acquire(ctx)

			if result != tt.wantAcquire {
				t.Errorf("acquire() result = %v, want %v", result, tt.wantAcquire)
			}
		})
	}
}

func TestLeaseLocker_tryAcquireOrRenew(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*LeaseLocker, *mockLock)
		wantSuccess bool
		checkState  func(*testing.T, *LeaseLocker, *mockLock)
	}{
		{
			name: "acquire new lock when none exists",
			setup: func(l *LeaseLocker, m *mockLock) {
				m.failGet = false
				m.record = LockRecord{}
			},
			wantSuccess: true,
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
			wantSuccess: false,
			checkState:  func(t *testing.T, l *LeaseLocker, m *mockLock) {},
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
			wantSuccess: false,
			checkState:  func(t *testing.T, l *LeaseLocker, m *mockLock) {},
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
			wantSuccess: true,
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
			wantSuccess: true,
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
			wantSuccess: false,
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

			if result != tt.wantSuccess {
				t.Errorf("tryAcquireOrRenew() = %v, want %v", result, tt.wantSuccess)
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

func TestLeaseLocker_isLeaseValid(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name           string
		record         LockRecord
		timeNow        time.Time
		wantValidLease bool
	}{
		{
			name: "valid lease",
			record: LockRecord{
				HolderIdentity:       "holder1",
				RenewTime:            metav1.NewTime(now.Add(-10 * time.Second)),
				LeaseDurationSeconds: 20,
			},
			timeNow:        now,
			wantValidLease: true,
		},
		{
			name: "expired lease",
			record: LockRecord{
				HolderIdentity:       "",
				RenewTime:            metav1.NewTime(now.Add(-30 * time.Second)),
				LeaseDurationSeconds: 20,
			},
			timeNow:        now,
			wantValidLease: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locker := &LeaseLocker{
				observedRecord: tt.record,
			}
			if got := locker.isLeaseValid(tt.timeNow); got != tt.wantValidLease {
				t.Errorf("isLeaseValid() = %v, want %v", got, tt.wantValidLease)
			}
		})
	}
}
