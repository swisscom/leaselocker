/*
Copyright 2015 The Kubernetes Authors.

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

// Modified by Swisscom (Schweiz) AG.
// Copyright 2024 Swisscom (Schweiz) AG

package leaselocker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// mostly taken from k8s.io/client-go@v0.28.0/tools/leaderelection/leaderelection.go:116
//

type Config struct {
	// Lock is the resource that will be used for locking
	Lock Interface

	// LeaseDuration is the duration that candidates waiting for lock will
	// wait to force acquire leadership. This is measured against time of
	// last renewal (or last observed ack depending on UseObservedTime).
	//
	// A client needs to wait a full LeaseDuration without observing a change to
	// the record before it can attempt to take over. When all clients are
	// shutdown and a new set of clients are started with different names against
	// the same lock record, they must wait the full LeaseDuration before
	// attempting to acquire the lease. Thus LeaseDuration should be as short as
	// possible (within your tolerance for clock skew rate) to avoid a possible
	// long waits in the scenario.
	//
	// Core clients default this value to 15 seconds.
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting master will retry
	// refreshing lock ownership before giving up.
	//
	// Core clients default this value to 10 seconds.
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaseLocker clients should wait
	// between tries of actions.
	//
	// Core clients default this value to 2 seconds.
	RetryPeriod time.Duration

	// UnlockWithRetryPeriod is the duration the LeaseLocker clients should retry
	// unlocking the lease if the first attempt fails.
	UnlockWithRetryPeriod time.Duration

	// ReleaseOnCancel should be set true if the lock should be released
	// when the run context is cancelled. If you set this to true, you must
	// ensure all code guarded by this lease has successfully completed
	// prior to cancelling the context, or you may have two processes
	// simultaneously acting on the critical path.
	ReleaseOnCancel bool

	// Defines whether to use the timestamp in the LockRecord (false) or use
	// the locally observed time (true) when deciding when to force a lock
	UseObservedTime bool

	// Name is the name of the resource lock for debugging
	Name string
}

type LeaseLocker struct {
	config Config

	//internal
	observedRecord     LockRecord
	observedTime       time.Time
	observedRecordLock sync.Mutex

	clock clock.Clock

	// managing renew goroutine
	renewWG       sync.WaitGroup
	lockCtx       context.Context
	lockCtxCancel context.CancelFunc

	// metrics tbd
}

// NOTE:
//The client does consider that timestamps in the lock record
//are accurate but note that these timestamps may have been
//produced by a local clock and might thus not be synced between participants.
//This behaviour can be controlled by setting the config.UseObservedTime flag

// NewLeaseLocker creates a new LeaseLocker instance which uses the given rest.Config to create a client to communicate
// with the k8s API
// namespacedName will have the Name transformed to lowercase automatically
func NewLeaseLocker(config *rest.Config, namespacedName types.NamespacedName, owner string) (*LeaseLocker, error) {
	client, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	lock, err := newLeaseLock(namespacedName, client.CoordinationV1(), ResourceLockConfig{
		Identity:      owner,
		EventRecorder: nil,
	})
	if err != nil {
		return nil, err
	}
	return newLeaseLockerWithConfig(Config{
		Lock:                  lock,
		LeaseDuration:         60 * time.Second,
		RenewDeadline:         10 * time.Second,
		RetryPeriod:           2 * time.Second,
		UnlockWithRetryPeriod: 10 * time.Second,
		ReleaseOnCancel:       false,
		UseObservedTime:       false,
		Name:                  lock.Describe() + ": " + lock.Identity(),
	})
}

// newLeaseLockerWithConfig creates a LeaseLocker from a Config
func newLeaseLockerWithConfig(llc Config) (*LeaseLocker, error) {
	if llc.LeaseDuration <= llc.RenewDeadline {
		return nil, fmt.Errorf("leaseDuration must be greater than renewDeadline")
	}
	if llc.RenewDeadline <= time.Duration(leaderelection.JitterFactor*float64(llc.RetryPeriod)) {
		return nil, fmt.Errorf("renewDeadline must be greater than retryPeriod*JitterFactor")
	}
	if llc.LeaseDuration < 1 {
		return nil, fmt.Errorf("leaseDuration must be greater than zero")
	}
	if llc.RenewDeadline < 1 {
		return nil, fmt.Errorf("renewDeadline must be greater than zero")
	}
	if llc.RetryPeriod < 1 {
		return nil, fmt.Errorf("retryPeriod must be greater than zero")
	}

	if llc.Lock == nil {
		return nil, fmt.Errorf("lock must not be nil")
	}
	id := llc.Lock.Identity()
	if id == "" {
		return nil, fmt.Errorf("lock identity is empty")
	}

	ll := LeaseLocker{
		config: llc,
		clock:  clock.RealClock{},
		//metrics: globalMetricsFactory.newLeaderMetrics(),
	}
	//le.metrics.leaderOff(le.config.Name)
	err := ll.fetchLockRecord(context.TODO())
	if err != nil {
		return nil, err
	}
	return &ll, nil
}
func (l *LeaseLocker) getLeaseHolder() string {
	return l.getObservedRecord().HolderIdentity
}

func (l *LeaseLocker) holdsLock() bool {
	return equality.Semantic.DeepEqual(l.getLeaseHolder(), l.config.Lock.Identity())
}

func (l *LeaseLocker) fetchLockRecord(ctx context.Context) error {
	lockRecord, _, err := l.config.Lock.Get(ctx)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	l.setObservedRecord(lockRecord)
	return nil
}

func (l *LeaseLocker) Lock(ctx context.Context) {
	l.lockCtx, l.lockCtxCancel = context.WithCancel(ctx)
	l.acquire(l.lockCtx)
	l.renewWG.Add(1)
	go func() {
		l.renew(l.lockCtx)
		l.renewWG.Done()
	}()
}

func (l *LeaseLocker) TryLock(ctx context.Context) bool {
	l.lockCtx, l.lockCtxCancel = context.WithCancel(ctx)
	// Try instant lock, otherwise we try after the lease duration
	// this is used if there is no current HolderIdentity
	locked := l.tryAcquireOrRenew(l.lockCtx)
	if locked {
		l.renewWG.Add(1)
		go func() {
			l.renew(l.lockCtx)
			l.renewWG.Done()
		}()
		return true
	}
	// Couldn't get instant lock, let's attempt after waiting for lease duration seconds + jitter
	// it might be that the lock is stale and free (someone forgot to unlock) and we only know this after
	// the lease duration
	// (this is a condition for acquiring a lock and is in the tryAcquireOrRenew method step 2)
	timeCtx, timeCtxCancel := context.WithTimeout(l.lockCtx, time.Second*time.Duration(l.getObservedRecord().LeaseDurationSeconds+1))
	defer timeCtxCancel()
	wait.Until(func() {
		locked = l.tryAcquireOrRenew(l.lockCtx)
		if locked {
			timeCtxCancel()
		}
	}, l.config.RetryPeriod, timeCtx.Done())

	if locked {
		l.renewWG.Add(1)
		go func() {
			l.renew(l.lockCtx)
			l.renewWG.Done()
		}()
		return true
	}
	return false
}

// Unlock releases the lock held by the LeaseLocker instance. Returns true if the lock was successfully released.
func (l *LeaseLocker) Unlock() bool {
	l.lockCtxCancel()
	l.renewWG.Wait()
	return l.release()
}

// UnlockWithRetry releases the lock held by the LeaseLocker instance and retries if the release fails.
func (l *LeaseLocker) UnlockWithRetry(ctx context.Context) {
	timerCtx, timerCancel := context.WithTimeout(ctx, l.config.UnlockWithRetryPeriod)
	defer timerCancel()
	wait.JitterUntil(func() {
		if l.Unlock() {
			timerCancel()
		}
	}, l.config.RetryPeriod, leaderelection.JitterFactor, true, timerCtx.Done())
}

func (l *LeaseLocker) acquire(ctx context.Context) bool {
	logger := log.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	succeeded := false
	desc := l.config.Lock.Describe()
	logger.Info("attempting to acquire lock lease", "desc", desc)
	wait.JitterUntil(func() {
		succeeded = l.tryAcquireOrRenew(ctx)
		//l.maybeReportTransition()
		if !succeeded {
			logger.V(4).Info("failed to acquire lease", "desc", desc)
			return
		}
		l.config.Lock.RecordEvent("acquired lock")
		//l.metrics.leaderOn(l.config.Name)
		logger.Info("successfully acquired lease", "desc", desc)
		cancel()
	}, l.config.RetryPeriod, leaderelection.JitterFactor, true, ctx.Done())
	return succeeded
}

// renew loops calling tryAcquireOrRenew and returns immediately when tryAcquireOrRenew fails or ctx signals done.
func (l *LeaseLocker) renew(ctx context.Context) {
	logger := log.FromContext(ctx)
	defer l.config.Lock.RecordEvent("lost lock")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait.Until(func() {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, l.config.RenewDeadline)
		defer timeoutCancel()
		err := wait.PollUntilContextCancel(timeoutCtx, l.config.RetryPeriod, true, func(ctx context.Context) (bool, error) {
			return l.tryAcquireOrRenew(ctx), nil
		})
		//l.maybeReportTransition()
		desc := l.config.Lock.Describe()
		if err == nil {
			logger.V(5).Info("successfully renewed lease", "desc", desc)
			return
		}
		if errors.Is(err, context.Canceled) {
			logger.V(5).Info("context cancelled while renewing lease", "err", err, "desc", desc)
		} else {
			logger.Error(err, "failed to renew lease", "desc", desc)
		}
		//l.metrics.leaderOff(l.config.Name)
		cancel()
	}, l.config.RetryPeriod, ctx.Done())

	// if we hold the lease, give it up
	if l.config.ReleaseOnCancel {
		l.release()
	}
}

// release attempts to release the lock lease if we have acquired it.
func (l *LeaseLocker) release() bool {
	logger := log.Log
	if !l.holdsLock() {
		return true
	}
	now := metav1.NewTime(l.clock.Now())
	lockRecord := LockRecord{
		LeaseTransitions:     l.observedRecord.LeaseTransitions,
		LeaseDurationSeconds: 1,
		RenewTime:            now,
		AcquireTime:          now,
	}
	if err := l.config.Lock.Update(context.Background(), lockRecord); err != nil {
		logger.Error(err, "failed to release lock")
		return false
	}

	logger.V(5).Info("successfully unlocked lease", "observed", l.getObservedRecord(), "desc", l.config.Lock.Describe())
	l.setObservedRecord(&lockRecord)
	return true
}

// tryAcquireOrRenew tries to acquire a lock lease if it is not already acquired,
// else it tries to renew the lease if it has already been acquired. Returns true
// on success else returns false.
func (l *LeaseLocker) tryAcquireOrRenew(ctx context.Context) bool {
	logger := log.FromContext(ctx)

	now := metav1.NewTime(l.clock.Now())
	lockRecord := LockRecord{
		HolderIdentity:       l.config.Lock.Identity(),
		LeaseDurationSeconds: int(l.config.LeaseDuration / time.Second),
		RenewTime:            now,
		AcquireTime:          now,
	}

	// 1. obtain or create the LockRecord
	oldLockRecord, _, err := l.config.Lock.Get(ctx)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			logger.Error(err, "error retrieving resource lock", "desc", l.config.Lock.Describe())
			return false
		}
		if err = l.config.Lock.Create(ctx, lockRecord); err != nil {
			logger.Error(err, "error initially creating lease lock record")
			return false
		}

		l.setObservedRecord(&lockRecord)

		return true
	}

	// 2. Record obtained, check the Identity & Time
	if !equality.Semantic.DeepEqual(l.getObservedRecord(), *oldLockRecord) {
		l.setObservedRecord(oldLockRecord)

	}
	renewTime := oldLockRecord.RenewTime
	if l.config.UseObservedTime {
		renewTime = metav1.NewTime(l.observedTime)
	}
	if len(oldLockRecord.HolderIdentity) > 0 &&
		renewTime.Add(time.Second*time.Duration(oldLockRecord.LeaseDurationSeconds)).After(now.Time) &&
		!l.holdsLock() {
		logger.V(4).Info("lock is held by another holder and has not yet expired", "holderIdentity", oldLockRecord.HolderIdentity)
		return false
	}

	// 3. We're going to try to update. The lockRecord is set to its default
	// here. Let's correct it before updating.
	if l.holdsLock() {
		lockRecord.AcquireTime = oldLockRecord.AcquireTime
		lockRecord.LeaseTransitions = oldLockRecord.LeaseTransitions
	} else {
		lockRecord.LeaseTransitions = oldLockRecord.LeaseTransitions + 1
	}

	// update the lock itself
	if err = l.config.Lock.Update(ctx, lockRecord); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.V(5).Info("context cancelled while renewing lease", "err", err, "desc", l.config.Lock.Describe())
		} else {
			logger.Error(err, "failed to update lock")
		}
		return false
	}

	l.setObservedRecord(&lockRecord)
	return true
}

//
//func (l *LeaseLocker) maybeReportTransition() {
//	if l.observedRecord.HolderIdentity == l.reportedLeader {
//		return
//	}
//	l.reportedLeader = l.observedRecord.HolderIdentity
//	if l.config.Callbacks.OnNewLeader != nil {
//		go l.config.Callbacks.OnNewLeader(l.reportedLeader)
//	}
//}

// Check will determine if the current lease is expired by more than timeout.
func (l *LeaseLocker) Check(maxTolerableExpiredLease time.Duration) error {
	if !l.holdsLock() {
		// Currently not concerned with the case that we are hot standby
		return nil
	}
	// If we are more than timeout seconds after the lease duration that is past the timeout
	// on the lease renew. Time to start reporting ourselves as unhealthy. We should have
	// died but conditions like deadlock can prevent this. (See #70819)
	if l.clock.Since(l.observedTime) > l.config.LeaseDuration+maxTolerableExpiredLease {
		return fmt.Errorf("failed locking attempt to renew leadership on lease %s", l.config.Name)
	}
	return nil
}

func (l *LeaseLocker) isLeaseValid(now time.Time) bool {
	return l.getObservedRecord().RenewTime.Add(time.Second * time.Duration(l.getObservedRecord().LeaseDurationSeconds)).After(now)
}

func (l *LeaseLocker) setObservedRecord(observedRecord *LockRecord) {
	l.observedRecordLock.Lock()
	defer l.observedRecordLock.Unlock()

	l.observedRecord = *observedRecord
	l.observedTime = l.clock.Now()
}

func (l *LeaseLocker) getObservedRecord() LockRecord {
	l.observedRecordLock.Lock()
	defer l.observedRecordLock.Unlock()
	return l.observedRecord
}
