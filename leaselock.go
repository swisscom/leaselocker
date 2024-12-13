/*
Copyright 2018 The Kubernetes Authors.

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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

type LeaseLock struct {
	// LeaseMeta should contain a Name and a Namespace of a
	// LeaseMeta object that the LeaseLock will attempt to lock.
	LeaseMeta  metav1.ObjectMeta
	Client     coordinationv1.LeasesGetter
	LockConfig ResourceLockConfig
	lease      *v1.Lease
}

// newLeaseLock will create a lock of type LeaseLock according to the input parameters
// nsn will have Name converted to lowercase
func newLeaseLock(nsn types.NamespacedName, coordinationClient coordinationv1.CoordinationV1Interface, rlc ResourceLockConfig) (Interface, error) {
	// Transform to lowercase name to conform to requirements
	nsn = types.NamespacedName{
		Namespace: nsn.Namespace,
		Name:      strings.ToLower(nsn.Name),
	}
	leaseLock := &LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: nsn.Namespace,
			Name:      nsn.Name,
		},
		Client:     coordinationClient,
		LockConfig: rlc,
	}

	return leaseLock, nil
}

// Get returns the lock record from a Lease spec
func (ll *LeaseLock) Get(ctx context.Context) (*LockRecord, []byte, error) {
	lease, err := ll.Client.Leases(ll.LeaseMeta.Namespace).Get(ctx, ll.LeaseMeta.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}
	ll.lease = lease
	record := LeaseSpecToLeaseLockRecord(&ll.lease.Spec)
	recordByte, err := json.Marshal(*record)
	if err != nil {
		return nil, nil, err
	}
	return record, recordByte, nil
}

// Create attempts to create a Lease
func (ll *LeaseLock) Create(ctx context.Context, lr LockRecord) error {
	var err error
	ll.lease, err = ll.Client.Leases(ll.LeaseMeta.Namespace).Create(ctx, &v1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ll.LeaseMeta.Name,
			Namespace: ll.LeaseMeta.Namespace,
		},
		Spec: LeaseLockRecordToLeaseSpec(&lr),
	}, metav1.CreateOptions{})
	return err
}

// Update will update an existing Lease spec.
func (ll *LeaseLock) Update(ctx context.Context, lr LockRecord) error {
	if ll.lease == nil {
		return errors.New("lease not initialized, call get or create first")
	}
	ll.lease.Spec = LeaseLockRecordToLeaseSpec(&lr)

	lease, err := ll.Client.Leases(ll.LeaseMeta.Namespace).Update(ctx, ll.lease, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	ll.lease = lease
	return nil
}

// RecordEvent in lock while adding metadata
func (ll *LeaseLock) RecordEvent(s string) {
	if ll.LockConfig.EventRecorder == nil {
		return
	}
	events := fmt.Sprintf("%v %v", ll.LockConfig.Identity, s)
	subject := &v1.Lease{ObjectMeta: ll.lease.ObjectMeta}
	// Populate the type meta, so we don't have to get it from the schema
	subject.Kind = "Lease"
	subject.APIVersion = v1.SchemeGroupVersion.String()
	ll.LockConfig.EventRecorder.Eventf(subject, corev1.EventTypeNormal, "Locking", events)
}

// Describe is used to convert details on current resource lock
// into a string
func (ll *LeaseLock) Describe() string {
	return fmt.Sprintf("%v/%v", ll.LeaseMeta.Namespace, ll.LeaseMeta.Name)
}

// Identity returns the Identity of the lock
func (ll *LeaseLock) Identity() string {
	return ll.LockConfig.Identity
}

func LeaseSpecToLeaseLockRecord(spec *v1.LeaseSpec) *LockRecord {
	var r LockRecord
	if spec.HolderIdentity != nil {
		r.HolderIdentity = *spec.HolderIdentity
	}
	if spec.LeaseDurationSeconds != nil {
		r.LeaseDurationSeconds = int(*spec.LeaseDurationSeconds)
	}
	if spec.LeaseTransitions != nil {
		r.LeaseTransitions = int(*spec.LeaseTransitions)
	}
	if spec.AcquireTime != nil {
		r.AcquireTime = metav1.Time{Time: spec.AcquireTime.Time}
	}
	if spec.RenewTime != nil {
		r.RenewTime = metav1.Time{Time: spec.RenewTime.Time}
	}
	return &r
}

func LeaseLockRecordToLeaseSpec(lr *LockRecord) v1.LeaseSpec {
	leaseDurationSeconds := int32(lr.LeaseDurationSeconds)
	leaseTransitions := int32(lr.LeaseTransitions)
	return v1.LeaseSpec{
		HolderIdentity:       &lr.HolderIdentity,
		LeaseDurationSeconds: &leaseDurationSeconds,
		AcquireTime:          &metav1.MicroTime{Time: lr.AcquireTime.Time},
		RenewTime:            &metav1.MicroTime{Time: lr.RenewTime.Time},
		LeaseTransitions:     &leaseTransitions,
	}
}
