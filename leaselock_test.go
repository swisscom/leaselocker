package leaselocker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestNewLeaseLock(t *testing.T) {
	fakeCoordClient := &fakeLeaseClient{}

	specs := []struct {
		name             string
		identity         string
		nsn              types.NamespacedName
		expectedDescribe string
	}{
		{
			name:             "Valid NamespacedName",
			identity:         "holder1",
			nsn:              types.NamespacedName{Name: "test", Namespace: "ns"},
			expectedDescribe: "ns/test",
		},
		{
			name:             "Uppercase Name in NamespacedName",
			identity:         "holder2",
			nsn:              types.NamespacedName{Name: "TEST", Namespace: "ns"},
			expectedDescribe: "ns/test",
		},
		{
			name:             "Nil NamespacedName",
			identity:         "holder3",
			nsn:              types.NamespacedName{},
			expectedDescribe: "/",
		},
	}

	for _, tt := range specs {
		t.Run(tt.name, func(t *testing.T) {
			rlc := ResourceLockConfig{Identity: tt.identity}
			got, err := newLeaseLock(tt.nsn, fakeCoordClient, rlc)
			if err != nil {
				t.Fatalf("unexpected error: got %v", err)
			}
			if got == nil {
				t.Fatal("got nil LeaseLock, want non-nil LeaseLock")
			}
			if got.Describe() != tt.expectedDescribe {
				t.Errorf("Describe(): got %v, want %v", got.Describe(), tt.expectedDescribe)
			}
			if got.Identity() != tt.identity {
				t.Errorf("Identity(): got %v, want %v", got.Identity(), tt.identity)
			}
		})
	}
}

func TestLeaseLock_Get(t *testing.T) {
	ctx := context.TODO()
	lease := &coordv1.Lease{
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       strPtr("holder"),
			LeaseDurationSeconds: int32Ptr(10),
		},
	}
	ll := &LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Client:    &fakeLeaseClient{lease: lease},
	}
	record, data, err := ll.Get(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record.HolderIdentity != "holder" {
		t.Errorf("unexpected HolderIdentity: %v", record.HolderIdentity)
	}
	if len(data) == 0 {
		t.Errorf("expected non-empty data")
	}
	// error case
	ll.Client = &fakeLeaseClient{getErr: errors.New("fail")}
	_, _, err = ll.Get(ctx)
	if err == nil {
		t.Errorf("expected error")
	}
}

func TestLeaseLock_Create(t *testing.T) {
	ctx := context.TODO()
	ll := &LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Client:    &fakeLeaseClient{},
	}
	lr := LockRecord{HolderIdentity: "holder"}
	err := ll.Create(ctx, lr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ll.lease == nil || ll.lease.Spec.HolderIdentity == nil || *ll.lease.Spec.HolderIdentity != "holder" {
		t.Errorf("lease not created correctly")
	}
	// error case
	ll.Client = &fakeLeaseClient{createErr: errors.New("fail")}
	err = ll.Create(ctx, lr)
	if err == nil {
		t.Errorf("expected error")
	}
}

func TestLeaseLock_Update(t *testing.T) {
	ctx := context.TODO()
	ll := &LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Client:    &fakeLeaseClient{lease: &coordv1.Lease{}},
		lease:     &coordv1.Lease{},
	}
	lr := LockRecord{HolderIdentity: "holder"}
	err := ll.Update(ctx, lr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ll.lease.Spec.HolderIdentity == nil || *ll.lease.Spec.HolderIdentity != "holder" {
		t.Errorf("lease not updated correctly")
	}
	// error case: lease not initialized
	ll2 := &LeaseLock{Client: &fakeLeaseClient{}}
	err = ll2.Update(ctx, lr)
	if err == nil {
		t.Errorf("expected error")
	}
	// error case: update fails
	ll.Client = &fakeLeaseClient{updateErr: errors.New("fail")}
	err = ll.Update(ctx, lr)
	if err == nil {
		t.Errorf("expected error")
	}
}

func TestLeaseLock_RecordEvent(t *testing.T) {
	tests := []struct {
		name          string
		eventRecorder *fakeRecorder
		eventMsg      string
	}{
		{
			name:          "With event recorder",
			eventRecorder: &fakeRecorder{},
			eventMsg:      "test event",
		},
		{
			name:          "Without event recorder",
			eventRecorder: nil,
			eventMsg:      "test event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rlc := ResourceLockConfig{
				Identity: "holder1",
			}
			if tt.eventRecorder != nil {
				rlc.EventRecorder = tt.eventRecorder
			}
			ll := &LeaseLock{
				LockConfig: rlc,
				lease: &coordv1.Lease{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-lease",
						Namespace: "test-ns",
					},
				},
			}

			ll.RecordEvent(tt.eventMsg)

			if tt.eventRecorder != nil {
				expectedMsg := fmt.Sprintf("%v %v", ll.LockConfig.Identity, tt.eventMsg)
				if len(tt.eventRecorder.Events) != 1 {
					t.Errorf("Expected 1 event, got %d", len(tt.eventRecorder.Events))
				}
				if !strings.Contains(tt.eventRecorder.Events[0], expectedMsg) {
					t.Errorf("Event message does not contain expected content, got %v, want %v", tt.eventRecorder.Events[0], expectedMsg)
				}
			}
		})
	}
}

func TestLeaseLock_Describe(t *testing.T) {
	ll := &LeaseLock{LeaseMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	want := "ns/test"
	if got := ll.Describe(); got != want {
		t.Errorf("Describe() = %v, want %v", got, want)
	}
}

func TestLeaseLock_Identity(t *testing.T) {
	ll := &LeaseLock{LockConfig: ResourceLockConfig{Identity: "id1"}}
	if got := ll.Identity(); got != "id1" {
		t.Errorf("Identity() = %v, want %v", got, "id1")
	}
}

func TestLeaseSpecToLeaseLockRecord(t *testing.T) {
	tm := time.Now()
	specs := []struct {
		name string
		spec *coordv1.LeaseSpec
		want LockRecord
	}{
		{
			name: "all fields",
			spec: &coordv1.LeaseSpec{
				HolderIdentity:       strPtr("holder1"),
				LeaseDurationSeconds: int32Ptr(30),
				LeaseTransitions:     int32Ptr(2),
				AcquireTime:          &metav1.MicroTime{Time: tm},
				RenewTime:            &metav1.MicroTime{Time: tm.Add(time.Second)},
			},
			want: LockRecord{
				HolderIdentity:       "holder1",
				LeaseDurationSeconds: 30,
				LeaseTransitions:     2,
				AcquireTime:          metav1.Time{Time: tm},
				RenewTime:            metav1.Time{Time: tm.Add(time.Second)},
			},
		},
		{
			name: "partial fields",
			spec: &coordv1.LeaseSpec{
				HolderIdentity: strPtr("holder2"),
			},
			want: LockRecord{HolderIdentity: "holder2"},
		},
		{
			name: "nil fields",
			spec: &coordv1.LeaseSpec{},
			want: LockRecord{},
		},
	}
	for _, tt := range specs {
		t.Run(tt.name, func(t *testing.T) {
			got := LeaseSpecToLeaseLockRecord(tt.spec)
			if !reflect.DeepEqual(*got, tt.want) {
				t.Errorf("got %+v, want %+v", *got, tt.want)
			}
		})
	}
}

func TestLeaseLockRecordToLeaseSpec(t *testing.T) {
	tm := time.Now()
	records := []struct {
		name string
		lr   LockRecord
	}{
		{
			name: "full LockRecord spec",
			lr: LockRecord{
				HolderIdentity:       "holder1",
				LeaseDurationSeconds: 30,
				LeaseTransitions:     2,
				AcquireTime:          metav1.Time{Time: tm},
				RenewTime:            metav1.Time{Time: tm.Add(time.Second)},
			},
		},
		{
			name: "empty LockRecord spec",
			lr:   LockRecord{},
		},
		{
			name: "only holder identity in LockRecord spec",
			lr: LockRecord{
				HolderIdentity: "holder2",
			},
		},
	}
	for _, tt := range records {
		t.Run(tt.name, func(t *testing.T) {
			got := LeaseLockRecordToLeaseSpec(&tt.lr)
			if tt.lr.HolderIdentity != "" && (got.HolderIdentity == nil || *got.HolderIdentity != tt.lr.HolderIdentity) {
				t.Errorf("HolderIdentity mismatch: got %v, want %v", got.HolderIdentity, tt.lr.HolderIdentity)
			}
			if got.LeaseDurationSeconds == nil || int(*got.LeaseDurationSeconds) != tt.lr.LeaseDurationSeconds {
				t.Errorf("LeaseDurationSeconds mismatch: got %v, want %v", got.LeaseDurationSeconds, tt.lr.LeaseDurationSeconds)
			}
			if got.LeaseTransitions == nil || int(*got.LeaseTransitions) != tt.lr.LeaseTransitions {
				t.Errorf("LeaseTransitions mismatch: got %v, want %v", got.LeaseTransitions, tt.lr.LeaseTransitions)
			}
			if got.AcquireTime == nil || !got.AcquireTime.Time.Equal(tt.lr.AcquireTime.Time) {
				t.Errorf("AcquireTime mismatch: got %v, want %v", got.AcquireTime, tt.lr.AcquireTime)
			}
			if got.RenewTime == nil || !got.RenewTime.Time.Equal(tt.lr.RenewTime.Time) {
				t.Errorf("RenewTime mismatch: got %v, want %v", got.RenewTime, tt.lr.RenewTime)
			}
		})
	}
}
