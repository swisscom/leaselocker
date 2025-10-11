package leaselocker

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	coordapplyv1 "k8s.io/client-go/applyconfigurations/coordination/v1"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

func (f *fakeLeaseClient) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return nil
}

// Mocks both LeasesGetter and LeaseInterface
type fakeLeaseClient struct {
	lease     *coordv1.Lease
	createErr error
	getErr    error
	updateErr error
}

// Implements coordinationv1.LeasesGetter
func (f *fakeLeaseClient) Leases(ns string) coordinationv1.LeaseInterface {
	return f
}

// Implements coordinationv1.LeaseInterface
func (f *fakeLeaseClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*coordv1.Lease, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.lease, nil
}
func (f *fakeLeaseClient) Create(ctx context.Context, lease *coordv1.Lease, opts metav1.CreateOptions) (*coordv1.Lease, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.lease = lease
	return lease, nil
}
func (f *fakeLeaseClient) Update(ctx context.Context, lease *coordv1.Lease, opts metav1.UpdateOptions) (*coordv1.Lease, error) {
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	f.lease = lease
	return lease, nil
}

// Unused LeaseInterface methods
func (f *fakeLeaseClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return nil
}
func (f *fakeLeaseClient) List(ctx context.Context, opts metav1.ListOptions) (*coordv1.LeaseList, error) {
	return nil, nil
}
func (f *fakeLeaseClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return nil, nil
}
func (f *fakeLeaseClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*coordv1.Lease, error) {
	return nil, nil
}
func (f *fakeLeaseClient) Apply(ctx context.Context, lease *coordapplyv1.LeaseApplyConfiguration, opts metav1.ApplyOptions) (*coordv1.Lease, error) {
	return nil, nil
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

// helpers
func strPtr(s string) *string { return &s }
func int32Ptr(i int32) *int32 { return &i }
