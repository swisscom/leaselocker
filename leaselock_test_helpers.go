package leaselocker

import (
	"context"
	"fmt"

	v2 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	v4 "k8s.io/client-go/applyconfigurations/coordination/v1"
	v3 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
)

func (f *fakeLeaseClient) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return nil
}

// Mocks both LeasesGetter and LeaseInterface
type fakeLeaseClient struct {
	lease     *v2.Lease
	createErr error
	getErr    error
	updateErr error
}

type fakeRecorder struct {
	Events []string
}

// Eventf implements record.EventRecorder interface
func (f *fakeRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	eventMsg := fmt.Sprintf(messageFmt, args...)
	fullMsg := fmt.Sprintf("%s %s: %s", eventtype, reason, eventMsg)
	f.Events = append(f.Events, fullMsg)
}

// Event implements record.EventRecorder interface
func (f *fakeRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	fullMsg := fmt.Sprintf("%s %s: %s", eventtype, reason, message)
	f.Events = append(f.Events, fullMsg)
}

// AnnotatedEventf implements record.EventRecorder interface
func (f *fakeRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	eventMsg := fmt.Sprintf(messageFmt, args...)
	fullMsg := fmt.Sprintf("%s %s: %s", eventtype, reason, eventMsg)
	f.Events = append(f.Events, fullMsg)
}

func (f *fakeLeaseClient) RESTClient() rest.Interface {
	return nil
}

// Implements coordinationv1.LeasesGetter
func (f *fakeLeaseClient) Leases(ns string) v3.LeaseInterface {
	return f
}

// Implements coordinationv1.LeaseInterface
func (f *fakeLeaseClient) Get(ctx context.Context, name string, opts v1.GetOptions) (*v2.Lease, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.lease, nil
}
func (f *fakeLeaseClient) Create(ctx context.Context, lease *v2.Lease, opts v1.CreateOptions) (*v2.Lease, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.lease = lease
	return lease, nil
}
func (f *fakeLeaseClient) Update(ctx context.Context, lease *v2.Lease, opts v1.UpdateOptions) (*v2.Lease, error) {
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	f.lease = lease
	return lease, nil
}

// Unused LeaseInterface methods
func (f *fakeLeaseClient) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return nil
}
func (f *fakeLeaseClient) List(ctx context.Context, opts v1.ListOptions) (*v2.LeaseList, error) {
	return nil, nil
}
func (f *fakeLeaseClient) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, nil
}
func (f *fakeLeaseClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (*v2.Lease, error) {
	return nil, nil
}
func (f *fakeLeaseClient) Apply(ctx context.Context, lease *v4.LeaseApplyConfiguration, opts v1.ApplyOptions) (*v2.Lease, error) {
	return nil, nil
}

// helpers
func strPtr(s string) *string { return &s }
func int32Ptr(i int32) *int32 { return &i }
