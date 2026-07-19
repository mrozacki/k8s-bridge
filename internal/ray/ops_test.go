package ray

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func opsScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(RayJobGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(RayJobListGVK, &unstructured.UnstructuredList{})
	return s
}

func suspendedRayJob() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(RayJobGVK)
	u.SetNamespace("ray")
	u.SetName("job-a")
	_ = unstructured.SetNestedField(u.Object, true, "spec", "suspend")
	return u
}

func TestGetFoundAndNotFound(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(opsScheme()).WithObjects(suspendedRayJob()).Build()

	u, found, err := Get(context.Background(), c, "ray", "job-a")
	if err != nil || !found || u == nil {
		t.Fatalf("Get(existing) = %v, %v, %v", u, found, err)
	}
	_, found, err = Get(context.Background(), c, "ray", "ghost")
	if err != nil || found {
		t.Fatalf("Get(missing) found=%v err=%v", found, err)
	}
}
