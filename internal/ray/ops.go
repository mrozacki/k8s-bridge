package ray

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Get fetches a RayJob as an unstructured object, returning (nil, false, nil)
// when it no longer exists (the reconcile trigger for cleaning up its workers).
func Get(ctx context.Context, c client.Client, namespace, name string) (*unstructured.Unstructured, bool, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(RayJobGVK)
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, u)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("getting RayJob %s/%s: %w", namespace, name, err)
	}
	return u, true, nil
}
