package raywebhook

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
)

func cfg() *rayconfig.Config {
	c := &rayconfig.Config{
		Namespace:       "ray",
		LocalQueue:      "main",
		ManagedClusters: []rayconfig.ManagedCluster{{Name: "shared", HeadAddress: "h:6379"}},
		PoolMappings:    []rayconfig.PoolMapping{{Pool: "batch", WorkloadPriorityClass: "normal-priority"}},
		Worker:          rayconfig.Worker{Image: "rayproject/ray:2.9.0"},
	}
	c.ApplyDefaults()
	return c
}

func TestDecide(t *testing.T) {
	cases := []struct {
		name                 string
		job                  ray.RayJob
		wantInject, wantDeny bool
	}{
		{"not inner workload", ray.RayJob{TargetCluster: ""}, false, false},
		{"unmanaged cluster", ray.RayJob{TargetCluster: "other", Pool: "batch"}, false, false},
		{"managed, unmapped pool", ray.RayJob{TargetCluster: "shared", Pool: "ghost"}, false, true},
		{"managed, no pool", ray.RayJob{TargetCluster: "shared", Pool: ""}, false, true},
		{"managed, mapped", ray.RayJob{Name: "job-a", TargetCluster: "shared", Pool: "batch"}, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			job := c.job
			d := Decide(&job, cfg())
			if d.InjectPin != c.wantInject || d.Deny != c.wantDeny {
				t.Errorf("Decide = {InjectPin:%v Deny:%v}, want {InjectPin:%v Deny:%v} (%s)",
					d.InjectPin, d.Deny, c.wantInject, c.wantDeny, d.Reason)
			}
			if c.wantInject && d.PinName != "wm-job-job-a" {
				t.Errorf("PinName = %q, want wm-job-job-a", d.PinName)
			}
		})
	}
}

func handler(t *testing.T) *Handler {
	t.Helper()
	decoder := admission.NewDecoder(runtime.NewScheme())
	return NewHandler(cfg(), decoder)
}

func rayJobRaw(t *testing.T, cluster, pool string, extraAnn map[string]string) []byte {
	t.Helper()
	ann := map[string]any{ray.PoolAnnotation: pool}
	for k, v := range extraAnn {
		ann[k] = v
	}
	obj := map[string]any{
		"apiVersion": "ray.io/v1",
		"kind":       "RayJob",
		"metadata":   map[string]any{"name": "job-a", "namespace": "ray", "annotations": ann},
		"spec":       map[string]any{"entrypoint": "python x.py"},
	}
	if cluster != "" {
		obj["spec"].(map[string]any)["clusterSelector"] = map[string]any{"ray.io/cluster": cluster}
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func rayJobRawWith(t *testing.T, cluster, pool string, extraSpec map[string]any) []byte {
	t.Helper()
	spec := map[string]any{"entrypoint": "python x.py"}
	for k, v := range extraSpec {
		spec[k] = v
	}
	if cluster != "" {
		spec["clusterSelector"] = map[string]any{"ray.io/cluster": cluster}
	}
	obj := map[string]any{
		"apiVersion": "ray.io/v1",
		"kind":       "RayJob",
		"metadata":   map[string]any{"name": "job-a", "namespace": "ray", "annotations": map[string]any{ray.PoolAnnotation: pool}},
		"spec":       spec,
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func request(raw []byte) admission.Request {
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Object: runtime.RawExtension{Raw: raw},
	}}
}

func TestHandleInjectsPin(t *testing.T) {
	resp := handler(t).Handle(context.Background(), request(rayJobRaw(t, "shared", "batch", nil)))
	if !resp.Allowed {
		t.Fatalf("expected allowed-with-patch, got denied: %s", resp.Result.Message)
	}
	found := false
	for _, p := range resp.Patches {
		// The pin is written into entrypointResources as a JSON string.
		if p.Path == "/spec/entrypointResources" {
			if s, ok := p.Value.(string); ok && s == `{"wm-job-job-a":1}` {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("expected /spec/entrypointResources pin patch, got %v", resp.Patches)
	}
}

func TestHandleMergesExistingEntrypointResources(t *testing.T) {
	raw := rayJobRawWith(t, "shared", "batch", map[string]any{"entrypointResources": `{"custom":3}`})
	resp := handler(t).Handle(context.Background(), request(raw))
	if !resp.Allowed {
		t.Fatalf("expected allowed, got denied: %s", resp.Result.Message)
	}
	var newVal string
	for _, p := range resp.Patches {
		if p.Path == "/spec/entrypointResources" {
			newVal, _ = p.Value.(string)
		}
	}
	// Both the submitter's key and the pin must survive.
	if !strings.Contains(newVal, `"custom":3`) || !strings.Contains(newVal, `"wm-job-job-a":1`) {
		t.Errorf("merged entrypointResources = %q, want both custom and pin", newVal)
	}
}

func TestHandleDeniesMalformedExistingEntrypointResources(t *testing.T) {
	// A submitter's malformed entrypointResources must be denied, not silently
	// overwritten with only the pin (review finding).
	raw := rayJobRawWith(t, "shared", "batch", map[string]any{"entrypointResources": `{"custom": 3`})
	resp := handler(t).Handle(context.Background(), request(raw))
	if resp.Allowed {
		t.Errorf("malformed existing entrypointResources should be denied, not overwritten")
	}
}

func TestHandleDeniesUnmappedPool(t *testing.T) {
	resp := handler(t).Handle(context.Background(), request(rayJobRaw(t, "shared", "ghost", nil)))
	if resp.Allowed {
		t.Errorf("inner job with unmapped pool should be denied")
	}
}

func TestHandleDeniesMalformedAnnotation(t *testing.T) {
	resp := handler(t).Handle(context.Background(),
		request(rayJobRaw(t, "shared", "batch", map[string]string{ray.WorkersAnnotation: "0x10"})))
	if resp.Allowed {
		t.Errorf("malformed worker-shape annotation should be denied at submit time")
	}
}

func TestHandleAllowsStandaloneRayJob(t *testing.T) {
	// No clusterSelector → not an inner workload → allowed untouched.
	resp := handler(t).Handle(context.Background(), request(rayJobRaw(t, "", "batch", nil)))
	if !resp.Allowed {
		t.Fatalf("standalone RayJob should be allowed")
	}
	if len(resp.Patches) != 0 {
		t.Errorf("standalone RayJob should not be patched")
	}
}
