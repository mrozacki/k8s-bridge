package ray

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func rayJobObj(annotations map[string]string, cluster string, suspend bool) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ray.io/v1",
		"kind":       "RayJob",
		"metadata": map[string]any{
			"name":        "job-a",
			"namespace":   "ray",
			"uid":         "uid-123",
			"annotations": toAnyMap(annotations),
		},
		"spec": map[string]any{
			"suspend":    suspend,
			"entrypoint": "python train.py",
		},
	}}
	if cluster != "" {
		_ = unstructured.SetNestedStringMap(u.Object, map[string]string{clusterSelectorKey: cluster}, "spec", "clusterSelector")
	}
	return u
}

func toAnyMap(m map[string]string) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

var testDefaults = Defaults{Workers: 1, CPUsPerWorker: 1, MemPerWorkerMB: 1024}

func TestFromUnstructuredCoreFields(t *testing.T) {
	u := rayJobObj(map[string]string{
		PoolAnnotation:             "batch",
		WorkersAnnotation:          "3",
		WorkerCPUsAnnotation:       "4",
		WorkerGPUsAnnotation:       "2",
		WorkerMemoryAnnotation:     "8Gi",
		LocalQueueAnnotation:       "team-a",
		RequiredTopologyAnnotation: "ray-bridge.x-k8s.io/rack",
	}, "shared", true)

	j, err := FromUnstructured(u, testDefaults)
	if err != nil {
		t.Fatalf("FromUnstructured: %v", err)
	}
	if j.Name != "job-a" || j.Namespace != "ray" || j.UID != "uid-123" {
		t.Errorf("identity = %s/%s uid=%s", j.Namespace, j.Name, j.UID)
	}
	if j.TargetCluster != "shared" || !j.IsInnerWorkload() {
		t.Errorf("target cluster = %q, IsInnerWorkload=%v", j.TargetCluster, j.IsInnerWorkload())
	}
	if !j.Suspended {
		t.Errorf("Suspended = false, want true")
	}
	if j.Pool != "batch" || j.LocalQueue != "team-a" {
		t.Errorf("pool=%q localQueue=%q", j.Pool, j.LocalQueue)
	}
	if j.RequiredTopology != "ray-bridge.x-k8s.io/rack" {
		t.Errorf("requiredTopology = %q", j.RequiredTopology)
	}
	if j.Workers != 3 || j.CPUsPerWorker != 4 || j.GPUsPerWorker != 2 {
		t.Errorf("shape workers=%d cpus=%d gpus=%d", j.Workers, j.CPUsPerWorker, j.GPUsPerWorker)
	}
	if j.MemPerWorkerMB != 8192 {
		t.Errorf("mem = %d MB, want 8192 (8Gi)", j.MemPerWorkerMB)
	}
}

func TestFromUnstructuredDefaultsWhenAnnotationsAbsent(t *testing.T) {
	j, err := FromUnstructured(rayJobObj(nil, "shared", false), testDefaults)
	if err != nil {
		t.Fatalf("FromUnstructured: %v", err)
	}
	if j.Workers != 1 || j.CPUsPerWorker != 1 || j.MemPerWorkerMB != 1024 || j.GPUsPerWorker != 0 {
		t.Errorf("defaults not applied: workers=%d cpus=%d mem=%d gpus=%d",
			j.Workers, j.CPUsPerWorker, j.MemPerWorkerMB, j.GPUsPerWorker)
	}
}

func TestFromUnstructuredNotInnerWorkloadWithoutClusterSelector(t *testing.T) {
	j, err := FromUnstructured(rayJobObj(nil, "", false), testDefaults)
	if err != nil {
		t.Fatalf("FromUnstructured: %v", err)
	}
	if j.IsInnerWorkload() {
		t.Errorf("RayJob without clusterSelector should not be an inner workload")
	}
}

func TestFromUnstructuredRejectsMalformedShape(t *testing.T) {
	for _, ann := range []map[string]string{
		{WorkersAnnotation: "three"},
		{WorkerCPUsAnnotation: "lots"},
		{WorkerMemoryAnnotation: "8gigs"},
		// Previously accepted silently by fmt.Sscan (review finding): a
		// leading-int-with-garbage and a C-style hex base must now be rejected.
		{WorkersAnnotation: "3abc"},
		{WorkerCPUsAnnotation: "0x10"},
		{WorkerCPUsAnnotation: "3.9"},
	} {
		if _, err := FromUnstructured(rayJobObj(ann, "shared", false), testDefaults); err == nil {
			t.Errorf("expected error for annotation %v", ann)
		}
	}
}

func TestFromUnstructuredClampsSubOneCountsUp(t *testing.T) {
	j, err := FromUnstructured(rayJobObj(map[string]string{
		WorkersAnnotation:    "0",
		WorkerCPUsAnnotation: "0",
	}, "shared", false), testDefaults)
	if err != nil {
		t.Fatalf("FromUnstructured: %v", err)
	}
	if j.Workers != 1 || j.CPUsPerWorker != 1 {
		t.Errorf("sub-one shape not clamped up: workers=%d cpus=%d", j.Workers, j.CPUsPerWorker)
	}
}

func TestIsTerminal(t *testing.T) {
	cases := []struct {
		dep, job string
		want     bool
	}{
		{"Complete", "", true},
		{"Failed", "", true},
		{"ValidationFailed", "", true},
		{"Running", "SUCCEEDED", true},
		{"Running", "FAILED", true},
		{"Running", "STOPPED", true},
		{"Running", "RUNNING", false},
		{"", "", false},
	}
	for _, c := range cases {
		j := &RayJob{DeploymentStatus: c.dep, JobStatus: c.job}
		if got := j.IsTerminal(); got != c.want {
			t.Errorf("IsTerminal(dep=%q job=%q) = %v, want %v", c.dep, c.job, got, c.want)
		}
	}
}
