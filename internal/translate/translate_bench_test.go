package translate

import (
	"testing"

	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

// Benchmarks back the performance analysis: translation must stay
// trivially cheap because it runs for every held job on every tick.
func BenchmarkToJobSet(b *testing.B) {
	b.ReportAllocs()
	cfg := testConfig()
	job := heldJob(1, "mixing", 8, 2)
	job.TresPerNode = "gres/gpu:2"
	job.RequiredSwitches = 1
	for i := 0; i < b.N; i++ {
		if _, err := ToJobSet(job, cfg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGPUsPerNodeParse(b *testing.B) {
	b.ReportAllocs()
	j := &slurm.Job{TresPerNode: "cpu=4,gres/gpu:a100:4,license/x:1"}
	for i := 0; i < b.N; i++ {
		_ = j.GPUsPerNode()
	}
}
