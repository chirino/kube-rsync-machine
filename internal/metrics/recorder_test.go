package metrics

import (
	"testing"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestRecorderRunPhaseMetrics(t *testing.T) {
	recorder := NewRecorder()
	registry := prometheus.NewRegistry()
	if err := recorder.Register(registry); err != nil {
		t.Fatal(err)
	}

	recorder.RecordRunPhase(RunKindBackup, "backup/demo", "", krmv1alpha1.RunPhasePreparing)
	recorder.RecordRunPhase(RunKindBackup, "backup/demo", krmv1alpha1.RunPhasePreparing, krmv1alpha1.RunPhaseRunning)
	recorder.RecordRunPhase(RunKindBackup, "backup/demo", krmv1alpha1.RunPhaseRunning, krmv1alpha1.RunPhaseSucceeded)

	if got := metricValue(t, recorder.activeRuns.WithLabelValues(string(RunKindBackup))); got != 0 {
		t.Fatalf("active runs = %v, want 0", got)
	}
	if got := metricValue(t, recorder.completedRuns.WithLabelValues(string(RunKindBackup))); got != 1 {
		t.Fatalf("completed runs = %v, want 1", got)
	}
	if got := metricValue(t, recorder.failedRuns.WithLabelValues(string(RunKindBackup))); got != 0 {
		t.Fatalf("failed runs = %v, want 0", got)
	}
}

func TestRecorderJobStatusTransitionsDeduplicate(t *testing.T) {
	recorder := NewRecorder()
	recorder.RecordGeneratedJobStatus(RunKindRestore, "restore-writer", "apps/restore", JobStatusActive)
	recorder.RecordGeneratedJobStatus(RunKindRestore, "restore-writer", "apps/restore", JobStatusActive)
	recorder.RecordGeneratedJobStatus(RunKindRestore, "restore-writer", "apps/restore", JobStatusFailed)

	active := recorder.jobStatusTransitions.WithLabelValues(string(RunKindRestore), "restore-writer", string(JobStatusActive))
	failed := recorder.jobStatusTransitions.WithLabelValues(string(RunKindRestore), "restore-writer", string(JobStatusFailed))
	if got := metricValue(t, active); got != 1 {
		t.Fatalf("active transitions = %v, want 1", got)
	}
	if got := metricValue(t, failed); got != 1 {
		t.Fatalf("failed transitions = %v, want 1", got)
	}
}

func metricValue(t *testing.T, metric prometheus.Metric) float64 {
	t.Helper()
	var out dto.Metric
	if err := metric.Write(&out); err != nil {
		t.Fatal(err)
	}
	switch {
	case out.Gauge != nil:
		return out.Gauge.GetValue()
	case out.Counter != nil:
		return out.Counter.GetValue()
	default:
		t.Fatalf("metric has no gauge or counter value: %#v", out)
		return 0
	}
}
