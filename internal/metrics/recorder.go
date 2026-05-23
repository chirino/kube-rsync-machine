package metrics

import (
	"sync"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "kube_rsync_machine"

type RunKind string

const (
	RunKindBackup  RunKind = "backup"
	RunKindRestore RunKind = "restore"
)

type JobStatus string

const (
	JobStatusActive    JobStatus = "active"
	JobStatusComplete  JobStatus = "complete"
	JobStatusFailed    JobStatus = "failed"
	JobStatusSuspended JobStatus = "suspended"
)

type Recorder struct {
	activeRuns           *prometheus.GaugeVec
	completedRuns        *prometheus.CounterVec
	failedRuns           *prometheus.CounterVec
	jobStatusTransitions *prometheus.CounterVec
	transferBytes        *prometheus.GaugeVec
	transferDuration     *prometheus.GaugeVec

	mu            sync.Mutex
	activeRunKeys map[string]RunKind
	jobStatuses   map[string]JobStatus
}

func NewRecorder() *Recorder {
	return &Recorder{
		activeRuns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_runs",
			Help:      "Current active BackupJob and RestoreJob resources observed by the controller.",
		}, []string{"run_kind"}),
		completedRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "completed_runs_total",
			Help:      "Total BackupJob and RestoreJob resources that reached a successful terminal phase.",
		}, []string{"run_kind"}),
		failedRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "failed_runs_total",
			Help:      "Total BackupJob and RestoreJob resources that reached the failed phase.",
		}, []string{"run_kind"}),
		jobStatusTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "generated_job_status_transitions_total",
			Help:      "Total generated Kubernetes Job status transitions observed by run controllers.",
		}, []string{"run_kind", "role", "status"}),
		transferBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "transfer_bytes",
			Help:      "Latest observed bytes transferred for a source or restore transfer.",
		}, []string{"run_kind", "transfer"}),
		transferDuration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "transfer_duration_seconds",
			Help:      "Latest observed duration in seconds for a source or restore transfer.",
		}, []string{"run_kind", "transfer"}),
		activeRunKeys: make(map[string]RunKind),
		jobStatuses:   make(map[string]JobStatus),
	}
}

func (r *Recorder) Register(registerer prometheus.Registerer) error {
	for _, collector := range []prometheus.Collector{
		r.activeRuns,
		r.completedRuns,
		r.failedRuns,
		r.jobStatusTransitions,
		r.transferBytes,
		r.transferDuration,
	} {
		if err := registerer.Register(collector); err != nil {
			return err
		}
	}
	return nil
}

func (r *Recorder) RecordRunPhase(runKind RunKind, runKey string, oldPhase, newPhase krmv1alpha1.RunPhase) {
	if r == nil || oldPhase == newPhase {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	stateKey := string(runKind) + "/" + runKey
	if !isTerminalPhase(newPhase) {
		if _, exists := r.activeRunKeys[stateKey]; !exists {
			r.activeRunKeys[stateKey] = runKind
			r.activeRuns.WithLabelValues(string(runKind)).Inc()
		}
		return
	}

	if seenKind, exists := r.activeRunKeys[stateKey]; exists {
		delete(r.activeRunKeys, stateKey)
		r.activeRuns.WithLabelValues(string(seenKind)).Dec()
	}
	switch newPhase {
	case krmv1alpha1.RunPhaseSucceeded:
		r.completedRuns.WithLabelValues(string(runKind)).Inc()
	case krmv1alpha1.RunPhaseFailed:
		r.failedRuns.WithLabelValues(string(runKind)).Inc()
	}
}

func (r *Recorder) RecordGeneratedJobStatus(runKind RunKind, role, jobKey string, status JobStatus) {
	if r == nil || status == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stateKey := string(runKind) + "/" + jobKey
	if r.jobStatuses[stateKey] == status {
		return
	}
	r.jobStatuses[stateKey] = status
	r.jobStatusTransitions.WithLabelValues(string(runKind), role, string(status)).Inc()
}

func (r *Recorder) RecordTransfer(runKind RunKind, transferKey string, bytesTransferred uint64, duration time.Duration) {
	if r == nil || transferKey == "" {
		return
	}
	r.transferBytes.WithLabelValues(string(runKind), transferKey).Set(float64(bytesTransferred))
	if duration >= 0 {
		r.transferDuration.WithLabelValues(string(runKind), transferKey).Set(duration.Seconds())
	}
}

func isTerminalPhase(phase krmv1alpha1.RunPhase) bool {
	return phase == krmv1alpha1.RunPhaseSucceeded || phase == krmv1alpha1.RunPhaseFailed || phase == krmv1alpha1.RunPhaseCanceled
}
