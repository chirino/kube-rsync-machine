package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DefaultRsyncMachineMountPath = "/backup"
	DefaultSourcePath            = "/"
	DefaultSnapshot              = "latest"
	DefaultMirrorSnapshot        = "current"
	DefaultSuccessfulRunHistory  = 5
	DefaultFailedRunHistory      = 5
)

type ConcurrencyPolicy string

const (
	ConcurrencyPolicyForbid  ConcurrencyPolicy = "Forbid"
	ConcurrencyPolicyReplace ConcurrencyPolicy = "Replace"
)

type CaptureMode string

const (
	CaptureModeDirect         CaptureMode = "Direct"
	CaptureModeVolumeSnapshot CaptureMode = "VolumeSnapshot"
	CaptureModeAuto           CaptureMode = "Auto"
)

type CleanupPolicy string

const (
	CleanupPolicyDelete          CleanupPolicy = "Delete"
	CleanupPolicyRetainOnFailure CleanupPolicy = "RetainOnFailure"
)

type BackupStrategyType string

const (
	BackupStrategySnapshot BackupStrategyType = "Snapshot"
	BackupStrategyMirror   BackupStrategyType = "Mirror"
)

type BackupJobTrigger string

const (
	BackupJobTriggerManual    BackupJobTrigger = "Manual"
	BackupJobTriggerScheduled BackupJobTrigger = "Scheduled"
)

type RunPhase string

const (
	RunPhasePending    RunPhase = "Pending"
	RunPhasePreparing  RunPhase = "Preparing"
	RunPhaseRunning    RunPhase = "Running"
	RunPhaseFinalizing RunPhase = "Finalizing"
	RunPhaseSucceeded  RunPhase = "Succeeded"
	RunPhaseFailed     RunPhase = "Failed"
	RunPhaseCanceled   RunPhase = "Canceled"
)

type TransferPhase string

const (
	TransferPhasePending   TransferPhase = "Pending"
	TransferPhasePreparing TransferPhase = "Preparing"
	TransferPhaseRunning   TransferPhase = "Running"
	TransferPhaseSucceeded TransferPhase = "Succeeded"
	TransferPhaseFailed    TransferPhase = "Failed"
	TransferPhaseSkipped   TransferPhase = "Skipped"
)

type Condition = metav1.Condition

type ObjectReference struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

func (r ObjectReference) NamespaceOr(defaultNamespace string) string {
	if r.Namespace != "" {
		return r.Namespace
	}
	return defaultNamespace
}

type RetentionPolicy struct {
	Hourly  int `json:"hourly,omitempty"`
	Daily   int `json:"daily,omitempty"`
	Weekly  int `json:"weekly,omitempty"`
	Monthly int `json:"monthly,omitempty"`
}

func (p RetentionPolicy) Empty() bool {
	return p.Hourly == 0 && p.Daily == 0 && p.Weekly == 0 && p.Monthly == 0
}

type BackupStrategy struct {
	Type BackupStrategyType `json:"type,omitempty"`
}

func (s BackupStrategy) TypeOrDefault() BackupStrategyType {
	if s.Type != "" {
		return s.Type
	}
	return BackupStrategySnapshot
}

type RsyncOptions struct {
	Delete        *bool `json:"delete,omitempty"`
	OneFileSystem *bool `json:"oneFileSystem,omitempty"`
}

func (o RsyncOptions) DeleteOrDefault() bool {
	if o.Delete != nil {
		return *o.Delete
	}
	return true
}

func (o RsyncOptions) OneFileSystemOrDefault() bool {
	if o.OneFileSystem != nil {
		return *o.OneFileSystem
	}
	return true
}

func (o RsyncOptions) HasOverrides() bool {
	return o.Delete != nil || o.OneFileSystem != nil
}

type RsyncMachine struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RsyncMachineSpec   `json:"spec,omitempty"`
	Status RsyncMachineStatus `json:"status,omitempty"`
}

type RsyncMachineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []RsyncMachine `json:"items"`
}

type RsyncMachineSpec struct {
	PVCName                   string                            `json:"pvcName"`
	Image                     string                            `json:"image,omitempty"`
	Strategy                  BackupStrategy                    `json:"strategy,omitempty"`
	Schedule                  string                            `json:"schedule,omitempty"`
	ConcurrencyPolicy         ConcurrencyPolicy                 `json:"concurrencyPolicy,omitempty"`
	Retention                 RetentionPolicy                   `json:"retention,omitempty"`
	RunHistory                RunHistory                        `json:"runHistory,omitempty"`
	NodeSelector              map[string]string                 `json:"nodeSelector,omitempty"`
	Affinity                  *corev1.Affinity                  `json:"affinity,omitempty"`
	Tolerations               []corev1.Toleration               `json:"tolerations,omitempty"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	SchedulerName             string                            `json:"schedulerName,omitempty"`
	PriorityClassName         string                            `json:"priorityClassName,omitempty"`
	RuntimeClassName          *string                           `json:"runtimeClassName,omitempty"`
	ImagePullSecrets          []corev1.LocalObjectReference     `json:"imagePullSecrets,omitempty"`
	SecurityContext           *corev1.PodSecurityContext        `json:"securityContext,omitempty"`
	Resources                 corev1.ResourceRequirements       `json:"resources,omitempty"`
}

func (s RsyncMachineSpec) ConcurrencyPolicyOrDefault() ConcurrencyPolicy {
	if s.ConcurrencyPolicy != "" {
		return s.ConcurrencyPolicy
	}
	return ConcurrencyPolicyForbid
}

type RsyncMachineStatus struct {
	RestorePointsUpdatedAt *metav1.Time   `json:"restorePointsUpdatedAt,omitempty"`
	RestorePointCount      int32          `json:"restorePointCount,omitempty"`
	RestorePoints          []RestorePoint `json:"restorePoints,omitempty"`
	LastScheduledAt        *metav1.Time   `json:"lastScheduledAt,omitempty"`
	Conditions             []Condition    `json:"conditions,omitempty"`
}

type RestorePoint struct {
	Snapshot         string       `json:"snapshot"`
	ResolvesTo       string       `json:"resolvesTo,omitempty"`
	Tier             string       `json:"tier,omitempty"`
	CreatedAt        *metav1.Time `json:"createdAt,omitempty"`
	BytesTransferred int64        `json:"bytesTransferred,omitempty"`
}

type BackupSource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupSourceSpec   `json:"spec,omitempty"`
	Status BackupSourceStatus `json:"status,omitempty"`
}

type BackupSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []BackupSource `json:"items"`
}

type BackupSourceSpec struct {
	MachineRef                ObjectReference                   `json:"machineRef"`
	PVC                       string                            `json:"pvc"`
	SourcePath                string                            `json:"sourcePath,omitempty"`
	DestinationPath           string                            `json:"destinationPath,omitempty"`
	Consistency               ConsistencyOptions                `json:"consistency,omitempty"`
	Rsync                     RsyncOptions                      `json:"rsync,omitempty"`
	NodeSelector              map[string]string                 `json:"nodeSelector,omitempty"`
	Affinity                  *corev1.Affinity                  `json:"affinity,omitempty"`
	Tolerations               []corev1.Toleration               `json:"tolerations,omitempty"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	SchedulerName             string                            `json:"schedulerName,omitempty"`
	PriorityClassName         string                            `json:"priorityClassName,omitempty"`
	RuntimeClassName          *string                           `json:"runtimeClassName,omitempty"`
	ImagePullSecrets          []corev1.LocalObjectReference     `json:"imagePullSecrets,omitempty"`
	SecurityContext           *corev1.PodSecurityContext        `json:"securityContext,omitempty"`
	Resources                 corev1.ResourceRequirements       `json:"resources,omitempty"`
}

func (s BackupSourceSpec) SourcePathOrDefault() string {
	if s.SourcePath != "" {
		return s.SourcePath
	}
	return DefaultSourcePath
}

type BackupSourceStatus struct {
	Conditions []Condition `json:"conditions,omitempty"`
}

type ConsistencyOptions struct {
	Capture                 CaptureMode   `json:"capture,omitempty"`
	VolumeSnapshotClassName string        `json:"volumeSnapshotClassName,omitempty"`
	CleanupPolicy           CleanupPolicy `json:"cleanupPolicy,omitempty"`
}

func (o ConsistencyOptions) CaptureOrDefault() CaptureMode {
	if o.Capture != "" {
		return o.Capture
	}
	return CaptureModeAuto
}

func (o ConsistencyOptions) CleanupPolicyOrDefault() CleanupPolicy {
	if o.CleanupPolicy != "" {
		return o.CleanupPolicy
	}
	return CleanupPolicyDelete
}

type RunHistory struct {
	Successful int `json:"successful,omitempty"`
	Failed     int `json:"failed,omitempty"`
}

func (h RunHistory) SuccessfulOrDefault() int {
	if h.Successful > 0 {
		return h.Successful
	}
	return DefaultSuccessfulRunHistory
}

func (h RunHistory) FailedOrDefault() int {
	if h.Failed > 0 {
		return h.Failed
	}
	return DefaultFailedRunHistory
}

type BackupJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupJobSpec   `json:"spec,omitempty"`
	Status BackupJobStatus `json:"status,omitempty"`
}

type BackupJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []BackupJob `json:"items"`
}

type BackupJobSpec struct {
	MachineRef ObjectReference  `json:"machineRef"`
	Trigger    BackupJobTrigger `json:"trigger,omitempty"`
}

func (s BackupJobSpec) TriggerOrDefault() BackupJobTrigger {
	if s.Trigger != "" {
		return s.Trigger
	}
	return BackupJobTriggerManual
}

type BackupJobStatus struct {
	Phase            RunPhase          `json:"phase,omitempty"`
	StartedAt        *metav1.Time      `json:"startedAt,omitempty"`
	CompletedAt      *metav1.Time      `json:"completedAt,omitempty"`
	SnapshotPath     string            `json:"snapshotPath,omitempty"`
	TargetPhase      string            `json:"targetPhase,omitempty"`
	LastCommand      *CommandStatus    `json:"lastCommand,omitempty"`
	IncludedMachines []ObjectReference `json:"includedMachines,omitempty"`
	MergedInto       *ObjectReference  `json:"mergedInto,omitempty"`
	Transfers        []TransferStatus  `json:"transfers,omitempty"`
	Conditions       []Condition       `json:"conditions,omitempty"`
}

type CommandStatus struct {
	ID             string       `json:"id,omitempty"`
	Type           string       `json:"type,omitempty"`
	SentAt         *metav1.Time `json:"sentAt,omitempty"`
	AcknowledgedAt *metav1.Time `json:"acknowledgedAt,omitempty"`
}

type TransferStatus struct {
	Source             string        `json:"source"`
	Phase              TransferPhase `json:"phase,omitempty"`
	Percent            uint32        `json:"percent,omitempty"`
	BytesTransferred   int64         `json:"bytesTransferred"`
	RateBytesPerSecond int64         `json:"rateBytesPerSecond,omitempty"`
	FilesTransferred   int64         `json:"filesTransferred"`
	TotalFiles         int64         `json:"totalFiles,omitempty"`
	TotalFileSize      int64         `json:"totalFileSize,omitempty"`
	BytesSent          int64         `json:"bytesSent,omitempty"`
	BytesReceived      int64         `json:"bytesReceived,omitempty"`
	Speedup            float64       `json:"speedup,omitempty"`
	Message            string        `json:"message,omitempty"`
	ExitCode           int32         `json:"exitCode,omitempty"`
	StartedAt          *metav1.Time  `json:"startedAt,omitempty"`
	CompletedAt        *metav1.Time  `json:"completedAt,omitempty"`
	CaptureMethod      CaptureMode   `json:"captureMethod,omitempty"`
	VolumeSnapshotName string        `json:"volumeSnapshotName,omitempty"`
	CaptureTime        *metav1.Time  `json:"captureTime,omitempty"`
}

type RestoreJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RestoreJobSpec   `json:"spec,omitempty"`
	Status RestoreJobStatus `json:"status,omitempty"`
}

type RestoreJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []RestoreJob `json:"items"`
}

type RestoreJobSpec struct {
	SourceRef                 ObjectReference                   `json:"sourceRef"`
	Snapshot                  string                            `json:"snapshot,omitempty"`
	Overrides                 RestoreOverrides                  `json:"overrides,omitempty"`
	NodeSelector              map[string]string                 `json:"nodeSelector,omitempty"`
	Affinity                  *corev1.Affinity                  `json:"affinity,omitempty"`
	Tolerations               []corev1.Toleration               `json:"tolerations,omitempty"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	SchedulerName             string                            `json:"schedulerName,omitempty"`
	PriorityClassName         string                            `json:"priorityClassName,omitempty"`
	RuntimeClassName          *string                           `json:"runtimeClassName,omitempty"`
	ImagePullSecrets          []corev1.LocalObjectReference     `json:"imagePullSecrets,omitempty"`
	SecurityContext           *corev1.PodSecurityContext        `json:"securityContext,omitempty"`
	Resources                 corev1.ResourceRequirements       `json:"resources,omitempty"`
}

func (s RestoreJobSpec) SnapshotOrDefault() string {
	if s.Snapshot != "" {
		return s.Snapshot
	}
	return DefaultSnapshot
}

type RestoreOverrides struct {
	Destination RestoreDestination `json:"destination,omitempty"`
	Rsync       RsyncOptions       `json:"rsync,omitempty"`
}

type RestoreDestination struct {
	Namespace string `json:"namespace,omitempty"`
	PVCName   string `json:"pvcName,omitempty"`
	Path      string `json:"path,omitempty"`
}

type RestoreJobStatus struct {
	Phase            RunPhase         `json:"phase,omitempty"`
	StartedAt        *metav1.Time     `json:"startedAt,omitempty"`
	CompletedAt      *metav1.Time     `json:"completedAt,omitempty"`
	RestoredSnapshot string           `json:"restoredSnapshot,omitempty"`
	ExitCode         int32            `json:"exitCode,omitempty"`
	Message          string           `json:"message,omitempty"`
	Transfers        []TransferStatus `json:"transfers,omitempty"`
	Conditions       []Condition      `json:"conditions,omitempty"`
}
