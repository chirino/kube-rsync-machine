package control

import "fmt"

const (
	RunKindBackup  = "backup"
	RunKindRestore = "restore"

	TargetCommandFinalizeBackupJob = "finalize_backup_run"
	TargetCommandRecoverSpace      = "recover_space"
	TargetCommandAbortRun          = "abort_run"
)

type RunKey struct {
	Namespace string
	Name      string
	Kind      string
}

func (k RunKey) Validate() error {
	if err := validateNamespaceName("run namespace", k.Namespace); err != nil {
		return err
	}
	if err := validateObjectName("run name", k.Name); err != nil {
		return err
	}
	switch k.Kind {
	case RunKindBackup, RunKindRestore:
		return nil
	case "":
		return fmt.Errorf("run kind is required")
	default:
		return fmt.Errorf("unsupported run kind %q", k.Kind)
	}
}

type RegisterTargetRequest struct {
	RunNamespace    string
	RunName         string
	RunKind         string
	TargetNamespace string
	TargetName      string
}

func (r RegisterTargetRequest) Key() RunKey {
	return RunKey{Namespace: r.RunNamespace, Name: r.RunName, Kind: r.RunKind}
}

type TargetCommand struct {
	CommandID string
	Type      string

	Finalize     *FinalizeBackupJob
	RecoverSpace *RecoverSpace
	Abort        *AbortRun
}

type TargetEventAck struct {
	LastSequence uint64
	Message      string
}

type TargetCommandAckEvent struct {
	RunNamespace    string
	RunName         string
	RunKind         string
	TargetNamespace string
	TargetName      string

	CommandID      string
	CommandType    string
	AcknowledgedAt string
	Message        string
}

func (e TargetCommandAckEvent) Key() RunKey {
	return RunKey{Namespace: e.RunNamespace, Name: e.RunName, Kind: e.RunKind}
}

type SourceEventAck struct {
	LastSequence uint64
	Message      string
}

func NewFinalizeBackupJobCommand(commandID string, finalize FinalizeBackupJob) TargetCommand {
	return TargetCommand{
		CommandID: commandID,
		Type:      TargetCommandFinalizeBackupJob,
		Finalize:  &finalize,
	}
}

func NewRecoverSpaceCommand(commandID string, recover RecoverSpace) TargetCommand {
	return TargetCommand{
		CommandID:    commandID,
		Type:         TargetCommandRecoverSpace,
		RecoverSpace: &recover,
	}
}

func NewAbortRunCommand(commandID string, abort AbortRun) TargetCommand {
	return TargetCommand{
		CommandID: commandID,
		Type:      TargetCommandAbortRun,
		Abort:     &abort,
	}
}

type FinalizeBackupJob struct {
	Timestamp        string
	Sources          []ExpectedSource
	BytesTransferred uint64
}

type ExpectedSource struct {
	Namespace       string
	Name            string
	DestinationPath string
}

type RecoverSpace struct {
	FailedSourceNamespace string
	FailedSourceName      string
	Reason                string
	MinAvailableBytes     uint64
	ProtectedSnapshots    []string
}

type AbortRun struct {
	Reason string
}

type TargetEvent struct {
	RunNamespace    string
	RunName         string
	RunKind         string
	TargetNamespace string
	TargetName      string

	Phase          string
	Message        string
	Snapshot       string
	BytesFreed     uint64
	Paths          []string
	RestorePoints  []RestorePoint
	RestoreScanned bool
	Conditions     []TargetCondition
	AvailableBytes uint64
	CapacityBytes  uint64
}

func (e TargetEvent) Key() RunKey {
	return RunKey{Namespace: e.RunNamespace, Name: e.RunName, Kind: e.RunKind}
}

type RestorePoint struct {
	Snapshot         string
	ResolvesTo       string
	Tier             string
	CreatedAt        string
	BytesTransferred int64
}

type TargetCondition struct {
	Type               string
	Status             string
	Reason             string
	Message            string
	LastTransitionTime string
	ObservedGeneration int64
}

type SourceEvent struct {
	RunNamespace string
	RunName      string
	RunKind      string

	SourceNamespace    string
	SourceName         string
	Phase              string
	Percent            uint32
	BytesTransferred   uint64
	RateBytesPerSecond uint64
	FilesTransferred   uint64
	TotalFiles         uint64
	TotalFileSize      uint64
	BytesSent          uint64
	BytesReceived      uint64
	Speedup            float64
	StatsComplete      bool
	StartedAt          string
	CompletedAt        string
	RsyncExitCode      int32
	Message            string
	CaptureMethod      string
	CaptureTime        string
	VolumeSnapshotName string
}

func (e SourceEvent) Key() RunKey {
	return RunKey{Namespace: e.RunNamespace, Name: e.RunName, Kind: e.RunKind}
}
