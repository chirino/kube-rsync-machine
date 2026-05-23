package controller

import (
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConditionValid           = "Valid"
	ConditionReady           = "Ready"
	ConditionTargetReady     = "TargetReady"
	ConditionTargetOverlap   = "TargetOverlap"
	ConditionSnapshotCapture = "SnapshotCapture"
	ConditionTransport       = "Transport"
	ConditionFailed          = "Failed"
	ConditionReplaced        = "Replaced"
	ConditionMergedIntoRun   = "MergedIntoRun"

	ReasonValid              = "Valid"
	ReasonInvalidSpec        = "InvalidSpec"
	ReasonResolvedReferences = "ResolvedReferences"
	ReasonMissingReference   = "MissingReference"
	ReasonTargetReady        = "TargetReady"
	ReasonTargetNotReady     = "TargetNotReady"
	ReasonNoTargetOverlap    = "NoTargetOverlap"
	ReasonActiveRunForTarget = "ActiveRunForTarget"
	ReasonActiveRestore      = "ActiveRestoreForTarget"
	ReasonReplaceUnsupported = "ReplaceUnsupported"
	ReasonSnapshotPreparing  = "SnapshotPreparing"
	ReasonSnapshotReady      = "SnapshotReady"
	ReasonSnapshotSkipped    = "SnapshotSkipped"
	ReasonSnapshotFailed     = "SnapshotFailed"
	ReasonTransportRunning   = "TransportRunning"
	ReasonTransportComplete  = "TransportComplete"
	ReasonTransportFailed    = "TransportFailed"
	ReasonReconcileError     = "ReconcileError"
	ReasonRunStarted         = "RunStarted"
	ReasonRunSucceeded       = "RunSucceeded"
	ReasonRunFailed          = "RunFailed"
	ReasonReplacedByRun      = "ReplacedByRun"
	ReasonMergedIntoRun      = "MergedIntoRun"
)

func ApplyTargetEventToBackupJob(run *krmv1alpha1.BackupJob, event control.TargetEvent) bool {
	if run == nil || event.RunNamespace != run.Namespace || event.RunName != run.Name || event.RunKind != control.RunKindBackup {
		return false
	}
	changed := false
	if run.Status.TargetPhase != event.Phase {
		run.Status.TargetPhase = event.Phase
		changed = true
	}
	switch event.Phase {
	case "Ready":
		if run.Status.Phase == krmv1alpha1.RunPhasePreparing || run.Status.Phase == "" {
			run.Status.Phase = krmv1alpha1.RunPhaseRunning
			changed = true
		}
		changed = setCondition(&run.Status.Conditions, ConditionTargetReady, metav1.ConditionTrue, ReasonTargetReady, "RsyncMachine reported ready", run.Generation) || changed
	case "Finalizing":
		if run.Status.Phase != krmv1alpha1.RunPhaseFinalizing {
			run.Status.Phase = krmv1alpha1.RunPhaseFinalizing
			changed = true
		}
	case "Completed":
		if run.Status.Phase != krmv1alpha1.RunPhaseSucceeded {
			run.Status.Phase = krmv1alpha1.RunPhaseSucceeded
			now := metav1.Now()
			run.Status.CompletedAt = &now
			changed = true
		}
		changed = setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionFalse, ReasonRunSucceeded, "BackupJob completed successfully", run.Generation) || changed
		changed = setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionTrue, ReasonTransportComplete, "Target transport finalized successfully", run.Generation) || changed
		if event.Snapshot != "" && run.Status.SnapshotPath != event.Snapshot {
			run.Status.SnapshotPath = event.Snapshot
			changed = true
		}
	case "Failed":
		if run.Status.Phase != krmv1alpha1.RunPhaseFailed {
			run.Status.Phase = krmv1alpha1.RunPhaseFailed
			now := metav1.Now()
			run.Status.CompletedAt = &now
			changed = true
		}
		message := event.Message
		if message == "" {
			message = "Backup target reported failure"
		}
		changed = setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionTrue, ReasonRunFailed, message, run.Generation) || changed
		changed = setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionFalse, ReasonTransportFailed, message, run.Generation) || changed
	}
	return changed
}

func ApplyTargetCommandAckToBackupJob(run *krmv1alpha1.BackupJob, event control.TargetCommandAckEvent) bool {
	if run == nil || event.RunNamespace != run.Namespace || event.RunName != run.Name || event.RunKind != control.RunKindBackup {
		return false
	}
	acknowledgedAt := parseMetaTime(event.AcknowledgedAt)
	if run.Status.LastCommand == nil {
		run.Status.LastCommand = &krmv1alpha1.CommandStatus{
			ID:             event.CommandID,
			Type:           event.CommandType,
			AcknowledgedAt: acknowledgedAt,
		}
		return true
	}
	if run.Status.LastCommand.ID != event.CommandID || run.Status.LastCommand.Type != event.CommandType {
		return false
	}
	if run.Status.LastCommand.AcknowledgedAt == nil || !run.Status.LastCommand.AcknowledgedAt.Equal(acknowledgedAt) {
		run.Status.LastCommand.AcknowledgedAt = acknowledgedAt
		return true
	}
	return false
}

func ApplySourceEventToBackupJob(run *krmv1alpha1.BackupJob, event control.SourceEvent) bool {
	if run == nil || event.RunNamespace != run.Namespace || event.RunName != run.Name || event.RunKind != control.RunKindBackup {
		return false
	}
	sourceID := event.SourceNamespace + "/" + event.SourceName
	phase := transferPhase(event.Phase)
	for i := range run.Status.Transfers {
		if run.Status.Transfers[i].Source == sourceID {
			changed := updateTransfer(&run.Status.Transfers[i], event, phase)
			return setTransferConditions(run, event, phase) || changed
		}
	}
	transfer := krmv1alpha1.TransferStatus{Source: sourceID}
	changed := updateTransfer(&transfer, event, phase)
	run.Status.Transfers = append(run.Status.Transfers, transfer)
	return setTransferConditions(run, event, phase) || changed || true
}

func ApplyTargetEventToRsyncMachine(target *krmv1alpha1.RsyncMachine, event control.TargetEvent) bool {
	if target == nil || event.TargetNamespace != target.Namespace || event.TargetName != target.Name {
		return false
	}
	changed := false
	if event.RestoreScanned || len(event.RestorePoints) > 0 {
		target.Status.RestorePoints = make([]krmv1alpha1.RestorePoint, len(event.RestorePoints))
		for i, point := range event.RestorePoints {
			target.Status.RestorePoints[i] = krmv1alpha1.RestorePoint{
				Snapshot:         point.Snapshot,
				ResolvesTo:       point.ResolvesTo,
				Tier:             point.Tier,
				CreatedAt:        parseMetaTime(point.CreatedAt),
				BytesTransferred: point.BytesTransferred,
			}
		}
		target.Status.RestorePointCount = int32(len(target.Status.RestorePoints))
		now := metav1.Now()
		target.Status.RestorePointsUpdatedAt = &now
		changed = true
	}
	if len(event.Conditions) > 0 {
		for _, condition := range event.Conditions {
			upsertCondition(&target.Status.Conditions, metav1.Condition{
				Type:               condition.Type,
				Status:             metav1.ConditionStatus(condition.Status),
				Reason:             condition.Reason,
				Message:            condition.Message,
				ObservedGeneration: condition.ObservedGeneration,
				LastTransitionTime: *parseMetaTime(condition.LastTransitionTime),
			})
		}
		changed = true
	}
	return changed
}

func ApplySourceEventToRestoreJob(run *krmv1alpha1.RestoreJob, event control.SourceEvent) bool {
	if run == nil || event.RunNamespace != run.Namespace || event.RunName != run.Name || event.RunKind != control.RunKindRestore {
		return false
	}
	changed := false
	phase := transferPhase(event.Phase)
	sourceID := event.SourceNamespace + "/" + event.SourceName
	transferFound := false
	for i := range run.Status.Transfers {
		if run.Status.Transfers[i].Source == sourceID {
			changed = updateTransfer(&run.Status.Transfers[i], event, phase) || changed
			transferFound = true
			break
		}
	}
	if !transferFound {
		transfer := krmv1alpha1.TransferStatus{Source: sourceID}
		changed = updateTransfer(&transfer, event, phase) || changed
		run.Status.Transfers = append(run.Status.Transfers, transfer)
		changed = true
	}
	switch event.Phase {
	case "Running":
		if run.Status.Phase != krmv1alpha1.RunPhaseRunning {
			run.Status.Phase = krmv1alpha1.RunPhaseRunning
			changed = true
		}
	case "Succeeded":
		if run.Status.Phase != krmv1alpha1.RunPhaseSucceeded {
			run.Status.Phase = krmv1alpha1.RunPhaseSucceeded
			now := metav1.Now()
			run.Status.CompletedAt = &now
			changed = true
		}
	case "Failed":
		if run.Status.Phase != krmv1alpha1.RunPhaseFailed {
			run.Status.Phase = krmv1alpha1.RunPhaseFailed
			now := metav1.Now()
			run.Status.CompletedAt = &now
			changed = true
		}
	}
	if event.RsyncExitCode != 0 && run.Status.ExitCode != event.RsyncExitCode {
		run.Status.ExitCode = event.RsyncExitCode
		changed = true
	}
	if event.Message != "" && run.Status.Message != event.Message {
		run.Status.Message = event.Message
		changed = true
	}
	return changed
}

func MarkFinalizeCommandSent(run *krmv1alpha1.BackupJob, command control.TargetCommand, sentAt metav1.Time) bool {
	if run == nil || command.CommandID == "" {
		return false
	}
	if run.Status.LastCommand != nil && run.Status.LastCommand.ID == command.CommandID && run.Status.LastCommand.Type == command.Type {
		if run.Status.LastCommand.SentAt == nil || !run.Status.LastCommand.SentAt.Equal(&sentAt) {
			run.Status.LastCommand.SentAt = &sentAt
			return true
		}
		return false
	}
	run.Status.LastCommand = &krmv1alpha1.CommandStatus{
		ID:     command.CommandID,
		Type:   command.Type,
		SentAt: &sentAt,
	}
	return true
}

func updateTransfer(transfer *krmv1alpha1.TransferStatus, event control.SourceEvent, phase krmv1alpha1.TransferPhase) bool {
	changed := false
	if transfer.Phase != phase {
		transfer.Phase = phase
		changed = true
	}
	if event.Percent != 0 && transfer.Percent != event.Percent {
		transfer.Percent = event.Percent
		changed = true
	}
	bytesTransferred := uint64ToInt64(event.BytesTransferred)
	if (event.BytesTransferred != 0 || event.StatsComplete) && transfer.BytesTransferred != bytesTransferred {
		transfer.BytesTransferred = bytesTransferred
		changed = true
	}
	rateBytesPerSecond := uint64ToInt64(event.RateBytesPerSecond)
	if event.RateBytesPerSecond != 0 && transfer.RateBytesPerSecond != rateBytesPerSecond {
		transfer.RateBytesPerSecond = rateBytesPerSecond
		changed = true
	}
	filesTransferred := uint64ToInt64(event.FilesTransferred)
	if (event.FilesTransferred != 0 || event.StatsComplete) && transfer.FilesTransferred != filesTransferred {
		transfer.FilesTransferred = filesTransferred
		changed = true
	}
	totalFiles := uint64ToInt64(event.TotalFiles)
	if event.TotalFiles != 0 && transfer.TotalFiles != totalFiles {
		transfer.TotalFiles = totalFiles
		changed = true
	}
	totalFileSize := uint64ToInt64(event.TotalFileSize)
	if event.TotalFileSize != 0 && transfer.TotalFileSize != totalFileSize {
		transfer.TotalFileSize = totalFileSize
		changed = true
	}
	bytesSent := uint64ToInt64(event.BytesSent)
	if event.BytesSent != 0 && transfer.BytesSent != bytesSent {
		transfer.BytesSent = bytesSent
		changed = true
	}
	bytesReceived := uint64ToInt64(event.BytesReceived)
	if event.BytesReceived != 0 && transfer.BytesReceived != bytesReceived {
		transfer.BytesReceived = bytesReceived
		changed = true
	}
	if event.Speedup != 0 && transfer.Speedup != event.Speedup {
		transfer.Speedup = event.Speedup
		changed = true
	}
	if event.Message != "" && transfer.Message != event.Message {
		transfer.Message = event.Message
		changed = true
	}
	if event.RsyncExitCode != 0 && transfer.ExitCode != event.RsyncExitCode {
		transfer.ExitCode = event.RsyncExitCode
		changed = true
	}
	if event.CaptureMethod != "" && string(transfer.CaptureMethod) != event.CaptureMethod {
		transfer.CaptureMethod = krmv1alpha1.CaptureMode(event.CaptureMethod)
		changed = true
	}
	if event.VolumeSnapshotName != "" && transfer.VolumeSnapshotName != event.VolumeSnapshotName {
		transfer.VolumeSnapshotName = event.VolumeSnapshotName
		changed = true
	}
	if event.CaptureTime != "" {
		captureTime := parseMetaTime(event.CaptureTime)
		if transfer.CaptureTime == nil || !transfer.CaptureTime.Equal(captureTime) {
			transfer.CaptureTime = captureTime
			changed = true
		}
	}
	if event.StartedAt != "" {
		startedAt := parseMetaTime(event.StartedAt)
		if transfer.StartedAt == nil || !transfer.StartedAt.Equal(startedAt) {
			transfer.StartedAt = startedAt
			changed = true
		}
	}
	if event.CompletedAt != "" {
		completedAt := parseMetaTime(event.CompletedAt)
		if transfer.CompletedAt == nil || !transfer.CompletedAt.Equal(completedAt) {
			transfer.CompletedAt = completedAt
			changed = true
		}
	}
	return changed
}

func transferPhase(phase string) krmv1alpha1.TransferPhase {
	switch phase {
	case "Preparing":
		return krmv1alpha1.TransferPhasePreparing
	case "Running":
		return krmv1alpha1.TransferPhaseRunning
	case "Succeeded":
		return krmv1alpha1.TransferPhaseSucceeded
	case "Failed":
		return krmv1alpha1.TransferPhaseFailed
	case "Skipped":
		return krmv1alpha1.TransferPhaseSkipped
	default:
		return krmv1alpha1.TransferPhasePending
	}
}

func upsertCondition(conditions *[]krmv1alpha1.Condition, condition krmv1alpha1.Condition) bool {
	if condition.LastTransitionTime.IsZero() {
		condition.LastTransitionTime = metav1.Now()
	}
	for i := range *conditions {
		if (*conditions)[i].Type == condition.Type {
			if conditionsEqual((*conditions)[i], condition) {
				return false
			}
			if (*conditions)[i].Status == condition.Status && !(*conditions)[i].LastTransitionTime.IsZero() {
				condition.LastTransitionTime = (*conditions)[i].LastTransitionTime
			}
			(*conditions)[i] = condition
			return true
		}
	}
	*conditions = append(*conditions, condition)
	return true
}

func setCondition(conditions *[]krmv1alpha1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string, observedGeneration int64) bool {
	if reason == "" {
		reason = "Observed"
	}
	return upsertCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observedGeneration,
		LastTransitionTime: metav1.Now(),
	})
}

func conditionsEqual(left, right krmv1alpha1.Condition) bool {
	return left.Type == right.Type &&
		left.Status == right.Status &&
		left.Reason == right.Reason &&
		left.Message == right.Message &&
		left.ObservedGeneration == right.ObservedGeneration
}

func setTransferConditions(run *krmv1alpha1.BackupJob, event control.SourceEvent, phase krmv1alpha1.TransferPhase) bool {
	message := event.Message
	if message == "" {
		message = "Source transfer " + string(phase)
	}
	changed := false
	switch phase {
	case krmv1alpha1.TransferPhasePreparing:
		changed = setCondition(&run.Status.Conditions, ConditionSnapshotCapture, metav1.ConditionUnknown, ReasonSnapshotPreparing, message, run.Generation) || changed
	case krmv1alpha1.TransferPhaseRunning:
		changed = setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionUnknown, ReasonTransportRunning, message, run.Generation) || changed
	case krmv1alpha1.TransferPhaseSucceeded:
		changed = setCondition(&run.Status.Conditions, ConditionSnapshotCapture, metav1.ConditionTrue, ReasonSnapshotReady, "Source capture is ready", run.Generation) || changed
		changed = setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionTrue, ReasonTransportComplete, message, run.Generation) || changed
	case krmv1alpha1.TransferPhaseFailed:
		changed = setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionFalse, ReasonTransportFailed, message, run.Generation) || changed
		changed = setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionTrue, ReasonRunFailed, message, run.Generation) || changed
	case krmv1alpha1.TransferPhaseSkipped:
		changed = setCondition(&run.Status.Conditions, ConditionSnapshotCapture, metav1.ConditionFalse, ReasonSnapshotSkipped, message, run.Generation) || changed
	}
	return changed
}

func uint64ToInt64(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}

func parseMetaTime(value string) *metav1.Time {
	if value == "" {
		now := metav1.Now()
		return &now
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		now := metav1.Now()
		return &now
	}
	meta := metav1.NewTime(parsed)
	return &meta
}
