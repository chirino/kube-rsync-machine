package controller

import (
	"testing"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestApplyTargetEventToBackupJob(t *testing.T) {
	run := backupRun("backup", "run-1", ref("backup", "demo"))
	run.Status.Phase = krmv1alpha1.RunPhasePreparing

	changed := ApplyTargetEventToBackupJob(&run, control.TargetEvent{
		RunNamespace: "backup",
		RunName:      "run-1",
		RunKind:      control.RunKindBackup,
		Phase:        "Completed",
		Snapshot:     "hourly/2026-05-20T12-00-00Z",
	})
	if !changed {
		t.Fatal("expected change")
	}
	if run.Status.Phase != krmv1alpha1.RunPhaseSucceeded || run.Status.SnapshotPath == "" || run.Status.CompletedAt == nil {
		t.Fatalf("unexpected status: %#v", run.Status)
	}
}

func TestApplySourceEventToBackupJobUpsertsTransfer(t *testing.T) {
	run := backupRun("backup", "run-1", ref("backup", "demo"))
	changed := ApplySourceEventToBackupJob(&run, control.SourceEvent{
		RunNamespace:       "backup",
		RunName:            "run-1",
		RunKind:            control.RunKindBackup,
		SourceNamespace:    "app",
		SourceName:         "files",
		Phase:              "Succeeded",
		Percent:            100,
		BytesTransferred:   2048,
		RateBytesPerSecond: 1024,
		FilesTransferred:   2,
		TotalFiles:         3,
		TotalFileSize:      4096,
		BytesSent:          2200,
		BytesReceived:      100,
		Speedup:            1.5,
		StatsComplete:      true,
		StartedAt:          "2026-05-20T12:00:00Z",
		CompletedAt:        "2026-05-20T12:00:02Z",
		CaptureMethod:      string(krmv1alpha1.CaptureModeVolumeSnapshot),
		VolumeSnapshotName: "snap-1",
		CaptureTime:        "2026-05-20T12:00:00Z",
	})
	if !changed || len(run.Status.Transfers) != 1 {
		t.Fatalf("unexpected transfers: %#v", run.Status.Transfers)
	}
	transfer := run.Status.Transfers[0]
	if transfer.Source != "app/files" || transfer.Phase != krmv1alpha1.TransferPhaseSucceeded || transfer.VolumeSnapshotName != "snap-1" || transfer.CaptureTime == nil {
		t.Fatalf("unexpected transfer: %#v", transfer)
	}
	if transfer.Percent != 100 || transfer.BytesTransferred != 2048 || transfer.RateBytesPerSecond != 1024 || transfer.FilesTransferred != 2 || transfer.TotalFiles != 3 || transfer.TotalFileSize != 4096 || transfer.BytesSent != 2200 || transfer.BytesReceived != 100 || transfer.Speedup != 1.5 || transfer.StartedAt == nil || transfer.CompletedAt == nil {
		t.Fatalf("unexpected transfer metrics: %#v", transfer)
	}
}

func TestApplySourceEventToBackupJobPersistsZeroSummaryCounts(t *testing.T) {
	run := backupRun("backup", "run-1", ref("backup", "demo"))
	changed := ApplySourceEventToBackupJob(&run, control.SourceEvent{
		RunNamespace:       "backup",
		RunName:            "run-1",
		RunKind:            control.RunKindBackup,
		SourceNamespace:    "app",
		SourceName:         "files",
		Phase:              "Succeeded",
		BytesTransferred:   0,
		FilesTransferred:   0,
		TotalFiles:         14278,
		TotalFileSize:      248560272,
		BytesSent:          437865,
		BytesReceived:      10983,
		RateBytesPerSecond: 299232,
		Speedup:            553.77,
		StatsComplete:      true,
	})
	if !changed || len(run.Status.Transfers) != 1 {
		t.Fatalf("unexpected transfers: %#v", run.Status.Transfers)
	}
	transfer := run.Status.Transfers[0]
	if transfer.BytesTransferred != 0 || transfer.FilesTransferred != 0 || transfer.TotalFiles != 14278 || transfer.TotalFileSize != 248560272 || transfer.BytesSent != 437865 || transfer.BytesReceived != 10983 {
		t.Fatalf("unexpected zero-transfer summary: %#v", transfer)
	}
}

func TestApplyTargetEventToRsyncMachine(t *testing.T) {
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	changed := ApplyTargetEventToRsyncMachine(&target, control.TargetEvent{
		TargetNamespace: "backup",
		TargetName:      "archive",
		RestorePoints: []control.RestorePoint{{
			Snapshot:         "latest",
			ResolvesTo:       "hourly/2026-05-20T12-00-00Z",
			CreatedAt:        "2026-05-20T12:00:00Z",
			BytesTransferred: 4096,
		}},
		Conditions: []control.TargetCondition{{
			Type:               "Ready",
			Status:             "True",
			Reason:             "TargetUsable",
			LastTransitionTime: "2026-05-20T12:00:00Z",
		}},
	})
	if !changed {
		t.Fatal("expected change")
	}
	if target.Status.RestorePointCount != 1 || target.Status.RestorePoints[0].Snapshot != "latest" || target.Status.RestorePointsUpdatedAt == nil {
		t.Fatalf("unexpected restore points: %#v", target.Status)
	}
	if target.Status.RestorePoints[0].BytesTransferred != 4096 {
		t.Fatalf("unexpected restore point bytes transferred: %#v", target.Status.RestorePoints[0])
	}
	if len(target.Status.Conditions) != 1 || target.Status.Conditions[0].Type != "Ready" || target.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("unexpected conditions: %#v", target.Status.Conditions)
	}
}

func TestApplyTargetEventToRsyncMachineClearsRestorePointsAfterScan(t *testing.T) {
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	now := metav1.Now()
	target.Status.RestorePointCount = 1
	target.Status.RestorePointsUpdatedAt = &now
	target.Status.RestorePoints = []krmv1alpha1.RestorePoint{{
		Snapshot: "latest",
	}}

	changed := ApplyTargetEventToRsyncMachine(&target, control.TargetEvent{
		TargetNamespace: "backup",
		TargetName:      "archive",
		RestoreScanned:  true,
	})
	if !changed {
		t.Fatal("expected scanned empty restore point list to change status")
	}
	if target.Status.RestorePointCount != 0 || len(target.Status.RestorePoints) != 0 {
		t.Fatalf("expected restore points to be cleared: %#v", target.Status)
	}
}

func TestMarkFinalizeCommandSent(t *testing.T) {
	run := backupRun("backup", "run-1", ref("backup", "demo"))
	if !ApplyTargetCommandAckToBackupJob(&run, control.TargetCommandAckEvent{
		RunNamespace:   "backup",
		RunName:        "run-1",
		RunKind:        control.RunKindBackup,
		CommandID:      "cmd-1",
		CommandType:    control.TargetCommandFinalizeBackupJob,
		AcknowledgedAt: "2026-05-20T12:00:05Z",
	}) {
		t.Fatal("expected early acknowledgement to change status")
	}
	if run.Status.LastCommand == nil || run.Status.LastCommand.AcknowledgedAt == nil {
		t.Fatalf("unexpected command acknowledgement status: %#v", run.Status.LastCommand)
	}
	now := metav1.Now()
	command := control.NewFinalizeBackupJobCommand("cmd-1", control.FinalizeBackupJob{Timestamp: "2026-05-20T12-00-00Z"})
	if !MarkFinalizeCommandSent(&run, command, now) {
		t.Fatal("expected command status to change")
	}
	if run.Status.LastCommand == nil || run.Status.LastCommand.ID != "cmd-1" || run.Status.LastCommand.Type != control.TargetCommandFinalizeBackupJob || run.Status.LastCommand.SentAt == nil || run.Status.LastCommand.AcknowledgedAt == nil {
		t.Fatalf("unexpected command status: %#v", run.Status.LastCommand)
	}
	if MarkFinalizeCommandSent(&run, command, now) {
		t.Fatal("duplicate command should not change status")
	}
	if ApplyTargetCommandAckToBackupJob(&run, control.TargetCommandAckEvent{
		RunNamespace:   "backup",
		RunName:        "run-1",
		RunKind:        control.RunKindBackup,
		CommandID:      "cmd-2",
		CommandType:    control.TargetCommandFinalizeBackupJob,
		AcknowledgedAt: "2026-05-20T12:00:06Z",
	}) {
		t.Fatal("acknowledgement for a different command should not change status")
	}
}
