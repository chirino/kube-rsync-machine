package manager

import (
	"context"
	"testing"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestControlEventApplierPersistsTargetAndSourceEvents(t *testing.T) {
	scheme := testScheme(t)
	run := krmv1alpha1.BackupJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "run-1"},
		Status: krmv1alpha1.BackupJobStatus{
			LastCommand: &krmv1alpha1.CommandStatus{
				ID:   "cmd-1",
				Type: control.TargetCommandFinalizeBackupJob,
			},
		},
	}
	target := krmv1alpha1.RsyncMachine{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "archive"},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}, &krmv1alpha1.RsyncMachine{}).
		WithObjects(&run, &target).
		Build()
	hub := control.NewEventHub(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := make(chan error, 1)
	go func() {
		errs <- (&ControlEventApplier{Client: client, Hub: hub}).Start(ctx)
	}()

	_, err := hub.PublishTarget(control.TargetEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
		Phase:           "Completed",
		Snapshot:        "hourly/2026-05-20T10-00-00Z",
		RestorePoints: []control.RestorePoint{{
			Snapshot: "latest",
		}},
		Conditions: []control.TargetCondition{{
			Type:   "Ready",
			Status: "True",
			Reason: "TargetUsable",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = hub.PublishTargetCommandAck(control.TargetCommandAckEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
		CommandID:       "cmd-1",
		CommandType:     control.TargetCommandFinalizeBackupJob,
		AcknowledgedAt:  "2026-05-20T10:00:05Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = hub.PublishSource(control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Succeeded",
	})
	if err != nil {
		t.Fatal(err)
	}

	var updatedRun krmv1alpha1.BackupJob
	waitFor(t, func() bool {
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "run-1"}, &updatedRun); err != nil {
			t.Fatal(err)
		}
		return updatedRun.Status.Phase == krmv1alpha1.RunPhaseSucceeded && len(updatedRun.Status.Transfers) == 1 && updatedRun.Status.LastCommand != nil && updatedRun.Status.LastCommand.AcknowledgedAt != nil
	})
	if updatedRun.Status.SnapshotPath != "hourly/2026-05-20T10-00-00Z" {
		t.Fatalf("unexpected snapshot path: %q", updatedRun.Status.SnapshotPath)
	}
	if updatedRun.Status.Transfers[0].Phase != krmv1alpha1.TransferPhaseSucceeded {
		t.Fatalf("unexpected transfer status: %#v", updatedRun.Status.Transfers)
	}
	wantAck, err := time.Parse(time.RFC3339, "2026-05-20T10:00:05Z")
	if err != nil {
		t.Fatal(err)
	}
	if !updatedRun.Status.LastCommand.AcknowledgedAt.Time.Equal(wantAck) {
		t.Fatalf("unexpected command acknowledgement: %#v", updatedRun.Status.LastCommand)
	}

	var updatedTarget krmv1alpha1.RsyncMachine
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "archive"}, &updatedTarget); err != nil {
		t.Fatal(err)
	}
	if updatedTarget.Status.RestorePointCount != 1 || len(updatedTarget.Status.Conditions) != 1 {
		t.Fatalf("unexpected target status: %#v", updatedTarget.Status)
	}

	cancel()
	select {
	case err := <-errs:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for applier to stop")
	}
}

func TestControlEventApplierPersistsRestoreEvents(t *testing.T) {
	scheme := testScheme(t)
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "restore-1"},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RestoreJob{}).
		WithObjects(&restore).
		Build()
	hub := control.NewEventHub(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := make(chan error, 1)
	go func() {
		errs <- (&ControlEventApplier{Client: client, Hub: hub}).Start(ctx)
	}()

	_, err := hub.PublishSource(control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "restore-1",
		RunKind:         control.RunKindRestore,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Failed",
		RsyncExitCode:   23,
		Message:         "rsync failed",
	})
	if err != nil {
		t.Fatal(err)
	}

	var updated krmv1alpha1.RestoreJob
	waitFor(t, func() bool {
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "restore-1"}, &updated); err != nil {
			t.Fatal(err)
		}
		return updated.Status.Phase == krmv1alpha1.RunPhaseFailed
	})
	if updated.Status.ExitCode != 23 || updated.Status.Message != "rsync failed" {
		t.Fatalf("unexpected restore status: %#v", updated.Status)
	}

	cancel()
	select {
	case err := <-errs:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for applier to stop")
	}
}

func TestControlEventApplierSkipsSourceProgressStatusUpdates(t *testing.T) {
	scheme := testScheme(t)
	run := krmv1alpha1.BackupJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "run-1"},
		Status: krmv1alpha1.BackupJobStatus{
			Transfers: []krmv1alpha1.TransferStatus{{
				Source: "app/files",
				Phase:  krmv1alpha1.TransferPhaseRunning,
			}},
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&run).
		Build()
	applier := ControlEventApplier{Client: client}

	err := applier.applySourceEventToBackupJob(context.Background(), control.SourceEvent{
		RunNamespace:       "backup",
		RunName:            "run-1",
		RunKind:            control.RunKindBackup,
		SourceNamespace:    "app",
		SourceName:         "files",
		Phase:              "Running",
		Percent:            42,
		BytesTransferred:   1024,
		RateBytesPerSecond: 512,
	})
	if err != nil {
		t.Fatal(err)
	}

	var updated krmv1alpha1.BackupJob
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "run-1"}, &updated); err != nil {
		t.Fatal(err)
	}
	transfer := updated.Status.Transfers[0]
	if transfer.Percent != 0 || transfer.BytesTransferred != 0 || transfer.RateBytesPerSecond != 0 {
		t.Fatalf("progress-only event should not persist transfer stats: %#v", transfer)
	}
}

func TestControlEventApplierPersistsSourceStartAndCompletion(t *testing.T) {
	scheme := testScheme(t)
	run := krmv1alpha1.BackupJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backup", Name: "run-1"},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&run).
		Build()
	applier := ControlEventApplier{Client: client}

	err := applier.applySourceEventToBackupJob(context.Background(), control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
		StartedAt:       "2026-05-22T20:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = applier.applySourceEventToBackupJob(context.Background(), control.SourceEvent{
		RunNamespace:       "backup",
		RunName:            "run-1",
		RunKind:            control.RunKindBackup,
		SourceNamespace:    "app",
		SourceName:         "files",
		Phase:              "Succeeded",
		CompletedAt:        "2026-05-22T20:01:00Z",
		BytesTransferred:   2048,
		RateBytesPerSecond: 1024,
		FilesTransferred:   2,
		StatsComplete:      true,
	})
	if err != nil {
		t.Fatal(err)
	}

	var updated krmv1alpha1.BackupJob
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "run-1"}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.Transfers) != 1 {
		t.Fatalf("expected one persisted transfer, got %#v", updated.Status.Transfers)
	}
	transfer := updated.Status.Transfers[0]
	if transfer.Phase != krmv1alpha1.TransferPhaseSucceeded || transfer.StartedAt == nil || transfer.CompletedAt == nil || transfer.BytesTransferred != 2048 {
		t.Fatalf("unexpected persisted transfer: %#v", transfer)
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := krmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for condition")
		case <-ticker.C:
		}
	}
}
