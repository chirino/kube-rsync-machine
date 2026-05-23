package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestAddToSchemeRegistersKnownTypes(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	gvks, _, err := scheme.ObjectKinds(&BackupJob{})
	if err != nil {
		t.Fatalf("ObjectKinds() error = %v", err)
	}
	if len(gvks) != 1 {
		t.Fatalf("ObjectKinds() returned %d GVKs, want 1", len(gvks))
	}
	if got := gvks[0]; got.GroupVersion() != SchemeGroupVersion || got.Kind != "BackupJob" {
		t.Fatalf("ObjectKinds() = %v, want %s BackupJob", got, SchemeGroupVersion)
	}
}

func TestRsyncMachineDeepCopyIsIndependent(t *testing.T) {
	now := metav1.Now()
	original := &RsyncMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "target",
			Labels: map[string]string{"app": "krm"},
		},
		Spec: RsyncMachineSpec{
			PVCName:      "backups",
			NodeSelector: map[string]string{"disk": "ssd"},
		},
		Status: RsyncMachineStatus{
			RestorePointsUpdatedAt: &now,
			RestorePoints: []RestorePoint{{
				Snapshot:  "latest",
				CreatedAt: &now,
			}},
			Conditions: []Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: 1,
				LastTransitionTime: now,
				Reason:             "Ready",
				Message:            "target is ready",
			}},
		},
	}

	copy := original.DeepCopy()
	copy.Labels["app"] = "changed"
	copy.Spec.NodeSelector["disk"] = "hdd"
	copy.Status.RestorePoints[0].Snapshot = "hourly/2026-05-20T12-00-00Z"
	copy.Status.Conditions[0].Status = metav1.ConditionFalse

	if original.Labels["app"] != "krm" {
		t.Fatalf("ObjectMeta labels were not deep copied")
	}
	if original.Spec.NodeSelector["disk"] != "ssd" {
		t.Fatalf("spec map was not deep copied")
	}
	if original.Status.RestorePoints[0].Snapshot != "latest" {
		t.Fatalf("restore points were not deep copied")
	}
	if original.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("conditions were not deep copied")
	}
}

func TestBackupJobDeepCopyCopiesCommandStatus(t *testing.T) {
	now := metav1.Now()
	original := &BackupJob{
		Status: BackupJobStatus{
			LastCommand: &CommandStatus{
				ID:     "cmd-1",
				Type:   "finalize_backup_run",
				SentAt: &now,
			},
		},
	}
	copy := original.DeepCopy()
	copy.Status.LastCommand.ID = "cmd-2"
	copy.Status.LastCommand.SentAt.Time = copy.Status.LastCommand.SentAt.AddDate(0, 0, 1)

	if original.Status.LastCommand.ID != "cmd-1" {
		t.Fatal("command status was not deep copied")
	}
	if original.Status.LastCommand.SentAt.Equal(copy.Status.LastCommand.SentAt) {
		t.Fatal("command timestamp pointer was not deep copied")
	}
}
