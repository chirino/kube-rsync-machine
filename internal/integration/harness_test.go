package integration

import (
	"testing"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestBackupJobCreatesTargetThenSourceJobsAcrossNamespaces(t *testing.T) {
	target := backupTarget("backup", "archive", "archive-pvc")
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	h := NewHarness(t, &target, &source, &plan, &run)

	h.ReconcileBackupJob("backup", "demo-run")
	h.ReconcileBackupJob("backup", "demo-run")

	updated := h.GetBackupJob("backup", "demo-run")
	if !controllerutil.ContainsFinalizer(&updated, controller.BackupJobFinalizer) {
		t.Fatal("expected backup job finalizer")
	}
	h.GetJob("backup", "krm-target-demo-run")
	assertExists(t, h, types.NamespacedName{Namespace: "backup", Name: "krm-target-demo-run"}, &corev1.Service{})
	assertNotFound(t, h, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
	assertExists(t, h, types.NamespacedName{Namespace: "backup", Name: "krm-tls-backup-demo-run-target-server-backup-archive"}, &corev1.Secret{})
	assertExists(t, h, types.NamespacedName{Namespace: "app-prod", Name: "krm-tls-backup-demo-run-source-sender-app-prod-files"}, &corev1.Secret{})

	targetJob := h.GetJob("backup", "krm-target-demo-run")
	targetJob.Status.Conditions = []batchv1.JobCondition{{
		Type:   batchv1.JobComplete,
		Status: corev1.ConditionTrue,
	}}
	if err := h.Client.Status().Update(h.Ctx, &targetJob); err != nil {
		t.Fatal(err)
	}
	h.ReconcileBackupJob("backup", "demo-run")

	sourceJob := h.GetJob("app-prod", "krm-source-files-demo-run")
	if sourceJob.Namespace != source.Namespace {
		t.Fatalf("expected source job in source namespace, got %q", sourceJob.Namespace)
	}
	updated = h.GetBackupJob("backup", "demo-run")
	if updated.Status.Phase != krmv1alpha1.RunPhaseRunning {
		t.Fatalf("unexpected phase: %s", updated.Status.Phase)
	}
}

func backupTarget(namespace, name, pvc string) krmv1alpha1.RsyncMachine {
	return krmv1alpha1.RsyncMachine{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: krmv1alpha1.RsyncMachineSpec{
			PVCName:                  pvc,
			AllowedSourceNamespaces:  []string{"*"},
			AllowedRestoreNamespaces: []string{"*"},
		},
	}
}

func backupSource(namespace, name, pvc, destinationPath string) krmv1alpha1.BackupSource {
	return krmv1alpha1.BackupSource{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: krmv1alpha1.BackupSourceSpec{
			MachineRef:      ref("backup", "archive"),
			PVC:             pvc,
			DestinationPath: destinationPath,
		},
	}
}

func rsyncMachine(namespace, name string, target krmv1alpha1.ObjectReference, sources []krmv1alpha1.ObjectReference) krmv1alpha1.RsyncMachine {
	return krmv1alpha1.RsyncMachine{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: krmv1alpha1.RsyncMachineSpec{
			PVCName:                  target.Name + "-pvc",
			AllowedSourceNamespaces:  []string{"*"},
			AllowedRestoreNamespaces: []string{"*"},
		},
	}
}

func backupRun(namespace, name string, plan krmv1alpha1.ObjectReference) krmv1alpha1.BackupJob {
	machine := krmv1alpha1.ObjectReference{Namespace: "backup", Name: "archive"}
	if plan.Name == "missing-plan" || plan.Name == "archive" {
		machine = plan
	}
	return krmv1alpha1.BackupJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: krmv1alpha1.BackupJobSpec{
			MachineRef: machine,
		},
	}
}

func ref(namespace, name string) krmv1alpha1.ObjectReference {
	return krmv1alpha1.ObjectReference{Namespace: namespace, Name: name}
}

func assertNotFound(t *testing.T, h *Harness, ref types.NamespacedName, obj client.Object) {
	t.Helper()
	err := h.Client.Get(h.Ctx, ref, obj)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected %s to be absent, got %v", ref.String(), err)
	}
}

func assertExists(t *testing.T, h *Harness, ref types.NamespacedName, obj client.Object) {
	t.Helper()
	if err := h.Client.Get(h.Ctx, ref, obj); err != nil {
		t.Fatalf("expected %s to exist: %v", ref.String(), err)
	}
}
