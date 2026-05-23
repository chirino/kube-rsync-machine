package integration

import (
	"testing"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBackupJobSourceJobFailureMarksRunFailed(t *testing.T) {
	target := backupTarget("backup", "archive", "archive-pvc")
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	h := NewHarness(t, &target, &source, &plan, &run)

	h.ReconcileBackupJob("backup", "demo-run")
	h.ReconcileBackupJob("backup", "demo-run")
	markJob(t, h, "backup", "krm-target-demo-run", batchv1.JobComplete)
	h.ReconcileBackupJob("backup", "demo-run")
	markJob(t, h, "app-prod", "krm-source-files-demo-run", batchv1.JobFailed)
	h.ReconcileBackupJob("backup", "demo-run")

	updated := h.GetBackupJob("backup", "demo-run")
	if updated.Status.Phase != krmv1alpha1.RunPhaseFailed || updated.Status.CompletedAt == nil {
		t.Fatalf("unexpected run status: %#v", updated.Status)
	}
}

func markJob(t *testing.T, h *Harness, namespace, name string, conditionType batchv1.JobConditionType) {
	t.Helper()
	job := h.GetJob(namespace, name)
	job.Status.Conditions = []batchv1.JobCondition{{
		Type:   conditionType,
		Status: corev1.ConditionTrue,
	}}
	if err := h.Client.Status().Update(h.Ctx, &job); err != nil {
		t.Fatal(err)
	}
}

func runLabels(namespace, name, kind, role string) map[string]string {
	return map[string]string{
		controller.LabelName:         controller.AppName,
		controller.LabelRunNamespace: namespace,
		controller.LabelRunKind:      kind,
		controller.LabelRun:          name,
		controller.LabelRole:         role,
	}
}

func failedSourceJob(namespace, name, runNamespace, runName string) *batchv1.Job {
	return FailedJob(namespace, name, runLabels(runNamespace, runName, "backup", controller.RoleSourceSender))
}

func completedTargetJob(namespace, name, runNamespace, runName string) *batchv1.Job {
	return CompletedJob(namespace, name, runLabels(runNamespace, runName, "backup", controller.RoleTargetServer))
}

func objectMeta(namespace, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: namespace, Name: name}
}
