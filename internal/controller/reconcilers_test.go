package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	"github.com/chirino/kube-rsync-machine/internal/snapshot"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestBackupJobReconcilerCreatesCredentialsAndJobs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := krmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	for i := 0; i < 2; i++ {
		_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
		if err != nil {
			t.Fatal(err)
		}
	}

	var updated krmv1alpha1.BackupJob
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePreparing {
		t.Fatalf("unexpected phase: %s", updated.Status.Phase)
	}
	if !controllerutil.ContainsFinalizer(&updated, BackupJobFinalizer) {
		t.Fatalf("expected backup job finalizer on non-terminal run")
	}
	if len(updated.Status.IncludedMachines) != 1 || updated.Status.IncludedMachines[0].Name != "archive" {
		t.Fatalf("unexpected included machines: %#v", updated.Status.IncludedMachines)
	}

	for _, ref := range []types.NamespacedName{
		{Namespace: "backup", Name: "krm-tls-backup-demo-run-target-server-backup-archive"},
		{Namespace: "app-prod", Name: "krm-tls-backup-demo-run-source-sender-app-prod-files"},
	} {
		var secret corev1.Secret
		if err := client.Get(context.Background(), ref, &secret); err != nil {
			t.Fatalf("expected secret %s: %v", ref.String(), err)
		}
		if len(secret.Data["ca.crt"]) == 0 || len(secret.Data["tls.crt"]) == 0 || len(secret.Data["tls.key"]) == 0 {
			t.Fatalf("secret %s missing TLS data", ref.String())
		}
	}
	for _, ref := range []types.NamespacedName{
		{Namespace: "backup", Name: "krm-target-demo-run"},
	} {
		var job batchv1.Job
		if err := client.Get(context.Background(), ref, &job); err != nil {
			t.Fatalf("expected job %s: %v", ref.String(), err)
		}
		assertControllerOwnerRef(t, job.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "BackupJob", run.Name, run.UID)
	}
	var service corev1.Service
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "krm-target-demo-run"}, &service); err != nil {
		t.Fatalf("expected target service: %v", err)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerAcquiresTargetGuardLease(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}

	var lease coordinationv1.Lease
	leaseRef := TargetGuardLeaseRef(types.NamespacedName{Namespace: "backup", Name: "archive"})
	if err := client.Get(ctx, leaseRef, &lease); err != nil {
		t.Fatalf("expected target guard lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "backup/demo-run" {
		t.Fatalf("unexpected lease holder: %#v", lease.Spec.HolderIdentity)
	}
	if lease.Labels[LabelRun] != "demo-run" || lease.Labels[LabelRole] != targetGuardRole || lease.Labels[labelTargetName] != "archive" {
		t.Fatalf("unexpected lease labels: %#v", lease.Labels)
	}
}

func TestBackupJobReconcilerHoldsWhenTargetReadyFalse(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	target.Status.Conditions = []krmv1alpha1.Condition{{
		Type:   "Ready",
		Status: metav1.ConditionFalse,
		Reason: "TargetUnusable",
	}}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}, &krmv1alpha1.RsyncMachine{}).
		WithObjects(&target, &source, &plan, &run).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePending || len(updated.Status.Conditions) != 1 {
		t.Fatalf("unexpected held status: %#v", updated.Status)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-target-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerForbidHoldsWhenTargetHasActiveRun(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	activePlan := rsyncMachine("backup", "active-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	activeRun := backupRun("backup", "active-run", ref("backup", "active-plan"))
	activeRun.Status.Phase = krmv1alpha1.RunPhaseRunning
	newPlan := rsyncMachine("backup", "new-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	newRun := backupRun("backup", "new-run", ref("backup", "new-plan"))
	controllerutil.AddFinalizer(&newRun, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &activePlan, &activeRun, &newPlan, &newRun).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "new-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected held overlap run to requeue")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "new-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePending || len(updated.Status.Conditions) != 1 || updated.Status.Conditions[0].Type != "TargetOverlap" {
		t.Fatalf("unexpected overlap status: %#v", updated.Status)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-target-new-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerHoldsWhenTargetHasActiveRestore(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	activeRestore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
		},
	}
	activeRestore.Status.Phase = krmv1alpha1.RunPhaseRunning
	newPlan := rsyncMachine("backup", "new-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	newRun := backupRun("backup", "new-run", ref("backup", "new-plan"))
	controllerutil.AddFinalizer(&newRun, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}, &krmv1alpha1.RestoreJob{}).
		WithObjects(&target, &source, &activeRestore, &newPlan, &newRun).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "new-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected backup to requeue while restore is active")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "new-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePending || len(updated.Status.Conditions) != 1 {
		t.Fatalf("unexpected restore overlap status: %#v", updated.Status)
	}
	condition := updated.Status.Conditions[0]
	if condition.Type != ConditionTargetOverlap || condition.Reason != ReasonActiveRestore || !strings.Contains(condition.Message, "waiting for restore to complete") {
		t.Fatalf("expected active restore condition, got %#v", condition)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-target-new-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerForbidHoldsWhenTargetGuardHeldByActiveRun(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	activeRun := backupRun("backup", "active-run", ref("backup", "missing-plan"))
	activeRun.Status.Phase = krmv1alpha1.RunPhaseRunning
	lease := targetGuardLease(activeRun, types.NamespacedName{Namespace: "backup", Name: "archive"})
	newPlan := rsyncMachine("backup", "new-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	newRun := backupRun("backup", "new-run", ref("backup", "new-plan"))
	controllerutil.AddFinalizer(&newRun, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &activeRun, lease, &newPlan, &newRun).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "new-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected lease-held run to requeue")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "new-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePending || updated.Status.Conditions[0].Reason != "ActiveRunForTarget" {
		t.Fatalf("unexpected lease overlap status: %#v", updated.Status)
	}
	var unchanged coordinationv1.Lease
	if err := client.Get(ctx, TargetGuardLeaseRef(types.NamespacedName{Namespace: "backup", Name: "archive"}), &unchanged); err != nil {
		t.Fatal(err)
	}
	if unchanged.Spec.HolderIdentity == nil || *unchanged.Spec.HolderIdentity != "backup/active-run" {
		t.Fatalf("target guard holder changed unexpectedly: %#v", unchanged.Spec.HolderIdentity)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-target-new-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerReplaceCancelsActiveRunAndStartsReplacement(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	target.Spec.ConcurrencyPolicy = krmv1alpha1.ConcurrencyPolicyReplace
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	activePlan := rsyncMachine("backup", "active-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	activeRun := backupRun("backup", "active-run", ref("backup", "active-plan"))
	controllerutil.AddFinalizer(&activeRun, BackupJobFinalizer)
	activeRun.Status.Phase = krmv1alpha1.RunPhaseRunning
	activeJob := jobWithCondition("backup", "krm-target-active-run", runLabels(activeRun, runKindBackup, RoleTargetServer), "")
	activeSecret := runSecret("backup", "active-tls", runLabels(activeRun, runKindBackup, RoleTargetServer))
	activeService := runService("backup", "krm-target-active-run", runLabels(activeRun, runKindBackup, RoleTargetServer))
	newPlan := rsyncMachine("backup", "new-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	newRun := backupRun("backup", "new-run", ref("backup", "new-plan"))
	controllerutil.AddFinalizer(&newRun, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &activePlan, &activeRun, activeJob, activeSecret, activeService, &newPlan, &newRun).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "new-run"}})
	if err != nil {
		t.Fatal(err)
	}

	var canceled krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "active-run"}, &canceled); err != nil {
		t.Fatal(err)
	}
	if canceled.Status.Phase != krmv1alpha1.RunPhaseCanceled || canceled.Status.CompletedAt == nil || canceled.Status.Conditions[0].Reason != "ReplacedByRun" {
		t.Fatalf("unexpected canceled active run status: %#v", canceled.Status)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-target-active-run"}, &batchv1.Job{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "active-tls"}, &corev1.Secret{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-target-active-run"}, &corev1.Service{})

	var replacement krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "new-run"}, &replacement); err != nil {
		t.Fatal(err)
	}
	if replacement.Status.Phase != krmv1alpha1.RunPhasePreparing {
		t.Fatalf("unexpected replacement phase: %s", replacement.Status.Phase)
	}
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-target-new-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerReplaceStealsTargetGuard(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	target.Spec.ConcurrencyPolicy = krmv1alpha1.ConcurrencyPolicyReplace
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	activeRun := backupRun("backup", "active-run", ref("backup", "missing-plan"))
	controllerutil.AddFinalizer(&activeRun, BackupJobFinalizer)
	activeRun.Status.Phase = krmv1alpha1.RunPhaseRunning
	lease := targetGuardLease(activeRun, types.NamespacedName{Namespace: "backup", Name: "archive"})
	newPlan := rsyncMachine("backup", "new-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	newRun := backupRun("backup", "new-run", ref("backup", "new-plan"))
	controllerutil.AddFinalizer(&newRun, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &activeRun, lease, &newPlan, &newRun).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "new-run"}})
	if err != nil {
		t.Fatal(err)
	}

	var canceled krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "active-run"}, &canceled); err != nil {
		t.Fatal(err)
	}
	if canceled.Status.Phase != krmv1alpha1.RunPhaseCanceled || canceled.Status.Conditions[0].Reason != "ReplacedByRun" {
		t.Fatalf("unexpected canceled target guard holder status: %#v", canceled.Status)
	}
	var stolen coordinationv1.Lease
	if err := client.Get(ctx, TargetGuardLeaseRef(types.NamespacedName{Namespace: "backup", Name: "archive"}), &stolen); err != nil {
		t.Fatal(err)
	}
	if stolen.Spec.HolderIdentity == nil || *stolen.Spec.HolderIdentity != "backup/new-run" {
		t.Fatalf("target guard was not stolen: %#v", stolen.Spec.HolderIdentity)
	}
	if stolen.Labels[LabelRun] != "new-run" {
		t.Fatalf("target guard labels were not transferred: %#v", stolen.Labels)
	}
	var replacement krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "new-run"}, &replacement); err != nil {
		t.Fatal(err)
	}
	if replacement.Status.Phase != krmv1alpha1.RunPhasePreparing {
		t.Fatalf("unexpected replacement phase: %s", replacement.Status.Phase)
	}
}

func TestBackupJobReconcilerCoalescesPendingRunsForTarget(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	planA := rsyncMachine("backup", "plan-a", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	planB := rsyncMachine("backup", "plan-b", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	firstRun := backupRun("backup", "first-run", ref("backup", "plan-a"))
	secondRun := backupRun("backup", "second-run", ref("backup", "plan-b"))
	controllerutil.AddFinalizer(&firstRun, BackupJobFinalizer)
	controllerutil.AddFinalizer(&secondRun, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &planA, &planB, &firstRun, &secondRun).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "first-run"}})
	if err != nil {
		t.Fatal(err)
	}

	var canonical krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "first-run"}, &canonical); err != nil {
		t.Fatal(err)
	}
	if canonical.Status.Phase != krmv1alpha1.RunPhasePreparing {
		t.Fatalf("unexpected canonical phase: %s", canonical.Status.Phase)
	}
	var merged krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "second-run"}, &merged); err != nil {
		t.Fatal(err)
	}
	if merged.Status.Phase != krmv1alpha1.RunPhaseCanceled || merged.Status.MergedInto == nil || merged.Status.MergedInto.Name != "first-run" || merged.Status.Conditions[0].Reason != "MergedIntoRun" {
		t.Fatalf("unexpected merged status: %#v", merged.Status)
	}
}

func TestBackupJobReconcilerForbidUsesCrossNamespaceMachineRefs(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	activePlan := rsyncMachine("schedules", "active-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	activeRun := backupRun("runs-a", "active-run", ref("schedules", "active-plan"))
	activeRun.Status.Phase = krmv1alpha1.RunPhasePreparing
	newPlan := rsyncMachine("schedules", "new-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	newRun := backupRun("runs-b", "new-run", ref("schedules", "new-plan"))
	controllerutil.AddFinalizer(&newRun, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &activePlan, &activeRun, &newPlan, &newRun).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "runs-b", Name: "new-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected cross-namespace overlap to requeue")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "runs-b", Name: "new-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePending || updated.Status.Conditions[0].Reason != "ActiveRunForTarget" {
		t.Fatalf("unexpected held cross-namespace status: %#v", updated.Status)
	}
}

func TestBackupJobReconcilerIgnoresTerminalRunsForOverlap(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	oldPlan := rsyncMachine("backup", "old-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	oldRun := backupRun("backup", "old-run", ref("backup", "old-plan"))
	oldRun.Status.Phase = krmv1alpha1.RunPhaseSucceeded
	newPlan := rsyncMachine("backup", "new-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	newRun := backupRun("backup", "new-run", ref("backup", "new-plan"))
	controllerutil.AddFinalizer(&newRun, BackupJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &oldPlan, &oldRun, &newPlan, &newRun).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "new-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "new-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePreparing {
		t.Fatalf("unexpected phase: %s", updated.Status.Phase)
	}
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-target-new-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerDoesNotReopenTerminalRun(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	run := backupRun("backup", "terminal-run", ref("backup", "missing-plan"))
	run.Status.Phase = krmv1alpha1.RunPhaseSucceeded
	completedAt := metav1.NewTime(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	run.Status.CompletedAt = &completedAt
	job := failedJob("backup", "source", runLabels(run, runKindBackup, RoleSourceSender))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&run, job).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "terminal-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "terminal-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseSucceeded || updated.Status.CompletedAt == nil || !updated.Status.CompletedAt.Equal(&completedAt) {
		t.Fatalf("terminal run was reopened or changed: %#v", updated.Status)
	}
}

func TestBackupJobReconcilerPrunesCompletedRunHistory(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	target.UID = types.UID("archive-uid")
	target.Spec.RunHistory = krmv1alpha1.RunHistory{Successful: 1, Failed: 1}
	targetPVC := &corev1.PersistentVolumeClaim{ObjectMeta: objectMeta("backup", "archive-pvc")}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	successOld := completedBackupJob("backup", "success-old", ref("backup", "demo"), krmv1alpha1.RunPhaseSucceeded, time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC))
	successOld.Spec.Trigger = krmv1alpha1.BackupJobTriggerScheduled
	controllerutil.AddFinalizer(&successOld, BackupJobFinalizer)
	successNew := completedBackupJob("backup", "success-new", ref("backup", "demo"), krmv1alpha1.RunPhaseSucceeded, time.Date(2026, 5, 20, 11, 0, 0, 0, time.UTC))
	successNew.Spec.Trigger = krmv1alpha1.BackupJobTriggerScheduled
	failedOld := completedBackupJob("backup", "failed-old", ref("backup", "demo"), krmv1alpha1.RunPhaseFailed, time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC))
	failedOld.Spec.Trigger = krmv1alpha1.BackupJobTriggerScheduled
	failedNew := completedBackupJob("backup", "failed-new", ref("backup", "demo"), krmv1alpha1.RunPhaseCanceled, time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	failedNew.Spec.Trigger = krmv1alpha1.BackupJobTriggerScheduled
	active := backupRun("backup", "active-run", ref("backup", "demo"))
	active.Status.Phase = krmv1alpha1.RunPhaseRunning
	oldJob := completedJob("backup", "old-job", runLabels(successOld, runKindBackup, RoleSourceSender))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, targetPVC, &source, &plan, &successOld, &successNew, &failedOld, &failedNew, &active, oldJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme}

	for i := 0; i < 2; i++ {
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "success-new"}})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "success-old"}, &krmv1alpha1.BackupJob{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "failed-old"}, &krmv1alpha1.BackupJob{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "old-job"}, &batchv1.Job{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "success-new"}, &krmv1alpha1.BackupJob{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "failed-new"}, &krmv1alpha1.BackupJob{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "active-run"}, &krmv1alpha1.BackupJob{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "archive-pvc"}, &corev1.PersistentVolumeClaim{})
}

func TestBackupJobReconcilerDoesNotPruneManualRunHistory(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Spec.RunHistory = krmv1alpha1.RunHistory{Successful: 1}
	targetPVC := &corev1.PersistentVolumeClaim{ObjectMeta: objectMeta("backup", "archive-pvc")}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	manualOld := completedBackupJob("backup", "manual-old", ref("backup", "demo"), krmv1alpha1.RunPhaseSucceeded, time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC))
	scheduledNew := completedBackupJob("backup", "scheduled-new", ref("backup", "demo"), krmv1alpha1.RunPhaseSucceeded, time.Date(2026, 5, 20, 11, 0, 0, 0, time.UTC))
	scheduledNew.Spec.Trigger = krmv1alpha1.BackupJobTriggerScheduled
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, targetPVC, &source, &plan, &manualOld, &scheduledNew).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme}

	for i := 0; i < 2; i++ {
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "scheduled-new"}})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "manual-old"}, &krmv1alpha1.BackupJob{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "scheduled-new"}, &krmv1alpha1.BackupJob{})
}

func TestBackupJobReconcilerSetsOwnerReferenceForScheduledRun(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.UID = types.UID("archive-uid")
	run := backupRun("backup", "scheduled-run", ref("backup", "archive"))
	run.Spec.Trigger = krmv1alpha1.BackupJobTriggerScheduled
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &run).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "scheduled-run"}})
	if err != nil {
		t.Fatal(err)
	}

	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "scheduled-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.OwnerReferences) != 1 {
		t.Fatalf("expected one owner reference, got %#v", updated.OwnerReferences)
	}
	owner := updated.OwnerReferences[0]
	if owner.APIVersion != krmv1alpha1.SchemeGroupVersion.String() || owner.Kind != "RsyncMachine" || owner.Name != "archive" || owner.UID != target.UID {
		t.Fatalf("unexpected owner reference: %#v", owner)
	}
}

func TestRsyncMachineReconcilerDeletesScheduledBackupJobsOnDelete(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	controllerutil.AddFinalizer(&target, RsyncMachineFinalizer)
	manual := backupRun("backup", "manual-run", ref("backup", "archive"))
	scheduled := backupRun("backup", "scheduled-run", ref("backup", "archive"))
	scheduled.Spec.Trigger = krmv1alpha1.BackupJobTriggerScheduled
	cronJob := batchv1.CronJob{ObjectMeta: objectMeta("backup", GeneratedCronJobName(target.Name))}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&target, &manual, &scheduled, &cronJob).
		Build()
	reconciler := RsyncMachineReconciler{Client: client, Scheme: scheme}
	if err := client.Delete(ctx, &target); err != nil {
		t.Fatal(err)
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "archive"}})
	if err != nil {
		t.Fatal(err)
	}

	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "scheduled-run"}, &krmv1alpha1.BackupJob{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: cronJob.Name}, &batchv1.CronJob{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "manual-run"}, &krmv1alpha1.BackupJob{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "archive"}, &krmv1alpha1.RsyncMachine{})
}

func TestRsyncMachineReconcilerCreatesScheduledBackupJob(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := rsyncMachine("backup", "archive", ref("backup", "archive"), nil, krmv1alpha1.RetentionPolicy{})
	target.CreationTimestamp = metav1.NewTime(time.Date(2026, 5, 22, 11, 0, 0, 0, time.UTC))
	target.Spec.Schedule = "*/5 * * * *"
	controllerutil.AddFinalizer(&target, RsyncMachineFinalizer)
	targetPVC := &corev1.PersistentVolumeClaim{ObjectMeta: objectMeta("backup", "archive-pvc")}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	cronJob := batchv1.CronJob{ObjectMeta: objectMeta("backup", GeneratedCronJobName(target.Name))}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RsyncMachine{}).
		WithObjects(&target, targetPVC, &source, &cronJob).
		Build()
	now := time.Date(2026, 5, 22, 12, 3, 0, 0, time.UTC)
	reconciler := RsyncMachineReconciler{Client: client, Scheme: scheme, Clock: func() time.Time { return now }}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "archive"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Minute {
		t.Fatalf("expected next schedule requeue after 2m, got %s", result.RequeueAfter)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: cronJob.Name}, &batchv1.CronJob{})

	var run krmv1alpha1.BackupJob
	runName := GeneratedScheduledBackupJobName("archive", time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: runName}, &run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.Trigger != krmv1alpha1.BackupJobTriggerScheduled || run.Spec.MachineRef.Name != "archive" {
		t.Fatalf("unexpected scheduled backup job: %#v", run)
	}
	assertControllerOwnerRef(t, run.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RsyncMachine", target.Name, target.UID)

	var updated krmv1alpha1.RsyncMachine
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "archive"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.LastScheduledAt == nil || !updated.Status.LastScheduledAt.Time.Equal(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected last scheduled time: %#v", updated.Status.LastScheduledAt)
	}
	assertCondition(t, updated.Status.Conditions, ConditionReady, metav1.ConditionTrue, ReasonResolvedReferences)
}

func TestRsyncMachineSchedulerScansScheduledMachines(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := rsyncMachine("backup", "archive", ref("backup", "archive"), nil, krmv1alpha1.RetentionPolicy{})
	target.CreationTimestamp = metav1.NewTime(time.Date(2026, 5, 22, 11, 0, 0, 0, time.UTC))
	target.Spec.Schedule = "*/5 * * * *"
	controllerutil.AddFinalizer(&target, RsyncMachineFinalizer)
	targetPVC := &corev1.PersistentVolumeClaim{ObjectMeta: objectMeta("backup", "archive-pvc")}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RsyncMachine{}).
		WithObjects(&target, targetPVC, &source).
		Build()
	now := time.Date(2026, 5, 22, 12, 3, 0, 0, time.UTC)
	reconciler := &RsyncMachineReconciler{Client: client, Scheme: scheme, Clock: func() time.Time { return now }}
	scheduler := RsyncMachineScheduler{Client: client, Reconciler: reconciler}

	scheduler.run(ctx)

	runName := GeneratedScheduledBackupJobName("archive", time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: runName}, &krmv1alpha1.BackupJob{})
}

func TestRsyncMachineReconcilerSkipsScheduleUntilNextDue(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	lastScheduled := metav1.NewTime(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	target := rsyncMachine("backup", "archive", ref("backup", "archive"), nil, krmv1alpha1.RetentionPolicy{})
	target.Spec.Schedule = "*/5 * * * *"
	target.Status.LastScheduledAt = &lastScheduled
	controllerutil.AddFinalizer(&target, RsyncMachineFinalizer)
	targetPVC := &corev1.PersistentVolumeClaim{ObjectMeta: objectMeta("backup", "archive-pvc")}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RsyncMachine{}).
		WithObjects(&target, targetPVC, &source).
		Build()
	now := time.Date(2026, 5, 22, 12, 3, 0, 0, time.UTC)
	reconciler := RsyncMachineReconciler{Client: client, Scheme: scheme, Clock: func() time.Time { return now }}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "archive"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Minute {
		t.Fatalf("expected next schedule requeue after 2m, got %s", result.RequeueAfter)
	}
	var runs krmv1alpha1.BackupJobList
	if err := client.List(ctx, &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 0 {
		t.Fatalf("expected no scheduled backup job before next due time, got %#v", runs.Items)
	}
}

func TestBackupJobReconcilerCreatesSourceJobsAfterTargetCompletes(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	targetJob := completedJob("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var sender batchv1.Job
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &sender); err != nil {
		t.Fatalf("expected source sender after target completion: %v", err)
	}
	assertOwnerRef(t, sender.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "BackupSource", source.Name, source.UID)
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseRunning {
		t.Fatalf("unexpected phase: %s", updated.Status.Phase)
	}
}

func TestBackupJobReconcilerSourceJobCanHaveBackupJobAndSourceOwners(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	run.UID = types.UID("run-uid")
	source := backupSource("backup", "files", "data-pvc", "sites/demo/files")
	source.UID = types.UID("source-uid")
	job := &batchv1.Job{ObjectMeta: objectMeta("backup", "krm-source-files-demo-run")}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&run, &source).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme}

	if err := reconciler.createSourceJobIfMissing(ctx, &run, &source, job); err != nil {
		t.Fatal(err)
	}

	var created batchv1.Job
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "krm-source-files-demo-run"}, &created); err != nil {
		t.Fatal(err)
	}
	assertControllerOwnerRef(t, created.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "BackupJob", run.Name, run.UID)
	assertOwnerRef(t, created.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "BackupSource", source.Name, source.UID)
}

func TestBackupJobReconcilerFailsRequiredSnapshotWhenUnsupported(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeVolumeSnapshot
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err == nil {
		t.Fatal("expected unsupported snapshot capture error")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseFailed || updated.Status.Conditions[0].Reason != "ReconcileError" {
		t.Fatalf("unexpected status: %#v", updated.Status)
	}
	if len(updated.Status.Transfers) != 1 || updated.Status.Transfers[0].Phase != krmv1alpha1.TransferPhaseFailed || updated.Status.Transfers[0].CaptureMethod != krmv1alpha1.CaptureModeVolumeSnapshot {
		t.Fatalf("unexpected transfer status: %#v", updated.Status.Transfers)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerAutoSnapshotFallsBackToDirectWhenUnsupported(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeAuto
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var sender batchv1.Job
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &sender); err != nil {
		t.Fatalf("expected direct sender fallback: %v", err)
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.Transfers) != 1 || updated.Status.Transfers[0].CaptureMethod != krmv1alpha1.CaptureModeDirect || updated.Status.Transfers[0].Message == "" {
		t.Fatalf("expected direct fallback transfer status: %#v", updated.Status.Transfers)
	}
}

func TestBackupJobReconcilerFailsRequiredSnapshotWhenSnapshotReportsError(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeVolumeSnapshot
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	failedSnapshot := snapshot.BuildVolumeSnapshot(run, source)
	failedSnapshot.SetCreationTimestamp(metav1.NewTime(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)))
	_ = unstructured.SetNestedMap(failedSnapshot.Object, map[string]interface{}{
		"creationTime": "2026-05-20T12:00:00Z",
		"error": map[string]interface{}{
			"message": "no compatible snapshotter",
		},
	}, "status")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob, failedSnapshot).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err == nil {
		t.Fatal("expected snapshot failure error")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseFailed || len(updated.Status.Transfers) != 1 {
		t.Fatalf("unexpected status: %#v", updated.Status)
	}
	transfer := updated.Status.Transfers[0]
	if transfer.Phase != krmv1alpha1.TransferPhaseFailed || transfer.VolumeSnapshotName != failedSnapshot.GetName() || transfer.CaptureTime == nil {
		t.Fatalf("unexpected transfer status: %#v", transfer)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerRequeuesWhileRequiredSnapshotNotReady(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeVolumeSnapshot
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	pendingSnapshot := snapshot.BuildVolumeSnapshot(run, source)
	pendingSnapshot.SetCreationTimestamp(metav1.Now())
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob, pendingSnapshot).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected snapshot readiness requeue")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.Transfers) != 1 || updated.Status.Transfers[0].Phase != krmv1alpha1.TransferPhasePreparing {
		t.Fatalf("unexpected transfer status: %#v", updated.Status.Transfers)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerFailsRequiredSnapshotReadinessTimeout(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeVolumeSnapshot
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	oldSnapshot := snapshot.BuildVolumeSnapshot(run, source)
	oldSnapshot.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-snapshotReadyTimeout - time.Minute)))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob, oldSnapshot).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err == nil {
		t.Fatal("expected snapshot timeout error")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseFailed || len(updated.Status.Transfers) != 1 || updated.Status.Transfers[0].Phase != krmv1alpha1.TransferPhaseFailed {
		t.Fatalf("unexpected timeout status: %#v", updated.Status)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerSnapshotAvailableDoesNotFailRequiredCapture(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeVolumeSnapshot
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var volumeSnapshot unstructured.Unstructured
	volumeSnapshot.SetAPIVersion(snapshot.SnapshotAPIVersion)
	volumeSnapshot.SetKind(snapshot.VolumeSnapshotKind)
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "krm-vs-backup-demo-run-app-prod-files"}, &volumeSnapshot); err != nil {
		t.Fatalf("expected generated volume snapshot: %v", err)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerCreatesSenderFromReadySnapshotPVC(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeVolumeSnapshot
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app-prod", Name: "data-pvc"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}
	readySnapshot := snapshot.BuildVolumeSnapshot(run, source)
	_ = unstructured.SetNestedMap(readySnapshot.Object, map[string]interface{}{
		"readyToUse":  true,
		"restoreSize": "12Gi",
	}, "status")
	restoreSize := resource.MustParse("12Gi")
	tempPVC := snapshot.BuildTemporaryPVCFromSnapshot(run, source, *sourcePVC, &restoreSize)
	tempPVC.Status.Phase = corev1.ClaimBound
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob, sourcePVC, readySnapshot, tempPVC).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var observedTempPVC corev1.PersistentVolumeClaim
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "krm-vspvc-backup-demo-run-app-prod-files"}, &observedTempPVC); err != nil {
		t.Fatalf("expected temporary snapshot pvc: %v", err)
	}
	var sender batchv1.Job
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &sender); err != nil {
		t.Fatalf("expected sender after snapshot pvc creation: %v", err)
	}
	if got := sender.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != observedTempPVC.Name {
		t.Fatalf("expected sender to mount temp pvc %q, got %q", observedTempPVC.Name, got)
	}
}

func TestBackupJobReconcilerRequeuesWhileTemporarySnapshotPVCNotBound(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeVolumeSnapshot
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app-prod", Name: "data-pvc"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}
	readySnapshot := snapshot.BuildVolumeSnapshot(run, source)
	_ = unstructured.SetNestedMap(readySnapshot.Object, map[string]interface{}{"readyToUse": true}, "status")
	tempPVC := snapshot.BuildTemporaryPVCFromSnapshot(run, source, *sourcePVC, nil)
	tempPVC.Status.Phase = corev1.ClaimPending
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob, sourcePVC, readySnapshot, tempPVC).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected temporary PVC binding requeue")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.Transfers) != 1 || updated.Status.Transfers[0].Phase != krmv1alpha1.TransferPhasePreparing || updated.Status.Transfers[0].VolumeSnapshotName == "" {
		t.Fatalf("unexpected transfer status: %#v", updated.Status.Transfers)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerFailsRequiredTemporarySnapshotPVCBindTimeout(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Consistency.Capture = krmv1alpha1.CaptureModeVolumeSnapshot
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app-prod", Name: "data-pvc"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}
	readySnapshot := snapshot.BuildVolumeSnapshot(run, source)
	_ = unstructured.SetNestedMap(readySnapshot.Object, map[string]interface{}{"readyToUse": true}, "status")
	tempPVC := snapshot.BuildTemporaryPVCFromSnapshot(run, source, *sourcePVC, nil)
	tempPVC.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-temporaryPVCBindingTimeout - time.Minute)))
	tempPVC.Status.Phase = corev1.ClaimPending
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob, sourcePVC, readySnapshot, tempPVC).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err == nil {
		t.Fatal("expected temporary PVC timeout error")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseFailed || len(updated.Status.Transfers) != 1 || updated.Status.Transfers[0].Phase != krmv1alpha1.TransferPhaseFailed {
		t.Fatalf("unexpected temporary PVC timeout status: %#v", updated.Status)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
}

func TestBackupJobReconcilerCreatesSourceJobsAfterTargetReadyEvent(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.TargetPhase = "Ready"
	targetJob := jobWithCondition("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer), "")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", Control: control.NewService(control.NewEventHub(4))}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var sender batchv1.Job
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &sender); err != nil {
		t.Fatalf("expected source sender after target ready event: %v", err)
	}
}

func TestBackupJobReconcilerEnqueuesFinalizeAfterSourcesComplete(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhaseRunning
	run.Status.TargetPhase = "Ready"
	sourceJob := completedJob("app-prod", "krm-source-files-demo-run", runLabels(run, runKindBackup, RoleSourceSender))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, sourceJob).
		Build()
	controlService := control.NewService(control.NewEventHub(4))
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", Control: controlService}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseFinalizing || updated.Status.LastCommand == nil {
		t.Fatalf("unexpected status: %#v", updated.Status)
	}

	commands, err := controlService.RegisterTarget(ctx, control.RegisterTargetRequest{
		RunNamespace:    "backup",
		RunName:         "demo-run",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
	})
	if err != nil {
		t.Fatal(err)
	}
	command := readTargetCommand(t, commands)
	if command.Type != control.TargetCommandFinalizeBackupJob || command.Finalize == nil || len(command.Finalize.Sources) != 1 {
		t.Fatalf("unexpected command: %#v", command)
	}
	firstTimestamp := command.Finalize.Timestamp
	if firstTimestamp == "" {
		t.Fatal("expected finalize timestamp")
	}

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	assertNoTargetCommand(t, commands)

	restartedControl := control.NewService(control.NewEventHub(4))
	reconciler.Control = restartedControl
	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	replayedCommands, err := restartedControl.RegisterTarget(ctx, control.RegisterTargetRequest{
		RunNamespace:    "backup",
		RunName:         "demo-run",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed := readTargetCommand(t, replayedCommands)
	if replayed.Finalize == nil || replayed.Finalize.Timestamp != firstTimestamp {
		t.Fatalf("expected restarted service to reuse finalize timestamp %q, got %#v", firstTimestamp, replayed)
	}
	assertNoTargetCommand(t, replayedCommands)
}

func TestBackupJobBytesTransferredSumsSucceededTransfers(t *testing.T) {
	transfers := []krmv1alpha1.TransferStatus{{
		Source:           "app-prod/files",
		Phase:            krmv1alpha1.TransferPhaseSucceeded,
		BytesTransferred: 4096,
	}, {
		Source:           "app-prod/cache",
		Phase:            krmv1alpha1.TransferPhaseFailed,
		BytesTransferred: 8192,
	}, {
		Source:           "app-prod/media",
		Phase:            krmv1alpha1.TransferPhaseSucceeded,
		BytesTransferred: 1024,
	}}

	if got := backupJobBytesTransferred(transfers); got != 5120 {
		t.Fatalf("backupJobBytesTransferred = %d, want 5120", got)
	}
}

func TestBackupJobReconcilerCompletesFinalizingRunWhenTargetJobCompleted(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhaseFinalizing
	run.Status.TargetPhase = "Ready"
	sentAt := metav1.NewTime(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	run.Status.LastCommand = &krmv1alpha1.CommandStatus{
		ID:     "finalize-demo-run",
		Type:   control.TargetCommandFinalizeBackupJob,
		SentAt: &sentAt,
	}
	run.Status.Transfers = []krmv1alpha1.TransferStatus{{
		Source: "app-prod/files",
		Phase:  krmv1alpha1.TransferPhaseSucceeded,
	}}
	targetJob := completedJob("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer))
	sourceJob := completedJob("app-prod", "krm-source-files-demo-run", runLabels(run, runKindBackup, RoleSourceSender))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob, sourceJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", Control: control.NewService(control.NewEventHub(4))}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseSucceeded {
		t.Fatalf("expected completed target job to finish finalizing run, got %#v", updated.Status)
	}
	if updated.Status.SnapshotPath != "hourly/2026-05-20T12-00-00Z" {
		t.Fatalf("expected snapshot path from finalize command timestamp, got %q", updated.Status.SnapshotPath)
	}
}

func TestBackupJobReconcilerEnqueuesRecoveryForOutOfSpaceSourceFailure(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhaseRunning
	run.Status.TargetPhase = "Ready"
	run.Status.Transfers = []krmv1alpha1.TransferStatus{{
		Source:   "app-prod/files",
		Phase:    krmv1alpha1.TransferPhaseFailed,
		ExitCode: 11,
		Message:  "rsync: write failed: No space left on device",
	}}
	sourceJob := failedJob("app-prod", "krm-source-files-demo-run", runLabels(run, runKindBackup, RoleSourceSender))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, sourceJob).
		Build()
	controlService := control.NewService(control.NewEventHub(4))
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", Control: controlService}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected recovery wait requeue")
	}
	commands, err := controlService.RegisterTarget(ctx, control.RegisterTargetRequest{
		RunNamespace:    "backup",
		RunName:         "demo-run",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
	})
	if err != nil {
		t.Fatal(err)
	}
	command := readTargetCommand(t, commands)
	if command.Type != control.TargetCommandRecoverSpace || command.RecoverSpace == nil || command.RecoverSpace.MinAvailableBytes == 0 {
		t.Fatalf("unexpected recovery command: %#v", command)
	}
}

func TestBackupJobReconcilerRetriesSourceAfterRecoveryAck(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhaseRunning
	run.Status.TargetPhase = "Ready"
	ackedAt := metav1.Now()
	run.Status.LastCommand = &krmv1alpha1.CommandStatus{
		ID:             "recover-space-app-prod-files",
		Type:           control.TargetCommandRecoverSpace,
		SentAt:         &ackedAt,
		AcknowledgedAt: &ackedAt,
	}
	run.Status.Transfers = []krmv1alpha1.TransferStatus{{
		Source:   "app-prod/files",
		Phase:    krmv1alpha1.TransferPhaseFailed,
		ExitCode: 11,
		Message:  "rsync: write failed: No space left on device",
	}}
	sourceJob := failedJob("app-prod", "krm-source-files-demo-run", runLabels(run, runKindBackup, RoleSourceSender))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, sourceJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", Control: control.NewService(control.NewEventHub(4))}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected retry requeue")
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-source-files-demo-run"}, &batchv1.Job{})
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Transfers[0].Phase != krmv1alpha1.TransferPhasePreparing || updated.Status.Transfers[0].ExitCode != 0 {
		t.Fatalf("unexpected retry transfer status: %#v", updated.Status.Transfers[0])
	}
}

func TestBackupJobReconcilerResumesFailedRunAfterRecoveryAck(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{Hourly: 24})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	plan := rsyncMachine("backup", "demo", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{Daily: 7})
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	ackedAt := metav1.Now()
	completedAt := metav1.Now()
	run.Status.Phase = krmv1alpha1.RunPhaseFailed
	run.Status.CompletedAt = &completedAt
	run.Status.TargetPhase = "Ready"
	run.Status.LastCommand = &krmv1alpha1.CommandStatus{
		ID:             "recover-space-app-prod-files",
		Type:           control.TargetCommandRecoverSpace,
		SentAt:         &ackedAt,
		AcknowledgedAt: &ackedAt,
	}
	run.Status.Transfers = []krmv1alpha1.TransferStatus{{
		Source:  "app-prod/files",
		Phase:   krmv1alpha1.TransferPhasePreparing,
		Message: "Retrying after target space recovery",
	}}
	targetJob := runningJob("backup", "krm-target-demo-run", runLabels(run, runKindBackup, RoleTargetServer))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&target, &source, &plan, &run, targetJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, Image: "krm:test", Control: control.NewService(control.NewEventHub(4))}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected resume requeue")
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseRunning || updated.Status.CompletedAt != nil {
		t.Fatalf("expected running recovered BackupJob, got phase=%s completedAt=%v", updated.Status.Phase, updated.Status.CompletedAt)
	}
}

func TestRestoreJobReconcilerCreatesCredentialsAndJob(t *testing.T) {
	scheme := testControllerScheme(t)
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Status.RestorePoints = []krmv1alpha1.RestorePoint{{
		Snapshot:   "latest",
		ResolvesTo: "hourly/2026-05-20T10-00-00Z",
	}, {
		Snapshot: "hourly/2026-05-20T10-00-00Z",
	}}
	source := backupSource("backup", "files", "data-pvc", "sites/demo/files")
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("backup", "files"),
			Snapshot:  "latest",
			Overrides: krmv1alpha1.RestoreOverrides{
				Destination: krmv1alpha1.RestoreDestination{
					PVCName: "restore-pvc",
					Path:    "/",
				},
			},
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RestoreJob{}).
		WithObjects(&target, &source, &restore).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	for i := 0; i < 2; i++ {
		_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "restore-files"}})
		if err != nil {
			t.Fatal(err)
		}
	}

	var updated krmv1alpha1.RestoreJob
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "restore-files"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePreparing {
		t.Fatalf("unexpected phase: %s", updated.Status.Phase)
	}
	if !controllerutil.ContainsFinalizer(&updated, RestoreJobFinalizer) {
		t.Fatalf("expected restore job finalizer on non-terminal run")
	}
	for _, ref := range []types.NamespacedName{
		{Namespace: "backup", Name: "krm-tls-backup-restore-files-target-server-backup-archive"},
		{Namespace: "backup", Name: "krm-tls-backup-restore-files-restore-writer-backup-files"},
	} {
		var secret corev1.Secret
		if err := client.Get(context.Background(), ref, &secret); err != nil {
			t.Fatalf("expected secret %s: %v", ref.String(), err)
		}
		switch secret.Labels[LabelRole] {
		case RoleTargetServer:
			assertControllerOwnerRef(t, secret.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RsyncMachine", target.Name, target.UID)
		case RoleRestoreWriter:
			assertControllerOwnerRef(t, secret.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RestoreJob", restore.Name, restore.UID)
		}
	}
	var job batchv1.Job
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "krm-restore-restore-files"}, &job); err != nil {
		t.Fatalf("expected restore job: %v", err)
	}
	assertControllerOwnerRef(t, job.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RestoreJob", restore.Name, restore.UID)
	var targetJob batchv1.Job
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "krm-restore-target-restore-files"}, &targetJob); err != nil {
		t.Fatalf("expected restore target job: %v", err)
	}
	assertControllerOwnerRef(t, targetJob.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RsyncMachine", target.Name, target.UID)
	var service corev1.Service
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "krm-restore-target-restore-files"}, &service); err != nil {
		t.Fatalf("expected restore target service: %v", err)
	}
	assertControllerOwnerRef(t, service.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RsyncMachine", target.Name, target.UID)
	if updated.Status.RestoredSnapshot != "hourly/2026-05-20T10-00-00Z" {
		t.Fatalf("expected latest to resolve to immutable snapshot, got %q", updated.Status.RestoredSnapshot)
	}
	if !contains(job.Spec.Template.Spec.Containers[0].Args, "hourly/2026-05-20T10-00-00Z") || !contains(job.Spec.Template.Spec.Containers[0].Args, "backup/sites/demo/files") {
		t.Fatalf("expected canonical snapshot and source path in args: %#v", job.Spec.Template.Spec.Containers[0].Args)
	}
}

func TestRestoreJobReconcilerFailsWhenSnapshotMissing(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Status.RestorePoints = []krmv1alpha1.RestorePoint{{
		Snapshot:   "latest",
		ResolvesTo: "hourly/2026-05-20T10-00-00Z",
	}, {
		Snapshot: "hourly/2026-05-20T10-00-00Z",
	}}
	source := backupSource("backup", "files", "data-pvc", "sites/demo/files")
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("backup", "files"),
			Snapshot:  "daily/2026-05-19T10-00-00Z",
		},
	}
	controllerutil.AddFinalizer(&restore, RestoreJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RestoreJob{}).
		WithObjects(&target, &source, &restore).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "restore-files"}})
	if err == nil || !strings.Contains(err.Error(), "is not present") {
		t.Fatalf("expected missing snapshot error, got %v", err)
	}
	var updated krmv1alpha1.RestoreJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "restore-files"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseFailed || !strings.Contains(updated.Status.Message, "daily/2026-05-19T10-00-00Z") {
		t.Fatalf("unexpected failed status: %#v", updated.Status)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-restore-restore-files"}, &batchv1.Job{})
}

func TestBackupSourceReconcilerRejectsUnauthorizedNamespace(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Spec.AllowedSourceNamespaces = nil
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupSource{}).
		WithObjects(&target, &source).
		Build()
	reconciler := BackupSourceReconciler{Client: client, Scheme: scheme}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-prod", Name: "files"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.BackupSource
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "files"}, &updated); err != nil {
		t.Fatal(err)
	}
	assertCondition(t, updated.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonInvalidSpec)
}

func TestCreateSecretIfMissingRejectsForgedGeneratedSecret(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	run := backupRun("backup", "demo-run", ref("backup", "archive"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	controlCA, err := tlsutil.NewCA("test-control-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	desiredCredentials, err := BuildBackupJobCredentialSecretsWithSigner(run, target, []krmv1alpha1.BackupSource{source}, time.Hour, controlCA)
	if err != nil {
		t.Fatal(err)
	}
	forgedCredentials, err := BuildBackupJobCredentialSecrets(run, target, []krmv1alpha1.BackupSource{source}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	desired := desiredCredentials[1].Secret()
	forged := forgedCredentials[1].Secret()
	forged.Name = desired.Name
	forged.Labels = desired.Labels
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(forged).Build()

	err = createSecretIfMissing(ctx, client, desired)
	if err == nil || !strings.Contains(err.Error(), "cannot be reused") {
		t.Fatalf("expected forged generated secret to be rejected, got %v", err)
	}
}

func TestRestoreJobReconcilerRejectsUnauthorizedNamespace(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Spec.AllowedRestoreNamespaces = []string{"backup"}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("app-prod", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
		},
	}
	controllerutil.AddFinalizer(&restore, RestoreJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RestoreJob{}).
		WithObjects(&target, &source, &restore).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-prod", Name: "restore-files"}})
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected unauthorized restore error, got %v", err)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-restore-restore-files"}, &batchv1.Job{})
}

func TestRestoreJobReconcilerRejectsCrossNamespaceDestination(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("app-prod", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
			Overrides: krmv1alpha1.RestoreOverrides{
				Destination: krmv1alpha1.RestoreDestination{Namespace: "other"},
			},
		},
	}
	controllerutil.AddFinalizer(&restore, RestoreJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RestoreJob{}).
		WithObjects(&target, &source, &restore).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-prod", Name: "restore-files"}})
	if err == nil || !strings.Contains(err.Error(), "must match RestoreJob namespace") {
		t.Fatalf("expected destination namespace error, got %v", err)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "other", Name: "krm-restore-restore-files"}, &batchv1.Job{})
}

func TestRestoreJobReconcilerCreatesCrossNamespaceTargetServerAndWriter(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Status.RestorePoints = []krmv1alpha1.RestorePoint{{
		Snapshot:   "latest",
		ResolvesTo: "hourly/2026-05-20T10-00-00Z",
	}}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("app-prod", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
			Snapshot:  "latest",
		},
	}
	controllerutil.AddFinalizer(&restore, RestoreJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RestoreJob{}).
		WithObjects(&target, &source, &restore).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-prod", Name: "restore-files"}})
	if err != nil {
		t.Fatalf("expected cross-namespace restore resources, got error: %v", err)
	}
	var updated krmv1alpha1.RestoreJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "restore-files"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePreparing || updated.Status.RestoredSnapshot != "hourly/2026-05-20T10-00-00Z" {
		t.Fatalf("unexpected status: %#v", updated.Status)
	}
	for _, ref := range []types.NamespacedName{
		{Namespace: "backup", Name: "krm-tls-app-prod-restore-files-target-server-backup-archive"},
		{Namespace: "app-prod", Name: "krm-tls-app-prod-restore-files-restore-writer-app-prod-files"},
		{Namespace: "backup", Name: "krm-restore-target-restore-files"},
		{Namespace: "app-prod", Name: "krm-restore-restore-files"},
	} {
		switch {
		case strings.HasPrefix(ref.Name, "krm-tls-"):
			var secret corev1.Secret
			if err := client.Get(ctx, ref, &secret); err != nil {
				t.Fatalf("expected secret %s: %v", ref.String(), err)
			}
		default:
			var job batchv1.Job
			if err := client.Get(ctx, ref, &job); err != nil {
				t.Fatalf("expected job %s: %v", ref.String(), err)
			}
		}
	}
	var service corev1.Service
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "krm-restore-target-restore-files"}, &service); err != nil {
		t.Fatalf("expected restore target service: %v", err)
	}
	assertControllerOwnerRef(t, service.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RsyncMachine", target.Name, target.UID)
	var targetJob batchv1.Job
	if err := client.Get(ctx, types.NamespacedName{Namespace: "backup", Name: "krm-restore-target-restore-files"}, &targetJob); err != nil {
		t.Fatal(err)
	}
	assertControllerOwnerRef(t, targetJob.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RsyncMachine", target.Name, target.UID)
	var writer batchv1.Job
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "krm-restore-restore-files"}, &writer); err != nil {
		t.Fatal(err)
	}
	assertControllerOwnerRef(t, writer.OwnerReferences, krmv1alpha1.SchemeGroupVersion.String(), "RestoreJob", restore.Name, restore.UID)
	if len(writer.Spec.Template.Spec.Volumes) != 2 {
		t.Fatalf("writer should only mount destination and tls volumes: %#v", writer.Spec.Template.Spec.Volumes)
	}
	if got := writer.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "data-pvc" {
		t.Fatalf("unexpected destination pvc: %q", got)
	}
}

func TestRestoreJobReconcilerHoldsWhenTargetReadyFalse(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Status.Conditions = []krmv1alpha1.Condition{{
		Type:   "Ready",
		Status: metav1.ConditionFalse,
		Reason: "TargetUnusable",
	}}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("app-prod", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
			Snapshot:  "latest",
			Overrides: krmv1alpha1.RestoreOverrides{
				Destination: krmv1alpha1.RestoreDestination{PVCName: "restore-pvc"},
			},
		},
	}
	controllerutil.AddFinalizer(&restore, RestoreJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RestoreJob{}, &krmv1alpha1.RsyncMachine{}).
		WithObjects(&target, &source, &restore).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-prod", Name: "restore-files"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.RestoreJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "restore-files"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePending || len(updated.Status.Conditions) != 1 || updated.Status.Conditions[0].Type != "TargetReady" {
		t.Fatalf("unexpected held status: %#v", updated.Status)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-restore-restore-files"}, &batchv1.Job{})
}

func TestRestoreJobReconcilerHoldsWhenTargetHasActiveBackup(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Spec.ConcurrencyPolicy = krmv1alpha1.ConcurrencyPolicyReplace
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	activePlan := rsyncMachine("backup", "active-plan", ref("backup", "archive"), []krmv1alpha1.ObjectReference{ref("app-prod", "files")}, krmv1alpha1.RetentionPolicy{})
	activeRun := backupRun("backup", "active-run", ref("backup", "active-plan"))
	activeRun.Status.Phase = krmv1alpha1.RunPhaseRunning
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("app-prod", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
			Snapshot:  "latest",
			Overrides: krmv1alpha1.RestoreOverrides{
				Destination: krmv1alpha1.RestoreDestination{PVCName: "restore-pvc"},
			},
		},
	}
	controllerutil.AddFinalizer(&restore, RestoreJobFinalizer)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}, &krmv1alpha1.RestoreJob{}).
		WithObjects(&target, &source, &activePlan, &activeRun, &restore).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme, Image: "krm:test"}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-prod", Name: "restore-files"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected restore to requeue while backup is active")
	}
	var updated krmv1alpha1.RestoreJob
	if err := client.Get(ctx, types.NamespacedName{Namespace: "app-prod", Name: "restore-files"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhasePending || len(updated.Status.Conditions) != 1 {
		t.Fatalf("unexpected backup overlap status: %#v", updated.Status)
	}
	condition := updated.Status.Conditions[0]
	if condition.Type != ConditionTargetOverlap || condition.Reason != ReasonActiveRunForTarget || !strings.Contains(condition.Message, "waiting for backup to complete") {
		t.Fatalf("expected active backup condition, got %#v", condition)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "krm-restore-target-restore-files"}, &batchv1.Job{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "krm-restore-restore-files"}, &batchv1.Job{})
}

func TestBackupJobReconcilerMarksSucceededWhenJobsComplete(t *testing.T) {
	scheme := testControllerScheme(t)
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhaseRunning
	targetJob := completedJob("backup", "target", runLabels(run, runKindBackup, RoleTargetServer))
	sourceJob := completedJob("app-prod", "source", runLabels(run, runKindBackup, RoleSourceSender))
	targetService := runService("backup", "target", runLabels(run, runKindBackup, RoleTargetServer))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&run, targetJob, sourceJob, targetService).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.BackupJob
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseSucceeded || updated.Status.CompletedAt == nil {
		t.Fatalf("unexpected status: %#v", updated.Status)
	}
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "target"}, &corev1.Service{})
}

func TestRestoreJobReconcilerMarksFailedWhenJobFails(t *testing.T) {
	scheme := testControllerScheme(t)
	restore := krmv1alpha1.RestoreJob{ObjectMeta: objectMeta("backup", "restore-files")}
	controllerutil.AddFinalizer(&restore, RestoreJobFinalizer)
	restore.Status.Phase = krmv1alpha1.RunPhaseRunning
	job := failedJob("app-prod", "restore", runLabelsForRef(types.NamespacedName{Namespace: "backup", Name: "restore-files"}, runKindRestore, RoleRestoreWriter))
	job.Status.Conditions[0].Reason = "BackoffLimitExceeded"
	job.Status.Conditions[0].Message = "restore container exited with status 23"
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.RestoreJob{}).
		WithObjects(&restore, job).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "restore-files"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated krmv1alpha1.RestoreJob
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "backup", Name: "restore-files"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != krmv1alpha1.RunPhaseFailed || updated.Status.CompletedAt == nil || !strings.Contains(updated.Status.Message, "status 23") {
		t.Fatalf("unexpected status: %#v", updated.Status)
	}
}

func TestBackupJobReconcilerCleansUpGeneratedResourcesOnDelete(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	lease := targetGuardLease(run, types.NamespacedName{Namespace: "backup", Name: "archive"})
	labels := runLabels(run, runKindBackup, RoleSourceSender)
	generatedJob := completedJob("app-prod", "source", labels)
	generatedSecret := runSecret("app-prod", "tls", labels)
	generatedService := runService("backup", "target", labels)
	generatedSnapshot := snapshot.BuildVolumeSnapshot(run, backupSource("app-prod", "files", "data-pvc", "sites/demo/files"))
	generatedPVC := &corev1.PersistentVolumeClaim{ObjectMeta: objectMetaWithLabels("app-prod", "snapshot-pvc", generatedSnapshot.GetLabels())}
	otherJob := completedJob("app-prod", "other", runLabels(run, runKindRestore, RoleRestoreWriter))
	otherSecret := runSecret("app-prod", "other-tls", runLabels(run, runKindRestore, RoleRestoreWriter))
	otherService := runService("backup", "other-target", runLabels(run, runKindRestore, RoleRestoreWriter))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&run, lease, generatedJob, generatedSecret, generatedService, generatedSnapshot, generatedPVC, otherJob, otherSecret, otherService).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}
	if err := client.Delete(ctx, &run); err != nil {
		t.Fatal(err)
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}

	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "source"}, &batchv1.Job{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "tls"}, &corev1.Secret{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "target"}, &corev1.Service{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "snapshot-pvc"}, &corev1.PersistentVolumeClaim{})
	assertVolumeSnapshotNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: generatedSnapshot.GetName()})
	assertExists(t, client, types.NamespacedName{Namespace: "app-prod", Name: "other"}, &batchv1.Job{})
	assertExists(t, client, types.NamespacedName{Namespace: "app-prod", Name: "other-tls"}, &corev1.Secret{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "other-target"}, &corev1.Service{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &krmv1alpha1.BackupJob{})
	assertNotFound(t, client, TargetGuardLeaseRef(types.NamespacedName{Namespace: "backup", Name: "archive"}), &coordinationv1.Lease{})
}

func TestBackupJobReconcilerReleasesTargetGuardForTerminalRun(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhaseSucceeded
	lease := targetGuardLease(run, types.NamespacedName{Namespace: "backup", Name: "archive"})
	targetService := runService("backup", "target", runLabels(run, runKindBackup, RoleTargetServer))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}).
		WithObjects(&run, lease, targetService).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}

	assertNotFound(t, client, TargetGuardLeaseRef(types.NamespacedName{Namespace: "backup", Name: "archive"}), &coordinationv1.Lease{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "target"}, &corev1.Service{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &krmv1alpha1.BackupJob{})
}

func TestBackupJobReconcilerCleansUpSnapshotResourcesForTerminalRun(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	run := backupRun("backup", "demo-run", ref("backup", "demo"))
	controllerutil.AddFinalizer(&run, BackupJobFinalizer)
	run.Status.Phase = krmv1alpha1.RunPhaseSucceeded
	generatedSnapshot := snapshot.BuildVolumeSnapshot(run, backupSource("app-prod", "files", "data-pvc", "sites/demo/files"))
	generatedPVC := &corev1.PersistentVolumeClaim{ObjectMeta: objectMetaWithLabels("app-prod", "snapshot-pvc", generatedSnapshot.GetLabels())}
	generatedJob := completedJob("app-prod", "source", runLabels(run, runKindBackup, RoleSourceSender))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&run, generatedSnapshot, generatedPVC, generatedJob).
		Build()
	reconciler := BackupJobReconciler{Client: client, Scheme: scheme, SnapshotCapabilities: snapshot.StaticCapabilities{Available: true}}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "backup", Name: "demo-run"}})
	if err != nil {
		t.Fatal(err)
	}

	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "snapshot-pvc"}, &corev1.PersistentVolumeClaim{})
	assertVolumeSnapshotNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: generatedSnapshot.GetName()})
	assertExists(t, client, types.NamespacedName{Namespace: "app-prod", Name: "source"}, &batchv1.Job{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "demo-run"}, &krmv1alpha1.BackupJob{})
}

func TestRestoreJobReconcilerCleansUpGeneratedResourcesOnDelete(t *testing.T) {
	scheme := testControllerScheme(t)
	ctx := context.Background()
	restore := krmv1alpha1.RestoreJob{ObjectMeta: objectMeta("backup", "restore-files")}
	controllerutil.AddFinalizer(&restore, RestoreJobFinalizer)
	runRef := types.NamespacedName{Namespace: "backup", Name: "restore-files"}
	labels := runLabelsForRef(runRef, runKindRestore, RoleRestoreWriter)
	generatedJob := completedJob("app-prod", "restore", labels)
	generatedSecret := runSecret("backup", "tls", labels)
	otherJob := completedJob("app-prod", "other", runLabelsForRef(runRef, runKindBackup, RoleSourceSender))
	otherSecret := runSecret("backup", "other-tls", runLabelsForRef(runRef, runKindBackup, RoleSourceSender))
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&restore, generatedJob, generatedSecret, otherJob, otherSecret).
		Build()
	reconciler := RestoreJobReconciler{Client: client, Scheme: scheme}
	if err := client.Delete(ctx, &restore); err != nil {
		t.Fatal(err)
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: runRef})
	if err != nil {
		t.Fatal(err)
	}

	assertNotFound(t, client, types.NamespacedName{Namespace: "app-prod", Name: "restore"}, &batchv1.Job{})
	assertNotFound(t, client, types.NamespacedName{Namespace: "backup", Name: "tls"}, &corev1.Secret{})
	assertExists(t, client, types.NamespacedName{Namespace: "app-prod", Name: "other"}, &batchv1.Job{})
	assertExists(t, client, types.NamespacedName{Namespace: "backup", Name: "other-tls"}, &corev1.Secret{})
	assertNotFound(t, client, runRef, &krmv1alpha1.RestoreJob{})
}

func testControllerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := krmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func completedJob(namespace, name string, labels map[string]string) *batchv1.Job {
	return jobWithCondition(namespace, name, labels, batchv1.JobComplete)
}

func failedJob(namespace, name string, labels map[string]string) *batchv1.Job {
	return jobWithCondition(namespace, name, labels, batchv1.JobFailed)
}

func runningJob(namespace, name string, labels map[string]string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: objectMetaWithLabels(namespace, name, labels),
	}
}

func completedBackupJob(namespace, name string, plan krmv1alpha1.ObjectReference, phase krmv1alpha1.RunPhase, completed time.Time) krmv1alpha1.BackupJob {
	run := backupRun(namespace, name, plan)
	run.Status.Phase = phase
	completedAt := metav1.NewTime(completed)
	run.Status.CompletedAt = &completedAt
	return run
}

func runSecret(namespace, name string, labels map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: objectMetaWithLabels(namespace, name, labels),
		Type:       corev1.SecretTypeTLS,
	}
}

func runService(namespace, name string, labels map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: objectMetaWithLabels(namespace, name, labels),
	}
}

func targetGuardLease(run krmv1alpha1.BackupJob, target types.NamespacedName) *coordinationv1.Lease {
	holder := backupRunGuardHolder(run)
	now := metav1.NewMicroTime(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   target.Namespace,
			Name:        TargetGuardLeaseRef(target).Name,
			Labels:      targetGuardLeaseLabels(run, target),
			Annotations: targetGuardLeaseAnnotations(target),
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: ptrInt32(targetGuardLeaseDurationSeconds),
			AcquireTime:          &now,
			RenewTime:            &now,
			LeaseTransitions:     ptrInt32(0),
		},
	}
}

func assertNotFound(t *testing.T, c client.Client, ref types.NamespacedName, obj client.Object) {
	t.Helper()
	err := c.Get(context.Background(), ref, obj)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected %s to be absent, got %v", ref.String(), err)
	}
}

func assertExists(t *testing.T, c client.Client, ref types.NamespacedName, obj client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), ref, obj); err != nil {
		t.Fatalf("expected %s to exist: %v", ref.String(), err)
	}
}

func assertControllerOwnerRef(t *testing.T, owners []metav1.OwnerReference, apiVersion, kind, name string, uid types.UID) {
	t.Helper()
	for _, owner := range owners {
		if owner.APIVersion == apiVersion && owner.Kind == kind && owner.Name == name {
			if owner.Controller == nil || !*owner.Controller {
				t.Fatalf("expected owner reference %s/%s to be controller: %#v", kind, name, owner)
			}
			if uid != "" && owner.UID != uid {
				t.Fatalf("expected owner reference UID %q, got %q", uid, owner.UID)
			}
			return
		}
	}
	t.Fatalf("expected controller owner reference %s %s/%s, got %#v", apiVersion, kind, name, owners)
}

func assertOwnerRef(t *testing.T, owners []metav1.OwnerReference, apiVersion, kind, name string, uid types.UID) {
	t.Helper()
	for _, owner := range owners {
		if owner.APIVersion == apiVersion && owner.Kind == kind && owner.Name == name {
			if owner.Controller != nil && *owner.Controller {
				t.Fatalf("expected owner reference %s/%s to be non-controller: %#v", kind, name, owner)
			}
			if uid != "" && owner.UID != uid {
				t.Fatalf("expected owner reference UID %q, got %q", uid, owner.UID)
			}
			return
		}
	}
	t.Fatalf("expected owner reference %s %s/%s, got %#v", apiVersion, kind, name, owners)
}

func assertVolumeSnapshotNotFound(t *testing.T, c client.Client, ref types.NamespacedName) {
	t.Helper()
	var obj unstructured.Unstructured
	obj.SetAPIVersion(snapshot.SnapshotAPIVersion)
	obj.SetKind(snapshot.VolumeSnapshotKind)
	assertNotFound(t, c, ref, &obj)
}

func assertCondition(t *testing.T, conditions []krmv1alpha1.Condition, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	for _, condition := range conditions {
		if condition.Type == conditionType {
			if condition.Status != status || condition.Reason != reason {
				t.Fatalf("unexpected %s condition: %#v", conditionType, condition)
			}
			return
		}
	}
	t.Fatalf("expected %s condition in %#v", conditionType, conditions)
}

func jobWithCondition(namespace, name string, labels map[string]string, conditionType batchv1.JobConditionType) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: objectMetaWithLabels(namespace, name, labels),
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   conditionType,
				Status: corev1.ConditionTrue,
			}},
		},
	}
}

func objectMetaWithLabels(namespace, name string, labels map[string]string) metav1.ObjectMeta {
	meta := objectMeta(namespace, name)
	meta.Labels = labels
	return meta
}

func readTargetCommand(t *testing.T, commands <-chan control.TargetCommand) control.TargetCommand {
	t.Helper()
	select {
	case command, ok := <-commands:
		if !ok {
			t.Fatal("command stream closed")
		}
		return command
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for target command")
	}
	return control.TargetCommand{}
}

func assertNoTargetCommand(t *testing.T, commands <-chan control.TargetCommand) {
	t.Helper()
	select {
	case command := <-commands:
		t.Fatalf("expected no target command, got %#v", command)
	case <-time.After(50 * time.Millisecond):
	}
}
