package controller

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	ptmmetrics "github.com/chirino/kube-rsync-machine/internal/metrics"
	"github.com/chirino/kube-rsync-machine/internal/snapshot"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	BackupJobFinalizer    = "backupjob.krm.chirino.github.io/finalizer"
	RestoreJobFinalizer   = "restorejob.krm.chirino.github.io/finalizer"
	RsyncMachineFinalizer = "rsyncmachine.krm.chirino.github.io/finalizer"
)

const (
	runKindBackup  = "backup"
	runKindRestore = "restore"
)

const (
	finalizeTimeLayout              = "2006-01-02T15-04-05Z"
	snapshotReadyRequeueAfter       = 15 * time.Second
	snapshotReadyTimeout            = 10 * time.Minute
	temporaryPVCBindRequeueAfter    = 15 * time.Second
	temporaryPVCBindingTimeout      = 5 * time.Minute
	ownedResourceReconcileAfter     = 10 * time.Minute
	scheduleScanInterval            = time.Minute
	scheduleLookback                = 24 * time.Hour
	targetRecoveryMinAvailableBytes = 64 * 1024 * 1024
	sourceSnapshotUnsupported       = "SourceSnapshotUnsupported"
	sourceSnapshotFailed            = "SourceSnapshotFailed"
	sourceSnapshotTimedOut          = "SourceSnapshotTimedOut"
	sourceSnapshotPVCBindTimedOut   = "SourceSnapshotPVCBindTimedOut"
	sourceSnapshotFallbackToDirect  = "SourceSnapshotFallbackToDirect"
)

const (
	targetGuardLeaseDurationSeconds int32 = 86400
	targetGuardRole                       = "target-guard"
	labelTargetNamespace                  = "krm.chirino.github.io/target-namespace"
	labelTargetName                       = "krm.chirino.github.io/target-name"
)

type BackupJobReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	Image                string
	ControlGRPCNamespace string
	ControlGRPCEndpoint  string
	Metrics              *ptmmetrics.Recorder
	Control              *control.Service
	SnapshotCapabilities snapshot.Capabilities
	ControlGRPCCA        []byte
	ControlGRPCSigner    *tlsutil.CA
	Recorder             record.EventRecorder
}

func (r *BackupJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run krmv1alpha1.BackupJob
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if owned, err := r.ensureScheduledBackupJobOwnerReference(ctx, &run); err != nil {
		return ctrl.Result{}, err
	} else if owned {
		return ctrl.Result{}, nil
	}
	if !run.DeletionTimestamp.IsZero() {
		snapshotAvailable := false
		if r.SnapshotCapabilities != nil {
			snapshotAvailable = r.SnapshotCapabilities.VolumeSnapshotAvailable(ctx)
		}
		if err := r.releaseTargetGuardsForRun(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, cleanupRunResourcesAndRemoveFinalizer(ctx, r.Client, &run, runKindBackup, BackupJobFinalizer, snapshotAvailable)
	}
	if isTerminalPhase(run.Status.Phase) {
		if run.Status.Phase == krmv1alpha1.RunPhaseFailed {
			resumed, err := r.resumeRecoveredBackupJob(ctx, &run)
			if err != nil {
				return ctrl.Result{}, err
			}
			if resumed {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
		}
		snapshotAvailable := false
		if r.SnapshotCapabilities != nil {
			snapshotAvailable = r.SnapshotCapabilities.VolumeSnapshotAvailable(ctx)
		}
		if err := r.releaseTargetGuardsForRun(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}
		if err := cleanupBackupJobSnapshotResources(ctx, r.Client, &run, snapshotAvailable); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.pruneBackupJobHistory(ctx, run)
	}
	if !isTerminalPhase(run.Status.Phase) && !controllerutil.ContainsFinalizer(&run, BackupJobFinalizer) {
		controllerutil.AddFinalizer(&run, BackupJobFinalizer)
		if err := r.Update(ctx, &run); err != nil {
			return ctrl.Result{}, fmt.Errorf("add backup job finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}
	if run.Status.Phase != "" && run.Status.Phase != krmv1alpha1.RunPhasePending {
		return r.reconcileBackupJobJobStatus(ctx, &run)
	}
	runSet, target, machine, err := r.resolveBackupJob(ctx, &run)
	if err != nil {
		return ctrl.Result{}, r.failBackupJob(ctx, &run, err)
	}
	if canonical, ok, err := r.coalescePendingBackupJobsForTarget(ctx, &run, target); err != nil {
		return ctrl.Result{}, r.failBackupJob(ctx, &run, err)
	} else if !ok {
		return ctrl.Result{}, r.cancelBackupJobMergedInto(ctx, &run, canonical)
	}
	if !TargetReady(target) {
		return ctrl.Result{}, r.holdBackupJobForTarget(ctx, &run, target)
	}
	if blocking, ok, err := r.findActiveBackupJobForTarget(ctx, run, target); err != nil {
		return ctrl.Result{}, r.failBackupJob(ctx, &run, err)
	} else if ok {
		if machine.Spec.ConcurrencyPolicyOrDefault() == krmv1alpha1.ConcurrencyPolicyReplace {
			if err := r.cancelBackupJobForReplacement(ctx, &blocking, run); err != nil {
				return ctrl.Result{}, r.failBackupJob(ctx, &run, err)
			}
		} else {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, r.holdBackupJobForOverlap(ctx, &run, target, blocking, machine)
		}
	}
	if restore, ok, err := r.findActiveRestoreJobForTarget(ctx, target); err != nil {
		return ctrl.Result{}, r.failBackupJob(ctx, &run, err)
	} else if ok {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, r.holdBackupJobForRestore(ctx, &run, target, restore)
	}
	blocking, acquired, err := r.acquireTargetGuard(ctx, &run, target, machine.Spec.ConcurrencyPolicyOrDefault() == krmv1alpha1.ConcurrencyPolicyReplace)
	if err != nil {
		return ctrl.Result{}, r.failBackupJob(ctx, &run, err)
	}
	if !acquired {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, r.holdBackupJobForOverlap(ctx, &run, target, *blocking, machine)
	}
	if err := r.ensureBackupJobCredentials(ctx, run, target, runSet); err != nil {
		return ctrl.Result{}, r.failBackupJob(ctx, &run, err)
	}
	targetJob, err := BuildServeTargetJobWithControl(run, target, runSet, r.Image, time.Now().UTC().Format(finalizeTimeLayout), r.dataPlaneControlOptions())
	if err != nil {
		return ctrl.Result{}, r.failBackupJob(ctx, &run, err)
	}
	if err := r.createBackupJobJobIfMissing(ctx, &run, targetJob); err != nil {
		return ctrl.Result{}, err
	}
	targetService := BuildTargetService(run, target)
	if err := r.createBackupJobServiceIfMissing(ctx, &run, targetService); err != nil {
		return ctrl.Result{}, err
	}
	run.Status.IncludedMachines = runSet.Machines
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	if run.Status.StartedAt == nil {
		now := metav1.Now()
		run.Status.StartedAt = &now
	}
	run.Status.Conditions = nil
	setCondition(&run.Status.Conditions, ConditionValid, metav1.ConditionTrue, ReasonResolvedReferences, "BackupJob references resolved successfully", run.Generation)
	setCondition(&run.Status.Conditions, ConditionTargetReady, metav1.ConditionTrue, ReasonTargetReady, fmt.Sprintf("RsyncMachine %s/%s is ready", target.Namespace, target.Name), run.Generation)
	setCondition(&run.Status.Conditions, ConditionTargetOverlap, metav1.ConditionFalse, ReasonNoTargetOverlap, "No active BackupJob is mutating the target", run.Generation)
	setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionFalse, ReasonRunStarted, "BackupJob started", run.Generation)
	if err := r.Status().Update(ctx, &run); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("set backup job preparing phase: %w", err)
	}
	r.recordRunEvent(&run, corev1.EventTypeNormal, ReasonRunStarted, "BackupJob started for RsyncMachine %s/%s", target.Namespace, target.Name)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindBackup, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return ctrl.Result{}, nil
}

func (r *BackupJobReconciler) coalescePendingBackupJobsForTarget(ctx context.Context, run *krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine) (types.NamespacedName, bool, error) {
	var runs krmv1alpha1.BackupJobList
	if err := r.List(ctx, &runs); err != nil {
		return types.NamespacedName{}, false, fmt.Errorf("list pending backup jobs: %w", err)
	}
	machineRef := types.NamespacedName{Namespace: target.Namespace, Name: target.Name}
	pending := make([]krmv1alpha1.BackupJob, 0, len(runs.Items)+1)
	for _, candidate := range runs.Items {
		if isTerminalPhase(candidate.Status.Phase) || candidate.Status.Phase != "" && candidate.Status.Phase != krmv1alpha1.RunPhasePending {
			continue
		}
		candidateMachineRef, ok, err := r.machineRefForBackupJob(ctx, candidate)
		if err != nil {
			return types.NamespacedName{}, false, err
		}
		if ok && candidateMachineRef == machineRef {
			pending = append(pending, candidate)
		}
	}
	if len(pending) <= 1 {
		return types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, true, nil
	}
	sort.Slice(pending, func(i, j int) bool {
		if !pending[i].CreationTimestamp.Equal(&pending[j].CreationTimestamp) {
			return pending[i].CreationTimestamp.Before(&pending[j].CreationTimestamp)
		}
		return namespacedKey(pending[i].Namespace, pending[i].Name) < namespacedKey(pending[j].Namespace, pending[j].Name)
	})
	canonical := types.NamespacedName{Namespace: pending[0].Namespace, Name: pending[0].Name}
	if canonical.Namespace == run.Namespace && canonical.Name == run.Name {
		for i := 1; i < len(pending); i++ {
			duplicate := pending[i]
			if err := r.cancelBackupJobMergedInto(ctx, &duplicate, canonical); err != nil {
				return canonical, true, err
			}
		}
		return canonical, true, nil
	}
	return canonical, false, nil
}

func (r *BackupJobReconciler) acquireTargetGuard(ctx context.Context, run *krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine, replace bool) (*krmv1alpha1.BackupJob, bool, error) {
	machineRef := types.NamespacedName{Namespace: target.Namespace, Name: target.Name}
	leaseRef := TargetGuardLeaseRef(machineRef)
	holder := backupRunGuardHolder(*run)
	for attempt := 0; attempt < 3; attempt++ {
		now := metav1.MicroTime{Time: time.Now().UTC()}
		var lease coordinationv1.Lease
		if err := r.Get(ctx, leaseRef, &lease); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, false, fmt.Errorf("get target guard lease %s: %w", leaseRef.String(), err)
			}
			lease = coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   leaseRef.Namespace,
					Name:        leaseRef.Name,
					Labels:      targetGuardLeaseLabels(*run, machineRef),
					Annotations: targetGuardLeaseAnnotations(machineRef),
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       &holder,
					LeaseDurationSeconds: ptrInt32(targetGuardLeaseDurationSeconds),
					AcquireTime:          &now,
					RenewTime:            &now,
					LeaseTransitions:     ptrInt32(0),
				},
			}
			if err := r.Create(ctx, &lease); err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue
				}
				return nil, false, fmt.Errorf("create target guard lease %s: %w", leaseRef.String(), err)
			}
			return nil, true, nil
		}

		existingHolder := ""
		if lease.Spec.HolderIdentity != nil {
			existingHolder = *lease.Spec.HolderIdentity
		}
		switch {
		case existingHolder == "" || existingHolder == holder:
			claimTargetGuardLease(&lease, *run, machineRef, holder, now, existingHolder)
			if err := r.Update(ctx, &lease); err != nil {
				if apierrors.IsConflict(err) {
					continue
				}
				return nil, false, fmt.Errorf("update target guard lease %s: %w", leaseRef.String(), err)
			}
			return nil, true, nil
		default:
			blocking, active, err := r.activeGuardHolder(ctx, existingHolder, machineRef)
			if err != nil {
				return nil, false, err
			}
			if active {
				if !replace {
					return &blocking, false, nil
				}
				if err := r.cancelBackupJobForReplacement(ctx, &blocking, *run); err != nil {
					return nil, false, err
				}
			}
			claimTargetGuardLease(&lease, *run, machineRef, holder, now, existingHolder)
			if err := r.Update(ctx, &lease); err != nil {
				if apierrors.IsConflict(err) {
					continue
				}
				return nil, false, fmt.Errorf("steal target guard lease %s from %q: %w", leaseRef.String(), existingHolder, err)
			}
			return nil, true, nil
		}
	}
	return nil, false, fmt.Errorf("target guard lease %s changed too frequently", leaseRef.String())
}

func (r *BackupJobReconciler) activeGuardHolder(ctx context.Context, holder string, target types.NamespacedName) (krmv1alpha1.BackupJob, bool, error) {
	holderRef, ok := parseBackupJobGuardHolder(holder)
	if !ok {
		return krmv1alpha1.BackupJob{}, false, nil
	}
	var run krmv1alpha1.BackupJob
	if err := r.Get(ctx, holderRef, &run); err != nil {
		if apierrors.IsNotFound(err) {
			return krmv1alpha1.BackupJob{}, false, nil
		}
		return krmv1alpha1.BackupJob{}, false, fmt.Errorf("read target guard holder %s: %w", holderRef.String(), err)
	}
	if !ActiveRunBlocksTargetMutation(run.Status.Phase) {
		return run, false, nil
	}
	holderTarget, ok, err := r.machineRefForBackupJob(ctx, run)
	if err != nil {
		return krmv1alpha1.BackupJob{}, false, fmt.Errorf("resolve target guard holder %s target: %w", holderRef.String(), err)
	}
	if ok && holderTarget != target {
		return run, false, nil
	}
	return run, true, nil
}

func (r *BackupJobReconciler) releaseTargetGuardsForRun(ctx context.Context, run *krmv1alpha1.BackupJob) error {
	var leases coordinationv1.LeaseList
	if err := r.List(ctx, &leases, client.MatchingLabels{
		LabelRunNamespace: run.Namespace,
		LabelRunKind:      runKindBackup,
		LabelRun:          run.Name,
		LabelRole:         targetGuardRole,
	}); err != nil {
		return fmt.Errorf("list target guard leases for BackupJob %s/%s: %w", run.Namespace, run.Name, err)
	}
	holder := backupRunGuardHolder(*run)
	for i := range leases.Items {
		lease := &leases.Items[i]
		if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != holder {
			continue
		}
		if err := r.Delete(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete target guard lease %s/%s: %w", lease.Namespace, lease.Name, err)
		}
	}
	return nil
}

func claimTargetGuardLease(lease *coordinationv1.Lease, run krmv1alpha1.BackupJob, target types.NamespacedName, holder string, now metav1.MicroTime, previousHolder string) {
	if previousHolder != holder {
		transitions := int32(0)
		if lease.Spec.LeaseTransitions != nil {
			transitions = *lease.Spec.LeaseTransitions + 1
		}
		lease.Spec.LeaseTransitions = &transitions
		lease.Spec.AcquireTime = &now
	}
	lease.Labels = targetGuardLeaseLabels(run, target)
	lease.Annotations = targetGuardLeaseAnnotations(target)
	lease.Spec.HolderIdentity = &holder
	lease.Spec.LeaseDurationSeconds = ptrInt32(targetGuardLeaseDurationSeconds)
	lease.Spec.RenewTime = &now
}

func targetGuardLeaseLabels(run krmv1alpha1.BackupJob, target types.NamespacedName) map[string]string {
	labels := runLabels(run, runKindBackup, targetGuardRole)
	labels[labelTargetNamespace] = target.Namespace
	labels[labelTargetName] = target.Name
	return labels
}

func targetGuardLeaseAnnotations(target types.NamespacedName) map[string]string {
	return map[string]string{
		"krm.chirino.github.io/target": target.String(),
	}
}

func backupRunGuardHolder(run krmv1alpha1.BackupJob) string {
	return types.NamespacedName{Namespace: run.Namespace, Name: run.Name}.String()
}

func parseBackupJobGuardHolder(holder string) (types.NamespacedName, bool) {
	parts := strings.Split(holder, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: parts[0], Name: parts[1]}, true
}

func ptrInt32(value int32) *int32 {
	return &value
}

func (r *BackupJobReconciler) machineRefForBackupJob(ctx context.Context, run krmv1alpha1.BackupJob) (types.NamespacedName, bool, error) {
	machineRef, err := ResolveObjectReference(run.Spec.MachineRef, run.Namespace)
	if err != nil {
		return types.NamespacedName{}, false, nil
	}
	var machine krmv1alpha1.RsyncMachine
	if err := r.Get(ctx, machineRef, &machine); err != nil {
		if apierrors.IsNotFound(err) {
			return types.NamespacedName{}, false, nil
		}
		return types.NamespacedName{}, false, err
	}
	return machineRef, true, nil
}

func (r *BackupJobReconciler) cancelBackupJobMergedInto(ctx context.Context, run *krmv1alpha1.BackupJob, canonical types.NamespacedName) error {
	oldPhase := run.Status.Phase
	now := metav1.Now()
	run.Status.Phase = krmv1alpha1.RunPhaseCanceled
	run.Status.CompletedAt = &now
	run.Status.MergedInto = &krmv1alpha1.ObjectReference{Namespace: canonical.Namespace, Name: canonical.Name}
	run.Status.Conditions = nil
	setCondition(&run.Status.Conditions, ConditionMergedIntoRun, metav1.ConditionTrue, ReasonMergedIntoRun, fmt.Sprintf("BackupJob %s/%s was merged into BackupJob %s/%s", run.Namespace, run.Name, canonical.Namespace, canonical.Name), run.Generation)
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cancel merged backup job %s/%s: %w", run.Namespace, run.Name, err)
	}
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindBackup, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return nil
}

func (r *BackupJobReconciler) cancelBackupJobForReplacement(ctx context.Context, active *krmv1alpha1.BackupJob, replacement krmv1alpha1.BackupJob) error {
	oldPhase := active.Status.Phase
	now := metav1.Now()
	active.Status.Phase = krmv1alpha1.RunPhaseCanceled
	active.Status.CompletedAt = &now
	active.Status.Conditions = nil
	setCondition(&active.Status.Conditions, ConditionReplaced, metav1.ConditionTrue, ReasonReplacedByRun, fmt.Sprintf("BackupJob %s/%s was replaced by BackupJob %s/%s", active.Namespace, active.Name, replacement.Namespace, replacement.Name), active.Generation)
	if err := r.Status().Update(ctx, active); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cancel active backup job %s/%s for replacement: %w", active.Namespace, active.Name, err)
	}
	snapshotAvailable := false
	if r.SnapshotCapabilities != nil {
		snapshotAvailable = r.SnapshotCapabilities.VolumeSnapshotAvailable(ctx)
	}
	if err := cleanupRunResources(ctx, r.Client, active, runKindBackup, snapshotAvailable); err != nil {
		r.recordRunEvent(active, corev1.EventTypeWarning, "CleanupFailed", "Cleanup after replacement failed: %v", err)
		return fmt.Errorf("cleanup replaced backup job %s/%s: %w", active.Namespace, active.Name, err)
	}
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindBackup, namespacedKey(active.Namespace, active.Name), oldPhase, active.Status.Phase)
	return nil
}

func (r *BackupJobReconciler) resolveBackupJob(ctx context.Context, run *krmv1alpha1.BackupJob) (TargetRunSet, krmv1alpha1.RsyncMachine, krmv1alpha1.RsyncMachine, error) {
	machineRef, err := ResolveObjectReference(run.Spec.MachineRef, run.Namespace)
	if err != nil {
		return TargetRunSet{}, krmv1alpha1.RsyncMachine{}, krmv1alpha1.RsyncMachine{}, fmt.Errorf("resolve machine: %w", err)
	}
	var target krmv1alpha1.RsyncMachine
	if err := r.Get(ctx, machineRef, &target); err != nil {
		return TargetRunSet{}, krmv1alpha1.RsyncMachine{}, krmv1alpha1.RsyncMachine{}, client.IgnoreNotFound(err)
	}
	var sourceList krmv1alpha1.BackupSourceList
	if err := r.List(ctx, &sourceList); err != nil {
		return TargetRunSet{}, krmv1alpha1.RsyncMachine{}, krmv1alpha1.RsyncMachine{}, fmt.Errorf("list backup sources: %w", err)
	}
	sourcesByRef := make(map[types.NamespacedName]krmv1alpha1.BackupSource, len(sourceList.Items))
	for _, source := range sourceList.Items {
		sourcesByRef[types.NamespacedName{Namespace: source.Namespace, Name: source.Name}] = source
	}
	runSet, err := CalculateTargetRunSet(target, sourcesByRef)
	if err != nil {
		return TargetRunSet{}, krmv1alpha1.RsyncMachine{}, krmv1alpha1.RsyncMachine{}, err
	}
	return runSet, target, target, nil
}

func (r *BackupJobReconciler) findActiveBackupJobForTarget(ctx context.Context, run krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine) (krmv1alpha1.BackupJob, bool, error) {
	var runs krmv1alpha1.BackupJobList
	if err := r.List(ctx, &runs); err != nil {
		return krmv1alpha1.BackupJob{}, false, fmt.Errorf("list backup jobs: %w", err)
	}
	machineRef := types.NamespacedName{Namespace: target.Namespace, Name: target.Name}
	for _, candidate := range runs.Items {
		if candidate.Namespace == run.Namespace && candidate.Name == run.Name {
			continue
		}
		if !ActiveRunBlocksTargetMutation(candidate.Status.Phase) {
			continue
		}
		candidateMachineRef, err := ResolveObjectReference(candidate.Spec.MachineRef, candidate.Namespace)
		if err != nil {
			continue
		}
		var machine krmv1alpha1.RsyncMachine
		if err := r.Get(ctx, candidateMachineRef, &machine); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return krmv1alpha1.BackupJob{}, false, err
		}
		if candidateMachineRef == machineRef {
			return candidate, true, nil
		}
	}
	return krmv1alpha1.BackupJob{}, false, nil
}

func (r *BackupJobReconciler) findActiveRestoreJobForTarget(ctx context.Context, target krmv1alpha1.RsyncMachine) (krmv1alpha1.RestoreJob, bool, error) {
	var restores krmv1alpha1.RestoreJobList
	if err := r.List(ctx, &restores); err != nil {
		return krmv1alpha1.RestoreJob{}, false, fmt.Errorf("list restore jobs: %w", err)
	}
	machineRef := types.NamespacedName{Namespace: target.Namespace, Name: target.Name}
	for _, restore := range restores.Items {
		if isTerminalPhase(restore.Status.Phase) {
			continue
		}
		restoreMachineRef, ok, err := machineRefForRestoreJobWithClient(ctx, r.Client, restore)
		if err != nil {
			return krmv1alpha1.RestoreJob{}, false, err
		}
		if ok && restoreMachineRef == machineRef {
			return restore, true, nil
		}
	}
	return krmv1alpha1.RestoreJob{}, false, nil
}

func (r *BackupJobReconciler) ensureBackupJobCredentials(ctx context.Context, run krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine, runSet TargetRunSet) error {
	sources := make([]krmv1alpha1.BackupSource, 0, len(runSet.Sources))
	for _, source := range runSet.Sources {
		sources = append(sources, source.Source)
	}
	credentials, err := BuildBackupJobCredentialSecretsWithSigner(run, target, sources, DefaultRunCertificateTTL, r.ControlGRPCSigner, r.ControlGRPCCA)
	if err != nil {
		return err
	}
	for _, credential := range credentials {
		secret := credential.Secret()
		if err := r.createBackupJobSecretIfMissing(ctx, &run, secret); err != nil {
			return err
		}
	}
	return nil
}

type preparedSourceJob struct {
	Source krmv1alpha1.BackupSource
	Opts   SourceJobOptions
}

func (r *BackupJobReconciler) createSourceJobs(ctx context.Context, run *krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine, runSet TargetRunSet) (ctrl.Result, bool, error) {
	snapshotAvailable := false
	if r.SnapshotCapabilities != nil {
		snapshotAvailable = r.SnapshotCapabilities.VolumeSnapshotAvailable(ctx)
	}
	prepared := make([]preparedSourceJob, 0, len(runSet.Sources))
	statusChanged := false
	for _, source := range runSet.Sources {
		decision := snapshot.DecideCaptureMode(source.Source.Spec.Consistency, snapshotAvailable)
		if !decision.Supported {
			statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
				Phase:         krmv1alpha1.TransferPhaseFailed,
				Message:       fmt.Sprintf("%s: %s", sourceSnapshotUnsupported, decision.Reason),
				CaptureMethod: decision.Method,
			}) || statusChanged
			return ctrl.Result{}, statusChanged, fmt.Errorf("source %s/%s snapshot capture unsupported: %s", source.Source.Namespace, source.Source.Name, decision.Reason)
		}
		opts := SourceJobOptions{}
		if decision.Fallback {
			statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
				Phase:         krmv1alpha1.TransferPhasePreparing,
				Message:       fmt.Sprintf("%s: %s", sourceSnapshotFallbackToDirect, decision.Reason),
				CaptureMethod: krmv1alpha1.CaptureModeDirect,
			}) || statusChanged
		}
		if decision.Method == krmv1alpha1.CaptureModeVolumeSnapshot {
			volumeSnapshot := snapshot.BuildVolumeSnapshot(*run, source.Source)
			if err := r.createVolumeSnapshotIfMissing(ctx, volumeSnapshot); err != nil {
				return ctrl.Result{}, statusChanged, err
			}
			observedSnapshot, err := r.getVolumeSnapshot(ctx, volumeSnapshot.GetNamespace(), volumeSnapshot.GetName())
			if err != nil {
				return ctrl.Result{}, statusChanged, err
			}
			captureTime := snapshot.VolumeSnapshotCreationTime(observedSnapshot)
			if failure, failed := snapshot.VolumeSnapshotFailure(observedSnapshot); failed {
				message := fmt.Sprintf("%s: %s", sourceSnapshotFailed, failure.Message)
				if decision.Requested == krmv1alpha1.CaptureModeAuto {
					statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
						Phase:              krmv1alpha1.TransferPhaseSkipped,
						Message:            fmt.Sprintf("%s; falling back to direct PVC copy", message),
						CaptureMethod:      krmv1alpha1.CaptureModeDirect,
						VolumeSnapshotName: volumeSnapshot.GetName(),
						CaptureTime:        captureTime,
					}) || statusChanged
					prepared = append(prepared, preparedSourceJob{Source: source.Source})
					continue
				}
				statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
					Phase:              krmv1alpha1.TransferPhaseFailed,
					Message:            message,
					CaptureMethod:      krmv1alpha1.CaptureModeVolumeSnapshot,
					VolumeSnapshotName: volumeSnapshot.GetName(),
					CaptureTime:        captureTime,
				}) || statusChanged
				return ctrl.Result{}, statusChanged, fmt.Errorf("source %s/%s snapshot failed: %s", source.Source.Namespace, source.Source.Name, failure.Message)
			}
			if !snapshot.VolumeSnapshotReady(observedSnapshot) {
				if resourceTimedOut(observedSnapshot, snapshotReadyTimeout, metav1.Now()) {
					message := fmt.Sprintf("%s after %s", sourceSnapshotTimedOut, snapshotReadyTimeout)
					if decision.Requested == krmv1alpha1.CaptureModeAuto {
						statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
							Phase:              krmv1alpha1.TransferPhaseSkipped,
							Message:            message + "; falling back to direct PVC copy",
							CaptureMethod:      krmv1alpha1.CaptureModeDirect,
							VolumeSnapshotName: volumeSnapshot.GetName(),
							CaptureTime:        captureTime,
						}) || statusChanged
						prepared = append(prepared, preparedSourceJob{Source: source.Source})
						continue
					}
					statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
						Phase:              krmv1alpha1.TransferPhaseFailed,
						Message:            message,
						CaptureMethod:      krmv1alpha1.CaptureModeVolumeSnapshot,
						VolumeSnapshotName: volumeSnapshot.GetName(),
						CaptureTime:        captureTime,
					}) || statusChanged
					return ctrl.Result{}, statusChanged, fmt.Errorf("source %s/%s snapshot readiness timed out after %s", source.Source.Namespace, source.Source.Name, snapshotReadyTimeout)
				}
				statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
					Phase:              krmv1alpha1.TransferPhasePreparing,
					Message:            "Waiting for VolumeSnapshot to become ready",
					CaptureMethod:      krmv1alpha1.CaptureModeVolumeSnapshot,
					VolumeSnapshotName: volumeSnapshot.GetName(),
					CaptureTime:        captureTime,
				}) || statusChanged
				return ctrl.Result{RequeueAfter: snapshotReadyRequeueAfter}, statusChanged, nil
			}
			var sourcePVC corev1.PersistentVolumeClaim
			if err := r.Get(ctx, types.NamespacedName{Namespace: source.Source.Namespace, Name: source.Source.Spec.PVC}, &sourcePVC); err != nil {
				return ctrl.Result{}, statusChanged, err
			}
			temporaryPVC := snapshot.BuildTemporaryPVCFromSnapshot(*run, source.Source, sourcePVC, snapshot.VolumeSnapshotRestoreSize(observedSnapshot))
			if err := r.createPersistentVolumeClaimIfMissing(ctx, temporaryPVC); err != nil {
				return ctrl.Result{}, statusChanged, err
			}
			var observedPVC corev1.PersistentVolumeClaim
			if err := r.Get(ctx, types.NamespacedName{Namespace: temporaryPVC.Namespace, Name: temporaryPVC.Name}, &observedPVC); err != nil {
				return ctrl.Result{}, statusChanged, err
			}
			if observedPVC.Status.Phase != corev1.ClaimBound {
				if resourceTimedOut(&observedPVC, temporaryPVCBindingTimeout, metav1.Now()) {
					message := fmt.Sprintf("%s after %s", sourceSnapshotPVCBindTimedOut, temporaryPVCBindingTimeout)
					if decision.Requested == krmv1alpha1.CaptureModeAuto {
						statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
							Phase:              krmv1alpha1.TransferPhaseSkipped,
							Message:            message + "; falling back to direct PVC copy",
							CaptureMethod:      krmv1alpha1.CaptureModeDirect,
							VolumeSnapshotName: volumeSnapshot.GetName(),
							CaptureTime:        captureTime,
						}) || statusChanged
						prepared = append(prepared, preparedSourceJob{Source: source.Source})
						continue
					}
					statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
						Phase:              krmv1alpha1.TransferPhaseFailed,
						Message:            message,
						CaptureMethod:      krmv1alpha1.CaptureModeVolumeSnapshot,
						VolumeSnapshotName: volumeSnapshot.GetName(),
						CaptureTime:        captureTime,
					}) || statusChanged
					return ctrl.Result{}, statusChanged, fmt.Errorf("source %s/%s temporary snapshot PVC binding timed out after %s", source.Source.Namespace, source.Source.Name, temporaryPVCBindingTimeout)
				}
				statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
					Phase:              krmv1alpha1.TransferPhasePreparing,
					Message:            "Waiting for temporary snapshot PVC to bind",
					CaptureMethod:      krmv1alpha1.CaptureModeVolumeSnapshot,
					VolumeSnapshotName: volumeSnapshot.GetName(),
					CaptureTime:        captureTime,
				}) || statusChanged
				return ctrl.Result{RequeueAfter: temporaryPVCBindRequeueAfter}, statusChanged, nil
			}
			opts.ClaimName = temporaryPVC.Name
			statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
				Phase:              krmv1alpha1.TransferPhasePreparing,
				Message:            "Using ready VolumeSnapshot temporary PVC",
				CaptureMethod:      krmv1alpha1.CaptureModeVolumeSnapshot,
				VolumeSnapshotName: volumeSnapshot.GetName(),
				CaptureTime:        captureTime,
			}) || statusChanged
		}
		prepared = append(prepared, preparedSourceJob{Source: source.Source, Opts: opts})
	}
	for _, source := range prepared {
		if source.Opts.ClaimName == "" {
			statusChanged = upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
				Phase:         krmv1alpha1.TransferPhasePreparing,
				CaptureMethod: krmv1alpha1.CaptureModeDirect,
			}) || statusChanged
		}
		job, err := BuildSendSourceJobWithControl(*run, source.Source, target, source.Opts, r.Image, r.dataPlaneControlOptions())
		if err != nil {
			return ctrl.Result{}, statusChanged, err
		}
		if err := r.createSourceJobIfMissing(ctx, run, &source.Source, job); err != nil {
			return ctrl.Result{}, statusChanged, err
		}
	}
	return ctrl.Result{}, statusChanged, nil
}

func (r *BackupJobReconciler) markSourceJobSucceeded(ctx context.Context, run *krmv1alpha1.BackupJob, job batchv1.Job) (bool, error) {
	runSet, _, _, err := r.resolveBackupJob(ctx, run)
	if err != nil {
		return false, err
	}
	for _, source := range runSet.Sources {
		if source.Source.Namespace != job.Namespace || GeneratedJobName("source-"+source.Source.Name, run.Name) != job.Name {
			continue
		}
		sourceID := source.Source.Namespace + "/" + source.Source.Name
		captureMethod := source.Source.Spec.Consistency.CaptureOrDefault()
		for _, transfer := range run.Status.Transfers {
			if transfer.Source == sourceID && transfer.Phase == krmv1alpha1.TransferPhaseSucceeded {
				return false, nil
			}
			if transfer.Source == sourceID && transfer.CaptureMethod != "" {
				captureMethod = transfer.CaptureMethod
			}
		}
		changed := upsertBackupJobTransfer(run, source.Source, krmv1alpha1.TransferStatus{
			Phase:         krmv1alpha1.TransferPhaseSucceeded,
			Message:       fmt.Sprintf("Source %s completed", sourceID),
			CaptureMethod: captureMethod,
		})
		if changed {
			r.recordRunEvent(run, corev1.EventTypeNormal, "SourceCompleted", "Source %s completed for BackupJob %s/%s", sourceID, run.Namespace, run.Name)
		}
		return changed, nil
	}
	return false, nil
}

func (r *BackupJobReconciler) reconcileBackupJobJobStatus(ctx context.Context, run *krmv1alpha1.BackupJob) (ctrl.Result, error) {
	if isTerminalPhase(run.Status.Phase) {
		if run.Status.Phase == krmv1alpha1.RunPhaseFailed {
			resumed, err := r.resumeRecoveredBackupJob(ctx, run)
			if err != nil {
				return ctrl.Result{}, err
			}
			if resumed {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
		}
		snapshotAvailable := false
		if r.SnapshotCapabilities != nil {
			snapshotAvailable = r.SnapshotCapabilities.VolumeSnapshotAvailable(ctx)
		}
		if err := cleanupBackupJobSnapshotResources(ctx, r.Client, run, snapshotAvailable); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, cleanupRunCredentialSecrets(ctx, r.Client, run, runKindBackup)
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.MatchingLabels{
		LabelRunNamespace: run.Namespace,
		LabelRunKind:      runKindBackup,
		LabelRun:          run.Name,
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("list backup job jobs: %w", err)
	}
	if len(jobs.Items) == 0 {
		return ctrl.Result{}, nil
	}
	oldPhase := run.Status.Phase
	phase := krmv1alpha1.RunPhasePreparing
	targetComplete := false
	sourceJobs := 0
	sourceJobsComplete := true
	sourceStatusChanged := false
	for _, job := range jobs.Items {
		r.Metrics.RecordGeneratedJobStatus(
			ptmmetrics.RunKindBackup,
			job.Labels[LabelRole],
			namespacedKey(job.Namespace, job.Name),
			generatedJobStatus(job),
		)
		if jobFailed(job) {
			if job.Labels[LabelRole] == RoleSourceSender {
				result, handled, err := r.reconcileRecoverableSourceJobFailure(ctx, run, job)
				if err != nil {
					return ctrl.Result{}, r.failBackupJob(ctx, run, err)
				}
				if handled {
					return result, nil
				}
			}
			phase = krmv1alpha1.RunPhaseFailed
			break
		}
		switch job.Labels[LabelRole] {
		case RoleTargetServer:
			targetComplete = targetComplete || jobComplete(job)
		case RoleSourceSender:
			sourceJobs++
			if jobComplete(job) {
				changed, err := r.markSourceJobSucceeded(ctx, run, job)
				if err != nil {
					return ctrl.Result{}, r.failBackupJob(ctx, run, err)
				}
				sourceStatusChanged = changed || sourceStatusChanged
			} else {
				sourceJobsComplete = false
			}
		}
	}
	if phase != krmv1alpha1.RunPhaseFailed {
		switch {
		case sourceJobs > 0 && targetComplete && sourceJobsComplete && r.Control != nil:
			runSet, target, _, err := r.resolveBackupJob(ctx, run)
			if err != nil {
				return ctrl.Result{}, r.failBackupJob(ctx, run, err)
			}
			if sourceJobs < len(runSet.Sources) {
				sourceResult, changed, err := r.createSourceJobs(ctx, run, target, runSet)
				sourceStatusChanged = changed
				if err != nil {
					return ctrl.Result{}, r.failBackupJob(ctx, run, err)
				}
				if sourceResult.Requeue || sourceResult.RequeueAfter > 0 {
					if sourceStatusChanged {
						if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
							return ctrl.Result{}, fmt.Errorf("update backup job source status: %w", err)
						}
					}
					return sourceResult, nil
				}
				phase = krmv1alpha1.RunPhaseRunning
			} else if backupRunFinalizeCommandSent(run) {
				if run.Status.SnapshotPath == "" {
					run.Status.SnapshotPath = backupRunSnapshotPathForStrategy(target, run)
				}
				phase = krmv1alpha1.RunPhaseSucceeded
			} else {
				if err := r.enqueueFinalizeCommand(ctx, run); err != nil {
					return ctrl.Result{}, r.failBackupJob(ctx, run, err)
				}
				phase = krmv1alpha1.RunPhaseFinalizing
			}
		case sourceJobs > 0 && sourceJobsComplete && r.Control != nil:
			runSet, target, _, err := r.resolveBackupJob(ctx, run)
			if err != nil {
				return ctrl.Result{}, r.failBackupJob(ctx, run, err)
			}
			if sourceJobs < len(runSet.Sources) {
				sourceResult, changed, err := r.createSourceJobs(ctx, run, target, runSet)
				sourceStatusChanged = changed
				if err != nil {
					return ctrl.Result{}, r.failBackupJob(ctx, run, err)
				}
				if sourceResult.Requeue || sourceResult.RequeueAfter > 0 {
					if sourceStatusChanged {
						if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
							return ctrl.Result{}, fmt.Errorf("update backup job source status: %w", err)
						}
					}
					return sourceResult, nil
				}
				phase = krmv1alpha1.RunPhaseRunning
			} else {
				if err := r.enqueueFinalizeCommand(ctx, run); err != nil {
					return ctrl.Result{}, r.failBackupJob(ctx, run, err)
				}
				phase = krmv1alpha1.RunPhaseFinalizing
			}
		case sourceJobs > 0 && targetComplete && sourceJobsComplete && r.Control == nil:
			runSet, target, _, err := r.resolveBackupJob(ctx, run)
			if err != nil {
				return ctrl.Result{}, r.failBackupJob(ctx, run, err)
			}
			if sourceJobs < len(runSet.Sources) {
				sourceResult, changed, err := r.createSourceJobs(ctx, run, target, runSet)
				sourceStatusChanged = changed
				if err != nil {
					return ctrl.Result{}, r.failBackupJob(ctx, run, err)
				}
				if sourceResult.Requeue || sourceResult.RequeueAfter > 0 {
					if sourceStatusChanged {
						if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
							return ctrl.Result{}, fmt.Errorf("update backup job source status: %w", err)
						}
					}
					return sourceResult, nil
				}
				phase = krmv1alpha1.RunPhaseRunning
			} else {
				phase = krmv1alpha1.RunPhaseSucceeded
			}
		case sourceJobs > 0:
			phase = krmv1alpha1.RunPhaseRunning
		case targetComplete || run.Status.TargetPhase == "Ready":
			runSet, target, _, err := r.resolveBackupJob(ctx, run)
			if err != nil {
				return ctrl.Result{}, r.failBackupJob(ctx, run, err)
			}
			sourceResult, changed, err := r.createSourceJobs(ctx, run, target, runSet)
			sourceStatusChanged = changed
			if err != nil {
				return ctrl.Result{}, r.failBackupJob(ctx, run, err)
			}
			if sourceResult.Requeue || sourceResult.RequeueAfter > 0 {
				if sourceStatusChanged {
					if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
						return ctrl.Result{}, fmt.Errorf("update backup job source status: %w", err)
					}
				}
				return sourceResult, nil
			}
			phase = krmv1alpha1.RunPhaseRunning
		default:
			phase = krmv1alpha1.RunPhasePreparing
		}
	}
	if phase == run.Status.Phase {
		if sourceStatusChanged {
			if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("update backup job source status: %w", err)
			}
		}
		if !isTerminalPhase(phase) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}
	run.Status.Phase = phase
	if phase == krmv1alpha1.RunPhaseSucceeded || phase == krmv1alpha1.RunPhaseFailed {
		now := metav1.Now()
		run.Status.CompletedAt = &now
	}
	switch phase {
	case krmv1alpha1.RunPhaseSucceeded:
		setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionFalse, ReasonRunSucceeded, "BackupJob completed successfully", run.Generation)
		setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionTrue, ReasonTransportComplete, "All source transfers completed", run.Generation)
	case krmv1alpha1.RunPhaseFailed:
		setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionTrue, ReasonRunFailed, "A generated backup Job failed", run.Generation)
		setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionFalse, ReasonTransportFailed, "A generated backup Job failed", run.Generation)
	}
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("update backup job job-derived status: %w", err)
	}
	switch phase {
	case krmv1alpha1.RunPhaseSucceeded:
		r.recordRunEvent(run, corev1.EventTypeNormal, ReasonRunSucceeded, "BackupJob completed successfully")
	case krmv1alpha1.RunPhaseFailed:
		r.recordRunEvent(run, corev1.EventTypeWarning, ReasonRunFailed, "BackupJob failed because a generated Job failed")
	}
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindBackup, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	if isTerminalPhase(run.Status.Phase) {
		if err := r.releaseTargetGuardsForRun(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		if err := cleanupRunCredentialSecrets(ctx, r.Client, run, runKindBackup); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.pruneBackupJobHistory(ctx, *run)
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func backupRunFinalizeCommandSent(run *krmv1alpha1.BackupJob) bool {
	return run.Status.LastCommand != nil &&
		run.Status.LastCommand.Type == control.TargetCommandFinalizeBackupJob &&
		run.Status.LastCommand.SentAt != nil
}

func backupRunFinalizeSnapshotPath(run *krmv1alpha1.BackupJob) string {
	if !backupRunFinalizeCommandSent(run) {
		return ""
	}
	return "hourly/" + run.Status.LastCommand.SentAt.Time.UTC().Format(finalizeTimeLayout)
}

func backupRunSnapshotPathForStrategy(target krmv1alpha1.RsyncMachine, run *krmv1alpha1.BackupJob) string {
	if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror {
		return krmv1alpha1.DefaultMirrorSnapshot
	}
	return backupRunFinalizeSnapshotPath(run)
}

func (r *BackupJobReconciler) enqueueFinalizeCommand(ctx context.Context, run *krmv1alpha1.BackupJob) error {
	runSet, _, _, err := r.resolveBackupJob(ctx, run)
	if err != nil {
		return err
	}
	sources := make([]control.ExpectedSource, 0, len(runSet.Sources))
	for _, source := range runSet.Sources {
		destinationPath := source.EffectiveDestinationPath
		if runSet.Retention.Empty() && destinationPath == "" {
			destinationPath = "."
		}
		sources = append(sources, control.ExpectedSource{
			Namespace:       source.Source.Namespace,
			Name:            source.Source.Name,
			DestinationPath: destinationPath,
		})
	}
	commandID := "finalize-" + dnsLabel(run.Name)
	sentAt := metav1.Now()
	timestamp := sentAt.Time.UTC().Format(finalizeTimeLayout)
	if run.Status.LastCommand != nil && run.Status.LastCommand.ID == commandID && run.Status.LastCommand.Type == control.TargetCommandFinalizeBackupJob && run.Status.LastCommand.SentAt != nil {
		sentAt = *run.Status.LastCommand.SentAt
		timestamp = sentAt.Time.UTC().Format(finalizeTimeLayout)
	}
	command := control.NewFinalizeBackupJobCommand(commandID, control.FinalizeBackupJob{
		Timestamp:        timestamp,
		Sources:          sources,
		BytesTransferred: backupJobBytesTransferred(run.Status.Transfers),
	})
	enqueued, err := r.Control.EnqueueTargetCommand(control.RunKey{Namespace: run.Namespace, Name: run.Name, Kind: control.RunKindBackup}, command)
	if err != nil {
		return err
	}
	if !enqueued && run.Status.LastCommand != nil && run.Status.LastCommand.ID == commandID {
		return nil
	}
	MarkFinalizeCommandSent(run, command, sentAt)
	return nil
}

func (r *BackupJobReconciler) reconcileRecoverableSourceJobFailure(ctx context.Context, run *krmv1alpha1.BackupJob, job batchv1.Job) (ctrl.Result, bool, error) {
	if r.Control == nil {
		return ctrl.Result{}, false, nil
	}
	source, target, ok, err := r.sourceAndTargetForBackupJob(ctx, run, job)
	if err != nil || !ok {
		return ctrl.Result{}, false, err
	}
	transferIndex := -1
	sourceID := source.Namespace + "/" + source.Name
	for i := range run.Status.Transfers {
		if run.Status.Transfers[i].Source == sourceID {
			transferIndex = i
			break
		}
	}
	if transferIndex < 0 || !backupTransferOutOfSpace(run.Status.Transfers[transferIndex]) {
		return ctrl.Result{}, false, nil
	}
	commandID := "recover-space-" + dnsLabel(source.Namespace+"-"+source.Name)
	if run.Status.LastCommand != nil && run.Status.LastCommand.ID == commandID && run.Status.LastCommand.Type == control.TargetCommandRecoverSpace {
		if run.Status.LastCommand.AcknowledgedAt == nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
		}
		if job.CreationTimestamp.After(run.Status.LastCommand.AcknowledgedAt.Time) {
			return ctrl.Result{}, false, nil
		}
		if err := r.Delete(ctx, &job); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, true, fmt.Errorf("delete failed source job after target recovery: %w", err)
		}
		run.Status.Phase = krmv1alpha1.RunPhaseRunning
		run.Status.CompletedAt = nil
		run.Status.Transfers[transferIndex].Phase = krmv1alpha1.TransferPhasePreparing
		run.Status.Transfers[transferIndex].Message = "Retrying after target space recovery"
		run.Status.Transfers[transferIndex].ExitCode = 0
		setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionFalse, ReasonRunStarted, "BackupJob recovered target space and is retrying", run.Generation)
		setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionUnknown, ReasonTransportRunning, "Retrying source transfer after target space recovery", run.Generation)
		if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, true, fmt.Errorf("update backup job recovery retry status: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, true, nil
	}
	minAvailableBytes, err := recoveryMinAvailableBytes(target)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	command := control.NewRecoverSpaceCommand(commandID, control.RecoverSpace{
		FailedSourceNamespace: source.Namespace,
		FailedSourceName:      source.Name,
		Reason:                "TargetOutOfSpace",
		MinAvailableBytes:     minAvailableBytes,
	})
	enqueued, err := r.Control.EnqueueTargetCommand(control.RunKey{Namespace: run.Namespace, Name: run.Name, Kind: control.RunKindBackup}, command)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if enqueued || run.Status.LastCommand == nil || run.Status.LastCommand.ID != commandID {
		sentAt := metav1.Now()
		MarkFinalizeCommandSent(run, command, sentAt)
		setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionUnknown, ReasonTransportRunning, "Waiting for target space recovery", run.Generation)
		if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, true, fmt.Errorf("update backup job recovery command status: %w", err)
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
}

func (r *BackupJobReconciler) resumeRecoveredBackupJob(ctx context.Context, run *krmv1alpha1.BackupJob) (bool, error) {
	if run.Status.LastCommand == nil ||
		run.Status.LastCommand.Type != control.TargetCommandRecoverSpace ||
		run.Status.LastCommand.AcknowledgedAt == nil ||
		!hasRecoveredTransferRetry(run.Status.Transfers) {
		return false, nil
	}
	run.Status.Phase = krmv1alpha1.RunPhaseRunning
	run.Status.CompletedAt = nil
	setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionFalse, ReasonRunStarted, "BackupJob recovered target space and is retrying", run.Generation)
	setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionUnknown, ReasonTransportRunning, "Retrying source transfer after target space recovery", run.Generation)
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("resume backup job after target recovery: %w", err)
	}
	return true, nil
}

func hasRecoveredTransferRetry(transfers []krmv1alpha1.TransferStatus) bool {
	for _, transfer := range transfers {
		if transfer.Phase == krmv1alpha1.TransferPhasePreparing &&
			strings.Contains(strings.ToLower(transfer.Message), "retrying after target space recovery") {
			return true
		}
	}
	return false
}

func (r *BackupJobReconciler) sourceAndTargetForBackupJob(ctx context.Context, run *krmv1alpha1.BackupJob, job batchv1.Job) (krmv1alpha1.BackupSource, krmv1alpha1.RsyncMachine, bool, error) {
	runSet, target, _, err := r.resolveBackupJob(ctx, run)
	if err != nil {
		return krmv1alpha1.BackupSource{}, krmv1alpha1.RsyncMachine{}, false, err
	}
	for _, source := range runSet.Sources {
		if source.Source.Namespace == job.Namespace && GeneratedJobName("source-"+source.Source.Name, run.Name) == job.Name {
			return source.Source, target, true, nil
		}
	}
	return krmv1alpha1.BackupSource{}, krmv1alpha1.RsyncMachine{}, false, nil
}

func recoveryMinAvailableBytes(target krmv1alpha1.RsyncMachine) (uint64, error) {
	value := target.Annotations[AnnotationTestRecoveryMinAvailable]
	if value == "" {
		return targetRecoveryMinAvailableBytes, nil
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s annotation: %w", AnnotationTestRecoveryMinAvailable, err)
	}
	if quantity.Value() <= 0 {
		return 0, fmt.Errorf("invalid %s annotation %q", AnnotationTestRecoveryMinAvailable, value)
	}
	return uint64(quantity.Value()), nil
}

func backupTransferOutOfSpace(transfer krmv1alpha1.TransferStatus) bool {
	message := strings.ToLower(transfer.Message)
	return strings.Contains(message, "no space left on device") ||
		strings.Contains(message, "enospc") ||
		strings.Contains(message, "disk full") ||
		transfer.ExitCode == 11 ||
		transfer.ExitCode == 12
}

func (r *BackupJobReconciler) failBackupJob(ctx context.Context, run *krmv1alpha1.BackupJob, cause error) error {
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhaseFailed
	now := metav1.Now()
	run.Status.CompletedAt = &now
	run.Status.Conditions = []krmv1alpha1.Condition{{
		Type:               ConditionFailed,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonReconcileError,
		Message:            cause.Error(),
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: run.Generation,
	}}
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("%w; additionally failed to update status: %v", cause, err)
	}
	if err := r.releaseTargetGuardsForRun(ctx, run); err != nil {
		return fmt.Errorf("%w; additionally failed to release target guard: %v", cause, err)
	}
	r.recordRunEvent(run, corev1.EventTypeWarning, ReasonRunFailed, "BackupJob failed: %v", cause)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindBackup, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return cause
}

func (r *BackupJobReconciler) ensureScheduledBackupJobOwnerReference(ctx context.Context, run *krmv1alpha1.BackupJob) (bool, error) {
	if !run.DeletionTimestamp.IsZero() || run.Spec.TriggerOrDefault() != krmv1alpha1.BackupJobTriggerScheduled {
		return false, nil
	}
	machineRef, err := ResolveObjectReference(run.Spec.MachineRef, run.Namespace)
	if err != nil || machineRef.Namespace != run.Namespace {
		return false, nil
	}
	for _, owner := range run.OwnerReferences {
		if owner.APIVersion == krmv1alpha1.SchemeGroupVersion.String() && owner.Kind == "RsyncMachine" && owner.Name == machineRef.Name {
			return false, nil
		}
	}
	var machine krmv1alpha1.RsyncMachine
	if err := r.Get(ctx, machineRef, &machine); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if r.Scheme == nil {
		return false, nil
	}
	if err := ctrl.SetControllerReference(&machine, run, r.Scheme); err != nil {
		return false, fmt.Errorf("set scheduled backup job owner reference: %w", err)
	}
	if err := r.Update(ctx, run); err != nil {
		return false, fmt.Errorf("update scheduled backup job owner reference: %w", err)
	}
	return true, nil
}

func (r *BackupJobReconciler) pruneBackupJobHistory(ctx context.Context, run krmv1alpha1.BackupJob) error {
	machineRef, err := ResolveObjectReference(run.Spec.MachineRef, run.Namespace)
	if err != nil {
		return nil
	}
	var machine krmv1alpha1.RsyncMachine
	if err := r.Get(ctx, machineRef, &machine); err != nil {
		return client.IgnoreNotFound(err)
	}
	var runs krmv1alpha1.BackupJobList
	if err := r.List(ctx, &runs); err != nil {
		return fmt.Errorf("list backup jobs for history pruning: %w", err)
	}

	var successful, failed []krmv1alpha1.BackupJob
	for _, candidate := range runs.Items {
		if candidate.Spec.TriggerOrDefault() != krmv1alpha1.BackupJobTriggerScheduled {
			continue
		}
		if !isTerminalPhase(candidate.Status.Phase) {
			continue
		}
		candidateMachineRef, err := ResolveObjectReference(candidate.Spec.MachineRef, candidate.Namespace)
		if err != nil || candidateMachineRef != machineRef {
			continue
		}
		switch candidate.Status.Phase {
		case krmv1alpha1.RunPhaseSucceeded:
			successful = append(successful, candidate)
		case krmv1alpha1.RunPhaseFailed, krmv1alpha1.RunPhaseCanceled:
			failed = append(failed, candidate)
		}
	}
	pruneCandidates := append(pruneBackupJobHistoryCandidates(successful, machine.Spec.RunHistory.SuccessfulOrDefault()), pruneBackupJobHistoryCandidates(failed, machine.Spec.RunHistory.FailedOrDefault())...)
	snapshotAvailable := false
	if r.SnapshotCapabilities != nil {
		snapshotAvailable = r.SnapshotCapabilities.VolumeSnapshotAvailable(ctx)
	}
	for i := range pruneCandidates {
		candidate := pruneCandidates[i]
		if err := cleanupRunResourcesAndRemoveFinalizer(ctx, r.Client, &candidate, runKindBackup, BackupJobFinalizer, snapshotAvailable); err != nil {
			return fmt.Errorf("cleanup pruned backup job %s/%s: %w", candidate.Namespace, candidate.Name, err)
		}
		if err := r.Delete(ctx, &candidate); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete pruned backup job %s/%s: %w", candidate.Namespace, candidate.Name, err)
		}
	}
	return nil
}

func pruneBackupJobHistoryCandidates(runs []krmv1alpha1.BackupJob, keep int) []krmv1alpha1.BackupJob {
	if keep < 0 {
		keep = 0
	}
	sort.Slice(runs, func(i, j int) bool {
		left := backupRunHistoryTime(runs[i])
		right := backupRunHistoryTime(runs[j])
		if !left.Equal(&right) {
			return right.Before(&left)
		}
		return namespacedKey(runs[i].Namespace, runs[i].Name) > namespacedKey(runs[j].Namespace, runs[j].Name)
	})
	if len(runs) <= keep {
		return nil
	}
	return runs[keep:]
}

func backupRunHistoryTime(run krmv1alpha1.BackupJob) metav1.Time {
	if run.Status.CompletedAt != nil {
		return *run.Status.CompletedAt
	}
	return run.CreationTimestamp
}

func backupJobBytesTransferred(transfers []krmv1alpha1.TransferStatus) uint64 {
	var total uint64
	for _, transfer := range transfers {
		if transfer.Phase == krmv1alpha1.TransferPhaseSucceeded && transfer.BytesTransferred > 0 {
			total += uint64(transfer.BytesTransferred)
		}
	}
	return total
}

func upsertBackupJobTransfer(run *krmv1alpha1.BackupJob, source krmv1alpha1.BackupSource, status krmv1alpha1.TransferStatus) bool {
	sourceID := source.Namespace + "/" + source.Name
	status.Source = sourceID
	for i := range run.Status.Transfers {
		if run.Status.Transfers[i].Source == sourceID {
			changed := mergeBackupJobTransferStatus(&run.Status.Transfers[i], status)
			return setTransferStatusConditions(run, status) || changed
		}
	}
	run.Status.Transfers = append(run.Status.Transfers, status)
	setTransferStatusConditions(run, status)
	return true
}

func mergeBackupJobTransferStatus(existing *krmv1alpha1.TransferStatus, update krmv1alpha1.TransferStatus) bool {
	changed := false
	if update.Phase != "" && existing.Phase != update.Phase {
		if update.Phase == krmv1alpha1.TransferPhaseFailed || !transferPhaseTerminal(existing.Phase) {
			existing.Phase = update.Phase
			changed = true
		}
	}
	if update.Message != "" && existing.Message != update.Message {
		existing.Message = update.Message
		changed = true
	}
	if update.ExitCode != 0 && existing.ExitCode != update.ExitCode {
		existing.ExitCode = update.ExitCode
		changed = true
	}
	if update.CaptureMethod != "" && existing.CaptureMethod != update.CaptureMethod {
		existing.CaptureMethod = update.CaptureMethod
		changed = true
	}
	if update.VolumeSnapshotName != "" && existing.VolumeSnapshotName != update.VolumeSnapshotName {
		existing.VolumeSnapshotName = update.VolumeSnapshotName
		changed = true
	}
	if update.CaptureTime != nil && (existing.CaptureTime == nil || !existing.CaptureTime.Equal(update.CaptureTime)) {
		existing.CaptureTime = update.CaptureTime
		changed = true
	}
	return changed
}

func transferPhaseTerminal(phase krmv1alpha1.TransferPhase) bool {
	return phase == krmv1alpha1.TransferPhaseSucceeded || phase == krmv1alpha1.TransferPhaseFailed
}

func setTransferStatusConditions(run *krmv1alpha1.BackupJob, status krmv1alpha1.TransferStatus) bool {
	message := status.Message
	if message == "" {
		message = "Source transfer " + string(status.Phase)
	}
	changed := false
	switch status.Phase {
	case krmv1alpha1.TransferPhasePreparing:
		if status.CaptureMethod == krmv1alpha1.CaptureModeVolumeSnapshot {
			changed = setCondition(&run.Status.Conditions, ConditionSnapshotCapture, metav1.ConditionUnknown, ReasonSnapshotPreparing, message, run.Generation) || changed
		}
		changed = setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionUnknown, ReasonTransportRunning, message, run.Generation) || changed
	case krmv1alpha1.TransferPhaseSucceeded:
		changed = setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionTrue, ReasonTransportComplete, message, run.Generation) || changed
		if status.CaptureMethod == krmv1alpha1.CaptureModeVolumeSnapshot {
			changed = setCondition(&run.Status.Conditions, ConditionSnapshotCapture, metav1.ConditionTrue, ReasonSnapshotReady, "VolumeSnapshot capture is ready", run.Generation) || changed
		}
	case krmv1alpha1.TransferPhaseFailed:
		reason := ReasonTransportFailed
		conditionType := ConditionTransport
		if status.CaptureMethod == krmv1alpha1.CaptureModeVolumeSnapshot {
			reason = ReasonSnapshotFailed
			conditionType = ConditionSnapshotCapture
		}
		changed = setCondition(&run.Status.Conditions, conditionType, metav1.ConditionFalse, reason, message, run.Generation) || changed
		changed = setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionTrue, ReasonRunFailed, message, run.Generation) || changed
	case krmv1alpha1.TransferPhaseSkipped:
		changed = setCondition(&run.Status.Conditions, ConditionSnapshotCapture, metav1.ConditionFalse, ReasonSnapshotSkipped, message, run.Generation) || changed
	}
	return changed
}

func resourceTimedOut(obj metav1.Object, timeout time.Duration, now metav1.Time) bool {
	if obj == nil || timeout <= 0 {
		return false
	}
	createdAt := obj.GetCreationTimestamp()
	if createdAt.Time.IsZero() {
		return false
	}
	return now.Time.Sub(createdAt.Time) >= timeout
}

func (r *BackupJobReconciler) holdBackupJobForTarget(ctx context.Context, run *krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine) error {
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhasePending
	run.Status.Conditions = nil
	setCondition(&run.Status.Conditions, ConditionTargetReady, metav1.ConditionFalse, ReasonTargetNotReady, fmt.Sprintf("RsyncMachine %s/%s is not ready", target.Namespace, target.Name), run.Generation)
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("hold backup job for target readiness: %w", err)
	}
	r.recordRunEvent(run, corev1.EventTypeWarning, ReasonTargetNotReady, "RsyncMachine %s/%s is not ready", target.Namespace, target.Name)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindBackup, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return nil
}

func (r *BackupJobReconciler) holdBackupJobForOverlap(ctx context.Context, run *krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine, blocking krmv1alpha1.BackupJob, machine krmv1alpha1.RsyncMachine) error {
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhasePending
	reason := ReasonActiveRunForTarget
	if machine.Spec.ConcurrencyPolicyOrDefault() == krmv1alpha1.ConcurrencyPolicyReplace {
		reason = ReasonReplaceUnsupported
	}
	run.Status.Conditions = nil
	setCondition(&run.Status.Conditions, ConditionTargetOverlap, metav1.ConditionTrue, reason, fmt.Sprintf("RsyncMachine %s/%s is already used by BackupJob %s/%s", target.Namespace, target.Name, blocking.Namespace, blocking.Name), run.Generation)
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("hold backup job for target overlap: %w", err)
	}
	r.recordRunEvent(run, corev1.EventTypeWarning, reason, "RsyncMachine %s/%s is already used by BackupJob %s/%s", target.Namespace, target.Name, blocking.Namespace, blocking.Name)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindBackup, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return nil
}

func (r *BackupJobReconciler) holdBackupJobForRestore(ctx context.Context, run *krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine, restore krmv1alpha1.RestoreJob) error {
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhasePending
	run.Status.Conditions = nil
	setCondition(&run.Status.Conditions, ConditionTargetOverlap, metav1.ConditionTrue, ReasonActiveRestore, fmt.Sprintf("RsyncMachine %s/%s is being restored by RestoreJob %s/%s; waiting for restore to complete", target.Namespace, target.Name, restore.Namespace, restore.Name), run.Generation)
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("hold backup job for active restore: %w", err)
	}
	r.recordRunEvent(run, corev1.EventTypeWarning, ReasonActiveRestore, "RsyncMachine %s/%s is being restored by RestoreJob %s/%s", target.Namespace, target.Name, restore.Namespace, restore.Name)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindBackup, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return nil
}

func (r *BackupJobReconciler) createSecretIfMissing(ctx context.Context, secret *corev1.Secret) error {
	return createSecretIfMissing(ctx, r.Client, secret)
}

func createSecretIfMissing(ctx context.Context, c client.Client, secret *corev1.Secret) error {
	var existing corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: secret.Namespace, Name: secret.Name}, &existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get secret %s/%s: %w", secret.Namespace, secret.Name, err)
		}
		if err := c.Create(ctx, secret); err != nil {
			return fmt.Errorf("create secret %s/%s: %w", secret.Namespace, secret.Name, err)
		}
		return nil
	}
	if err := validateExistingGeneratedSecret(existing, *secret); err != nil {
		return fmt.Errorf("existing secret %s/%s cannot be reused: %w", secret.Namespace, secret.Name, err)
	}
	return nil
}

func validateExistingGeneratedSecret(existing, desired corev1.Secret) error {
	for key, value := range desired.Labels {
		if existing.Labels[key] != value {
			return fmt.Errorf("label %s=%q is required", key, value)
		}
	}
	if existing.Type != corev1.SecretTypeTLS {
		return fmt.Errorf("type must be %s", corev1.SecretTypeTLS)
	}
	expectedIdentity, err := tlsutil.BundleIdentity(tlsutil.Bundle{
		CACertPEM: desired.Data[tlsutil.SecretCAFile],
		CertPEM:   desired.Data[tlsutil.SecretCertFile],
		KeyPEM:    desired.Data[tlsutil.SecretKeyFile],
	})
	if err != nil {
		return fmt.Errorf("desired credential identity: %w", err)
	}
	if !bytes.Equal(existing.Data[tlsutil.SecretCAFile], desired.Data[tlsutil.SecretCAFile]) {
		return fmt.Errorf("%s does not match the expected CA", tlsutil.SecretCAFile)
	}
	return tlsutil.VerifyIdentity(tlsutil.Bundle{
		CACertPEM: existing.Data[tlsutil.SecretCAFile],
		CertPEM:   existing.Data[tlsutil.SecretCertFile],
		KeyPEM:    existing.Data[tlsutil.SecretKeyFile],
	}, expectedIdentity, time.Now())
}

func (r *BackupJobReconciler) createJobIfMissing(ctx context.Context, job *batchv1.Job) error {
	return createJobIfMissing(ctx, r.Client, job)
}

func (r *BackupJobReconciler) createBackupJobJobIfMissing(ctx context.Context, run *krmv1alpha1.BackupJob, job *batchv1.Job) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, run, job); err != nil {
		return fmt.Errorf("set backup job owner reference on job %s/%s: %w", job.Namespace, job.Name, err)
	}
	return r.createJobIfMissing(ctx, job)
}

func (r *BackupJobReconciler) createSourceJobIfMissing(ctx context.Context, run *krmv1alpha1.BackupJob, source *krmv1alpha1.BackupSource, job *batchv1.Job) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, run, job); err != nil {
		return fmt.Errorf("set backup job owner reference on job %s/%s: %w", job.Namespace, job.Name, err)
	}
	if source != nil && source.Namespace == job.Namespace && r.Scheme != nil {
		owner, err := ownerReferenceFor(r.Scheme, source)
		if err != nil {
			return fmt.Errorf("build backup source owner reference for job %s/%s: %w", job.Namespace, job.Name, err)
		}
		job.OwnerReferences = appendOwnerReference(job.OwnerReferences, owner)
	}
	return r.createJobIfMissing(ctx, job)
}

func (r *BackupJobReconciler) createBackupJobSecretIfMissing(ctx context.Context, run *krmv1alpha1.BackupJob, secret *corev1.Secret) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, run, secret); err != nil {
		return fmt.Errorf("set backup job owner reference on secret %s/%s: %w", secret.Namespace, secret.Name, err)
	}
	return r.createSecretIfMissing(ctx, secret)
}

func (r *BackupJobReconciler) createBackupJobServiceIfMissing(ctx context.Context, run *krmv1alpha1.BackupJob, service *corev1.Service) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, run, service); err != nil {
		return fmt.Errorf("set backup job owner reference on service %s/%s: %w", service.Namespace, service.Name, err)
	}
	return r.createServiceIfMissing(ctx, service)
}

func setControllerReferenceIfSameNamespace(scheme *runtime.Scheme, owner client.Object, object client.Object) error {
	if scheme == nil || owner == nil || object == nil || owner.GetNamespace() != object.GetNamespace() {
		return nil
	}
	return ctrl.SetControllerReference(owner, object, scheme)
}

func ownerReferenceFor(scheme *runtime.Scheme, owner client.Object) (metav1.OwnerReference, error) {
	gvks, _, err := scheme.ObjectKinds(owner)
	if err != nil {
		return metav1.OwnerReference{}, err
	}
	if len(gvks) == 0 {
		return metav1.OwnerReference{}, fmt.Errorf("no registered kind for %T", owner)
	}
	return metav1.OwnerReference{
		APIVersion: gvks[0].GroupVersion().String(),
		Kind:       gvks[0].Kind,
		Name:       owner.GetName(),
		UID:        owner.GetUID(),
	}, nil
}

func appendOwnerReference(owners []metav1.OwnerReference, owner metav1.OwnerReference) []metav1.OwnerReference {
	for i := range owners {
		if owners[i].APIVersion == owner.APIVersion && owners[i].Kind == owner.Kind && owners[i].Name == owner.Name {
			owners[i] = owner
			return owners
		}
	}
	return append(owners, owner)
}

func createJobIfMissing(ctx context.Context, c client.Client, job *batchv1.Job) error {
	var existing batchv1.Job
	if err := c.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, &existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get job %s/%s: %w", job.Namespace, job.Name, err)
		}
		if err := c.Create(ctx, job); err != nil {
			return fmt.Errorf("create job %s/%s: %w", job.Namespace, job.Name, err)
		}
	}
	return nil
}

func (r *BackupJobReconciler) createServiceIfMissing(ctx context.Context, service *corev1.Service) error {
	return createServiceIfMissing(ctx, r.Client, service)
}

func createServiceIfMissing(ctx context.Context, c client.Client, service *corev1.Service) error {
	var existing corev1.Service
	err := c.Get(ctx, types.NamespacedName{Namespace: service.Namespace, Name: service.Name}, &existing)
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, service)
	}
	if err != nil {
		return err
	}
	return nil
}

func (r *BackupJobReconciler) createPersistentVolumeClaimIfMissing(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	var existing corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: pvc.Name}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, pvc)
	}
	if err != nil {
		return err
	}
	return nil
}

func (r *BackupJobReconciler) createVolumeSnapshotIfMissing(ctx context.Context, volumeSnapshot *unstructured.Unstructured) error {
	var existing unstructured.Unstructured
	existing.SetAPIVersion(volumeSnapshot.GetAPIVersion())
	existing.SetKind(volumeSnapshot.GetKind())
	err := r.Get(ctx, types.NamespacedName{Namespace: volumeSnapshot.GetNamespace(), Name: volumeSnapshot.GetName()}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, volumeSnapshot)
	}
	if err != nil {
		return err
	}
	return nil
}

func (r *BackupJobReconciler) getVolumeSnapshot(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	var observed unstructured.Unstructured
	observed.SetAPIVersion(snapshot.SnapshotAPIVersion)
	observed.SetKind(snapshot.VolumeSnapshotKind)
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &observed); err != nil {
		return nil, err
	}
	return &observed, nil
}

func (r *BackupJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("backupjob-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&krmv1alpha1.BackupJob{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Service{}).
		Watches(&krmv1alpha1.RsyncMachine{}, handler.EnqueueRequestsFromMapFunc(r.backupJobsForMachine)).
		Watches(&krmv1alpha1.BackupSource{}, handler.EnqueueRequestsFromMapFunc(r.backupJobsForSourceMachine)).
		Complete(r)
}

func (r *BackupJobReconciler) backupJobsForMachine(ctx context.Context, obj client.Object) []reconcile.Request {
	machine, ok := obj.(*krmv1alpha1.RsyncMachine)
	if !ok {
		return nil
	}
	machineRef := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
	var runs krmv1alpha1.BackupJobList
	if err := r.List(ctx, &runs); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(runs.Items))
	for _, run := range runs.Items {
		runMachineRef, err := ResolveObjectReference(run.Spec.MachineRef, run.Namespace)
		if err == nil && runMachineRef == machineRef {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}})
		}
	}
	return requests
}

func (r *BackupJobReconciler) backupJobsForSourceMachine(ctx context.Context, obj client.Object) []reconcile.Request {
	source, ok := obj.(*krmv1alpha1.BackupSource)
	if !ok {
		return nil
	}
	machineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
	if err != nil {
		return nil
	}
	var runs krmv1alpha1.BackupJobList
	if err := r.List(ctx, &runs); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(runs.Items))
	for _, run := range runs.Items {
		runMachineRef, err := ResolveObjectReference(run.Spec.MachineRef, run.Namespace)
		if err == nil && runMachineRef == machineRef {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}})
		}
	}
	return requests
}

func (r *BackupJobReconciler) dataPlaneControlOptions() DataPlaneControlOptions {
	namespace := r.ControlGRPCNamespace
	if namespace == "" {
		namespace = ControlGRPCNamespace
	}
	endpoint := r.ControlGRPCEndpoint
	if endpoint == "" {
		endpoint = DefaultDataPlaneControlOptions(namespace).GRPCEndpoint
	}
	return DataPlaneControlOptions{GRPCEndpoint: endpoint, GRPCNamespace: namespace}
}

type RestoreJobReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	Image                string
	ControlGRPCNamespace string
	ControlGRPCEndpoint  string
	Metrics              *ptmmetrics.Recorder
	ControlGRPCCA        []byte
	ControlGRPCSigner    *tlsutil.CA
	Recorder             record.EventRecorder
}

func (r *RestoreJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run krmv1alpha1.RestoreJob
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, cleanupRunResourcesAndRemoveFinalizer(ctx, r.Client, &run, runKindRestore, RestoreJobFinalizer, false)
	}
	if !isTerminalPhase(run.Status.Phase) && !controllerutil.ContainsFinalizer(&run, RestoreJobFinalizer) {
		controllerutil.AddFinalizer(&run, RestoreJobFinalizer)
		if err := r.Update(ctx, &run); err != nil {
			return ctrl.Result{}, fmt.Errorf("add restore job finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}
	if run.Status.Phase != "" && run.Status.Phase != krmv1alpha1.RunPhasePending {
		return ctrl.Result{}, r.reconcileRestoreJobJobStatus(ctx, &run)
	}
	sourceRef, err := ResolveObjectReference(run.Spec.SourceRef, run.Namespace)
	if err != nil {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, fmt.Errorf("resolve source: %w", err))
	}
	var source krmv1alpha1.BackupSource
	if err := r.Get(ctx, sourceRef, &source); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	machineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
	if err != nil {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, fmt.Errorf("resolve source machine: %w", err))
	}
	var target krmv1alpha1.RsyncMachine
	if err := r.Get(ctx, machineRef, &target); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !SourceNamespaceAllowed(target, source.Namespace) {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, fmt.Errorf("BackupSource namespace %s is not allowed to attach to RsyncMachine %s", source.Namespace, machineRef.String()))
	}
	if !RestoreNamespaceAllowed(target, run.Namespace) {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, fmt.Errorf("RestoreJob namespace %s is not allowed to restore from RsyncMachine %s", run.Namespace, machineRef.String()))
	}
	if destinationNamespace := run.Spec.Overrides.Destination.Namespace; destinationNamespace != "" && destinationNamespace != run.Namespace {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, fmt.Errorf("RestoreJob destination namespace %s must match RestoreJob namespace %s", destinationNamespace, run.Namespace))
	}
	if !TargetReady(target) {
		return ctrl.Result{}, r.holdRestoreJobForTarget(ctx, &run, target)
	}
	if blocking, ok, err := r.findActiveBackupJobForTarget(ctx, target); err != nil {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, err)
	} else if ok {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, r.holdRestoreJobForBackup(ctx, &run, target, blocking)
	}
	restoredSnapshot, err := resolveRestoreSnapshot(run.Spec.SnapshotOrDefault(), target)
	if err != nil {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, err)
	}
	run.Status.RestoredSnapshot = restoredSnapshot
	destinationNamespace := run.Spec.Overrides.Destination.Namespace
	if destinationNamespace == "" {
		destinationNamespace = run.Namespace
	}
	credentials, err := BuildRestoreJobCredentialSecretsWithSigner(run, target, source, destinationNamespace, DefaultRunCertificateTTL, r.ControlGRPCSigner, r.ControlGRPCCA)
	if err != nil {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, err)
	}
	for _, credential := range credentials {
		secret := credential.Secret()
		if credential.Labels[LabelRole] == RoleTargetServer {
			if err := r.createRestoreTargetSecretIfMissing(ctx, &target, secret); err != nil {
				return ctrl.Result{}, err
			}
			continue
		}
		if err := r.createRestoreWriterSecretIfMissing(ctx, &run, secret); err != nil {
			return ctrl.Result{}, err
		}
	}
	targetJob, err := BuildRestoreTargetJobWithControl(run, source, target, r.Image, r.dataPlaneControlOptions())
	if err != nil {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, err)
	}
	if err := r.createRestoreTargetJobIfMissing(ctx, &target, targetJob); err != nil {
		return ctrl.Result{}, err
	}
	targetService := BuildRestoreTargetService(run, target)
	if err := r.createRestoreTargetServiceIfMissing(ctx, &target, targetService); err != nil {
		return ctrl.Result{}, err
	}
	job, err := BuildRestoreJobWithControl(run, source, target, r.Image, r.dataPlaneControlOptions())
	if err != nil {
		return ctrl.Result{}, r.failRestoreJob(ctx, &run, err)
	}
	if err := r.createRestoreWriterJobIfMissing(ctx, &run, job); err != nil {
		return ctrl.Result{}, err
	}
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhasePreparing
	run.Status.Conditions = nil
	if run.Status.StartedAt == nil {
		now := metav1.Now()
		run.Status.StartedAt = &now
	}
	setCondition(&run.Status.Conditions, ConditionValid, metav1.ConditionTrue, ReasonResolvedReferences, "RestoreJob references resolved successfully", run.Generation)
	setCondition(&run.Status.Conditions, ConditionTargetReady, metav1.ConditionTrue, ReasonTargetReady, fmt.Sprintf("RsyncMachine %s/%s is ready", target.Namespace, target.Name), run.Generation)
	setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionFalse, ReasonRunStarted, "RestoreJob started", run.Generation)
	if err := r.Status().Update(ctx, &run); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("set restore job preparing phase: %w", err)
	}
	r.recordRunEvent(&run, corev1.EventTypeNormal, ReasonRunStarted, "RestoreJob started for RsyncMachine %s/%s", target.Namespace, target.Name)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindRestore, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return ctrl.Result{}, nil
}

func (r *RestoreJobReconciler) createRestoreTargetSecretIfMissing(ctx context.Context, target *krmv1alpha1.RsyncMachine, secret *corev1.Secret) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, target, secret); err != nil {
		return fmt.Errorf("set rsync machine owner reference on restore target secret %s/%s: %w", secret.Namespace, secret.Name, err)
	}
	return createSecretIfMissing(ctx, r.Client, secret)
}

func (r *RestoreJobReconciler) createRestoreWriterSecretIfMissing(ctx context.Context, run *krmv1alpha1.RestoreJob, secret *corev1.Secret) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, run, secret); err != nil {
		return fmt.Errorf("set restore job owner reference on restore writer secret %s/%s: %w", secret.Namespace, secret.Name, err)
	}
	return createSecretIfMissing(ctx, r.Client, secret)
}

func (r *RestoreJobReconciler) createRestoreTargetJobIfMissing(ctx context.Context, target *krmv1alpha1.RsyncMachine, job *batchv1.Job) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, target, job); err != nil {
		return fmt.Errorf("set rsync machine owner reference on restore target job %s/%s: %w", job.Namespace, job.Name, err)
	}
	return createJobIfMissing(ctx, r.Client, job)
}

func (r *RestoreJobReconciler) createRestoreTargetServiceIfMissing(ctx context.Context, target *krmv1alpha1.RsyncMachine, service *corev1.Service) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, target, service); err != nil {
		return fmt.Errorf("set rsync machine owner reference on restore target service %s/%s: %w", service.Namespace, service.Name, err)
	}
	return createServiceIfMissing(ctx, r.Client, service)
}

func (r *RestoreJobReconciler) createRestoreWriterJobIfMissing(ctx context.Context, run *krmv1alpha1.RestoreJob, job *batchv1.Job) error {
	if err := setControllerReferenceIfSameNamespace(r.Scheme, run, job); err != nil {
		return fmt.Errorf("set restore job owner reference on restore writer job %s/%s: %w", job.Namespace, job.Name, err)
	}
	return createJobIfMissing(ctx, r.Client, job)
}

func (r *RestoreJobReconciler) holdRestoreJobForTarget(ctx context.Context, run *krmv1alpha1.RestoreJob, target krmv1alpha1.RsyncMachine) error {
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhasePending
	run.Status.Conditions = nil
	setCondition(&run.Status.Conditions, ConditionTargetReady, metav1.ConditionFalse, ReasonTargetNotReady, fmt.Sprintf("RsyncMachine %s/%s is not ready", target.Namespace, target.Name), run.Generation)
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("hold restore job for target readiness: %w", err)
	}
	r.recordRunEvent(run, corev1.EventTypeWarning, ReasonTargetNotReady, "RsyncMachine %s/%s is not ready", target.Namespace, target.Name)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindRestore, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return nil
}

func (r *RestoreJobReconciler) findActiveBackupJobForTarget(ctx context.Context, target krmv1alpha1.RsyncMachine) (krmv1alpha1.BackupJob, bool, error) {
	machineRef := types.NamespacedName{Namespace: target.Namespace, Name: target.Name}
	leaseRef := TargetGuardLeaseRef(machineRef)
	var lease coordinationv1.Lease
	if err := r.Get(ctx, leaseRef, &lease); err != nil {
		if !apierrors.IsNotFound(err) {
			return krmv1alpha1.BackupJob{}, false, fmt.Errorf("get target guard lease %s: %w", leaseRef.String(), err)
		}
	} else if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
		blocking, active, err := restoreActiveBackupGuardHolder(ctx, r.Client, *lease.Spec.HolderIdentity, machineRef)
		if err != nil {
			return krmv1alpha1.BackupJob{}, false, err
		}
		if active {
			return blocking, true, nil
		}
	}

	var runs krmv1alpha1.BackupJobList
	if err := r.List(ctx, &runs); err != nil {
		return krmv1alpha1.BackupJob{}, false, fmt.Errorf("list backup jobs: %w", err)
	}
	for _, candidate := range runs.Items {
		if !ActiveRunBlocksTargetMutation(candidate.Status.Phase) {
			continue
		}
		candidateMachineRef, ok, err := machineRefForBackupJobWithClient(ctx, r.Client, candidate)
		if err != nil {
			return krmv1alpha1.BackupJob{}, false, err
		}
		if ok && candidateMachineRef == machineRef {
			return candidate, true, nil
		}
	}
	return krmv1alpha1.BackupJob{}, false, nil
}

func restoreActiveBackupGuardHolder(ctx context.Context, c client.Client, holder string, target types.NamespacedName) (krmv1alpha1.BackupJob, bool, error) {
	holderRef, ok := parseBackupJobGuardHolder(holder)
	if !ok {
		return krmv1alpha1.BackupJob{}, false, nil
	}
	var run krmv1alpha1.BackupJob
	if err := c.Get(ctx, holderRef, &run); err != nil {
		if apierrors.IsNotFound(err) {
			return krmv1alpha1.BackupJob{}, false, nil
		}
		return krmv1alpha1.BackupJob{}, false, fmt.Errorf("read target guard holder %s: %w", holderRef.String(), err)
	}
	if !ActiveRunBlocksTargetMutation(run.Status.Phase) {
		return run, false, nil
	}
	holderTarget, ok, err := machineRefForBackupJobWithClient(ctx, c, run)
	if err != nil {
		return krmv1alpha1.BackupJob{}, false, fmt.Errorf("resolve target guard holder %s target: %w", holderRef.String(), err)
	}
	if ok && holderTarget != target {
		return run, false, nil
	}
	return run, true, nil
}

func machineRefForBackupJobWithClient(ctx context.Context, c client.Client, run krmv1alpha1.BackupJob) (types.NamespacedName, bool, error) {
	machineRef, err := ResolveObjectReference(run.Spec.MachineRef, run.Namespace)
	if err != nil {
		return types.NamespacedName{}, false, nil
	}
	var machine krmv1alpha1.RsyncMachine
	if err := c.Get(ctx, machineRef, &machine); err != nil {
		if apierrors.IsNotFound(err) {
			return types.NamespacedName{}, false, nil
		}
		return types.NamespacedName{}, false, err
	}
	return machineRef, true, nil
}

func machineRefForRestoreJobWithClient(ctx context.Context, c client.Client, run krmv1alpha1.RestoreJob) (types.NamespacedName, bool, error) {
	sourceRef, err := ResolveObjectReference(run.Spec.SourceRef, run.Namespace)
	if err != nil {
		return types.NamespacedName{}, false, nil
	}
	var source krmv1alpha1.BackupSource
	if err := c.Get(ctx, sourceRef, &source); err != nil {
		if apierrors.IsNotFound(err) {
			return types.NamespacedName{}, false, nil
		}
		return types.NamespacedName{}, false, err
	}
	machineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
	if err != nil {
		return types.NamespacedName{}, false, nil
	}
	var machine krmv1alpha1.RsyncMachine
	if err := c.Get(ctx, machineRef, &machine); err != nil {
		if apierrors.IsNotFound(err) {
			return types.NamespacedName{}, false, nil
		}
		return types.NamespacedName{}, false, err
	}
	if !SourceNamespaceAllowed(machine, source.Namespace) || !RestoreNamespaceAllowed(machine, run.Namespace) {
		return types.NamespacedName{}, false, nil
	}
	if destinationNamespace := run.Spec.Overrides.Destination.Namespace; destinationNamespace != "" && destinationNamespace != run.Namespace {
		return types.NamespacedName{}, false, nil
	}
	return machineRef, true, nil
}

func (r *RestoreJobReconciler) holdRestoreJobForBackup(ctx context.Context, run *krmv1alpha1.RestoreJob, target krmv1alpha1.RsyncMachine, backup krmv1alpha1.BackupJob) error {
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhasePending
	run.Status.Conditions = nil
	setCondition(&run.Status.Conditions, ConditionTargetOverlap, metav1.ConditionTrue, ReasonActiveRunForTarget, fmt.Sprintf("RsyncMachine %s/%s is being mutated by BackupJob %s/%s; waiting for backup to complete", target.Namespace, target.Name, backup.Namespace, backup.Name), run.Generation)
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("hold restore job for active backup: %w", err)
	}
	r.recordRunEvent(run, corev1.EventTypeWarning, ReasonActiveRunForTarget, "RsyncMachine %s/%s is being mutated by BackupJob %s/%s", target.Namespace, target.Name, backup.Namespace, backup.Name)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindRestore, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return nil
}

func (r *RestoreJobReconciler) reconcileRestoreJobJobStatus(ctx context.Context, run *krmv1alpha1.RestoreJob) error {
	if isTerminalPhase(run.Status.Phase) {
		return cleanupRunCredentialSecrets(ctx, r.Client, run, runKindRestore)
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.MatchingLabels{
		LabelRunNamespace: run.Namespace,
		LabelRunKind:      runKindRestore,
		LabelRun:          run.Name,
	}); err != nil {
		return fmt.Errorf("list restore job jobs: %w", err)
	}
	if len(jobs.Items) == 0 {
		return nil
	}
	oldPhase := run.Status.Phase
	phase := krmv1alpha1.RunPhaseRunning
	allComplete := true
	for _, job := range jobs.Items {
		r.Metrics.RecordGeneratedJobStatus(
			ptmmetrics.RunKindRestore,
			job.Labels[LabelRole],
			namespacedKey(job.Namespace, job.Name),
			generatedJobStatus(job),
		)
		if jobFailed(job) {
			phase = krmv1alpha1.RunPhaseFailed
			break
		}
		if !jobComplete(job) {
			allComplete = false
		}
	}
	if phase != krmv1alpha1.RunPhaseFailed && allComplete {
		phase = krmv1alpha1.RunPhaseSucceeded
		if run.Status.RestoredSnapshot == "" {
			run.Status.RestoredSnapshot = run.Spec.SnapshotOrDefault()
		}
	}
	if phase == run.Status.Phase {
		return nil
	}
	run.Status.Phase = phase
	if phase == krmv1alpha1.RunPhaseSucceeded || phase == krmv1alpha1.RunPhaseFailed {
		now := metav1.Now()
		run.Status.CompletedAt = &now
	}
	if phase == krmv1alpha1.RunPhaseFailed {
		run.Status.Message = restoreJobFailureMessage(jobs.Items)
	}
	switch phase {
	case krmv1alpha1.RunPhaseSucceeded:
		setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionFalse, ReasonRunSucceeded, "RestoreJob completed successfully", run.Generation)
		setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionTrue, ReasonTransportComplete, "Restore writer completed", run.Generation)
	case krmv1alpha1.RunPhaseFailed:
		setCondition(&run.Status.Conditions, ConditionFailed, metav1.ConditionTrue, ReasonRunFailed, "A generated restore Job failed", run.Generation)
		setCondition(&run.Status.Conditions, ConditionTransport, metav1.ConditionFalse, ReasonTransportFailed, "A generated restore Job failed", run.Generation)
	}
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("update restore job job-derived status: %w", err)
	}
	switch phase {
	case krmv1alpha1.RunPhaseSucceeded:
		r.recordRunEvent(run, corev1.EventTypeNormal, ReasonRunSucceeded, "RestoreJob completed successfully")
	case krmv1alpha1.RunPhaseFailed:
		r.recordRunEvent(run, corev1.EventTypeWarning, ReasonRunFailed, "RestoreJob failed because a generated Job failed")
	}
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindRestore, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	if isTerminalPhase(run.Status.Phase) {
		return cleanupRunCredentialSecrets(ctx, r.Client, run, runKindRestore)
	}
	return nil
}

func (r *RestoreJobReconciler) failRestoreJob(ctx context.Context, run *krmv1alpha1.RestoreJob, cause error) error {
	oldPhase := run.Status.Phase
	run.Status.Phase = krmv1alpha1.RunPhaseFailed
	run.Status.Message = cause.Error()
	now := metav1.Now()
	run.Status.CompletedAt = &now
	run.Status.Conditions = []krmv1alpha1.Condition{{
		Type:               ConditionFailed,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonReconcileError,
		Message:            cause.Error(),
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: run.Generation,
	}}
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("%w; additionally failed to update status: %v", cause, err)
	}
	r.recordRunEvent(run, corev1.EventTypeWarning, ReasonRunFailed, "RestoreJob failed: %v", cause)
	r.Metrics.RecordRunPhase(ptmmetrics.RunKindRestore, namespacedKey(run.Namespace, run.Name), oldPhase, run.Status.Phase)
	return cause
}

func resolveRestoreSnapshot(requested string, target krmv1alpha1.RsyncMachine) (string, error) {
	if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror {
		if requested == "" || requested == krmv1alpha1.DefaultSnapshot || requested == krmv1alpha1.DefaultMirrorSnapshot {
			return krmv1alpha1.DefaultMirrorSnapshot, nil
		}
		return "", fmt.Errorf("mirror RsyncMachine %s/%s only supports snapshot %q", target.Namespace, target.Name, krmv1alpha1.DefaultMirrorSnapshot)
	}
	if requested == "" {
		requested = krmv1alpha1.DefaultSnapshot
	}
	if requested != krmv1alpha1.DefaultSnapshot {
		normalized, err := cleanSnapshotPath(requested)
		if err != nil {
			return "", err
		}
		if len(target.Status.RestorePoints) == 0 {
			return normalized, nil
		}
		for _, point := range target.Status.RestorePoints {
			if point.Snapshot == normalized {
				return normalized, nil
			}
		}
		return "", fmt.Errorf("snapshot %q is not present in RsyncMachine %s/%s restore points", normalized, target.Namespace, target.Name)
	}
	for _, point := range target.Status.RestorePoints {
		if point.Snapshot == krmv1alpha1.DefaultSnapshot {
			if point.ResolvesTo == "" {
				return "", fmt.Errorf("snapshot %q in RsyncMachine %s/%s does not resolve to an immutable snapshot", requested, target.Namespace, target.Name)
			}
			normalized, err := cleanSnapshotPath(point.ResolvesTo)
			if err != nil {
				return "", fmt.Errorf("invalid resolved snapshot %q: %w", point.ResolvesTo, err)
			}
			return normalized, nil
		}
	}
	return "", fmt.Errorf("snapshot %q is not present in RsyncMachine %s/%s restore points", requested, target.Namespace, target.Name)
}

func cleanSnapshotPath(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("snapshot path is required")
	}
	if path.IsAbs(value) {
		return "", fmt.Errorf("snapshot path %q must be relative", value)
	}
	if strings.Contains(value, "\\") {
		return "", fmt.Errorf("snapshot path %q must use '/' separators", value)
	}
	for _, part := range strings.Split(value, "/") {
		if part == ".." {
			return "", fmt.Errorf("snapshot path %q must not contain '..'", value)
		}
	}
	cleaned := path.Clean(value)
	if cleaned == "." {
		return "", fmt.Errorf("snapshot path %q must name a directory", value)
	}
	return cleaned, nil
}

func restoreJobFailureMessage(jobs []batchv1.Job) string {
	for _, job := range jobs {
		if !jobFailed(job) {
			continue
		}
		role := job.Labels[LabelRole]
		if role == "" {
			role = "restore"
		}
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				if condition.Message != "" {
					return fmt.Sprintf("%s Job %s/%s failed: %s", role, job.Namespace, job.Name, condition.Message)
				}
				if condition.Reason != "" {
					return fmt.Sprintf("%s Job %s/%s failed: %s", role, job.Namespace, job.Name, condition.Reason)
				}
			}
		}
		return fmt.Sprintf("%s Job %s/%s failed", role, job.Namespace, job.Name)
	}
	return "restore Job failed"
}

func (r *RestoreJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("restorejob-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&krmv1alpha1.RestoreJob{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Service{}).
		Watches(&krmv1alpha1.RsyncMachine{}, handler.EnqueueRequestsFromMapFunc(r.restoreJobsForMachine)).
		Watches(&krmv1alpha1.BackupSource{}, handler.EnqueueRequestsFromMapFunc(r.restoreJobsForSource)).
		Complete(r)
}

func (r *RestoreJobReconciler) restoreJobsForMachine(ctx context.Context, obj client.Object) []reconcile.Request {
	machine, ok := obj.(*krmv1alpha1.RsyncMachine)
	if !ok {
		return nil
	}
	machineRef := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
	var restores krmv1alpha1.RestoreJobList
	if err := r.List(ctx, &restores); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(restores.Items))
	for _, restore := range restores.Items {
		restoreMachineRef, ok, err := machineRefForRestoreJobWithClient(ctx, r.Client, restore)
		if err == nil && ok && restoreMachineRef == machineRef {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}})
		}
	}
	return requests
}

func (r *RestoreJobReconciler) restoreJobsForSource(ctx context.Context, obj client.Object) []reconcile.Request {
	source, ok := obj.(*krmv1alpha1.BackupSource)
	if !ok {
		return nil
	}
	sourceRef := types.NamespacedName{Namespace: source.Namespace, Name: source.Name}
	var restores krmv1alpha1.RestoreJobList
	if err := r.List(ctx, &restores); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(restores.Items))
	for _, restore := range restores.Items {
		restoreSourceRef, err := ResolveObjectReference(restore.Spec.SourceRef, restore.Namespace)
		if err == nil && restoreSourceRef == sourceRef {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}})
		}
	}
	return requests
}

func (r *RestoreJobReconciler) dataPlaneControlOptions() DataPlaneControlOptions {
	namespace := r.ControlGRPCNamespace
	if namespace == "" {
		namespace = ControlGRPCNamespace
	}
	endpoint := r.ControlGRPCEndpoint
	if endpoint == "" {
		endpoint = DefaultDataPlaneControlOptions(namespace).GRPCEndpoint
	}
	return DataPlaneControlOptions{GRPCEndpoint: endpoint, GRPCNamespace: namespace}
}

func cleanupRunResourcesAndRemoveFinalizer(ctx context.Context, c client.Client, run client.Object, runKind, finalizer string, snapshotAvailable bool) error {
	if !controllerutil.ContainsFinalizer(run, finalizer) {
		return nil
	}
	if err := cleanupRunResources(ctx, c, run, runKind, snapshotAvailable); err != nil {
		return err
	}
	controllerutil.RemoveFinalizer(run, finalizer)
	if err := c.Update(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove %s run finalizer: %w", runKind, err)
	}
	return nil
}

func cleanupRunResources(ctx context.Context, c client.Client, run client.Object, runKind string, snapshotAvailable bool) error {
	labels := client.MatchingLabels{
		LabelRunNamespace: run.GetNamespace(),
		LabelRunKind:      runKind,
		LabelRun:          run.GetName(),
	}
	if err := deleteLabeledRunJobs(ctx, c, labels); err != nil {
		return fmt.Errorf("cleanup %s run jobs: %w", runKind, err)
	}
	if err := deleteLabeledRunPersistentVolumeClaims(ctx, c, labels); err != nil {
		return fmt.Errorf("cleanup %s run persistent volume claims: %w", runKind, err)
	}
	if err := deleteLabeledRunSecrets(ctx, c, labels); err != nil {
		return fmt.Errorf("cleanup %s run secrets: %w", runKind, err)
	}
	if err := deleteLabeledRunServices(ctx, c, labels); err != nil {
		return fmt.Errorf("cleanup %s run services: %w", runKind, err)
	}
	if snapshotAvailable {
		if err := deleteLabeledRunVolumeSnapshots(ctx, c, labels); err != nil {
			return fmt.Errorf("cleanup %s run volume snapshots: %w", runKind, err)
		}
	}
	return nil
}

func cleanupBackupJobSnapshotResources(ctx context.Context, c client.Client, run client.Object, snapshotAvailable bool) error {
	labels := client.MatchingLabels{
		LabelRunNamespace: run.GetNamespace(),
		LabelRunKind:      runKindBackup,
		LabelRun:          run.GetName(),
	}
	if err := deleteLabeledRunPersistentVolumeClaims(ctx, c, labels); err != nil {
		return fmt.Errorf("cleanup backup job snapshot persistent volume claims: %w", err)
	}
	if snapshotAvailable {
		if err := deleteLabeledRunVolumeSnapshots(ctx, c, labels); err != nil {
			return fmt.Errorf("cleanup backup job volume snapshots: %w", err)
		}
	}
	return nil
}

func cleanupRunCredentialSecrets(ctx context.Context, c client.Client, run client.Object, runKind string) error {
	return deleteLabeledRunSecrets(ctx, c, client.MatchingLabels{
		LabelRunNamespace: run.GetNamespace(),
		LabelRunKind:      runKind,
		LabelRun:          run.GetName(),
	})
}

func deleteLabeledRunJobs(ctx context.Context, c client.Client, labels client.MatchingLabels) error {
	var jobs batchv1.JobList
	if err := c.List(ctx, &jobs, labels); err != nil {
		return err
	}
	propagation := metav1.DeletePropagationBackground
	for i := range jobs.Items {
		if err := c.Delete(ctx, &jobs.Items[i], &client.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func deleteLabeledRunPersistentVolumeClaims(ctx context.Context, c client.Client, labels client.MatchingLabels) error {
	var pvcs corev1.PersistentVolumeClaimList
	if err := c.List(ctx, &pvcs, labels); err != nil {
		return err
	}
	for i := range pvcs.Items {
		if err := c.Delete(ctx, &pvcs.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func deleteLabeledRunSecrets(ctx context.Context, c client.Client, labels client.MatchingLabels) error {
	var secrets corev1.SecretList
	if err := c.List(ctx, &secrets, labels); err != nil {
		return err
	}
	for i := range secrets.Items {
		if err := c.Delete(ctx, &secrets.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func deleteLabeledRunServices(ctx context.Context, c client.Client, labels client.MatchingLabels) error {
	var services corev1.ServiceList
	if err := c.List(ctx, &services, labels); err != nil {
		return err
	}
	for i := range services.Items {
		if err := c.Delete(ctx, &services.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func deleteLabeledRunVolumeSnapshots(ctx context.Context, c client.Client, labels client.MatchingLabels) error {
	var snapshots unstructured.UnstructuredList
	snapshots.SetAPIVersion(snapshot.SnapshotAPIVersion)
	snapshots.SetKind(snapshot.VolumeSnapshotKind + "List")
	if err := c.List(ctx, &snapshots, labels); err != nil {
		return err
	}
	for i := range snapshots.Items {
		if err := c.Delete(ctx, &snapshots.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

type RsyncMachineReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Image    string
	Recorder record.EventRecorder
	Clock    func() time.Time
}

func (r *RsyncMachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var target krmv1alpha1.RsyncMachine
	if err := r.Get(ctx, req.NamespacedName, &target); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !target.DeletionTimestamp.IsZero() {
		if err := r.deleteScheduledBackupJobsForMachine(ctx, target); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.deleteGeneratedCronJob(ctx, target); err != nil {
			return ctrl.Result{}, err
		}
		if controllerutil.ContainsFinalizer(&target, RsyncMachineFinalizer) {
			controllerutil.RemoveFinalizer(&target, RsyncMachineFinalizer)
			if err := r.Update(ctx, &target); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("remove rsync machine finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&target, RsyncMachineFinalizer) {
		controllerutil.AddFinalizer(&target, RsyncMachineFinalizer)
		if err := r.Update(ctx, &target); err != nil {
			return ctrl.Result{}, fmt.Errorf("add rsync machine finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}
	changed := false
	if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror && (len(target.Status.RestorePoints) > 0 || target.Status.RestorePointCount != 0) {
		target.Status.RestorePoints = nil
		target.Status.RestorePointCount = 0
		now := metav1.Now()
		target.Status.RestorePointsUpdatedAt = &now
		changed = true
	}
	if target.Spec.PVCName == "" {
		changed = setCondition(&target.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonInvalidSpec, "spec.pvcName is required", target.Generation) || changed
		changed = setCondition(&target.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonInvalidSpec, "RsyncMachine PVC is not configured", target.Generation) || changed
	} else {
		var pvc corev1.PersistentVolumeClaim
		ref := types.NamespacedName{Namespace: target.Namespace, Name: target.Spec.PVCName}
		if err := r.Get(ctx, ref, &pvc); err != nil {
			if apierrors.IsNotFound(err) {
				changed = setCondition(&target.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonMissingReference, fmt.Sprintf("PersistentVolumeClaim %s was not found", ref.String()), target.Generation) || changed
				changed = setCondition(&target.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonMissingReference, fmt.Sprintf("PersistentVolumeClaim %s was not found", ref.String()), target.Generation) || changed
			} else {
				changed = setCondition(&target.Status.Conditions, ConditionValid, metav1.ConditionUnknown, ReasonMissingReference, fmt.Sprintf("PersistentVolumeClaim %s could not be read: %v", ref.String(), err), target.Generation) || changed
			}
		} else if valid, reason, message := r.validateRsyncMachineSources(ctx, target); valid != metav1.ConditionTrue {
			changed = setCondition(&target.Status.Conditions, ConditionValid, valid, reason, message, target.Generation) || changed
			changed = setCondition(&target.Status.Conditions, ConditionReady, metav1.ConditionFalse, reason, message, target.Generation) || changed
		} else {
			changed = setCondition(&target.Status.Conditions, ConditionValid, metav1.ConditionTrue, ReasonResolvedReferences, fmt.Sprintf("PersistentVolumeClaim %s and sources are available", ref.String()), target.Generation) || changed
			changed = setCondition(&target.Status.Conditions, ConditionReady, metav1.ConditionTrue, ReasonResolvedReferences, "RsyncMachine is ready", target.Generation) || changed
		}
	}
	if changed {
		if err := r.Status().Update(ctx, &target); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("update rsync machine conditions: %w", err)
		}
	}
	if target.Spec.Schedule == "" {
		if err := r.deleteGeneratedCronJob(ctx, target); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: ownedResourceReconcileAfter}, nil
	}
	if err := r.deleteGeneratedCronJob(ctx, target); err != nil {
		return ctrl.Result{}, err
	}
	result, err := r.reconcileSchedule(ctx, &target, r.now())
	if err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}

func (r *RsyncMachineReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock().UTC()
	}
	return time.Now().UTC()
}

func (r *RsyncMachineReconciler) reconcileSchedule(ctx context.Context, target *krmv1alpha1.RsyncMachine, now time.Time) (ctrl.Result, error) {
	schedule, err := cron.ParseStandard(target.Spec.Schedule)
	if err != nil {
		changed := setCondition(&target.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonInvalidSpec, fmt.Sprintf("spec.schedule is invalid: %v", err), target.Generation)
		if changed {
			if updateErr := r.Status().Update(ctx, target); updateErr != nil && !apierrors.IsNotFound(updateErr) {
				return ctrl.Result{}, fmt.Errorf("update invalid schedule condition: %w", updateErr)
			}
		}
		return ctrl.Result{RequeueAfter: ownedResourceReconcileAfter}, nil
	}
	scheduledAt, due := nextDueSchedule(schedule, target, now)
	if !due {
		next := schedule.Next(now)
		return ctrl.Result{RequeueAfter: requeueAfter(now, next)}, nil
	}
	if err := r.createScheduledBackupJob(ctx, target, scheduledAt); err != nil {
		return ctrl.Result{}, err
	}
	target.Status.LastScheduledAt = &metav1.Time{Time: scheduledAt}
	if err := r.Status().Update(ctx, target); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("update rsync machine last scheduled time: %w", err)
	}
	next := schedule.Next(scheduledAt)
	return ctrl.Result{RequeueAfter: requeueAfter(now, next)}, nil
}

func (r *RsyncMachineReconciler) createScheduledBackupJob(ctx context.Context, target *krmv1alpha1.RsyncMachine, scheduledAt time.Time) error {
	run := &krmv1alpha1.BackupJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: krmv1alpha1.SchemeGroupVersion.String(),
			Kind:       "BackupJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: target.Namespace,
			Name:      GeneratedScheduledBackupJobName(target.Name, scheduledAt),
			Labels: map[string]string{
				LabelName:    AppName,
				LabelMachine: target.Name,
				LabelRole:    RoleMachineTrigger,
			},
			Annotations: map[string]string{
				"krm.chirino.github.io/scheduled-at": scheduledAt.UTC().Format(time.RFC3339),
			},
		},
		Spec: krmv1alpha1.BackupJobSpec{
			MachineRef: krmv1alpha1.ObjectReference{
				Namespace: target.Namespace,
				Name:      target.Name,
			},
			Trigger: krmv1alpha1.BackupJobTriggerScheduled,
		},
	}
	if r.Scheme != nil {
		if err := ctrl.SetControllerReference(target, run, r.Scheme); err != nil {
			return fmt.Errorf("set scheduled backup job owner reference: %w", err)
		}
	}
	if err := r.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create scheduled backup job: %w", err)
	}
	return nil
}

func nextDueSchedule(schedule cron.Schedule, machine *krmv1alpha1.RsyncMachine, now time.Time) (time.Time, bool) {
	now = now.UTC()
	if machine.Status.LastScheduledAt != nil {
		next := schedule.Next(machine.Status.LastScheduledAt.Time.UTC())
		return next, !next.After(now)
	}
	start := machine.CreationTimestamp.Time.UTC()
	if start.IsZero() || start.Before(now.Add(-scheduleLookback)) {
		start = now.Add(-scheduleLookback)
	}
	cursor := start.Add(-time.Second)
	var last time.Time
	for i := 0; i < 10000; i++ {
		next := schedule.Next(cursor)
		if next.After(now) {
			return last, !last.IsZero()
		}
		last = next
		cursor = next
	}
	return last, !last.IsZero()
}

func requeueAfter(now, next time.Time) time.Duration {
	if next.IsZero() {
		return ownedResourceReconcileAfter
	}
	delay := next.Sub(now)
	if delay <= 0 {
		return time.Second
	}
	if delay > ownedResourceReconcileAfter {
		return ownedResourceReconcileAfter
	}
	return delay
}

func (r *RsyncMachineReconciler) deleteGeneratedCronJob(ctx context.Context, target krmv1alpha1.RsyncMachine) error {
	var cronJob batchv1.CronJob
	cronJobName := types.NamespacedName{Namespace: target.Namespace, Name: GeneratedCronJobName(target.Name)}
	if err := r.Get(ctx, cronJobName, &cronJob); err != nil {
		return client.IgnoreNotFound(err)
	}
	return client.IgnoreNotFound(r.Delete(ctx, &cronJob))
}

func (r *RsyncMachineReconciler) deleteScheduledBackupJobsForMachine(ctx context.Context, target krmv1alpha1.RsyncMachine) error {
	machineRef := types.NamespacedName{Namespace: target.Namespace, Name: target.Name}
	var runs krmv1alpha1.BackupJobList
	if err := r.List(ctx, &runs); err != nil {
		return fmt.Errorf("list scheduled backup jobs for rsync machine deletion: %w", err)
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if run.Spec.TriggerOrDefault() != krmv1alpha1.BackupJobTriggerScheduled {
			continue
		}
		runMachineRef, err := ResolveObjectReference(run.Spec.MachineRef, run.Namespace)
		if err != nil || runMachineRef != machineRef {
			continue
		}
		if err := r.Delete(ctx, run); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete scheduled backup job %s/%s: %w", run.Namespace, run.Name, err)
		}
	}
	return nil
}

func (r *RsyncMachineReconciler) validateRsyncMachineSources(ctx context.Context, machine krmv1alpha1.RsyncMachine) (metav1.ConditionStatus, string, string) {
	switch machine.Spec.Strategy.TypeOrDefault() {
	case krmv1alpha1.BackupStrategySnapshot, krmv1alpha1.BackupStrategyMirror:
	default:
		return metav1.ConditionFalse, ReasonInvalidSpec, fmt.Sprintf("spec.strategy.type %q is not supported", machine.Spec.Strategy.Type)
	}
	if machine.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror && !machine.Spec.Retention.Empty() {
		return metav1.ConditionFalse, ReasonInvalidSpec, "spec.retention must be empty when spec.strategy.type is Mirror"
	}
	var sourceList krmv1alpha1.BackupSourceList
	if err := r.List(ctx, &sourceList); err != nil {
		return metav1.ConditionUnknown, ReasonMissingReference, fmt.Sprintf("BackupSources could not be listed: %v", err)
	}
	machineRef := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
	found := false
	for _, source := range sourceList.Items {
		sourceRef := types.NamespacedName{Namespace: source.Namespace, Name: source.Name}
		sourceMachineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
		if err != nil {
			continue
		}
		if sourceMachineRef != machineRef {
			continue
		}
		if !SourceNamespaceAllowed(machine, source.Namespace) {
			continue
		}
		found = true
		if _, err := EffectiveDestinationPathForStrategy(machine, source); err != nil {
			return metav1.ConditionFalse, ReasonInvalidSpec, fmt.Sprintf("BackupSource %s has invalid destinationPath: %v", sourceRef.String(), err)
		}
	}
	if !found {
		return metav1.ConditionFalse, ReasonInvalidSpec, "at least one BackupSource must reference this RsyncMachine"
	}
	sourcesByRef := make(map[types.NamespacedName]krmv1alpha1.BackupSource, len(sourceList.Items))
	for _, source := range sourceList.Items {
		sourcesByRef[types.NamespacedName{Namespace: source.Namespace, Name: source.Name}] = source
	}
	if overlaps, err := DetectMirrorDestinationPathOverlaps(machine, sourcesByRef); err != nil {
		return metav1.ConditionFalse, ReasonInvalidSpec, err.Error()
	} else if len(overlaps) > 0 {
		return metav1.ConditionFalse, ReasonInvalidSpec, fmt.Sprintf("mirror target path overlap at %q with delete enabled for sources %s", overlaps[0].Path, namespacedNameList(overlaps[0].Sources))
	}
	return metav1.ConditionTrue, ReasonResolvedReferences, "RsyncMachine references resolved successfully"
}

func (r *RsyncMachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("rsyncmachine-controller")
	}
	if err := mgr.Add(&RsyncMachineScheduler{
		Client:     r.Client,
		Reconciler: r,
		Interval:   scheduleScanInterval,
	}); err != nil {
		return fmt.Errorf("add rsync machine scheduler: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&krmv1alpha1.RsyncMachine{}).
		Watches(&krmv1alpha1.BackupSource{}, handler.EnqueueRequestsFromMapFunc(machineForSource)).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.machinesForPVC)).
		Complete(r)
}

type RsyncMachineScheduler struct {
	client.Client
	Reconciler *RsyncMachineReconciler
	Interval   time.Duration
}

func (s *RsyncMachineScheduler) NeedLeaderElection() bool {
	return true
}

func (s *RsyncMachineScheduler) Start(ctx context.Context) error {
	if s == nil || s.Reconciler == nil {
		return fmt.Errorf("rsync machine scheduler requires a reconciler")
	}
	interval := s.Interval
	if interval <= 0 {
		interval = scheduleScanInterval
	}
	s.run(ctx)
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		s.run(ctx)
	}, interval)
	return nil
}

func (s *RsyncMachineScheduler) run(ctx context.Context) {
	var machines krmv1alpha1.RsyncMachineList
	if err := s.List(ctx, &machines); err != nil {
		return
	}
	for _, machine := range machines.Items {
		if machine.Spec.Schedule == "" || !machine.DeletionTimestamp.IsZero() {
			continue
		}
		_, _ = s.Reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}})
	}
}

func machineForSource(_ context.Context, obj client.Object) []reconcile.Request {
	source, ok := obj.(*krmv1alpha1.BackupSource)
	if !ok {
		return nil
	}
	machineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
	if err != nil {
		return nil
	}
	return []reconcile.Request{{NamespacedName: machineRef}}
}

func (r *RsyncMachineReconciler) machinesForPVC(ctx context.Context, obj client.Object) []reconcile.Request {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return nil
	}
	var machines krmv1alpha1.RsyncMachineList
	if err := r.List(ctx, &machines, client.InNamespace(pvc.Namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(machines.Items))
	for _, machine := range machines.Items {
		if machine.Spec.PVCName == pvc.Name {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}})
		}
	}
	return requests
}

type BackupSourceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

func (r *BackupSourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var source krmv1alpha1.BackupSource
	if err := r.Get(ctx, req.NamespacedName, &source); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	changed := false
	if source.Spec.PVC == "" {
		changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonInvalidSpec, "spec.pvc is required", source.Generation) || changed
		changed = setCondition(&source.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonInvalidSpec, "BackupSource PVC is not configured", source.Generation) || changed
	} else if machineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace); err != nil {
		changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonInvalidSpec, fmt.Sprintf("machineRef is invalid: %v", err), source.Generation) || changed
		changed = setCondition(&source.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonInvalidSpec, "BackupSource machine is not configured", source.Generation) || changed
	} else if _, err := EffectiveDestinationPath(source); err != nil {
		changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonInvalidSpec, fmt.Sprintf("destinationPath is invalid: %v", err), source.Generation) || changed
		changed = setCondition(&source.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonInvalidSpec, "BackupSource destinationPath is invalid", source.Generation) || changed
	} else {
		var machine krmv1alpha1.RsyncMachine
		if err := r.Get(ctx, machineRef, &machine); err != nil {
			if apierrors.IsNotFound(err) {
				changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonMissingReference, fmt.Sprintf("RsyncMachine %s was not found", machineRef.String()), source.Generation) || changed
				changed = setCondition(&source.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonMissingReference, fmt.Sprintf("RsyncMachine %s was not found", machineRef.String()), source.Generation) || changed
			} else {
				changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionUnknown, ReasonMissingReference, fmt.Sprintf("RsyncMachine %s could not be read: %v", machineRef.String(), err), source.Generation) || changed
			}
		} else if !SourceNamespaceAllowed(machine, source.Namespace) {
			changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonInvalidSpec, fmt.Sprintf("BackupSource namespace %s is not allowed to attach to RsyncMachine %s", source.Namespace, machineRef.String()), source.Generation) || changed
			changed = setCondition(&source.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonInvalidSpec, "BackupSource namespace is not allowed by the referenced RsyncMachine", source.Generation) || changed
		} else {
			var pvc corev1.PersistentVolumeClaim
			ref := types.NamespacedName{Namespace: source.Namespace, Name: source.Spec.PVC}
			if err := r.Get(ctx, ref, &pvc); err != nil {
				if apierrors.IsNotFound(err) {
					changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionFalse, ReasonMissingReference, fmt.Sprintf("PersistentVolumeClaim %s was not found", ref.String()), source.Generation) || changed
					changed = setCondition(&source.Status.Conditions, ConditionReady, metav1.ConditionFalse, ReasonMissingReference, fmt.Sprintf("PersistentVolumeClaim %s was not found", ref.String()), source.Generation) || changed
				} else {
					changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionUnknown, ReasonMissingReference, fmt.Sprintf("PersistentVolumeClaim %s could not be read: %v", ref.String(), err), source.Generation) || changed
				}
			} else {
				changed = setCondition(&source.Status.Conditions, ConditionValid, metav1.ConditionTrue, ReasonResolvedReferences, fmt.Sprintf("RsyncMachine %s and PersistentVolumeClaim %s are available", machineRef.String(), ref.String()), source.Generation) || changed
				changed = setCondition(&source.Status.Conditions, ConditionReady, metav1.ConditionTrue, ReasonResolvedReferences, "BackupSource is ready", source.Generation) || changed
			}
		}
	}
	if changed {
		if err := r.Status().Update(ctx, &source); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("update backup source conditions: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

func (r *BackupSourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("backupsource-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&krmv1alpha1.BackupSource{}).
		Watches(&krmv1alpha1.RsyncMachine{}, handler.EnqueueRequestsFromMapFunc(r.sourcesForMachine)).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.sourcesForPVC)).
		Complete(r)
}

func (r *BackupSourceReconciler) sourcesForMachine(ctx context.Context, obj client.Object) []reconcile.Request {
	machine, ok := obj.(*krmv1alpha1.RsyncMachine)
	if !ok {
		return nil
	}
	machineRef := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
	var sources krmv1alpha1.BackupSourceList
	if err := r.List(ctx, &sources); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(sources.Items))
	for _, source := range sources.Items {
		sourceMachineRef, err := ResolveObjectReference(source.Spec.MachineRef, source.Namespace)
		if err == nil && sourceMachineRef == machineRef {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: source.Namespace, Name: source.Name}})
		}
	}
	return requests
}

func (r *BackupSourceReconciler) sourcesForPVC(ctx context.Context, obj client.Object) []reconcile.Request {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return nil
	}
	var sources krmv1alpha1.BackupSourceList
	if err := r.List(ctx, &sources, client.InNamespace(pvc.Namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(sources.Items))
	for _, source := range sources.Items {
		if source.Spec.PVC == pvc.Name {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: source.Namespace, Name: source.Name}})
		}
	}
	return requests
}

func isTerminalPhase(phase krmv1alpha1.RunPhase) bool {
	return phase == krmv1alpha1.RunPhaseSucceeded || phase == krmv1alpha1.RunPhaseFailed || phase == krmv1alpha1.RunPhaseCanceled
}

func jobComplete(job batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailed(job batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func generatedJobStatus(job batchv1.Job) ptmmetrics.JobStatus {
	if jobComplete(job) {
		return ptmmetrics.JobStatusComplete
	}
	if jobFailed(job) {
		return ptmmetrics.JobStatusFailed
	}
	if job.Spec.Suspend != nil && *job.Spec.Suspend {
		return ptmmetrics.JobStatusSuspended
	}
	return ptmmetrics.JobStatusActive
}

func namespacedKey(namespace, name string) string {
	return types.NamespacedName{Namespace: namespace, Name: name}.String()
}

func (r *BackupJobReconciler) recordRunEvent(run *krmv1alpha1.BackupJob, eventType, reason, messageFmt string, args ...any) {
	if r == nil || r.Recorder == nil || run == nil {
		return
	}
	r.Recorder.Eventf(run, eventType, reason, messageFmt, args...)
}

func (r *RestoreJobReconciler) recordRunEvent(run *krmv1alpha1.RestoreJob, eventType, reason, messageFmt string, args ...any) {
	if r == nil || r.Recorder == nil || run == nil {
		return
	}
	r.Recorder.Eventf(run, eventType, reason, messageFmt, args...)
}
