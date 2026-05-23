package manager

import (
	"context"
	"fmt"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	"github.com/chirino/kube-rsync-machine/internal/controller"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ControlEventApplier struct {
	Client client.Client
	Hub    *control.EventHub
}

func (a *ControlEventApplier) Start(ctx context.Context) error {
	if a.Client == nil {
		return fmt.Errorf("client is required")
	}
	if a.Hub == nil {
		return fmt.Errorf("control event hub is required")
	}
	events, err := a.Hub.SubscribeAll(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return nil
			}
			if err := a.apply(ctx, event); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (a *ControlEventApplier) apply(ctx context.Context, event control.ControlEvent) error {
	switch event.Kind {
	case control.EventKindTarget:
		if event.Target == nil {
			return nil
		}
		if err := a.applyTargetEventToRun(ctx, *event.Target); err != nil {
			return err
		}
		return a.applyTargetEventToTarget(ctx, *event.Target)
	case control.EventKindTargetCommandAck:
		if event.CommandAck == nil {
			return nil
		}
		return a.applyTargetCommandAckToRun(ctx, *event.CommandAck)
	case control.EventKindSource:
		if event.Source == nil {
			return nil
		}
		switch event.Source.RunKind {
		case control.RunKindBackup:
			return a.applySourceEventToBackupJob(ctx, *event.Source)
		case control.RunKindRestore:
			return a.applySourceEventToRestoreJob(ctx, *event.Source)
		default:
			return nil
		}
	default:
		return nil
	}
}

func (a *ControlEventApplier) applyTargetEventToRun(ctx context.Context, event control.TargetEvent) error {
	if event.RunKind != control.RunKindBackup {
		return nil
	}
	key := types.NamespacedName{Namespace: event.RunNamespace, Name: event.RunName}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var run krmv1alpha1.BackupJob
		if err := a.Client.Get(ctx, key, &run); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !controller.ApplyTargetEventToBackupJob(&run, event) {
			return nil
		}
		return a.Client.Status().Update(ctx, &run)
	})
}

func (a *ControlEventApplier) applyTargetCommandAckToRun(ctx context.Context, event control.TargetCommandAckEvent) error {
	if event.RunKind != control.RunKindBackup {
		return nil
	}
	key := types.NamespacedName{Namespace: event.RunNamespace, Name: event.RunName}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var run krmv1alpha1.BackupJob
		if err := a.Client.Get(ctx, key, &run); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !controller.ApplyTargetCommandAckToBackupJob(&run, event) {
			return nil
		}
		return a.Client.Status().Update(ctx, &run)
	})
}

func (a *ControlEventApplier) applySourceEventToBackupJob(ctx context.Context, event control.SourceEvent) error {
	if !shouldPersistSourceEvent(event) {
		return nil
	}
	key := types.NamespacedName{Namespace: event.RunNamespace, Name: event.RunName}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var run krmv1alpha1.BackupJob
		if err := a.Client.Get(ctx, key, &run); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !controller.ApplySourceEventToBackupJob(&run, event) {
			return nil
		}
		return a.Client.Status().Update(ctx, &run)
	})
}

func (a *ControlEventApplier) applySourceEventToRestoreJob(ctx context.Context, event control.SourceEvent) error {
	if !shouldPersistSourceEvent(event) {
		return nil
	}
	key := types.NamespacedName{Namespace: event.RunNamespace, Name: event.RunName}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var run krmv1alpha1.RestoreJob
		if err := a.Client.Get(ctx, key, &run); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !controller.ApplySourceEventToRestoreJob(&run, event) {
			return nil
		}
		return a.Client.Status().Update(ctx, &run)
	})
}

func shouldPersistSourceEvent(event control.SourceEvent) bool {
	return event.StartedAt != "" || event.CompletedAt != "" || event.Phase != "Running"
}

func (a *ControlEventApplier) applyTargetEventToTarget(ctx context.Context, event control.TargetEvent) error {
	key := types.NamespacedName{Namespace: event.TargetNamespace, Name: event.TargetName}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var target krmv1alpha1.RsyncMachine
		if err := a.Client.Get(ctx, key, &target); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !controller.ApplyTargetEventToRsyncMachine(&target, event) {
			return nil
		}
		return a.Client.Status().Update(ctx, &target)
	})
}
