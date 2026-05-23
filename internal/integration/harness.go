package integration

import (
	"context"
	"testing"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type Harness struct {
	T      *testing.T
	Ctx    context.Context
	Scheme *runtime.Scheme
	Client client.Client
	Image  string
}

func NewHarness(t *testing.T, objects ...client.Object) *Harness {
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
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&krmv1alpha1.BackupJob{}, &krmv1alpha1.RestoreJob{}, &krmv1alpha1.RsyncMachine{}, &batchv1.Job{}).
		WithObjects(objects...).
		Build()
	return &Harness{
		T:      t,
		Ctx:    context.Background(),
		Scheme: scheme,
		Client: c,
		Image:  "krm:test",
	}
}

func (h *Harness) ReconcileBackupJob(namespace, name string) {
	h.T.Helper()
	reconciler := controller.BackupJobReconciler{Client: h.Client, Scheme: h.Scheme, Image: h.Image}
	_, err := reconciler.Reconcile(h.Ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	if err != nil {
		h.T.Fatal(err)
	}
}

func (h *Harness) GetBackupJob(namespace, name string) krmv1alpha1.BackupJob {
	h.T.Helper()
	var run krmv1alpha1.BackupJob
	if err := h.Client.Get(h.Ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		h.T.Fatal(err)
	}
	return run
}

func (h *Harness) GetJob(namespace, name string) batchv1.Job {
	h.T.Helper()
	var job batchv1.Job
	if err := h.Client.Get(h.Ctx, types.NamespacedName{Namespace: namespace, Name: name}, &job); err != nil {
		h.T.Fatal(err)
	}
	return job
}
