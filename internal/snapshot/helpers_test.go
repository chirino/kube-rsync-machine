package snapshot

import (
	"reflect"
	"strings"
	"testing"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func TestDecideCaptureMode(t *testing.T) {
	tests := []struct {
		name       string
		capture    krmv1alpha1.CaptureMode
		discovered bool
		want       CaptureDecision
	}{
		{
			name:       "default auto uses snapshot when available",
			discovered: true,
			want:       CaptureDecision{Requested: krmv1alpha1.CaptureModeAuto, Method: krmv1alpha1.CaptureModeVolumeSnapshot, Supported: true, Reason: "VolumeSnapshotDiscovered"},
		},
		{
			name:       "volume snapshot available",
			capture:    krmv1alpha1.CaptureModeVolumeSnapshot,
			discovered: true,
			want:       CaptureDecision{Requested: krmv1alpha1.CaptureModeVolumeSnapshot, Method: krmv1alpha1.CaptureModeVolumeSnapshot, Supported: true, Reason: "VolumeSnapshotDiscovered"},
		},
		{
			name:    "volume snapshot required but missing",
			capture: krmv1alpha1.CaptureModeVolumeSnapshot,
			want:    CaptureDecision{Requested: krmv1alpha1.CaptureModeVolumeSnapshot, Method: krmv1alpha1.CaptureModeVolumeSnapshot, Supported: false, Reason: "VolumeSnapshotAPINotDiscovered"},
		},
		{
			name:       "auto uses snapshot when available",
			capture:    krmv1alpha1.CaptureModeAuto,
			discovered: true,
			want:       CaptureDecision{Requested: krmv1alpha1.CaptureModeAuto, Method: krmv1alpha1.CaptureModeVolumeSnapshot, Supported: true, Reason: "VolumeSnapshotDiscovered"},
		},
		{
			name:    "auto falls back to direct",
			capture: krmv1alpha1.CaptureModeAuto,
			want:    CaptureDecision{Requested: krmv1alpha1.CaptureModeAuto, Method: krmv1alpha1.CaptureModeDirect, Supported: true, Fallback: true, Reason: "VolumeSnapshotAPINotDiscovered"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecideCaptureMode(krmv1alpha1.ConsistencyOptions{Capture: tt.capture}, tt.discovered)
			if got != tt.want {
				t.Fatalf("unexpected decision:\n got: %#v\nwant: %#v", got, tt.want)
			}
		})
	}
}

func TestBuildVolumeSnapshotUsesUnstructuredSnapshotAPI(t *testing.T) {
	run := backupRun("backup", "demo-20260520")
	source := backupSource("app-prod", "files", "data-pvc")
	source.Spec.Consistency.VolumeSnapshotClassName = "fast-snapshots"

	snapshot := BuildVolumeSnapshot(run, source)

	if snapshot.GetAPIVersion() != SnapshotAPIVersion || snapshot.GetKind() != VolumeSnapshotKind {
		t.Fatalf("unexpected GVK: %s %s", snapshot.GetAPIVersion(), snapshot.GetKind())
	}
	if snapshot.GetNamespace() != "app-prod" || snapshot.GetName() != "krm-vs-backup-demo-20260520-app-prod-files" {
		t.Fatalf("unexpected identity: %s/%s", snapshot.GetNamespace(), snapshot.GetName())
	}
	pvcName, found, err := unstructured.NestedString(snapshot.Object, "spec", "source", "persistentVolumeClaimName")
	if err != nil || !found || pvcName != "data-pvc" {
		t.Fatalf("unexpected pvc source: value=%q found=%v err=%v object=%#v", pvcName, found, err, snapshot.Object)
	}
	className, found, err := unstructured.NestedString(snapshot.Object, "spec", "volumeSnapshotClassName")
	if err != nil || !found || className != "fast-snapshots" {
		t.Fatalf("unexpected snapshot class: value=%q found=%v err=%v", className, found, err)
	}
	wantLabels := Labels(types.NamespacedName{Namespace: "backup", Name: "demo-20260520"}, types.NamespacedName{Namespace: "app-prod", Name: "files"}, RoleSourceVolumeSnapshot)
	if !reflect.DeepEqual(snapshot.GetLabels(), wantLabels) {
		t.Fatalf("unexpected labels:\n got: %#v\nwant: %#v", snapshot.GetLabels(), wantLabels)
	}
}

func TestBuildTemporaryPVCFromSnapshotCopiesSourceShape(t *testing.T) {
	run := backupRun("backup", "demo-20260520")
	source := backupSource("app-prod", "files", "data-pvc")
	storageClass := "fast"
	volumeMode := corev1.PersistentVolumeFilesystem
	sourcePVC := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app-prod", Name: "data-pvc"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			VolumeMode:       &volumeMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}
	restoreSize := resource.MustParse("12Gi")

	pvc := BuildTemporaryPVCFromSnapshot(run, source, sourcePVC, &restoreSize)

	if pvc.Namespace != "app-prod" || pvc.Name != "krm-vspvc-backup-demo-20260520-app-prod-files" {
		t.Fatalf("unexpected identity: %s/%s", pvc.Namespace, pvc.Name)
	}
	if pvc.Labels[LabelRole] != RoleSourceSnapshotPVC || pvc.Labels[LabelSource] != "app-prod-files" {
		t.Fatalf("unexpected labels: %#v", pvc.Labels)
	}
	if !reflect.DeepEqual(pvc.Spec.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) {
		t.Fatalf("unexpected access modes: %#v", pvc.Spec.AccessModes)
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast" {
		t.Fatalf("unexpected storage class: %#v", pvc.Spec.StorageClassName)
	}
	if pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		t.Fatalf("unexpected volume mode: %#v", pvc.Spec.VolumeMode)
	}
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(restoreSize) != 0 {
		t.Fatalf("unexpected storage request: %s", got.String())
	}
	if pvc.Spec.DataSource == nil {
		t.Fatalf("expected snapshot dataSource")
	}
	if pvc.Spec.DataSource.APIGroup == nil || *pvc.Spec.DataSource.APIGroup != SnapshotAPIGroup || pvc.Spec.DataSource.Kind != VolumeSnapshotKind {
		t.Fatalf("unexpected dataSource: %#v", pvc.Spec.DataSource)
	}
	if pvc.Spec.DataSource.Name != "krm-vs-backup-demo-20260520-app-prod-files" {
		t.Fatalf("unexpected dataSource name: %q", pvc.Spec.DataSource.Name)
	}
}

func TestVolumeSnapshotStatusHelpers(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"readyToUse":   true,
			"restoreSize":  "12Gi",
			"creationTime": "2026-05-20T12:00:00Z",
		},
	}}

	if !VolumeSnapshotReady(obj) {
		t.Fatal("expected snapshot ready")
	}
	if got := VolumeSnapshotRestoreSize(obj); got == nil || got.String() != "12Gi" {
		t.Fatalf("unexpected restore size: %v", got)
	}
	if got := VolumeSnapshotCreationTime(obj); got == nil || got.Format("2006-01-02T15:04:05Z") != "2026-05-20T12:00:00Z" {
		t.Fatalf("unexpected creation time: %v", got)
	}
}

func TestVolumeSnapshotFailureParsesStatusError(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"error": map[string]interface{}{
				"message": "snapshotter failed to create snapshot",
				"time":    "2026-05-20T12:01:00Z",
			},
		},
	}}

	failure, failed := VolumeSnapshotFailure(obj)
	if !failed {
		t.Fatal("expected snapshot failure")
	}
	if failure.Reason != "VolumeSnapshotError" || failure.Message != "snapshotter failed to create snapshot" {
		t.Fatalf("unexpected failure: %#v", failure)
	}
	if failure.Time == nil || failure.Time.Format("2006-01-02T15:04:05Z") != "2026-05-20T12:01:00Z" {
		t.Fatalf("unexpected failure time: %v", failure.Time)
	}
}

func TestGeneratedNamesAreDNSLabelsWithHashSuffix(t *testing.T) {
	run := types.NamespacedName{Namespace: "backup", Name: strings.Repeat("run-", 20)}
	sourceA := types.NamespacedName{Namespace: "app-prod", Name: strings.Repeat("source-a-", 12)}
	sourceB := types.NamespacedName{Namespace: "app-prod", Name: strings.Repeat("source-b-", 12)}

	nameA := GeneratedVolumeSnapshotName(run, sourceA)
	nameB := GeneratedVolumeSnapshotName(run, sourceB)
	if len(nameA) > 63 || len(nameB) > 63 {
		t.Fatalf("expected DNS label length, got %d and %d", len(nameA), len(nameB))
	}
	if nameA == nameB {
		t.Fatalf("expected hash suffix to avoid truncation collision: %q", nameA)
	}
	if strings.HasPrefix(nameA, "-") || strings.HasSuffix(nameA, "-") || strings.Contains(nameA, "_") {
		t.Fatalf("generated name is not a clean DNS label: %q", nameA)
	}
}

func backupRun(namespace, name string) krmv1alpha1.BackupJob {
	return krmv1alpha1.BackupJob{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

func backupSource(namespace, name, pvc string) krmv1alpha1.BackupSource {
	return krmv1alpha1.BackupSource{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: krmv1alpha1.BackupSourceSpec{
			PVC: pvc,
		},
	}
}
