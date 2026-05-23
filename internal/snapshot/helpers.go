package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

const (
	AppName = "kube-rsync-machine"

	SnapshotAPIGroup   = "snapshot.storage.k8s.io"
	SnapshotAPIVersion = SnapshotAPIGroup + "/v1"
	VolumeSnapshotKind = "VolumeSnapshot"

	LabelName         = "app.kubernetes.io/name"
	LabelRunNamespace = "krm.chirino.github.io/run-namespace"
	LabelRunKind      = "krm.chirino.github.io/run-kind"
	LabelRun          = "krm.chirino.github.io/run"
	LabelSource       = "krm.chirino.github.io/source"
	LabelRole         = "krm.chirino.github.io/resource-role"

	RunKindBackup = "backup"

	RoleSourceVolumeSnapshot = "source-volume-snapshot"
	RoleSourceSnapshotPVC    = "source-snapshot-pvc"
)

type CaptureDecision struct {
	Requested krmv1alpha1.CaptureMode
	Method    krmv1alpha1.CaptureMode
	Supported bool
	Fallback  bool
	Reason    string
}

type VolumeSnapshotFailureStatus struct {
	Reason  string
	Message string
	Time    *metav1.Time
}

func DecideCaptureMode(consistency krmv1alpha1.ConsistencyOptions, snapshotDiscovered bool) CaptureDecision {
	requested := consistency.CaptureOrDefault()
	switch requested {
	case krmv1alpha1.CaptureModeVolumeSnapshot:
		if snapshotDiscovered {
			return CaptureDecision{Requested: requested, Method: krmv1alpha1.CaptureModeVolumeSnapshot, Supported: true, Reason: "VolumeSnapshotDiscovered"}
		}
		return CaptureDecision{Requested: requested, Method: krmv1alpha1.CaptureModeVolumeSnapshot, Supported: false, Reason: "VolumeSnapshotAPINotDiscovered"}
	case krmv1alpha1.CaptureModeAuto:
		if snapshotDiscovered {
			return CaptureDecision{Requested: requested, Method: krmv1alpha1.CaptureModeVolumeSnapshot, Supported: true, Reason: "VolumeSnapshotDiscovered"}
		}
		return CaptureDecision{Requested: requested, Method: krmv1alpha1.CaptureModeDirect, Supported: true, Fallback: true, Reason: "VolumeSnapshotAPINotDiscovered"}
	default:
		return CaptureDecision{Requested: krmv1alpha1.CaptureModeDirect, Method: krmv1alpha1.CaptureModeDirect, Supported: true, Reason: "DirectCaptureRequested"}
	}
}

func BuildVolumeSnapshot(run krmv1alpha1.BackupJob, source krmv1alpha1.BackupSource) *unstructured.Unstructured {
	snapshot := &unstructured.Unstructured{}
	snapshot.SetAPIVersion(SnapshotAPIVersion)
	snapshot.SetKind(VolumeSnapshotKind)
	snapshot.SetNamespace(source.Namespace)
	snapshot.SetName(GeneratedVolumeSnapshotName(runRef(run), sourceRef(source)))
	snapshot.SetLabels(Labels(runRef(run), sourceRef(source), RoleSourceVolumeSnapshot))

	spec := map[string]interface{}{
		"source": map[string]interface{}{
			"persistentVolumeClaimName": source.Spec.PVC,
		},
	}
	if source.Spec.Consistency.VolumeSnapshotClassName != "" {
		spec["volumeSnapshotClassName"] = source.Spec.Consistency.VolumeSnapshotClassName
	}
	_ = unstructured.SetNestedMap(snapshot.Object, spec, "spec")
	return snapshot
}

func BuildTemporaryPVCFromSnapshot(run krmv1alpha1.BackupJob, source krmv1alpha1.BackupSource, sourcePVC corev1.PersistentVolumeClaim, restoreSize *resource.Quantity) *corev1.PersistentVolumeClaim {
	requests := corev1.ResourceList{}
	if storage, ok := sourcePVC.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		requests[corev1.ResourceStorage] = storage.DeepCopy()
	}
	if restoreSize != nil && !restoreSize.IsZero() {
		requests[corev1.ResourceStorage] = restoreSize.DeepCopy()
	}

	apiGroup := SnapshotAPIGroup
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: sourceObjectMeta(run, source, RoleSourceSnapshotPVC, GeneratedTemporaryPVCName(runRef(run), sourceRef(source))),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      append([]corev1.PersistentVolumeAccessMode(nil), sourcePVC.Spec.AccessModes...),
			StorageClassName: sourcePVC.Spec.StorageClassName,
			VolumeMode:       sourcePVC.Spec.VolumeMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: requests,
			},
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     VolumeSnapshotKind,
				Name:     GeneratedVolumeSnapshotName(runRef(run), sourceRef(source)),
			},
		},
	}
}

func VolumeSnapshotReady(snapshot *unstructured.Unstructured) bool {
	if snapshot == nil {
		return false
	}
	ready, found, err := unstructured.NestedBool(snapshot.Object, "status", "readyToUse")
	return err == nil && found && ready
}

func VolumeSnapshotRestoreSize(snapshot *unstructured.Unstructured) *resource.Quantity {
	if snapshot == nil {
		return nil
	}
	value, found, err := unstructured.NestedString(snapshot.Object, "status", "restoreSize")
	if err != nil || !found || value == "" {
		return nil
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return nil
	}
	return &quantity
}

func VolumeSnapshotCreationTime(snapshot *unstructured.Unstructured) *metav1.Time {
	if snapshot == nil {
		return nil
	}
	value, found, err := unstructured.NestedString(snapshot.Object, "status", "creationTime")
	if err != nil || !found || value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	metaTime := metav1.NewTime(parsed)
	return &metaTime
}

func VolumeSnapshotFailure(snapshot *unstructured.Unstructured) (VolumeSnapshotFailureStatus, bool) {
	if snapshot == nil {
		return VolumeSnapshotFailureStatus{}, false
	}
	if errObj, found, err := unstructured.NestedMap(snapshot.Object, "status", "error"); err == nil && found {
		failure := VolumeSnapshotFailureStatus{Reason: "VolumeSnapshotError"}
		if reason, ok := errObj["reason"].(string); ok && reason != "" {
			failure.Reason = reason
		}
		if message, ok := errObj["message"].(string); ok {
			failure.Message = message
		}
		if value, ok := errObj["time"].(string); ok && value != "" {
			if parsed, err := time.Parse(time.RFC3339, value); err == nil {
				metaTime := metav1.NewTime(parsed)
				failure.Time = &metaTime
			}
		}
		if failure.Message == "" {
			failure.Message = failure.Reason
		}
		return failure, true
	}
	conditions, found, err := unstructured.NestedSlice(snapshot.Object, "status", "conditions")
	if err != nil || !found {
		return VolumeSnapshotFailureStatus{}, false
	}
	for _, condition := range conditions {
		conditionObj, ok := condition.(map[string]interface{})
		if !ok || conditionObj["type"] != "Ready" || conditionObj["status"] != "False" {
			continue
		}
		failure := VolumeSnapshotFailureStatus{Reason: "VolumeSnapshotNotReady"}
		if reason, ok := conditionObj["reason"].(string); ok && reason != "" {
			failure.Reason = reason
		}
		if message, ok := conditionObj["message"].(string); ok {
			failure.Message = message
		}
		if failure.Message == "" {
			failure.Message = failure.Reason
		}
		return failure, true
	}
	return VolumeSnapshotFailureStatus{}, false
}

func GeneratedVolumeSnapshotName(run, source types.NamespacedName) string {
	return dnsLabel("krm-vs-" + runID(run) + "-" + source.Namespace + "-" + source.Name)
}

func GeneratedTemporaryPVCName(run, source types.NamespacedName) string {
	return dnsLabel("krm-vspvc-" + runID(run) + "-" + source.Namespace + "-" + source.Name)
}

func Labels(run, source types.NamespacedName, role string) map[string]string {
	return map[string]string{
		LabelName:         AppName,
		LabelRunNamespace: run.Namespace,
		LabelRunKind:      RunKindBackup,
		LabelRun:          run.Name,
		LabelSource:       dnsLabel(source.Namespace + "-" + source.Name),
		LabelRole:         role,
	}
}

func sourceObjectMeta(run krmv1alpha1.BackupJob, source krmv1alpha1.BackupSource, role, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Namespace: source.Namespace,
		Name:      name,
		Labels:    Labels(runRef(run), sourceRef(source), role),
	}
}

func runRef(run krmv1alpha1.BackupJob) types.NamespacedName {
	return types.NamespacedName{Namespace: run.Namespace, Name: run.Name}
}

func sourceRef(source krmv1alpha1.BackupSource) types.NamespacedName {
	return types.NamespacedName{Namespace: source.Namespace, Name: source.Name}
}

func runID(run types.NamespacedName) string {
	if run.Namespace == "" {
		return run.Name
	}
	return run.Namespace + "-" + run.Name
}

func dnsLabel(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "krm"
	}
	if len(out) <= 63 {
		return out
	}

	sum := sha256.Sum256([]byte(out))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := strings.TrimRight(out[:63-len(suffix)-1], "-")
	if prefix == "" {
		return suffix
	}
	return prefix + "-" + suffix
}
