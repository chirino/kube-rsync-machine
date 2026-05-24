package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (in *RsyncMachine) DeepCopyInto(out *RsyncMachine) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *RsyncMachine) DeepCopy() *RsyncMachine {
	if in == nil {
		return nil
	}
	out := new(RsyncMachine)
	in.DeepCopyInto(out)
	return out
}

func (in *RsyncMachine) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *RsyncMachineList) DeepCopyInto(out *RsyncMachineList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]RsyncMachine, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *RsyncMachineList) DeepCopy() *RsyncMachineList {
	if in == nil {
		return nil
	}
	out := new(RsyncMachineList)
	in.DeepCopyInto(out)
	return out
}

func (in *RsyncMachineList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *RsyncMachineSpec) DeepCopyInto(out *RsyncMachineSpec) {
	*out = *in
	if in.AllowedSourceNamespaces != nil {
		out.AllowedSourceNamespaces = make([]string, len(in.AllowedSourceNamespaces))
		copy(out.AllowedSourceNamespaces, in.AllowedSourceNamespaces)
	}
	if in.AllowedRestoreNamespaces != nil {
		out.AllowedRestoreNamespaces = make([]string, len(in.AllowedRestoreNamespaces))
		copy(out.AllowedRestoreNamespaces, in.AllowedRestoreNamespaces)
	}
	out.NodeSelector = copyStringMap(in.NodeSelector)
	if in.Affinity != nil {
		out.Affinity = in.Affinity.DeepCopy()
	}
	if in.Tolerations != nil {
		out.Tolerations = make([]corev1.Toleration, len(in.Tolerations))
		for i := range in.Tolerations {
			in.Tolerations[i].DeepCopyInto(&out.Tolerations[i])
		}
	}
	if in.TopologySpreadConstraints != nil {
		out.TopologySpreadConstraints = make([]corev1.TopologySpreadConstraint, len(in.TopologySpreadConstraints))
		for i := range in.TopologySpreadConstraints {
			in.TopologySpreadConstraints[i].DeepCopyInto(&out.TopologySpreadConstraints[i])
		}
	}
	if in.RuntimeClassName != nil {
		out.RuntimeClassName = new(string)
		*out.RuntimeClassName = *in.RuntimeClassName
	}
	if in.ImagePullSecrets != nil {
		out.ImagePullSecrets = make([]corev1.LocalObjectReference, len(in.ImagePullSecrets))
		copy(out.ImagePullSecrets, in.ImagePullSecrets)
	}
	if in.SecurityContext != nil {
		out.SecurityContext = in.SecurityContext.DeepCopy()
	}
	in.Resources.DeepCopyInto(&out.Resources)
}

func (in *RsyncMachineStatus) DeepCopyInto(out *RsyncMachineStatus) {
	*out = *in
	out.RestorePointsUpdatedAt = copyTime(in.RestorePointsUpdatedAt)
	out.LastScheduledAt = copyTime(in.LastScheduledAt)
	if in.RestorePoints != nil {
		out.RestorePoints = make([]RestorePoint, len(in.RestorePoints))
		for i := range in.RestorePoints {
			in.RestorePoints[i].DeepCopyInto(&out.RestorePoints[i])
		}
	}
	out.Conditions = copyConditions(in.Conditions)
}

func (in *RestorePoint) DeepCopyInto(out *RestorePoint) {
	*out = *in
	out.CreatedAt = copyTime(in.CreatedAt)
}

func (in *BackupSource) DeepCopyInto(out *BackupSource) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *BackupSource) DeepCopy() *BackupSource {
	if in == nil {
		return nil
	}
	out := new(BackupSource)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupSource) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *BackupSourceList) DeepCopyInto(out *BackupSourceList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]BackupSource, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *BackupSourceList) DeepCopy() *BackupSourceList {
	if in == nil {
		return nil
	}
	out := new(BackupSourceList)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupSourceList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *BackupSourceSpec) DeepCopyInto(out *BackupSourceSpec) {
	*out = *in
	in.Rsync.DeepCopyInto(&out.Rsync)
	out.NodeSelector = copyStringMap(in.NodeSelector)
	if in.Affinity != nil {
		out.Affinity = in.Affinity.DeepCopy()
	}
	if in.Tolerations != nil {
		out.Tolerations = make([]corev1.Toleration, len(in.Tolerations))
		for i := range in.Tolerations {
			in.Tolerations[i].DeepCopyInto(&out.Tolerations[i])
		}
	}
	if in.TopologySpreadConstraints != nil {
		out.TopologySpreadConstraints = make([]corev1.TopologySpreadConstraint, len(in.TopologySpreadConstraints))
		for i := range in.TopologySpreadConstraints {
			in.TopologySpreadConstraints[i].DeepCopyInto(&out.TopologySpreadConstraints[i])
		}
	}
	if in.RuntimeClassName != nil {
		out.RuntimeClassName = new(string)
		*out.RuntimeClassName = *in.RuntimeClassName
	}
	if in.ImagePullSecrets != nil {
		out.ImagePullSecrets = make([]corev1.LocalObjectReference, len(in.ImagePullSecrets))
		copy(out.ImagePullSecrets, in.ImagePullSecrets)
	}
	if in.SecurityContext != nil {
		out.SecurityContext = in.SecurityContext.DeepCopy()
	}
	in.Resources.DeepCopyInto(&out.Resources)
}

func (in *BackupSourceStatus) DeepCopyInto(out *BackupSourceStatus) {
	*out = *in
	out.Conditions = copyConditions(in.Conditions)
}

func (in *BackupJob) DeepCopyInto(out *BackupJob) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *BackupJob) DeepCopy() *BackupJob {
	if in == nil {
		return nil
	}
	out := new(BackupJob)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupJob) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *BackupJobList) DeepCopyInto(out *BackupJobList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]BackupJob, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *BackupJobList) DeepCopy() *BackupJobList {
	if in == nil {
		return nil
	}
	out := new(BackupJobList)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupJobList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *BackupJobSpec) DeepCopyInto(out *BackupJobSpec) {
	*out = *in
}

func (in *BackupJobStatus) DeepCopyInto(out *BackupJobStatus) {
	*out = *in
	out.StartedAt = copyTime(in.StartedAt)
	out.CompletedAt = copyTime(in.CompletedAt)
	if in.LastCommand != nil {
		out.LastCommand = new(CommandStatus)
		in.LastCommand.DeepCopyInto(out.LastCommand)
	}
	out.IncludedMachines = copyObjectReferences(in.IncludedMachines)
	if in.MergedInto != nil {
		out.MergedInto = new(ObjectReference)
		*out.MergedInto = *in.MergedInto
	}
	if in.Transfers != nil {
		out.Transfers = make([]TransferStatus, len(in.Transfers))
		for i := range in.Transfers {
			in.Transfers[i].DeepCopyInto(&out.Transfers[i])
		}
	}
	out.Conditions = copyConditions(in.Conditions)
}

func (in *TransferStatus) DeepCopyInto(out *TransferStatus) {
	*out = *in
	out.StartedAt = copyTime(in.StartedAt)
	out.CompletedAt = copyTime(in.CompletedAt)
	out.CaptureTime = copyTime(in.CaptureTime)
}

func (in *CommandStatus) DeepCopyInto(out *CommandStatus) {
	*out = *in
	out.SentAt = copyTime(in.SentAt)
	out.AcknowledgedAt = copyTime(in.AcknowledgedAt)
}

func (in *RestoreJob) DeepCopyInto(out *RestoreJob) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *RestoreJob) DeepCopy() *RestoreJob {
	if in == nil {
		return nil
	}
	out := new(RestoreJob)
	in.DeepCopyInto(out)
	return out
}

func (in *RestoreJob) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *RestoreJobList) DeepCopyInto(out *RestoreJobList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]RestoreJob, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *RestoreJobList) DeepCopy() *RestoreJobList {
	if in == nil {
		return nil
	}
	out := new(RestoreJobList)
	in.DeepCopyInto(out)
	return out
}

func (in *RestoreJobList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *RestoreJobSpec) DeepCopyInto(out *RestoreJobSpec) {
	*out = *in
	in.Overrides.DeepCopyInto(&out.Overrides)
	out.NodeSelector = copyStringMap(in.NodeSelector)
	if in.Affinity != nil {
		out.Affinity = in.Affinity.DeepCopy()
	}
	if in.Tolerations != nil {
		out.Tolerations = make([]corev1.Toleration, len(in.Tolerations))
		for i := range in.Tolerations {
			in.Tolerations[i].DeepCopyInto(&out.Tolerations[i])
		}
	}
	if in.TopologySpreadConstraints != nil {
		out.TopologySpreadConstraints = make([]corev1.TopologySpreadConstraint, len(in.TopologySpreadConstraints))
		for i := range in.TopologySpreadConstraints {
			in.TopologySpreadConstraints[i].DeepCopyInto(&out.TopologySpreadConstraints[i])
		}
	}
	if in.RuntimeClassName != nil {
		out.RuntimeClassName = new(string)
		*out.RuntimeClassName = *in.RuntimeClassName
	}
	if in.ImagePullSecrets != nil {
		out.ImagePullSecrets = make([]corev1.LocalObjectReference, len(in.ImagePullSecrets))
		copy(out.ImagePullSecrets, in.ImagePullSecrets)
	}
	if in.SecurityContext != nil {
		out.SecurityContext = in.SecurityContext.DeepCopy()
	}
	in.Resources.DeepCopyInto(&out.Resources)
}

func (in *RestoreOverrides) DeepCopyInto(out *RestoreOverrides) {
	*out = *in
	in.Rsync.DeepCopyInto(&out.Rsync)
}

func (in *RestoreJobStatus) DeepCopyInto(out *RestoreJobStatus) {
	*out = *in
	out.StartedAt = copyTime(in.StartedAt)
	out.CompletedAt = copyTime(in.CompletedAt)
	if in.Transfers != nil {
		out.Transfers = make([]TransferStatus, len(in.Transfers))
		for i := range in.Transfers {
			in.Transfers[i].DeepCopyInto(&out.Transfers[i])
		}
	}
	out.Conditions = copyConditions(in.Conditions)
}

func (in *RsyncOptions) DeepCopyInto(out *RsyncOptions) {
	*out = *in
	if in.Delete != nil {
		out.Delete = new(bool)
		*out.Delete = *in.Delete
	}
	if in.OneFileSystem != nil {
		out.OneFileSystem = new(bool)
		*out.OneFileSystem = *in.OneFileSystem
	}
}

func copyTime(in *metav1.Time) *metav1.Time {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyObjectReferences(in []ObjectReference) []ObjectReference {
	if in == nil {
		return nil
	}
	return append([]ObjectReference(nil), in...)
}

func copyConditions(in []Condition) []Condition {
	if in == nil {
		return nil
	}
	out := make([]Condition, len(in))
	for i := range in {
		in[i].DeepCopyInto(&out[i])
	}
	return out
}
