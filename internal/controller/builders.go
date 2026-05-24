package controller

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	AppName = "kube-rsync-machine"

	LabelName         = "app.kubernetes.io/name"
	LabelMachine      = "krm.chirino.github.io/machine"
	LabelRunNamespace = "krm.chirino.github.io/run-namespace"
	LabelRunKind      = "krm.chirino.github.io/run-kind"
	LabelRun          = "krm.chirino.github.io/run"
	LabelSource       = "krm.chirino.github.io/source"
	LabelRole         = "krm.chirino.github.io/resource-role"

	RoleMachineTrigger = "machine-trigger"
	RoleTargetServer   = "target-server"
	RoleSourceSender   = "source-sender"
	RoleRestoreWriter  = "restore-writer"
	RoleTLSSecret      = "tls-secret"

	DefaultImage           = "ghcr.io/chirino/kube-rsync-machine:latest"
	TLSMountPath           = "/var/run/kube-rsync-machine/tls"
	ControlGRPCEndpoint    = "kube-rsync-machine-controller-manager.kube-rsync-machine-operator.svc:8083"
	ControlGRPCNamespace   = "kube-rsync-machine-operator"
	ControlGRPCService     = "kube-rsync-machine-controller-manager"
	RsyncPort              = int32(873)
	DefaultJobBackoffLimit = int32(0)

	AnnotationTestTargetEmptyDirSizeLimit = "krm.chirino.github.io/test-target-empty-dir-size-limit"
	AnnotationTestTargetSeedSnapshotBytes = "krm.chirino.github.io/test-target-seed-snapshot-bytes"
	AnnotationTestSourceBackoffLimit      = "krm.chirino.github.io/test-source-backoff-limit"
	AnnotationTestRecoveryMinAvailable    = "krm.chirino.github.io/test-recovery-min-available"
)

type DataPlaneControlOptions struct {
	GRPCEndpoint  string
	GRPCNamespace string
}

func targetPodSpec(target krmv1alpha1.RsyncMachine, spec corev1.PodSpec) corev1.PodSpec {
	spec.NodeSelector = target.Spec.NodeSelector
	spec.Affinity = target.Spec.Affinity
	spec.Tolerations = target.Spec.Tolerations
	spec.TopologySpreadConstraints = target.Spec.TopologySpreadConstraints
	spec.SchedulerName = target.Spec.SchedulerName
	spec.PriorityClassName = target.Spec.PriorityClassName
	spec.RuntimeClassName = target.Spec.RuntimeClassName
	spec.ImagePullSecrets = target.Spec.ImagePullSecrets
	spec.SecurityContext = dataPlanePodSecurityContext(target)
	return spec
}

func dataPlanePodSecurityContext(target krmv1alpha1.RsyncMachine) *corev1.PodSecurityContext {
	if target.Spec.SecurityContext != nil {
		return target.Spec.SecurityContext
	}
	return &corev1.PodSecurityContext{
		RunAsUser:  int64Ptr(0),
		RunAsGroup: int64Ptr(0),
	}
}

func int64Ptr(value int64) *int64 {
	return &value
}

func restoreWriterPodSpec(restore krmv1alpha1.RestoreJob, spec corev1.PodSpec) corev1.PodSpec {
	spec.NodeSelector = restore.Spec.NodeSelector
	spec.Affinity = restore.Spec.Affinity
	spec.Tolerations = restore.Spec.Tolerations
	spec.TopologySpreadConstraints = restore.Spec.TopologySpreadConstraints
	spec.SchedulerName = restore.Spec.SchedulerName
	spec.PriorityClassName = restore.Spec.PriorityClassName
	spec.RuntimeClassName = restore.Spec.RuntimeClassName
	spec.ImagePullSecrets = restore.Spec.ImagePullSecrets
	spec.SecurityContext = restoreWriterPodSecurityContext(restore)
	return spec
}

func restoreWriterPodSecurityContext(restore krmv1alpha1.RestoreJob) *corev1.PodSecurityContext {
	if restore.Spec.SecurityContext != nil {
		return restore.Spec.SecurityContext
	}
	return &corev1.PodSecurityContext{
		RunAsUser:  int64Ptr(0),
		RunAsGroup: int64Ptr(0),
	}
}

func sourceSenderPodSpec(source krmv1alpha1.BackupSource, spec corev1.PodSpec) corev1.PodSpec {
	spec.NodeSelector = source.Spec.NodeSelector
	spec.Affinity = source.Spec.Affinity
	spec.Tolerations = source.Spec.Tolerations
	spec.TopologySpreadConstraints = source.Spec.TopologySpreadConstraints
	spec.SchedulerName = source.Spec.SchedulerName
	spec.PriorityClassName = source.Spec.PriorityClassName
	spec.RuntimeClassName = source.Spec.RuntimeClassName
	spec.ImagePullSecrets = source.Spec.ImagePullSecrets
	spec.SecurityContext = sourceSenderPodSecurityContext(source)
	return spec
}

func sourceSenderPodSecurityContext(source krmv1alpha1.BackupSource) *corev1.PodSecurityContext {
	if source.Spec.SecurityContext != nil {
		return source.Spec.SecurityContext
	}
	return &corev1.PodSecurityContext{
		RunAsUser:  int64Ptr(0),
		RunAsGroup: int64Ptr(0),
	}
}

func machineImage(target krmv1alpha1.RsyncMachine, fallback string) string {
	if target.Spec.Image != "" {
		return target.Spec.Image
	}
	if fallback != "" {
		return fallback
	}
	return DefaultImage
}

func applyMachineResources(target krmv1alpha1.RsyncMachine, containers []corev1.Container) []corev1.Container {
	for i := range containers {
		containers[i].Resources = target.Spec.Resources
	}
	return containers
}

func DefaultDataPlaneControlOptions(namespace string) DataPlaneControlOptions {
	if namespace == "" {
		namespace = ControlGRPCNamespace
	}
	return DataPlaneControlOptions{
		GRPCEndpoint:  fmt.Sprintf("%s.%s.svc:8083", ControlGRPCService, namespace),
		GRPCNamespace: namespace,
	}
}

func (o DataPlaneControlOptions) withDefaults() DataPlaneControlOptions {
	if o.GRPCNamespace == "" {
		o.GRPCNamespace = ControlGRPCNamespace
	}
	if o.GRPCEndpoint == "" {
		o.GRPCEndpoint = DefaultDataPlaneControlOptions(o.GRPCNamespace).GRPCEndpoint
	}
	return o
}

func appendControlGRPCArgs(args []string, opts DataPlaneControlOptions) []string {
	opts = opts.withDefaults()
	return append(args,
		"--control-grpc-endpoint", opts.GRPCEndpoint,
		"--control-grpc-namespace", opts.GRPCNamespace,
	)
}

func jobBackoffLimit() *int32 {
	value := DefaultJobBackoffLimit
	return &value
}

func BuildServeTargetJob(run krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine, runSet TargetRunSet, image, timestamp string) (*batchv1.Job, error) {
	return BuildServeTargetJobWithControl(run, target, runSet, image, timestamp, DataPlaneControlOptions{})
}

func BuildServeTargetJobWithControl(run krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine, runSet TargetRunSet, image, timestamp string, controlOptions DataPlaneControlOptions) (*batchv1.Job, error) {
	image = machineImage(target, image)
	runID := RunID(run)
	args := []string{
		"serve-target",
		"--target", krmv1alpha1.DefaultRsyncMachineMountPath,
		"--run-id", runID,
		"--tls-dir", TLSMountPath,
		"--run-namespace", run.Namespace,
		"--run-name", run.Name,
		"--target-namespace", target.Namespace,
		"--target-name", target.Name,
	}
	args = appendControlGRPCArgs(args, controlOptions)
	if timestamp != "" && target.Spec.Strategy.TypeOrDefault() != krmv1alpha1.BackupStrategyMirror {
		args = append(args, "--timestamp", timestamp)
	}
	if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror {
		args = append(args, "--strategy", "mirror")
	} else {
		args = appendRetentionArgs(args, runSet.Retention)
	}
	for _, source := range runSet.Sources {
		destination := source.EffectiveDestinationPath
		if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror && destination == "" {
			destination = "."
		}
		args = append(args, "--source", fmt.Sprintf("%s/%s=%s", source.Source.Namespace, source.Source.Name, destination))
	}
	labels := runLabels(run, runKindBackup, RoleTargetServer)
	targetVolumeSource := corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: target.Spec.PVCName,
		},
	}
	if limit := target.Annotations[AnnotationTestTargetEmptyDirSizeLimit]; limit != "" {
		quantity, err := resource.ParseQuantity(limit)
		if err != nil {
			return nil, fmt.Errorf("invalid %s annotation: %w", AnnotationTestTargetEmptyDirSizeLimit, err)
		}
		targetVolumeSource = corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: &quantity,
			},
		}
	}
	initContainers, err := targetInitContainers(target, image)
	if err != nil {
		return nil, err
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: target.Namespace,
			Name:      GeneratedJobName("target", run.Name),
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: jobBackoffLimit(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: targetPodSpec(target, corev1.PodSpec{
					RestartPolicy:  corev1.RestartPolicyNever,
					InitContainers: initContainers,
					Containers: applyMachineResources(target, []corev1.Container{{
						Name:  "serve-target",
						Image: image,
						Args:  args,
						Ports: []corev1.ContainerPort{{
							Name:          "rsync",
							ContainerPort: RsyncPort,
						}},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "target",
							MountPath: krmv1alpha1.DefaultRsyncMachineMountPath,
						}, {
							Name:      "tls",
							MountPath: TLSMountPath,
							ReadOnly:  true,
						}},
					}}),
					Volumes: []corev1.Volume{{
						Name:         "target",
						VolumeSource: targetVolumeSource,
					}, {
						Name: "tls",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: GeneratedTLSSecretName(RunID(run), RoleTargetServer, target.Namespace, target.Name),
							},
						},
					}},
				}),
			},
		},
	}, nil
}

func BuildTargetService(run krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine) *corev1.Service {
	labels := runLabels(run, runKindBackup, RoleTargetServer)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: target.Namespace,
			Name:      GeneratedServiceName("target", run.Name),
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name: "rsync",
				Port: RsyncPort,
			}},
		},
	}
}

func targetInitContainers(target krmv1alpha1.RsyncMachine, image string) ([]corev1.Container, error) {
	value := target.Annotations[AnnotationTestTargetSeedSnapshotBytes]
	if value == "" {
		return nil, nil
	}
	bytes, err := strconv.ParseInt(value, 10, 64)
	if err != nil || bytes < 0 {
		return nil, fmt.Errorf("invalid %s annotation %q", AnnotationTestTargetSeedSnapshotBytes, value)
	}
	if bytes == 0 {
		return nil, nil
	}
	seedPath := krmv1alpha1.DefaultRsyncMachineMountPath + "/hourly/2000-01-01T00-00-00Z/seed.bin"
	command := fmt.Sprintf("mkdir -p %q && head -c %d /dev/zero > %q", path.Dir(seedPath), bytes, seedPath)
	return applyMachineResources(target, []corev1.Container{{
		Name:    "seed-old-snapshot",
		Image:   image,
		Command: []string{"/bin/sh", "-c", command},
		VolumeMounts: []corev1.VolumeMount{{
			Name:      "target",
			MountPath: krmv1alpha1.DefaultRsyncMachineMountPath,
		}},
	}}), nil
}

type SourceJobOptions struct {
	TargetPath string
	ClaimName  string
}

func BuildSendSourceJob(run krmv1alpha1.BackupJob, source krmv1alpha1.BackupSource, target krmv1alpha1.RsyncMachine, opts SourceJobOptions, image string) (*batchv1.Job, error) {
	return BuildSendSourceJobWithControl(run, source, target, opts, image, DataPlaneControlOptions{})
}

func BuildSendSourceJobWithControl(run krmv1alpha1.BackupJob, source krmv1alpha1.BackupSource, target krmv1alpha1.RsyncMachine, opts SourceJobOptions, image string, controlOptions DataPlaneControlOptions) (*batchv1.Job, error) {
	image = machineImage(target, image)
	targetPath := opts.TargetPath
	if targetPath == "" {
		partialPath, err := TransferDestinationPath(target, RunID(run), source)
		if err != nil {
			return nil, err
		}
		targetPath = partialPath
	}
	claimName := opts.ClaimName
	if claimName == "" {
		claimName = source.Spec.PVC
	}
	labels := runLabels(run, runKindBackup, RoleSourceSender)
	labels[LabelSource] = labelValue(source.Namespace, source.Name)
	args := []string{
		"send-source",
		"--source", path.Join("/source", strings.TrimPrefix(source.Spec.SourcePathOrDefault(), "/")),
		"--target", targetPath,
		"--target-endpoint", TargetServiceEndpoint(run, target),
		"--tls-dir", TLSMountPath,
		"--run-namespace", run.Namespace,
		"--run-name", run.Name,
		"--source-namespace", source.Namespace,
		"--source-name", source.Name,
		"--target-namespace", target.Namespace,
		"--target-name", target.Name,
	}
	args = appendControlGRPCArgs(args, controlOptions)
	args = appendRsyncArgs(args, source.Spec.Rsync)
	backoffLimit := DefaultJobBackoffLimit
	if value := source.Annotations[AnnotationTestSourceBackoffLimit]; value != "" {
		parsed, err := strconv.ParseInt(value, 10, 32)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("invalid %s annotation %q", AnnotationTestSourceBackoffLimit, value)
		}
		backoffLimit = int32(parsed)
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: source.Namespace,
			Name:      GeneratedJobName("source-"+source.Name, run.Name),
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: sourceSenderPodSpec(source, corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:      "send-source",
						Image:     image,
						Args:      args,
						Resources: source.Spec.Resources,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "source",
							MountPath: "/source",
							ReadOnly:  true,
						}, {
							Name:      "tls",
							MountPath: TLSMountPath,
							ReadOnly:  true,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "source",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: claimName,
								ReadOnly:  true,
							},
						},
					}, {
						Name: "tls",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: GeneratedTLSSecretName(RunID(run), RoleSourceSender, source.Namespace, source.Name),
							},
						},
					}},
				}),
			},
		},
	}, nil
}

func TargetServiceEndpoint(run krmv1alpha1.BackupJob, target krmv1alpha1.RsyncMachine) string {
	return fmt.Sprintf("%s.%s.svc:%d", GeneratedServiceName("target", run.Name), target.Namespace, RsyncPort)
}

func RestoreTargetServiceEndpoint(restore krmv1alpha1.RestoreJob, target krmv1alpha1.RsyncMachine) string {
	return fmt.Sprintf("%s.%s.svc:%d", GeneratedServiceName("restore-target", restore.Name), target.Namespace, RsyncPort)
}

func BuildRestoreTargetJob(restore krmv1alpha1.RestoreJob, source krmv1alpha1.BackupSource, target krmv1alpha1.RsyncMachine, image string) (*batchv1.Job, error) {
	return BuildRestoreTargetJobWithControl(restore, source, target, image, DataPlaneControlOptions{})
}

func BuildRestoreTargetJobWithControl(restore krmv1alpha1.RestoreJob, source krmv1alpha1.BackupSource, target krmv1alpha1.RsyncMachine, image string, controlOptions DataPlaneControlOptions) (*batchv1.Job, error) {
	image = machineImage(target, image)
	effectivePath, err := EffectiveDestinationPathForStrategy(target, source)
	if err != nil {
		return nil, err
	}
	snapshot := restore.Status.RestoredSnapshot
	if snapshot == "" {
		snapshot = restore.Spec.SnapshotOrDefault()
	}
	if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror && snapshot == krmv1alpha1.DefaultSnapshot {
		snapshot = krmv1alpha1.DefaultMirrorSnapshot
	}
	restoreSource := effectivePath
	if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror && restoreSource == "" {
		restoreSource = "."
	}
	labels := runLabelsForRef(types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}, runKindRestore, RoleTargetServer)
	args := []string{
		"serve-target",
		"--target", krmv1alpha1.DefaultRsyncMachineMountPath,
		"--tls-dir", TLSMountPath,
		"--run-namespace", restore.Namespace,
		"--run-name", restore.Name,
		"--target-namespace", target.Namespace,
		"--target-name", target.Name,
		"--restore-snapshot", snapshot,
		"--restore-source", restoreSource,
		"--restore-writer", fmt.Sprintf("%s/%s", source.Namespace, source.Name),
	}
	args = appendControlGRPCArgs(args, controlOptions)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: target.Namespace,
			Name:      GeneratedJobName("restore-target", restore.Name),
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: jobBackoffLimit(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: targetPodSpec(target, corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: applyMachineResources(target, []corev1.Container{{
						Name:  "serve-target",
						Image: image,
						Args:  args,
						Ports: []corev1.ContainerPort{{
							Name:          "rsync",
							ContainerPort: RsyncPort,
						}},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "target",
							MountPath: krmv1alpha1.DefaultRsyncMachineMountPath,
							ReadOnly:  true,
						}, {
							Name:      "tls",
							MountPath: TLSMountPath,
							ReadOnly:  true,
						}},
					}}),
					Volumes: []corev1.Volume{{
						Name: "target",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: target.Spec.PVCName,
								ReadOnly:  true,
							},
						},
					}, {
						Name: "tls",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: GeneratedTLSSecretName(RunIDForRef(types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}), RoleTargetServer, target.Namespace, target.Name),
							},
						},
					}},
				}),
			},
		},
	}, nil
}

func BuildRestoreTargetService(restore krmv1alpha1.RestoreJob, target krmv1alpha1.RsyncMachine) *corev1.Service {
	labels := runLabelsForRef(types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}, runKindRestore, RoleTargetServer)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: target.Namespace,
			Name:      GeneratedServiceName("restore-target", restore.Name),
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name: "rsync",
				Port: RsyncPort,
			}},
		},
	}
}

func BuildRestoreJob(restore krmv1alpha1.RestoreJob, source krmv1alpha1.BackupSource, target krmv1alpha1.RsyncMachine, image string) (*batchv1.Job, error) {
	return BuildRestoreJobWithControl(restore, source, target, image, DataPlaneControlOptions{})
}

func BuildRestoreJobWithControl(restore krmv1alpha1.RestoreJob, source krmv1alpha1.BackupSource, target krmv1alpha1.RsyncMachine, image string, controlOptions DataPlaneControlOptions) (*batchv1.Job, error) {
	image = machineImage(target, image)
	destinationNamespace := restore.Spec.Overrides.Destination.Namespace
	if destinationNamespace == "" {
		destinationNamespace = source.Namespace
	}
	destinationPVC := restore.Spec.Overrides.Destination.PVCName
	if destinationPVC == "" {
		destinationPVC = source.Spec.PVC
	}
	destinationPath := restore.Spec.Overrides.Destination.Path
	if destinationPath == "" {
		destinationPath = source.Spec.SourcePathOrDefault()
	}
	effectivePath, err := EffectiveDestinationPathForStrategy(target, source)
	if err != nil {
		return nil, err
	}
	snapshot := restore.Status.RestoredSnapshot
	if snapshot == "" {
		snapshot = restore.Spec.SnapshotOrDefault()
	}
	if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror && snapshot == krmv1alpha1.DefaultSnapshot {
		snapshot = krmv1alpha1.DefaultMirrorSnapshot
	}
	snapshotSource := effectivePath
	if target.Spec.Strategy.TypeOrDefault() == krmv1alpha1.BackupStrategyMirror && snapshotSource == "" {
		snapshotSource = "."
	}
	labels := runLabelsForRef(types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}, runKindRestore, RoleRestoreWriter)
	labels[LabelSource] = labelValue(source.Namespace, source.Name)
	args := []string{
		"restore",
		"--snapshot", snapshot,
		"--snapshot-source", snapshotSource,
		"--target-endpoint", RestoreTargetServiceEndpoint(restore, target),
		"--destination", path.Join("/restore", strings.TrimPrefix(destinationPath, "/")),
		"--tls-dir", TLSMountPath,
		"--run-namespace", restore.Namespace,
		"--run-name", restore.Name,
		"--source-namespace", source.Namespace,
		"--source-name", source.Name,
		"--target-namespace", target.Namespace,
		"--target-name", target.Name,
	}
	args = appendControlGRPCArgs(args, controlOptions)
	rsync := source.Spec.Rsync
	if restore.Spec.Overrides.Rsync.HasOverrides() {
		rsync = restore.Spec.Overrides.Rsync
	}
	args = appendRsyncArgs(args, rsync)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: destinationNamespace,
			Name:      GeneratedJobName("restore", restore.Name),
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: jobBackoffLimit(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: restoreWriterPodSpec(restore, corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:      "restore",
						Image:     image,
						Args:      args,
						Resources: restore.Spec.Resources,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "destination",
							MountPath: "/restore",
						}, {
							Name:      "tls",
							MountPath: TLSMountPath,
							ReadOnly:  true,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "destination",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: destinationPVC,
							},
						},
					}, {
						Name: "tls",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: GeneratedTLSSecretName(RunIDForRef(types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}), RoleRestoreWriter, source.Namespace, source.Name),
							},
						},
					}},
				}),
			},
		},
	}, nil
}

func GeneratedCronJobName(planName string) string {
	return dnsLabel("krm-" + planName)
}

func GeneratedScheduledBackupJobName(machineName string, scheduledAt time.Time) string {
	timestamp := scheduledAt.UTC().Format("20060102-1504")
	prefix := dnsLabel(machineName)
	maxPrefix := 63 - len("krm--") - len(timestamp)
	if len(prefix) > maxPrefix {
		prefix = strings.TrimRight(prefix[:maxPrefix], "-")
	}
	if prefix == "" {
		prefix = "machine"
	}
	return "krm-" + prefix + "-" + timestamp
}

func GeneratedJobName(prefix, runName string) string {
	return dnsLabel("krm-" + prefix + "-" + runName)
}

func GeneratedServiceName(prefix, runName string) string {
	return dnsLabel("krm-" + prefix + "-" + runName)
}

func RunID(run krmv1alpha1.BackupJob) string {
	return RunIDForRef(types.NamespacedName{Namespace: run.Namespace, Name: run.Name})
}

func RunIDForRef(ref types.NamespacedName) string {
	if ref.Namespace == "" {
		return ref.Name
	}
	return ref.Namespace + "-" + ref.Name
}

func GeneratedTLSSecretName(runID, role, namespace, name string) string {
	return dnsLabel("krm-tls-" + runID + "-" + role + "-" + namespace + "-" + name)
}

func BuildTLSSecret(namespace, name string, labels map[string]string, bundle tlsutil.Bundle) *corev1.Secret {
	secretLabels := map[string]string{LabelName: AppName, LabelRole: RoleTLSSecret}
	for key, value := range labels {
		secretLabels[key] = value
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    secretLabels,
		},
		Type: corev1.SecretTypeTLS,
		Data: bundle.SecretData(),
	}
}

func appendRetentionArgs(args []string, retention krmv1alpha1.RetentionPolicy) []string {
	if retention.Hourly > 0 {
		args = append(args, "--retention-hourly", fmt.Sprint(retention.Hourly))
	}
	if retention.Daily > 0 {
		args = append(args, "--retention-daily", fmt.Sprint(retention.Daily))
	}
	if retention.Weekly > 0 {
		args = append(args, "--retention-weekly", fmt.Sprint(retention.Weekly))
	}
	if retention.Monthly > 0 {
		args = append(args, "--retention-monthly", fmt.Sprint(retention.Monthly))
	}
	return args
}

func appendRsyncArgs(args []string, options krmv1alpha1.RsyncOptions) []string {
	if options.DeleteOrDefault() {
		args = append(args, "--delete")
	}
	if options.OneFileSystemOrDefault() {
		args = append(args, "--one-file-system")
	}
	return args
}

func runLabels(run krmv1alpha1.BackupJob, runKind, role string) map[string]string {
	return runLabelsForRef(types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, runKind, role)
}

func runLabelsForRef(ref types.NamespacedName, runKind, role string) map[string]string {
	return map[string]string{
		LabelName:         AppName,
		LabelRunNamespace: ref.Namespace,
		LabelRunKind:      runKind,
		LabelRun:          ref.Name,
		LabelRole:         role,
	}
}

func labelValue(parts ...string) string {
	return dnsLabel(strings.Join(parts, "-"))
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
	if len(out) > 63 {
		out = strings.TrimRight(out[:63], "-")
	}
	return out
}
