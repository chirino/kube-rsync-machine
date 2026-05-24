package controller

import (
	"reflect"
	"testing"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGeneratedScheduledBackupJobNameKeepsTimestamp(t *testing.T) {
	name := GeneratedScheduledBackupJobName("a-very-long-machine-name-that-would-otherwise-truncate-the-suffix", time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	if len(name) > 63 {
		t.Fatalf("generated name is too long: %q", name)
	}
	if want := "20260522-1200"; name[len(name)-len(want):] != want {
		t.Fatalf("expected generated name %q to keep timestamp suffix %q", name, want)
	}
}

func TestBuildServeTargetJob(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	source := backupSource("app-prod", "files", "data", "sites/demo/files")
	runSet := TargetRunSet{
		Retention: krmv1alpha1.RetentionPolicy{Hourly: 24, Daily: 7},
		Sources: []ResolvedSource{{
			Source:                   source,
			EffectiveDestinationPath: "app-prod/sites/demo/files",
		}},
	}

	job, err := BuildServeTargetJob(run, target, runSet, "krm:test", "2026-05-20T10-00-00Z")
	if err != nil {
		t.Fatalf("BuildServeTargetJob returned error: %v", err)
	}
	if job.Namespace != "backup" {
		t.Fatalf("unexpected namespace: %q", job.Namespace)
	}
	container := job.Spec.Template.Spec.Containers[0]
	for _, want := range []string{"serve-target", "--target", "/backup", "--run-id", "backup-demo-20260520", "--tls-dir", TLSMountPath, "--control-grpc-endpoint", ControlGRPCEndpoint, "--control-grpc-namespace", ControlGRPCNamespace, "--run-namespace", "backup", "--run-name", "demo-20260520", "--target-namespace", "backup", "--target-name", "archive", "--timestamp", "2026-05-20T10-00-00Z", "--retention-hourly", "24", "--retention-daily", "7", "--source", "app-prod/files=app-prod/sites/demo/files"} {
		if !contains(container.Args, want) {
			t.Fatalf("expected arg %q in %#v", want, container.Args)
		}
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "archive-pvc" {
		t.Fatalf("unexpected target pvc: %q", got)
	}
	if got := job.Spec.Template.Spec.Volumes[1].Secret.SecretName; got != "krm-tls-backup-demo-20260520-target-server-backup-archive" {
		t.Fatalf("unexpected tls secret: %q", got)
	}
	if len(container.Ports) != 1 || container.Ports[0].ContainerPort != RsyncPort {
		t.Fatalf("unexpected target receiver ports: %#v", container.Ports)
	}
}

func TestBuildMirrorDataPlaneJobsUseRootRelativeTargets(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Spec.Strategy.Type = krmv1alpha1.BackupStrategyMirror
	source := backupSource("app-prod", "files", "data", "/")
	runSet := TargetRunSet{
		Sources: []ResolvedSource{{
			Source:                   source,
			EffectiveDestinationPath: "",
		}},
	}

	targetJob, err := BuildServeTargetJob(run, target, runSet, "krm:test", "2026-05-20T10-00-00Z")
	if err != nil {
		t.Fatalf("BuildServeTargetJob returned error: %v", err)
	}
	targetArgs := targetJob.Spec.Template.Spec.Containers[0].Args
	for _, want := range []string{"--strategy", "mirror", "--source", "app-prod/files=."} {
		if !contains(targetArgs, want) {
			t.Fatalf("expected arg %q in %#v", want, targetArgs)
		}
	}
	for _, unexpected := range []string{"--retention-hourly", "--timestamp"} {
		if contains(targetArgs, unexpected) {
			t.Fatalf("did not expect arg %q in %#v", unexpected, targetArgs)
		}
	}

	sourceJob, err := BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	sourceArgs := sourceJob.Spec.Template.Spec.Containers[0].Args
	for _, want := range []string{"--source", "/source", "--target", "."} {
		if !contains(sourceArgs, want) {
			t.Fatalf("expected source arg %q in %#v", want, sourceArgs)
		}
	}
}

func TestBuildServeTargetJobAppliesTargetScheduling(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	runtimeClassName := "kata"
	target.Spec.NodeSelector = map[string]string{"topology.kubernetes.io/zone": "us-east-1a"}
	target.Spec.Affinity = &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "storage-writer"},
				},
				TopologyKey: "kubernetes.io/hostname",
			}},
		},
	}
	target.Spec.Tolerations = []corev1.Toleration{{
		Key:      "storage",
		Operator: corev1.TolerationOpEqual,
		Value:    "backup",
		Effect:   corev1.TaintEffectNoSchedule,
	}}
	target.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{LabelRole: RoleTargetServer},
		},
	}}
	target.Spec.SchedulerName = "custom-scheduler"
	target.Spec.PriorityClassName = "backup-priority"
	target.Spec.RuntimeClassName = &runtimeClassName
	target.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry-creds"}}
	target.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: int64Ptr(2000)}
	target.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
	}
	source := backupSource("app-prod", "files", "data", "sites/demo/files")
	runSet := TargetRunSet{Sources: []ResolvedSource{{
		Source:                   source,
		EffectiveDestinationPath: "app-prod/sites/demo/files",
	}}}

	job, err := BuildServeTargetJob(run, target, runSet, "krm:test", "2026-05-20T10-00-00Z")
	if err != nil {
		t.Fatalf("BuildServeTargetJob returned error: %v", err)
	}
	podSpec := job.Spec.Template.Spec
	if !reflect.DeepEqual(podSpec.NodeSelector, target.Spec.NodeSelector) {
		t.Fatalf("unexpected node selector: %#v", podSpec.NodeSelector)
	}
	if !reflect.DeepEqual(podSpec.Affinity, target.Spec.Affinity) {
		t.Fatalf("unexpected affinity: %#v", podSpec.Affinity)
	}
	if !reflect.DeepEqual(podSpec.Tolerations, target.Spec.Tolerations) {
		t.Fatalf("unexpected tolerations: %#v", podSpec.Tolerations)
	}
	if !reflect.DeepEqual(podSpec.TopologySpreadConstraints, target.Spec.TopologySpreadConstraints) {
		t.Fatalf("unexpected topology spread constraints: %#v", podSpec.TopologySpreadConstraints)
	}
	if podSpec.SchedulerName != target.Spec.SchedulerName {
		t.Fatalf("unexpected scheduler name: %q", podSpec.SchedulerName)
	}
	if podSpec.PriorityClassName != target.Spec.PriorityClassName {
		t.Fatalf("unexpected priority class name: %q", podSpec.PriorityClassName)
	}
	if !reflect.DeepEqual(podSpec.RuntimeClassName, target.Spec.RuntimeClassName) {
		t.Fatalf("unexpected runtime class name: %#v", podSpec.RuntimeClassName)
	}
	if !reflect.DeepEqual(podSpec.ImagePullSecrets, target.Spec.ImagePullSecrets) {
		t.Fatalf("unexpected image pull secrets: %#v", podSpec.ImagePullSecrets)
	}
	if !reflect.DeepEqual(podSpec.SecurityContext, target.Spec.SecurityContext) {
		t.Fatalf("unexpected security context: %#v", podSpec.SecurityContext)
	}
	if !reflect.DeepEqual(podSpec.Containers[0].Resources, target.Spec.Resources) {
		t.Fatalf("unexpected resources: %#v", podSpec.Containers[0].Resources)
	}
}

func TestBuildDataPlaneJobsUseMachineImageOverride(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
		},
	}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Spec.Image = "registry.example.com/krm/custom:dev"
	runSet := TargetRunSet{Sources: []ResolvedSource{{
		Source:                   source,
		EffectiveDestinationPath: "app-prod/sites/demo/files",
	}}}

	targetJob, err := BuildServeTargetJob(run, target, runSet, "krm:test", "2026-05-20T10-00-00Z")
	if err != nil {
		t.Fatalf("BuildServeTargetJob returned error: %v", err)
	}
	sourceJob, err := BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	restoreTargetJob, err := BuildRestoreTargetJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreTargetJob returned error: %v", err)
	}
	restoreJob, err := BuildRestoreJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreJob returned error: %v", err)
	}
	for name, job := range map[string]string{
		"target":         targetJob.Spec.Template.Spec.Containers[0].Image,
		"source":         sourceJob.Spec.Template.Spec.Containers[0].Image,
		"restore-target": restoreTargetJob.Spec.Template.Spec.Containers[0].Image,
		"restore":        restoreJob.Spec.Template.Spec.Containers[0].Image,
	} {
		if job != target.Spec.Image {
			t.Fatalf("expected %s job to use image %q, got %q", name, target.Spec.Image, job)
		}
	}
}

func TestBuildMirrorRestoreJobsUseCurrentSnapshot(t *testing.T) {
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
		},
		Status: krmv1alpha1.RestoreJobStatus{
			RestoredSnapshot: krmv1alpha1.DefaultMirrorSnapshot,
		},
	}
	source := backupSource("app-prod", "files", "data-pvc", "/")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Spec.Strategy.Type = krmv1alpha1.BackupStrategyMirror

	restoreTargetJob, err := BuildRestoreTargetJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreTargetJob returned error: %v", err)
	}
	targetArgs := restoreTargetJob.Spec.Template.Spec.Containers[0].Args
	for _, want := range []string{"--restore-snapshot", "current", "--restore-source", "."} {
		if !contains(targetArgs, want) {
			t.Fatalf("expected restore target arg %q in %#v", want, targetArgs)
		}
	}

	restoreJob, err := BuildRestoreJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreJob returned error: %v", err)
	}
	restoreArgs := restoreJob.Spec.Template.Spec.Containers[0].Args
	for _, want := range []string{"--snapshot", "current", "--snapshot-source", "."} {
		if !contains(restoreArgs, want) {
			t.Fatalf("expected restore arg %q in %#v", want, restoreArgs)
		}
	}
}

func TestBuildDataPlaneJobsRunAsRootByDefault(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
		},
	}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	runSet := TargetRunSet{Sources: []ResolvedSource{{
		Source:                   source,
		EffectiveDestinationPath: "app-prod/sites/demo/files",
	}}}

	jobs := map[string]*batchv1.Job{}
	var err error
	jobs["target"], err = BuildServeTargetJob(run, target, runSet, "krm:test", "2026-05-20T10-00-00Z")
	if err != nil {
		t.Fatalf("BuildServeTargetJob returned error: %v", err)
	}
	jobs["source"], err = BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	jobs["restore-target"], err = BuildRestoreTargetJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreTargetJob returned error: %v", err)
	}
	jobs["restore"], err = BuildRestoreJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreJob returned error: %v", err)
	}
	for name, job := range jobs {
		securityContext := job.Spec.Template.Spec.SecurityContext
		if securityContext == nil || securityContext.RunAsUser == nil || *securityContext.RunAsUser != 0 || securityContext.RunAsGroup == nil || *securityContext.RunAsGroup != 0 {
			t.Fatalf("expected %s job to run as root by default, got %#v", name, securityContext)
		}
	}
}

func TestBuildRestoreJobAppliesRestoreScheduling(t *testing.T) {
	runtimeClassName := "kata"
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef:                 ref("backup", "files"),
			NodeSelector:              map[string]string{"topology.kubernetes.io/zone": "us-east-1a"},
			SchedulerName:             "restore-scheduler",
			PriorityClassName:         "restore-priority",
			RuntimeClassName:          &runtimeClassName,
			ImagePullSecrets:          []corev1.LocalObjectReference{{Name: "restore-registry-creds"}},
			SecurityContext:           &corev1.PodSecurityContext{FSGroup: int64Ptr(82), RunAsUser: int64Ptr(82), RunAsGroup: int64Ptr(82)},
			Resources:                 corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")}},
			Tolerations:               []corev1.Toleration{{Key: "restore", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.DoNotSchedule}},
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "restore-node",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"true"},
					}}}},
				},
			}},
		},
	}
	source := backupSource("backup", "files", "source-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})

	job, err := BuildRestoreJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreJob returned error: %v", err)
	}
	podSpec := job.Spec.Template.Spec
	if !reflect.DeepEqual(podSpec.NodeSelector, restore.Spec.NodeSelector) {
		t.Fatalf("unexpected node selector: %#v", podSpec.NodeSelector)
	}
	if !reflect.DeepEqual(podSpec.Affinity, restore.Spec.Affinity) {
		t.Fatalf("unexpected affinity: %#v", podSpec.Affinity)
	}
	if !reflect.DeepEqual(podSpec.Tolerations, restore.Spec.Tolerations) {
		t.Fatalf("unexpected tolerations: %#v", podSpec.Tolerations)
	}
	if !reflect.DeepEqual(podSpec.TopologySpreadConstraints, restore.Spec.TopologySpreadConstraints) {
		t.Fatalf("unexpected topology spread constraints: %#v", podSpec.TopologySpreadConstraints)
	}
	if podSpec.SchedulerName != restore.Spec.SchedulerName {
		t.Fatalf("unexpected scheduler name: %q", podSpec.SchedulerName)
	}
	if podSpec.PriorityClassName != restore.Spec.PriorityClassName {
		t.Fatalf("unexpected priority class name: %q", podSpec.PriorityClassName)
	}
	if !reflect.DeepEqual(podSpec.RuntimeClassName, restore.Spec.RuntimeClassName) {
		t.Fatalf("unexpected runtime class name: %#v", podSpec.RuntimeClassName)
	}
	if !reflect.DeepEqual(podSpec.ImagePullSecrets, restore.Spec.ImagePullSecrets) {
		t.Fatalf("unexpected image pull secrets: %#v", podSpec.ImagePullSecrets)
	}
	if !reflect.DeepEqual(podSpec.SecurityContext, restore.Spec.SecurityContext) {
		t.Fatalf("unexpected security context: %#v", podSpec.SecurityContext)
	}
	if !reflect.DeepEqual(podSpec.Containers[0].Resources, restore.Spec.Resources) {
		t.Fatalf("unexpected resources: %#v", podSpec.Containers[0].Resources)
	}
}

func TestBuildSendSourceJobAppliesSourceScheduling(t *testing.T) {
	runtimeClassName := "kata"
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.NodeSelector = map[string]string{"topology.kubernetes.io/zone": "us-east-1a"}
	source.Spec.SchedulerName = "source-scheduler"
	source.Spec.PriorityClassName = "source-priority"
	source.Spec.RuntimeClassName = &runtimeClassName
	source.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "source-registry-creds"}}
	source.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: int64Ptr(82), RunAsUser: int64Ptr(82), RunAsGroup: int64Ptr(82)}
	source.Spec.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")}}
	source.Spec.Tolerations = []corev1.Toleration{{Key: "source", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
	source.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.DoNotSchedule}}
	source.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      "source-node",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"true"},
			}}}},
		},
	}}
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})

	job, err := BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	podSpec := job.Spec.Template.Spec
	if !reflect.DeepEqual(podSpec.NodeSelector, source.Spec.NodeSelector) {
		t.Fatalf("unexpected node selector: %#v", podSpec.NodeSelector)
	}
	if !reflect.DeepEqual(podSpec.Affinity, source.Spec.Affinity) {
		t.Fatalf("unexpected affinity: %#v", podSpec.Affinity)
	}
	if !reflect.DeepEqual(podSpec.Tolerations, source.Spec.Tolerations) {
		t.Fatalf("unexpected tolerations: %#v", podSpec.Tolerations)
	}
	if !reflect.DeepEqual(podSpec.TopologySpreadConstraints, source.Spec.TopologySpreadConstraints) {
		t.Fatalf("unexpected topology spread constraints: %#v", podSpec.TopologySpreadConstraints)
	}
	if podSpec.SchedulerName != source.Spec.SchedulerName {
		t.Fatalf("unexpected scheduler name: %q", podSpec.SchedulerName)
	}
	if podSpec.PriorityClassName != source.Spec.PriorityClassName {
		t.Fatalf("unexpected priority class name: %q", podSpec.PriorityClassName)
	}
	if !reflect.DeepEqual(podSpec.RuntimeClassName, source.Spec.RuntimeClassName) {
		t.Fatalf("unexpected runtime class name: %#v", podSpec.RuntimeClassName)
	}
	if !reflect.DeepEqual(podSpec.ImagePullSecrets, source.Spec.ImagePullSecrets) {
		t.Fatalf("unexpected image pull secrets: %#v", podSpec.ImagePullSecrets)
	}
	if !reflect.DeepEqual(podSpec.SecurityContext, source.Spec.SecurityContext) {
		t.Fatalf("unexpected security context: %#v", podSpec.SecurityContext)
	}
	if !reflect.DeepEqual(podSpec.Containers[0].Resources, source.Spec.Resources) {
		t.Fatalf("unexpected resources: %#v", podSpec.Containers[0].Resources)
	}
}

func TestBuildDataPlaneJobsUseSingleAttemptBackoff(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
		},
	}
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	runSet := TargetRunSet{Sources: []ResolvedSource{{
		Source:                   source,
		EffectiveDestinationPath: "app-prod/sites/demo/files",
	}}}

	jobs := map[string]*batchv1.Job{}
	var err error
	jobs["target"], err = BuildServeTargetJob(run, target, runSet, "krm:test", "2026-05-20T10-00-00Z")
	if err != nil {
		t.Fatalf("BuildServeTargetJob returned error: %v", err)
	}
	jobs["source"], err = BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	jobs["restore-target"], err = BuildRestoreTargetJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreTargetJob returned error: %v", err)
	}
	jobs["restore"], err = BuildRestoreJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreJob returned error: %v", err)
	}
	for name, job := range jobs {
		if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != DefaultJobBackoffLimit {
			t.Fatalf("expected %s job backoffLimit %d, got %#v", name, DefaultJobBackoffLimit, job.Spec.BackoffLimit)
		}
	}
}

func TestBuildServeTargetJobUsesCustomControlNamespace(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	source := backupSource("app-prod", "files", "data", "sites/demo/files")
	runSet := TargetRunSet{Sources: []ResolvedSource{{
		Source:                   source,
		EffectiveDestinationPath: "app-prod/sites/demo/files",
	}}}

	job, err := BuildServeTargetJobWithControl(run, target, runSet, "krm:test", "2026-05-20T10-00-00Z", DefaultDataPlaneControlOptions("krm-alt-system"))
	if err != nil {
		t.Fatalf("BuildServeTargetJobWithControl returned error: %v", err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	for _, want := range []string{"--control-grpc-endpoint", "kube-rsync-machine-controller-manager.krm-alt-system.svc:8083", "--control-grpc-namespace", "krm-alt-system"} {
		if !contains(args, want) {
			t.Fatalf("expected arg %q in %#v", want, args)
		}
	}
}

func TestBuildServeTargetJobCanUseConstrainedTestVolume(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Annotations = map[string]string{
		AnnotationTestTargetEmptyDirSizeLimit: "40Mi",
		AnnotationTestTargetSeedSnapshotBytes: "1024",
	}
	source := backupSource("app-prod", "files", "data", "sites/demo/files")
	runSet := TargetRunSet{Sources: []ResolvedSource{{
		Source:                   source,
		EffectiveDestinationPath: "app-prod/sites/demo/files",
	}}}

	job, err := BuildServeTargetJob(run, target, runSet, "krm:test", "2026-05-20T10-00-00Z")
	if err != nil {
		t.Fatalf("BuildServeTargetJob returned error: %v", err)
	}
	volume := job.Spec.Template.Spec.Volumes[0]
	if volume.EmptyDir == nil || volume.EmptyDir.SizeLimit == nil || volume.EmptyDir.SizeLimit.String() != "40Mi" {
		t.Fatalf("expected constrained emptyDir target volume, got %#v", volume.VolumeSource)
	}
	if len(job.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected seed init container, got %#v", job.Spec.Template.Spec.InitContainers)
	}
}

func TestBuildSendSourceJob(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	deleteExtra := true
	oneFileSystem := true
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	source.Spec.SourcePath = "/var/lib/app"
	source.Spec.Rsync = krmv1alpha1.RsyncOptions{
		Delete:        &deleteExtra,
		OneFileSystem: &oneFileSystem,
	}

	job, err := BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	if job.Namespace != "app-prod" {
		t.Fatalf("unexpected namespace: %q", job.Namespace)
	}
	container := job.Spec.Template.Spec.Containers[0]
	for _, want := range []string{"send-source", "--source", "/source/var/lib/app", "--target", ".partial/backup-demo-20260520/app-prod/sites/demo/files", "--target-endpoint", "krm-target-demo-20260520.backup.svc:873", "--tls-dir", TLSMountPath, "--control-grpc-endpoint", ControlGRPCEndpoint, "--control-grpc-namespace", ControlGRPCNamespace, "--run-namespace", "backup", "--run-name", "demo-20260520", "--source-namespace", "app-prod", "--source-name", "files", "--target-namespace", "backup", "--target-name", "archive", "--delete", "--one-file-system"} {
		if !contains(container.Args, want) {
			t.Fatalf("expected arg %q in %#v", want, container.Args)
		}
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "data-pvc" {
		t.Fatalf("unexpected source pvc: %q", got)
	}
	if !job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly {
		t.Fatalf("expected source pvc to be read-only")
	}
	if got := job.Spec.Template.Spec.Volumes[1].Secret.SecretName; got != "krm-tls-backup-demo-20260520-source-sender-app-prod-files" {
		t.Fatalf("unexpected tls secret: %q", got)
	}
}

func TestBuildSendSourceJobUsesDefaultRsyncOptions(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})

	job, err := BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	for _, want := range []string{"--delete", "--one-file-system"} {
		if !contains(args, want) {
			t.Fatalf("expected default rsync arg %q in %#v", want, args)
		}
	}
}

func TestBuildSendSourceJobAllowsDisablingDefaultRsyncOptions(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	disabled := false
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Spec.Rsync = krmv1alpha1.RsyncOptions{
		Delete:        &disabled,
		OneFileSystem: &disabled,
	}
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})

	job, err := BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	if contains(args, "--delete") {
		t.Fatalf("did not expect --delete in %#v", args)
	}
	if contains(args, "--one-file-system") {
		t.Fatalf("did not expect --one-file-system in %#v", args)
	}
}

func TestBuildSendSourceJobUsesTestBackoffLimit(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	source.Annotations = map[string]string{AnnotationTestSourceBackoffLimit: "0"}
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})

	job, err := BuildSendSourceJob(run, source, target, SourceJobOptions{}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("unexpected backoff limit: %#v", job.Spec.BackoffLimit)
	}
}

func TestBuildSendSourceJobCanMountSnapshotPVC(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	source := backupSource("app-prod", "files", "data-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})

	job, err := BuildSendSourceJob(run, source, target, SourceJobOptions{ClaimName: "snapshot-pvc"}, "krm:test")
	if err != nil {
		t.Fatalf("BuildSendSourceJob returned error: %v", err)
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "snapshot-pvc" {
		t.Fatalf("unexpected source pvc: %q", got)
	}
}

func TestBuildTargetService(t *testing.T) {
	run := backupRun("backup", "demo-20260520", ref("backup", "demo"))
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})

	service := BuildTargetService(run, target)

	if service.Namespace != "backup" || service.Name != "krm-target-demo-20260520" {
		t.Fatalf("unexpected service identity: %s/%s", service.Namespace, service.Name)
	}
	if service.Spec.Ports[0].Name != "rsync" || service.Spec.Ports[0].Port != RsyncPort {
		t.Fatalf("unexpected ports: %#v", service.Spec.Ports)
	}
	if service.Spec.Selector[LabelRole] != RoleTargetServer || service.Spec.Selector[LabelRun] != "demo-20260520" {
		t.Fatalf("unexpected selector: %#v", service.Spec.Selector)
	}
}

func TestBuildRestoreJob(t *testing.T) {
	deleteExtra := true
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("backup", "files"),
			Snapshot:  "hourly/2026-05-20T10-00-00Z",
			Overrides: krmv1alpha1.RestoreOverrides{
				Destination: krmv1alpha1.RestoreDestination{
					Namespace: "backup",
					PVCName:   "restore-pvc",
					Path:      "/restore-here",
				},
				Rsync: krmv1alpha1.RsyncOptions{
					Delete: &deleteExtra,
				},
			},
		},
	}
	source := backupSource("backup", "files", "source-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})

	job, err := BuildRestoreJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreJob returned error: %v", err)
	}
	if job.Namespace != "backup" {
		t.Fatalf("unexpected namespace: %q", job.Namespace)
	}
	container := job.Spec.Template.Spec.Containers[0]
	for _, want := range []string{"restore", "--snapshot", "hourly/2026-05-20T10-00-00Z", "--snapshot-source", "backup/sites/demo/files", "--target-endpoint", "krm-restore-target-restore-files.backup.svc:873", "--destination", "/restore/restore-here", "--tls-dir", TLSMountPath, "--control-grpc-endpoint", ControlGRPCEndpoint, "--control-grpc-namespace", ControlGRPCNamespace, "--run-namespace", "backup", "--run-name", "restore-files", "--source-namespace", "backup", "--source-name", "files", "--target-namespace", "backup", "--target-name", "archive", "--delete"} {
		if !contains(container.Args, want) {
			t.Fatalf("expected arg %q in %#v", want, container.Args)
		}
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "restore-pvc" {
		t.Fatalf("unexpected destination pvc: %q", got)
	}
	if len(job.Spec.Template.Spec.Volumes) != 2 {
		t.Fatalf("expected only destination and tls volumes: %#v", job.Spec.Template.Spec.Volumes)
	}
	if got := job.Spec.Template.Spec.Volumes[1].Secret.SecretName; got != "krm-tls-backup-restore-files-restore-writer-backup-files" {
		t.Fatalf("unexpected tls secret: %q", got)
	}
}

func TestBuildRestoreTargetJobAndService(t *testing.T) {
	restore := krmv1alpha1.RestoreJob{
		ObjectMeta: objectMeta("backup", "restore-files"),
		Spec: krmv1alpha1.RestoreJobSpec{
			SourceRef: ref("app-prod", "files"),
			Snapshot:  "hourly/2026-05-20T10-00-00Z",
		},
	}
	restore.Status.RestoredSnapshot = "hourly/2026-05-20T10-00-00Z"
	source := backupSource("app-prod", "files", "source-pvc", "sites/demo/files")
	target := backupTarget("backup", "archive", "archive-pvc", krmv1alpha1.RetentionPolicy{})
	target.Spec.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "topology.kubernetes.io/zone",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"us-east-1a"},
					}},
				}},
			},
		},
	}
	target.Spec.Tolerations = []corev1.Toleration{{
		Key:      "storage",
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	}}

	job, err := BuildRestoreTargetJob(restore, source, target, "krm:test")
	if err != nil {
		t.Fatalf("BuildRestoreTargetJob returned error: %v", err)
	}
	if job.Namespace != "backup" || job.Name != "krm-restore-target-restore-files" {
		t.Fatalf("unexpected restore target job identity: %s/%s", job.Namespace, job.Name)
	}
	container := job.Spec.Template.Spec.Containers[0]
	for _, want := range []string{"serve-target", "--target", "/backup", "--tls-dir", TLSMountPath, "--control-grpc-endpoint", ControlGRPCEndpoint, "--control-grpc-namespace", ControlGRPCNamespace, "--run-namespace", "backup", "--run-name", "restore-files", "--target-namespace", "backup", "--target-name", "archive", "--restore-snapshot", "hourly/2026-05-20T10-00-00Z", "--restore-source", "app-prod/sites/demo/files", "--restore-writer", "app-prod/files"} {
		if !contains(container.Args, want) {
			t.Fatalf("expected arg %q in %#v", want, container.Args)
		}
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "archive-pvc" {
		t.Fatalf("unexpected target pvc: %q", got)
	}
	if !job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly {
		t.Fatal("expected target pvc to be mounted read-only")
	}
	if got := job.Spec.Template.Spec.Volumes[1].Secret.SecretName; got != "krm-tls-backup-restore-files-target-server-backup-archive" {
		t.Fatalf("unexpected target tls secret: %q", got)
	}
	if !reflect.DeepEqual(job.Spec.Template.Spec.Affinity, target.Spec.Affinity) {
		t.Fatalf("unexpected restore target affinity: %#v", job.Spec.Template.Spec.Affinity)
	}
	if !reflect.DeepEqual(job.Spec.Template.Spec.Tolerations, target.Spec.Tolerations) {
		t.Fatalf("unexpected restore target tolerations: %#v", job.Spec.Template.Spec.Tolerations)
	}

	service := BuildRestoreTargetService(restore, target)
	if service.Namespace != "backup" || service.Name != "krm-restore-target-restore-files" {
		t.Fatalf("unexpected service identity: %s/%s", service.Namespace, service.Name)
	}
	if service.Spec.Selector[LabelRole] != RoleTargetServer || service.Spec.Selector[LabelRunKind] != runKindRestore {
		t.Fatalf("unexpected service selector: %#v", service.Spec.Selector)
	}
}

func TestBuildTLSSecret(t *testing.T) {
	bundle := tlsutil.Bundle{
		CACertPEM: []byte("ca"),
		CertPEM:   []byte("crt"),
		KeyPEM:    []byte("key"),
	}
	secret := BuildTLSSecret("backup", "krm-tls-demo", map[string]string{LabelRun: "demo"}, bundle)
	if secret.Namespace != "backup" || secret.Name != "krm-tls-demo" {
		t.Fatalf("unexpected secret identity: %s/%s", secret.Namespace, secret.Name)
	}
	if secret.Type != "kubernetes.io/tls" {
		t.Fatalf("unexpected secret type: %s", secret.Type)
	}
	if string(secret.Data["ca.crt"]) != "ca" || string(secret.Data["tls.crt"]) != "crt" || string(secret.Data["tls.key"]) != "key" {
		t.Fatalf("unexpected secret data: %#v", secret.Data)
	}
	if secret.Labels[LabelRole] != RoleTLSSecret || secret.Labels[LabelRun] != "demo" {
		t.Fatalf("unexpected labels: %#v", secret.Labels)
	}
}

func rsyncMachine(namespace, name string, target krmv1alpha1.ObjectReference, sources []krmv1alpha1.ObjectReference, retention krmv1alpha1.RetentionPolicy) krmv1alpha1.RsyncMachine {
	return krmv1alpha1.RsyncMachine{
		ObjectMeta: objectMeta(namespace, name),
		Spec: krmv1alpha1.RsyncMachineSpec{
			PVCName:                  target.Name + "-pvc",
			AllowedSourceNamespaces:  []string{"*"},
			AllowedRestoreNamespaces: []string{"*"},
			Retention:                retention,
		},
	}
}

func backupRun(namespace, name string, plan krmv1alpha1.ObjectReference) krmv1alpha1.BackupJob {
	machine := krmv1alpha1.ObjectReference{Namespace: "backup", Name: "archive"}
	if plan.Name == "missing-plan" || plan.Name == "archive" {
		machine = plan
	}
	return krmv1alpha1.BackupJob{
		ObjectMeta: objectMeta(namespace, name),
		Spec: krmv1alpha1.BackupJobSpec{
			MachineRef: machine,
		},
	}
}

func backupSource(namespace, name, pvc, destinationPath string) krmv1alpha1.BackupSource {
	return backupSourceForMachine(namespace, name, pvc, destinationPath, ref("backup", "archive"))
}

func backupSourceForMachine(namespace, name, pvc, destinationPath string, machine krmv1alpha1.ObjectReference) krmv1alpha1.BackupSource {
	return krmv1alpha1.BackupSource{
		ObjectMeta: objectMeta(namespace, name),
		Spec: krmv1alpha1.BackupSourceSpec{
			MachineRef:      machine,
			PVC:             pvc,
			DestinationPath: destinationPath,
		},
	}
}

func ref(namespace, name string) krmv1alpha1.ObjectReference {
	return krmv1alpha1.ObjectReference{Namespace: namespace, Name: name}
}

func objectMeta(namespace, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: namespace, Name: name}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
