package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const integrationNamespaceEnv = "KRM_INTEGRATION_NAMESPACE"
const integrationContextEnv = "KRM_INTEGRATION_CONTEXT"
const operatorNamespace = "kube-rsync-machine-operator"
const machineNamespace = "kube-rsync-machine"

var defaultKindIntegrationScenarios = map[string]bool{
	"install":          true,
	"backup":           true,
	"mirror":           true,
	"restore":          true,
	"data-path":        true,
	"scheduling":       true,
	"cleanup":          true,
	"target-not-ready": true,
}

func TestKindInstallAndManagerReady(t *testing.T) {
	requireKindIntegration(t, "install")

	kubectl(t, "", "wait", "--for=condition=Established", "crd/backupjobs.krm.chirino.github.io", "--timeout=60s")
	kubectl(t, operatorNamespace, "rollout", "status", "deployment/kube-rsync-machine-controller-manager", "--timeout=120s")
}

func TestKindBackupJobGeneratesResources(t *testing.T) {
	requireKindIntegration(t, "backup")
	ns := newKindNamespace(t, "backup")
	applyYAML(t, backupScenarioYAML(ns, "demo-run"))

	waitForKubectl(t, 90*time.Second, "target job", machineNamespace, "get", "job/krm-target-demo-run")
	waitForKubectl(t, 30*time.Second, "target service", machineNamespace, "get", "service/krm-target-demo-run")
	waitForSelectedName(t, 30*time.Second, "target tls secret", machineNamespace, "secret", runSelector(machineNamespace, "demo-run", "backup", "target-server"))
	waitForSelectedName(t, 30*time.Second, "source tls secret", ns, "secret", runSelector(machineNamespace, "demo-run", "backup", "source-sender"))
	waitForJSONPathIn(t, 60*time.Second, machineNamespace, "backupjob/demo-run", "{.status.phase}", "Preparing", "Running", "Finalizing")
}

func TestKindRestoreJobGeneratesWriterJob(t *testing.T) {
	requireKindIntegration(t, "restore")
	ns := newKindNamespace(t, "restore")
	applyYAML(t, basePVCsYAML(ns)+"---\n"+backupObjectsYAML(ns))
	patchTargetRestorePoints(t, machineNamespace, "archive")
	applyYAML(t, restoreRunYAML(ns))

	waitForKubectl(t, 90*time.Second, "restore target job", machineNamespace, "get", "job/krm-restore-target-restore-files")
	waitForKubectl(t, 90*time.Second, "restore writer job", ns, "get", "job/krm-restore-restore-files")
	waitForSelectedName(t, 30*time.Second, "restore target secret", machineNamespace, "secret", runSelector(ns, "restore-files", "restore", "target-server"))
	waitForSelectedName(t, 30*time.Second, "restore writer secret", ns, "secret", runSelector(ns, "restore-files", "restore", "restore-writer"))
	waitForJSONPathIn(t, 60*time.Second, ns, "restorejob/restore-files", "{.status.phase}", "Preparing", "Failed")
}

func TestKindBackupAndRestoreMovesData(t *testing.T) {
	requireKindIntegration(t, "data-path")
	ns := newKindNamespace(t, "data")
	applyYAML(t, basePVCsYAML(ns)+"---\n"+backupObjectsYAML(ns))
	seedPVCFile(t, ns, "source-pvc", "nested/data.txt", "payload-from-kind\n")
	applyYAML(t, backupRunYAML(ns, "data-run"))

	waitForJSONPath(t, 180*time.Second, machineNamespace, "backupjob/data-run", "{.status.phase}", "Succeeded")
	waitForJSONPathSet(t, 60*time.Second, machineNamespace, "backupjob/data-run", "{.status.snapshotPath}")
	assertPVCFile(t, machineNamespace, "archive-pvc", "latest/"+ns+"/app/files/nested/data.txt", "payload-from-kind\n")

	applyYAML(t, restoreRunYAML(ns))
	waitForJSONPath(t, 180*time.Second, ns, "restorejob/restore-files", "{.status.phase}", "Succeeded")
	assertPVCFile(t, ns, "restore-pvc", "nested/data.txt", "payload-from-kind\n")
}

func TestKindMirrorBackupCopiesSourceRootToTargetRoot(t *testing.T) {
	requireKindIntegration(t, "mirror")
	ns := newKindNamespace(t, "mirror")
	applyYAML(t, basePVCsYAML(ns)+"---\n"+mirrorBackupObjectsYAML(ns))
	seedPVCFile(t, ns, "source-pvc", "nested/data.txt", "payload-from-mirror\n")
	seedPVCFile(t, ns, "source-pvc", "top-level.txt", "top-level-mirror\n")
	applyYAML(t, backupRunYAML(ns, "mirror-run"))

	waitForJSONPath(t, 180*time.Second, machineNamespace, "backupjob/mirror-run", "{.status.phase}", "Succeeded")
	waitForJSONPath(t, 60*time.Second, machineNamespace, "backupjob/mirror-run", "{.status.snapshotPath}", "current")
	assertPVCFileTree(t, ns, "source-pvc", machineNamespace, "archive-pvc")
	for _, unexpected := range []string{".partial", "latest", "hourly", "daily", "weekly", "monthly"} {
		assertPVCPathAbsent(t, machineNamespace, "archive-pvc", unexpected)
	}
}

func TestKindOutOfSpaceRecoveryRetriesSource(t *testing.T) {
	requireKindIntegration(t, "out-of-space")
	ns := newKindNamespace(t, "space")
	applyYAML(t, basePVCsYAML(ns)+"---\n"+outOfSpaceBackupObjectsYAML(ns))
	seedPVCFileOfSize(t, ns, "source-pvc", "large/data.bin", 16*1024*1024)
	applyYAML(t, backupRunYAML(ns, "space-run"))

	waitForJSONPath(t, 180*time.Second, machineNamespace, "backupjob/space-run", "{.status.lastCommand.type}", "recover_space")
	waitForJSONPathSet(t, 120*time.Second, machineNamespace, "backupjob/space-run", "{.status.lastCommand.acknowledgedAt}")
	waitForJSONPath(t, 240*time.Second, machineNamespace, "backupjob/space-run", "{.status.phase}", "Succeeded")
	waitForJSONPathSet(t, 60*time.Second, machineNamespace, "backupjob/space-run", "{.status.snapshotPath}")
}

func TestKindManagerNamespaceCanDiffer(t *testing.T) {
	requireKindIntegration(t, "manager-namespace")
	managerNS := newKindNamespace(t, "manager")
	workloadNS := newKindNamespace(t, "mgns")

	scaleManager(t, 0)
	deployManagerToNamespace(t, managerNS)
	t.Cleanup(func() {
		_, _ = kubectlOutput(managerNS, "delete", "deployment/kube-rsync-machine-controller-manager", "--ignore-not-found=true", "--wait=false")
		_, _ = kubectlOutput(managerNS, "delete", "service/kube-rsync-machine-controller-manager", "--ignore-not-found=true", "--wait=false")
		scaleManager(t, 1)
	})

	applyYAML(t, basePVCsYAML(workloadNS)+"---\n"+backupObjectsYAML(workloadNS))
	seedPVCFile(t, workloadNS, "source-pvc", "nested/data.txt", "payload-from-alt-manager\n")
	applyYAML(t, backupRunYAML(workloadNS, "alt-manager-run"))

	waitForJSONPath(t, 240*time.Second, machineNamespace, "backupjob/alt-manager-run", "{.status.phase}", "Succeeded")
	assertPVCFile(t, machineNamespace, "archive-pvc", "latest/"+workloadNS+"/app/files/nested/data.txt", "payload-from-alt-manager\n")
}

func TestKindScheduledPlanCreatesBackupJob(t *testing.T) {
	requireKindIntegration(t, "scheduling")
	ns := newKindNamespace(t, "schedule")
	applyYAML(t, scheduledPlanYAML(ns))

	waitForKubectlAbsent(t, 90*time.Second, "legacy plan cronjob", machineNamespace, "get", "cronjob/krm-scheduled")
	waitForSelectedName(t, 150*time.Second, "scheduled backup job", machineNamespace, "backupjob", "krm.chirino.github.io/machine=scheduled,krm.chirino.github.io/resource-role=machine-trigger")
	waitForJSONPathSet(t, 30*time.Second, machineNamespace, "rsyncmachine/scheduled", "{.status.lastScheduledAt}")
}

func TestKindGeneratedResourceCleanupOnBackupJobDelete(t *testing.T) {
	requireKindIntegration(t, "cleanup")
	ns := newKindNamespace(t, "cleanup")
	applyYAML(t, backupScenarioYAML(ns, "cleanup-run"))

	waitForKubectl(t, 90*time.Second, "target job", machineNamespace, "get", "job/krm-target-cleanup-run")
	kubectl(t, machineNamespace, "delete", "backupjob/cleanup-run", "--wait=true", "--timeout=90s")
	waitForKubectlAbsent(t, 60*time.Second, "target job cleanup", machineNamespace, "get", "job/krm-target-cleanup-run")
	waitForKubectlAbsent(t, 60*time.Second, "target service cleanup", machineNamespace, "get", "service/krm-target-cleanup-run")
}

func TestKindTargetNotReadyHoldsBackupJob(t *testing.T) {
	requireKindIntegration(t, "target-not-ready")
	ns := newKindNamespace(t, "notready")
	applyYAML(t, targetNotReadyYAML(ns))
	applyYAML(t, heldBackupJobYAML(ns))

	waitForJSONPath(t, 60*time.Second, machineNamespace, "backupjob/held-run", "{.status.phase}", "Pending")
	waitForJSONPath(t, 30*time.Second, machineNamespace, "backupjob/held-run", "{.status.conditions[0].reason}", "TargetNotReady")
}

func TestKindManagerRestartContinuesBackupJobReconciliation(t *testing.T) {
	requireKindIntegration(t, "restart")
	ns := newKindNamespace(t, "restart")
	applyYAML(t, backupScenarioYAML(ns, "restart-run"))

	waitForKubectl(t, 90*time.Second, "target job before restart", machineNamespace, "get", "job/krm-target-restart-run")
	restartManager(t)
	patchJobCondition(t, machineNamespace, "krm-target-restart-run", "Complete")

	waitForKubectl(t, 90*time.Second, "source job after manager restart", ns, "get", "job/krm-source-files-restart-run")
	waitForJSONPathIn(t, 60*time.Second, machineNamespace, "backupjob/restart-run", "{.status.phase}", "Running", "Finalizing", "Succeeded")
}

func TestKindFinalizerCleanupRecoversAfterManagerInterruption(t *testing.T) {
	requireKindIntegration(t, "finalizer-retry")
	ns := newKindNamespace(t, "finalizer")
	applyYAML(t, backupScenarioYAML(ns, "finalizer-run"))

	waitForKubectl(t, 90*time.Second, "target job before interrupted delete", machineNamespace, "get", "job/krm-target-finalizer-run")
	waitForJSONPath(t, 60*time.Second, machineNamespace, "backupjob/finalizer-run", "{.metadata.finalizers[0]}", "backupjob.krm.chirino.github.io/finalizer")

	scaleManager(t, 0)
	kubectl(t, machineNamespace, "delete", "backupjob/finalizer-run", "--wait=false")
	waitForJSONPathSet(t, 30*time.Second, machineNamespace, "backupjob/finalizer-run", "{.metadata.deletionTimestamp}")
	scaleManager(t, 1)

	waitForKubectlAbsent(t, 90*time.Second, "backup job finalizer removal after manager restart", machineNamespace, "get", "backupjob/finalizer-run")
	waitForKubectlAbsent(t, 60*time.Second, "target job finalizer cleanup", machineNamespace, "get", "job/krm-target-finalizer-run")
	waitForKubectlAbsent(t, 60*time.Second, "target service finalizer cleanup", machineNamespace, "get", "service/krm-target-finalizer-run")
}

func TestKindTargetRestorePointStatusDrivesRestoreSnapshotResolution(t *testing.T) {
	requireKindIntegration(t, "restore-points")
	ns := newKindNamespace(t, "points")
	applyYAML(t, basePVCsYAML(ns)+"---\n"+backupObjectsYAML(ns))
	patchTargetRestorePoints(t, machineNamespace, "archive")
	applyYAML(t, restoreRunYAML(ns))

	waitForJSONPath(t, 30*time.Second, machineNamespace, "rsyncmachine/archive", "{.status.restorePointCount}", "2")
	waitForJSONPath(t, 30*time.Second, machineNamespace, "rsyncmachine/archive", "{.status.restorePoints[0].snapshot}", "latest")
	waitForJSONPath(t, 60*time.Second, ns, "restorejob/restore-files", "{.status.restoredSnapshot}", "hourly/2026-05-20T12-00-00Z")
	waitForKubectl(t, 90*time.Second, "restore job from target restore point status", ns, "get", "job/krm-restore-restore-files")
}

func TestKindWatchDrivenReconciliationCreatesSourceJobFromTargetJobStatus(t *testing.T) {
	requireKindIntegration(t, "watch-reconcile")
	ns := newKindNamespace(t, "watch")
	applyYAML(t, backupScenarioYAML(ns, "watch-run"))

	waitForKubectl(t, 90*time.Second, "target job", machineNamespace, "get", "job/krm-target-watch-run")
	patchJobCondition(t, machineNamespace, "krm-target-watch-run", "Complete")

	waitForKubectl(t, 90*time.Second, "source job created from watched target job status", ns, "get", "job/krm-source-files-watch-run")
	waitForJSONPathIn(t, 60*time.Second, machineNamespace, "backupjob/watch-run", "{.status.phase}", "Running", "Finalizing", "Succeeded")
}

func TestKindSourceFailureMarksBackupJobFailed(t *testing.T) {
	requireKindIntegration(t, "source-failure")
	ns := newKindNamespace(t, "srcfail")
	applyYAML(t, backupScenarioYAML(ns, "source-failure-run"))

	waitForKubectl(t, 90*time.Second, "target job", machineNamespace, "get", "job/krm-target-source-failure-run")
	patchJobCondition(t, machineNamespace, "krm-target-source-failure-run", "Complete")
	waitForKubectl(t, 90*time.Second, "source job", ns, "get", "job/krm-source-files-source-failure-run")
	patchJobCondition(t, ns, "krm-source-files-source-failure-run", "Failed")

	waitForJSONPath(t, 90*time.Second, machineNamespace, "backupjob/source-failure-run", "{.status.phase}", "Failed")
	waitForJSONPath(t, 30*time.Second, machineNamespace, "backupjob/source-failure-run", "{.status.conditions[?(@.type==\"Failed\")].reason}", "RunFailed")
}

func TestKindOptionalCSISnapshotScenario(t *testing.T) {
	requireKindIntegration(t, "csi")
	if _, err := kubectlOutput("", "get", "crd/volumesnapshots.snapshot.storage.k8s.io"); err != nil {
		t.Skip("snapshot.storage.k8s.io CRDs are not installed")
	}
	t.Skip("CSI snapshot data-path scenario is scaffolded but waits for a test snapshot driver")
}

func TestKindScenarioSelection(t *testing.T) {
	t.Setenv("KRM_INTEGRATION_SCENARIOS", "")
	if !scenarioSelected("backup") {
		t.Fatal("backup should run by default")
	}
	for _, scenario := range []string{"csi", "restart", "finalizer-retry", "restore-points", "watch-reconcile", "source-failure", "out-of-space"} {
		if scenarioSelected(scenario) {
			t.Fatalf("%s should require explicit selection", scenario)
		}
	}
	t.Setenv("KRM_INTEGRATION_SCENARIOS", "restore,target-.*,restart")
	if !scenarioSelected("restore") || !scenarioSelected("target-not-ready") || !scenarioSelected("restart") {
		t.Fatal("literal and regexp selectors should match")
	}
	if scenarioSelected("backup") {
		t.Fatal("unselected scenarios should not match")
	}
}

func requireKindIntegration(t *testing.T, scenario string) {
	t.Helper()
	if os.Getenv("KRM_INTEGRATION") != "1" {
		t.Skip("set KRM_INTEGRATION=1 or run make test-integration to enable kind integration tests")
	}
	if !scenarioSelected(scenario) {
		t.Skipf("scenario %q not selected by KRM_INTEGRATION_SCENARIOS", scenario)
	}
}

func scenarioSelected(name string) bool {
	filter := strings.TrimSpace(os.Getenv("KRM_INTEGRATION_SCENARIOS"))
	if filter == "" {
		return defaultKindIntegrationScenarios[name]
	}
	for _, item := range strings.Split(filter, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if item == name {
			return true
		}
		if re, err := regexp.Compile(item); err == nil && re.MatchString(name) {
			return true
		}
	}
	return false
}

func newKindNamespace(t *testing.T, suffix string) string {
	t.Helper()
	base := os.Getenv(integrationNamespaceEnv)
	if base == "" {
		base = "krm-it"
	}
	name := fmt.Sprintf("%s-%s-%d", base, suffix, time.Now().UnixNano())
	kubectl(t, "", "create", "namespace", name)
	ensureMachineNamespace(t)
	if os.Getenv("KRM_INTEGRATION_PRESERVE") != "1" {
		t.Cleanup(func() {
			_, _ = kubectlOutput("", "delete", "namespace", name, "--ignore-not-found=true", "--wait=false")
			cleanupMachineNamespace(t)
		})
	}
	return name
}

func ensureMachineNamespace(t *testing.T) {
	t.Helper()
	if _, err := kubectlOutput("", "get", "namespace/"+machineNamespace); err != nil {
		kubectl(t, "", "create", "namespace", machineNamespace)
	}
	cleanupMachineNamespace(t)
}

func cleanupMachineNamespace(t *testing.T) {
	t.Helper()
	_, _ = kubectlOutput(machineNamespace, "delete", "backupjob,restorejob,backupsource,rsyncmachine,job,service,secret,cronjob,pod,pvc", "--all", "--ignore-not-found=true", "--wait=false")
}

func applyYAML(t *testing.T, manifest string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := append(kubectlContextArgs(), "apply", "-f", "-")
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = strings.NewReader(manifest)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl apply failed: %v\n%s", err, out.String())
	}
}

func deployManagerToNamespace(t *testing.T, namespace string) {
	t.Helper()
	root, err := repositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := append(kubectlContextArgs(), "kustomize", filepath.Join(root, "config/default"))
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	var rendered bytes.Buffer
	cmd.Stdout = &rendered
	cmd.Stderr = &rendered
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl kustomize failed: %v\n%s", err, rendered.String())
	}
	manifest := strings.ReplaceAll(rendered.String(), "kube-rsync-machine-operator", namespace)
	applyYAML(t, manifest)
	allowManagerServiceAccounts(t, "kube-rsync-machine-operator", namespace)
	image := integrationImage()
	imageArg := "--image=" + image
	patch := fmt.Sprintf(`{"spec":{"template":{"spec":{"containers":[{"name":"manager","image":%q,"args":["manager","--leader-elect","--metrics-bind-address=:8080","--health-probe-bind-address=:8081",%q]}]}}}}`, image, imageArg)
	kubectl(t, namespace, "patch", "deployment/kube-rsync-machine-controller-manager", "--type=strategic", "-p", patch)
	kubectlWithTimeout(t, namespace, 240*time.Second, "rollout", "status", "deployment/kube-rsync-machine-controller-manager", "--timeout=180s")
	waitForJSONPath(t, 120*time.Second, namespace, "deployment/kube-rsync-machine-controller-manager", "{.status.readyReplicas}", "1")
}

func allowManagerServiceAccounts(t *testing.T, namespaces ...string) {
	t.Helper()
	var subjects []string
	for _, namespace := range namespaces {
		subjects = append(subjects, fmt.Sprintf(`{"kind":"ServiceAccount","name":"kube-rsync-machine-controller-manager","namespace":%q}`, namespace))
	}
	patch := fmt.Sprintf(`{"subjects":[%s]}`, strings.Join(subjects, ","))
	kubectl(t, "", "patch", "clusterrolebinding/kube-rsync-machine-manager-rolebinding", "--type=merge", "-p", patch)
}

func repositoryRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Clean(filepath.Join(cwd, "..", "..")), nil
}

func kubectl(t *testing.T, namespace string, args ...string) {
	t.Helper()
	if out, err := kubectlOutput(namespace, args...); err != nil {
		t.Fatalf("kubectl %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func kubectlWithTimeout(t *testing.T, namespace string, timeout time.Duration, args ...string) {
	t.Helper()
	if out, err := kubectlOutputWithTimeout(namespace, timeout, args...); err != nil {
		t.Fatalf("kubectl %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func kubectlOutput(namespace string, args ...string) (string, error) {
	return kubectlOutputWithTimeout(namespace, 30*time.Second, args...)
}

func kubectlOutputWithTimeout(namespace string, timeout time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	fullArgs := make([]string, 0, len(args)+4)
	fullArgs = append(fullArgs, kubectlContextArgs()...)
	if namespace != "" {
		fullArgs = append(fullArgs, "-n", namespace)
	}
	fullArgs = append(fullArgs, args...)
	cmd := exec.CommandContext(ctx, "kubectl", fullArgs...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

func kubectlContextArgs() []string {
	if contextName := os.Getenv(integrationContextEnv); contextName != "" {
		return []string{"--context", contextName}
	}
	cluster := os.Getenv("KIND_CLUSTER")
	if cluster == "" {
		cluster = "kube-rsync-machine"
	}
	return []string{"--context", "kind-" + cluster}
}

func waitForKubectl(t *testing.T, timeout time.Duration, description, namespace string, args ...string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, err := kubectlOutput(namespace, args...)
		if err == nil {
			return
		}
		last = out
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s\nlast output:\n%s", description, last)
}

func waitForKubectlAbsent(t *testing.T, timeout time.Duration, description, namespace string, args ...string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, err := kubectlOutput(namespace, args...)
		if err != nil {
			return
		}
		last = out
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s to be absent\nlast output:\n%s", description, last)
}

func waitForSelectedName(t *testing.T, timeout time.Duration, description, namespace, resource, selector string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, err := kubectlOutput(namespace, "get", resource, "-l", selector, "-o", "name")
		last = strings.TrimSpace(out)
		if err == nil && last != "" {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s selected by %q, last value %q", description, selector, last)
}

func waitForJSONPath(t *testing.T, timeout time.Duration, namespace, resource, path, want string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, err := kubectlOutput(namespace, "get", resource, "-o", "jsonpath="+path)
		last = strings.TrimSpace(out)
		if err == nil && last == want {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s %s to equal %q, last value %q", resource, path, want, last)
}

func waitForJSONPathSet(t *testing.T, timeout time.Duration, namespace, resource, path string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, err := kubectlOutput(namespace, "get", resource, "-o", "jsonpath="+path)
		last = strings.TrimSpace(out)
		if err == nil && last != "" {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s %s to be set, last value %q", resource, path, last)
}

func waitForJSONPathIn(t *testing.T, timeout time.Duration, namespace, resource, path string, wants ...string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, err := kubectlOutput(namespace, "get", resource, "-o", "jsonpath="+path)
		last = strings.TrimSpace(out)
		if err == nil {
			for _, want := range wants {
				if last == want {
					return
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s %s to be one of %q, last value %q", resource, path, wants, last)
}

func restartManager(t *testing.T) {
	t.Helper()
	kubectl(t, operatorNamespace, "rollout", "restart", "deployment/kube-rsync-machine-controller-manager")
	kubectlWithTimeout(t, operatorNamespace, 240*time.Second, "rollout", "status", "deployment/kube-rsync-machine-controller-manager", "--timeout=180s")
}

func scaleManager(t *testing.T, replicas int) {
	t.Helper()
	if replicas == 0 {
		t.Cleanup(func() {
			scaleManager(t, 1)
		})
	}
	kubectl(t, operatorNamespace, "scale", "deployment/kube-rsync-machine-controller-manager", fmt.Sprintf("--replicas=%d", replicas))
	if replicas > 0 {
		kubectlWithTimeout(t, operatorNamespace, 240*time.Second, "rollout", "status", "deployment/kube-rsync-machine-controller-manager", "--timeout=180s")
		waitForJSONPath(t, 120*time.Second, operatorNamespace, "deployment/kube-rsync-machine-controller-manager", "{.status.readyReplicas}", fmt.Sprintf("%d", replicas))
		return
	}
	waitForJSONPathIn(t, 120*time.Second, operatorNamespace, "deployment/kube-rsync-machine-controller-manager", "{.status.readyReplicas}", "", "0")
}

func seedPVCFile(t *testing.T, namespace, pvcName, path, content string) {
	t.Helper()
	pod := "krm-pvc-seed-" + dnsSafe(pvcName)
	applyYAML(t, pvcToolPodYAML(namespace, pod, pvcName))
	waitForKubectl(t, 90*time.Second, "pvc seed pod ready", namespace, "wait", "--for=condition=Ready", "pod/"+pod, "--timeout=90s")
	t.Cleanup(func() {
		_, _ = kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s")
	})
	command := fmt.Sprintf("mkdir -p %q && printf %q > %q", "/data/"+dir(path), content, "/data/"+path)
	kubectl(t, namespace, "exec", pod, "--", "sh", "-c", command)
	if out, err := kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s"); err != nil {
		t.Fatalf("kubectl delete pod/%s failed: %v\n%s", pod, err, out)
	}
}

func seedPVCFileOfSize(t *testing.T, namespace, pvcName, path string, bytes int) {
	t.Helper()
	pod := "krm-pvc-seed-" + dnsSafe(pvcName)
	applyYAML(t, pvcToolPodYAML(namespace, pod, pvcName))
	waitForKubectl(t, 90*time.Second, "pvc seed pod ready", namespace, "wait", "--for=condition=Ready", "pod/"+pod, "--timeout=90s")
	t.Cleanup(func() {
		_, _ = kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s")
	})
	command := fmt.Sprintf("mkdir -p %q && head -c %d /dev/zero > %q", "/data/"+dir(path), bytes, "/data/"+path)
	kubectl(t, namespace, "exec", pod, "--", "sh", "-c", command)
	if out, err := kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s"); err != nil {
		t.Fatalf("kubectl delete pod/%s failed: %v\n%s", pod, err, out)
	}
}

func assertPVCFile(t *testing.T, namespace, pvcName, path, want string) {
	t.Helper()
	pod := "krm-pvc-read-" + dnsSafe(pvcName)
	applyYAML(t, pvcToolPodYAML(namespace, pod, pvcName))
	waitForKubectl(t, 90*time.Second, "pvc read pod ready", namespace, "wait", "--for=condition=Ready", "pod/"+pod, "--timeout=90s")
	t.Cleanup(func() {
		_, _ = kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s")
	})
	out, err := kubectlOutput(namespace, "exec", pod, "--", "cat", "/data/"+path)
	if err != nil {
		t.Fatalf("read %s from pvc %s failed: %v\n%s", path, pvcName, err, out)
	}
	if out != want {
		t.Fatalf("unexpected pvc %s file %s content: got %q want %q", pvcName, path, out, want)
	}
	if out, err := kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s"); err != nil {
		t.Fatalf("kubectl delete pod/%s failed: %v\n%s", pod, err, out)
	}
}

func assertPVCFileSize(t *testing.T, namespace, pvcName, path string, want int) {
	t.Helper()
	pod := "krm-pvc-read-" + dnsSafe(pvcName)
	applyYAML(t, pvcToolPodYAML(namespace, pod, pvcName))
	waitForKubectl(t, 90*time.Second, "pvc read pod ready", namespace, "wait", "--for=condition=Ready", "pod/"+pod, "--timeout=90s")
	t.Cleanup(func() {
		_, _ = kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s")
	})
	out, err := kubectlOutput(namespace, "exec", pod, "--", "stat", "-c", "%s", "/data/"+path)
	if err != nil {
		t.Fatalf("stat %s from pvc %s failed: %v\n%s", path, pvcName, err, out)
	}
	if got := strings.TrimSpace(out); got != fmt.Sprint(want) {
		t.Fatalf("unexpected pvc %s file %s size: got %q want %d", pvcName, path, got, want)
	}
	if out, err := kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s"); err != nil {
		t.Fatalf("kubectl delete pod/%s failed: %v\n%s", pod, err, out)
	}
}

func assertPVCFileTree(t *testing.T, sourceNamespace, sourcePVC, targetNamespace, targetPVC string) {
	t.Helper()
	sourceManifest := pvcFileManifest(t, sourceNamespace, sourcePVC)
	targetManifest := pvcFileManifest(t, targetNamespace, targetPVC)
	if sourceManifest != targetManifest {
		t.Fatalf("pvc file trees differ:\nsource %s/%s:\n%s\ntarget %s/%s:\n%s", sourceNamespace, sourcePVC, sourceManifest, targetNamespace, targetPVC, targetManifest)
	}
}

func pvcFileManifest(t *testing.T, namespace, pvcName string) string {
	t.Helper()
	pod := "krm-pvc-read-" + dnsSafe(pvcName)
	applyYAML(t, pvcToolPodYAML(namespace, pod, pvcName))
	waitForKubectl(t, 90*time.Second, "pvc manifest pod ready", namespace, "wait", "--for=condition=Ready", "pod/"+pod, "--timeout=90s")
	t.Cleanup(func() {
		_, _ = kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s")
	})
	command := `cd /data && find . -type f | sort | while IFS= read -r file; do sha256sum "$file"; done`
	out, err := kubectlOutput(namespace, "exec", pod, "--", "sh", "-c", command)
	if err != nil {
		t.Fatalf("build file manifest for pvc %s failed: %v\n%s", pvcName, err, out)
	}
	if deleteOut, err := kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s"); err != nil {
		t.Fatalf("kubectl delete pod/%s failed: %v\n%s", pod, err, deleteOut)
	}
	return strings.TrimSpace(out)
}

func assertPVCPathAbsent(t *testing.T, namespace, pvcName, path string) {
	t.Helper()
	pod := "krm-pvc-read-" + dnsSafe(pvcName)
	applyYAML(t, pvcToolPodYAML(namespace, pod, pvcName))
	waitForKubectl(t, 90*time.Second, "pvc path check pod ready", namespace, "wait", "--for=condition=Ready", "pod/"+pod, "--timeout=90s")
	t.Cleanup(func() {
		_, _ = kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s")
	})
	out, err := kubectlOutput(namespace, "exec", pod, "--", "test", "!", "-e", "/data/"+path)
	if err != nil {
		t.Fatalf("expected path %s to be absent from pvc %s: %v\n%s", path, pvcName, err, out)
	}
	if deleteOut, err := kubectlOutputWithTimeout(namespace, 90*time.Second, "delete", "pod/"+pod, "--wait=true", "--timeout=60s"); err != nil {
		t.Fatalf("kubectl delete pod/%s failed: %v\n%s", pod, err, deleteOut)
	}
}

func pvcToolPodYAML(namespace, name, pvcName string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  restartPolicy: Never
  securityContext:
    runAsUser: 0
  containers:
    - name: tool
      image: %[4]s
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %[3]s
`, namespace, name, pvcName, integrationImage())
}

func integrationImage() string {
	if image := os.Getenv("KRM_IMAGE"); image != "" {
		return image
	}
	return "kube-rsync-machine:kind"
}

func dir(path string) string {
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		return path[:idx]
	}
	return "."
}

func dnsSafe(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func patchJobCondition(t *testing.T, namespace, job, conditionType string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	condition := fmt.Sprintf(`{"type":%q,"status":"True","lastTransitionTime":%q,"reason":"IntegrationForced","message":"kind integration forced %s"}`, conditionType, now, conditionType)
	switch conditionType {
	case "Complete":
		payload := fmt.Sprintf(`{"status":{"active":0,"ready":0,"succeeded":1,"completionTime":%q,"conditions":[{"type":"SuccessCriteriaMet","status":"True","lastTransitionTime":%q,"reason":"IntegrationForced","message":"kind integration forced success criteria"},%s]}}`, now, now, condition)
		kubectl(t, namespace, "patch", "job/"+job, "--subresource=status", "--type=merge", "-p", payload)
	case "Failed":
		payload := fmt.Sprintf(`{"status":{"active":0,"ready":0,"failed":1,"conditions":[{"type":"FailureTarget","status":"True","lastTransitionTime":%q,"reason":"IntegrationForced","message":"kind integration forced failure target"},%s]}}`, now, condition)
		kubectl(t, namespace, "patch", "job/"+job, "--subresource=status", "--type=merge", "-p", payload)
	default:
		payload := fmt.Sprintf(`{"status":{"conditions":[%s]}}`, condition)
		kubectl(t, namespace, "patch", "job/"+job, "--subresource=status", "--type=merge", "-p", payload)
	}
}

func patchTargetRestorePoints(t *testing.T, namespace, target string) {
	t.Helper()
	payload := `{"status":{"restorePointsUpdatedAt":"2026-05-20T12:01:00Z","restorePointCount":2,"restorePoints":[{"snapshot":"latest","resolvesTo":"hourly/2026-05-20T12-00-00Z","tier":"latest","createdAt":"2026-05-20T12:00:00Z"},{"snapshot":"hourly/2026-05-20T12-00-00Z","tier":"hourly","createdAt":"2026-05-20T12:00:00Z"}],"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-05-20T12:01:00Z","reason":"RestorePointsAvailable","message":"integration test restore points are available"}]}}`
	kubectl(t, namespace, "patch", "rsyncmachine/"+target, "--subresource=status", "--type=merge", "-p", payload)
}

func runSelector(namespace, run, kind, role string) string {
	return strings.Join([]string{
		"app.kubernetes.io/name=kube-rsync-machine",
		"krm.chirino.github.io/run-namespace=" + namespace,
		"krm.chirino.github.io/run=" + run,
		"krm.chirino.github.io/run-kind=" + kind,
		"krm.chirino.github.io/resource-role=" + role,
	}, ",")
}

func basePVCsYAML(ns string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: archive-pvc
  namespace: %[2]s
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: source-pvc
  namespace: %[1]s
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: restore-pvc
  namespace: %[1]s
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 1Gi
`, ns, machineNamespace)
}

func backupObjectsYAML(ns string) string {
	return fmt.Sprintf(`apiVersion: krm.chirino.github.io/v1alpha1
kind: RsyncMachine
metadata:
  name: archive
  namespace: %[2]s
spec:
  pvcName: archive-pvc
  allowedSourceNamespaces:
    - %[1]s
  allowedRestoreNamespaces:
    - %[1]s
  concurrencyPolicy: Forbid
  retention:
    hourly: 2
  runHistory:
    successful: 1
    failed: 1
---
apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupSource
metadata:
  name: files
  namespace: %[1]s
spec:
  machineRef:
    namespace: %[2]s
    name: archive
  pvc: source-pvc
  sourcePath: /
  destinationPath: app/files
  consistency:
    capture: Direct
`, ns, machineNamespace)
}

func mirrorBackupObjectsYAML(ns string) string {
	return fmt.Sprintf(`apiVersion: krm.chirino.github.io/v1alpha1
kind: RsyncMachine
metadata:
  name: archive
  namespace: %[2]s
spec:
  pvcName: archive-pvc
  allowedSourceNamespaces:
    - %[1]s
  allowedRestoreNamespaces:
    - %[1]s
  strategy:
    type: Mirror
  concurrencyPolicy: Forbid
  runHistory:
    successful: 1
    failed: 1
---
apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupSource
metadata:
  name: files
  namespace: %[1]s
spec:
  machineRef:
    namespace: %[2]s
    name: archive
  pvc: source-pvc
  sourcePath: /
  destinationPath: /
  consistency:
    capture: Direct
`, ns, machineNamespace)
}

func outOfSpaceBackupObjectsYAML(ns string) string {
	return fmt.Sprintf(`apiVersion: krm.chirino.github.io/v1alpha1
kind: RsyncMachine
metadata:
  name: archive
  namespace: %[2]s
  annotations:
    krm.chirino.github.io/test-target-empty-dir-size-limit: 40Mi
    krm.chirino.github.io/test-target-seed-snapshot-bytes: "29360128"
    krm.chirino.github.io/test-recovery-min-available: 24Mi
spec:
  pvcName: archive-pvc
  allowedSourceNamespaces:
    - %[1]s
  allowedRestoreNamespaces:
    - %[1]s
  concurrencyPolicy: Forbid
  retention:
    hourly: 2
  runHistory:
    successful: 1
    failed: 1
---
apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupSource
metadata:
  name: files
  namespace: %[1]s
  annotations:
    krm.chirino.github.io/test-source-backoff-limit: "0"
spec:
  machineRef:
    namespace: %[2]s
    name: archive
  pvc: source-pvc
  sourcePath: /
  destinationPath: app/files
  consistency:
    capture: Direct
`, ns, machineNamespace)
}

func backupScenarioYAML(ns, run string) string {
	return basePVCsYAML(ns) + "---\n" + backupObjectsYAML(ns) + "---\n" + backupRunYAML(ns, run)
}

func backupRunYAML(ns, run string) string {
	return fmt.Sprintf(`apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupJob
metadata:
  name: %[2]s
  namespace: %[3]s
spec:
  machineRef:
    name: archive
  trigger: Manual
`, ns, run, machineNamespace)
}

func restoreScenarioYAML(ns string) string {
	return basePVCsYAML(ns) + "---\n" + backupObjectsYAML(ns) + "---\n" + restoreRunYAML(ns)
}

func restoreRunYAML(ns string) string {
	return fmt.Sprintf(`apiVersion: krm.chirino.github.io/v1alpha1
kind: RestoreJob
metadata:
  name: restore-files
  namespace: %[1]s
spec:
  sourceRef:
    name: files
  snapshot: latest
  overrides:
    destination:
      pvcName: restore-pvc
      path: /
`, ns)
}

func scheduledPlanYAML(ns string) string {
	return basePVCsYAML(ns) + "---\n" + backupObjectsYAML(ns) + fmt.Sprintf(`---
apiVersion: krm.chirino.github.io/v1alpha1
kind: RsyncMachine
metadata:
  name: scheduled
  namespace: %[2]s
spec:
  schedule: "* * * * *"
  pvcName: archive-pvc
  allowedSourceNamespaces:
    - %[1]s
  allowedRestoreNamespaces:
    - %[1]s
  concurrencyPolicy: Forbid
---
apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupSource
metadata:
  name: scheduled-files
  namespace: %[1]s
spec:
  machineRef:
    namespace: %[2]s
    name: scheduled
  pvc: source-pvc
  sourcePath: /
  destinationPath: app/files
`, ns, machineNamespace)
}

func targetNotReadyYAML(ns string) string {
	return basePVCsYAML(ns) + "---\n" + fmt.Sprintf(`apiVersion: krm.chirino.github.io/v1alpha1
kind: RsyncMachine
metadata:
  name: archive
  namespace: %[2]s
spec:
  pvcName: missing-archive-pvc
  allowedSourceNamespaces:
    - %[1]s
  allowedRestoreNamespaces:
    - %[1]s
---
apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupSource
metadata:
  name: files
  namespace: %[1]s
spec:
  machineRef:
    namespace: %[2]s
    name: archive
  pvc: source-pvc
  destinationPath: app/files
`, ns, machineNamespace)
}

func heldBackupJobYAML(ns string) string {
	return fmt.Sprintf(`apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupJob
metadata:
  name: held-run
  namespace: %[2]s
spec:
  machineRef:
    name: archive
`, ns, machineNamespace)
}
