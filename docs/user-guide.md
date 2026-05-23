# kube-rsync-machine User Guide

`kube-rsync-machine` backs up Kubernetes PersistentVolumeClaims (PVCs) to a
target PVC and restores selected snapshots back to a PVC. It uses operator
custom resources to describe sources, targets, schedules, backup jobs, and
restore jobs. Backups are kept as space-efficient snapshots (using hardlinks)
with hourly, weekly, and monthly retention tiers, and older retained data is
cleaned up automatically to stay within the configured policy.

The project is still early backup software. Test backup and restore workflows
with disposable data before using it for important workloads.

## Concepts

- `BackupSource`: a source PVC and the path inside snapshots where its data is
  stored.
- `RsyncMachine`: the destination PVC that stores snapshots, the sources to
  collect, retention settings, run history, and an optional cron schedule.
- `BackupJob`: one execution of a machine backup.
- `RestoreJob`: one restore from a machine snapshot into a destination PVC.

The CRDs relate to each other like this:

- One `BackupSource` references exactly one `RsyncMachine` in
  `spec.machineRef`. The source selects the PVC path to copy, and the machine
  selects the target PVC and retention policy.
- One `RsyncMachine` can have many `BackupSource` objects pointing at it.
- One `BackupJob` references exactly one `RsyncMachine` in `spec.machineRef`.
  Scheduled machines create many `BackupJob` objects over time, and manual runs
  are also represented as `BackupJob` objects.
- One `RestoreJob` references exactly one `BackupSource` in `spec.sourceRef`.
  The restore uses that source's `spec.machineRef` to find the snapshot store
  and uses the source to select the path within that snapshot to restore.
- Many `BackupJob` and `RestoreJob` objects can exist for the same
  `RsyncMachine`, but the operator allows only one active run to mutate or read
  a machine's snapshot tree at a time.

Snapshots are stored on the target PVC in a hardlink-based layout:

- `latest`: the most recent successful snapshot.
- `hourly/<timestamp>`: immutable hourly snapshots.
- `daily/<yyyy-mm-dd>`, `weekly/<week-start-yyyy-mm-dd>`, `monthly/<yyyy-mm>`:
  tier aliases created from successful backup jobs according to retention
  settings. Weekly snapshots use the Monday that starts the week.

## Install the Operator

Build or publish the image you want the cluster to use, then install the CRDs,
RBAC, manager deployment, and service:

```sh
kubectl --context <cluster-context> apply -k config/default
```

The default installation creates the operator in the
`kube-rsync-machine-operator` namespace and runs the image
`ghcr.io/chirino/kube-rsync-machine:latest`.
If you use a different image tag, set it through kustomize before applying the
manifests.

Check that the manager is ready:

```sh
kubectl --context <cluster-context> -n kube-rsync-machine-operator rollout status \
  deployment/kube-rsync-machine-controller-manager
```

## Create Backup Resources

The examples below back up the `app-data` PVC in the `default` namespace into
the `app-backups` PVC in the `kube-rsync-machine` namespace.

First create the `kube-rsync-machine` namespace and a target PVC there large
enough to hold retained backups. Then create a `BackupSource` in the
application namespace for each PVC path you want to protect:

```yaml
apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupSource
metadata:
  name: app-data
  namespace: default
spec:
  machineRef:
    namespace: kube-rsync-machine
    name: app-hourly
  pvc: app-data
  destinationPath: app/data
```

Important source fields:

- `spec.machineRef`: `RsyncMachine` that stores snapshots for this source.
- `spec.pvc`: source PVC to mount and back up.
- `spec.sourcePath`: path inside the source PVC. Defaults to `/`.
- `spec.destinationPath`: relative path under each snapshot where this source is
  written, after the source namespace prefix. Defaults to the namespace root.
  `destinationPath: files` writes under `<source-namespace>/files`; omit it or
  set it to `/` to write under `<source-namespace>`.
- `spec.consistency.capture`: `Direct`, `VolumeSnapshot`, or `Auto`. Defaults
  to `Auto`, which uses CSI `VolumeSnapshot` when available and falls back to
  direct rsync.
- `spec.rsync.delete`: defaults to `true`; removes destination files that no
  longer exist in the source.
- `spec.rsync.oneFileSystem`: defaults to `true`; prevents rsync from crossing
  nested filesystem mount boundaries under the source path.
  Rsync always uses numeric UID/GID values internally because data-plane Pods do
  not share a common user/group database.
- Source-side scheduling, image pull secret, pod security context, and resource
  fields control the generated sender Job that mounts this source PVC. See
  [Advanced Job Scheduling and Security](#advanced-job-scheduling-and-security)
  for details.

Create a `RsyncMachine` for the target PVC, retention policy, and optional
schedule:

```yaml
apiVersion: krm.chirino.github.io/v1alpha1
kind: RsyncMachine
metadata:
  name: app-hourly
  namespace: kube-rsync-machine
spec:
  pvcName: app-backups
  schedule: "0 * * * *"
  retention:
    hourly: 24
    daily: 7
    weekly: 4
    monthly: 6
```

When `spec.schedule` is set, the operator creates scheduled `BackupJob`
objects. Leave `schedule` empty if you only want manual runs.

`spec.image` overrides the manager's default data-plane image for this machine.
Scheduling fields such as `nodeSelector`, `affinity`, `tolerations`,
`topologySpreadConstraints`, `schedulerName`, `priorityClassName`, and
`runtimeClassName` are applied to target-side pods that mount the machine PVC.
`imagePullSecrets` is applied to generated Jobs that use the image.
`securityContext` is applied to generated pods, and `resources` is applied to
generated containers.

Apply the resources:

```sh
kubectl --context <cluster-context> apply -f backup-source.yaml
kubectl --context <cluster-context> apply -f machine.yaml
```

## Run a Backup

For a manual backup, create a `BackupJob`:

```yaml
apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupJob
metadata:
  name: app-hourly-manual
  namespace: kube-rsync-machine
spec:
  machineRef:
    name: app-hourly
```

Apply it:

```sh
kubectl --context <cluster-context> apply -f backup-run.yaml
```

The operator starts one target-side Job and one source-side Job per source. It
uses short-lived mTLS credentials for the data-plane transfer and reports
progress through `BackupJob.status`.

Watch the backup:

```sh
kubectl --context <cluster-context> -n kube-rsync-machine get backupjobs
kubectl --context <cluster-context> -n kube-rsync-machine describe backupjob app-hourly-manual
kubectl --context <cluster-context> -n kube-rsync-machine get jobs \
  -l app.kubernetes.io/name=kube-rsync-machine
kubectl --context <cluster-context> -n default get jobs \
  -l app.kubernetes.io/name=kube-rsync-machine
```

After a successful run, inspect restore points on the machine:

```sh
kubectl --context <cluster-context> -n kube-rsync-machine get rsyncmachine app-hourly \
  -o jsonpath='{range .status.restorePoints[*]}{.snapshot}{"\t"}{.resolvesTo}{"\n"}{end}'
```

Use the value in `snapshot` when creating a restore. `latest` is the default.

## Restore a PVC

Create or choose the destination PVC before starting a restore. Restoring into
the original PVC is allowed, but restoring into a separate PVC first is safer
because it lets you inspect the result before replacing application data.

```yaml
apiVersion: krm.chirino.github.io/v1alpha1
kind: RestoreJob
metadata:
  name: app-data-restore
  namespace: default
spec:
  sourceRef:
    name: app-data
  overrides:
    destination:
      pvcName: app-data-restore
```

Apply it:

```sh
kubectl --context <cluster-context> apply -f restore-run.yaml
```

Restore defaults:

- `spec.snapshot` defaults to `latest`.
- `spec.overrides.destination.namespace` defaults to the source namespace.
- `spec.overrides.destination.pvcName` defaults to the source PVC.
- `spec.overrides.destination.path` defaults to the source path.
- Restore rsync options default to the source rsync options unless restore
  overrides are provided.
- Restore-side scheduling, image pull secret, pod security context, and resource
  fields control the generated restore writer Job that mounts the destination
  PVC. See [Advanced Job Scheduling and Security](#advanced-job-scheduling-and-security)
  for details.

Watch the restore:

```sh
kubectl --context <cluster-context> -n default get restorejobs
kubectl --context <cluster-context> -n default describe restorejob app-data-restore
kubectl --context <cluster-context> -n default get jobs \
  -l krm.chirino.github.io/run=app-data-restore
```

The operator blocks restores while a backup is actively mutating the same
machine, and it blocks backups while a restore is active against that machine.

## CRD Reference

Generate the field reference from the CRD YAML:

```sh
make docs-crd-reference
```

The generated output is written to
[docs/crd-reference.md](crd-reference.md).

## Advanced Job Scheduling and Security

Generated data-plane Jobs mount different PVCs in different namespaces. Configure
placement and security on the resource that owns the PVC being mounted:

- `RsyncMachine.spec`: target receiver Jobs that mount the machine backup PVC.
- `BackupSource.spec`: source sender Jobs that mount the source PVC.
- `RestoreJob.spec`: restore writer Jobs that mount the destination PVC.

All three resources support the same pod placement and execution controls:

- `nodeSelector`
- `affinity`
- `tolerations`
- `topologySpreadConstraints`
- `schedulerName`
- `priorityClassName`
- `runtimeClassName`
- `imagePullSecrets`
- `securityContext`
- `resources`

Use these fields when source, target, or restore PVCs are zone-local, require a
specific storage node, use a private data-plane image, or need a UID/GID/fsGroup
that matches the application data. Sender and restore writer Jobs default to
running as root so they can preserve ownership and write into fresh PVCs unless
you set an explicit `securityContext`. Rsync always uses numeric IDs.

Example source sender controls:

```yaml
apiVersion: krm.chirino.github.io/v1alpha1
kind: BackupSource
metadata:
  name: app-data
  namespace: default
spec:
  machineRef:
    namespace: kube-rsync-machine
    name: app-hourly
  pvc: app-data
  sourcePath: /
  nodeSelector:
    topology.kubernetes.io/zone: us-east-1a
  securityContext:
    runAsUser: 0
    runAsGroup: 0
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
```

Example restore writer controls:

```yaml
apiVersion: krm.chirino.github.io/v1alpha1
kind: RestoreJob
metadata:
  name: app-data-restore
  namespace: default
spec:
  sourceRef:
    name: app-data
  overrides:
    destination:
      pvcName: app-data-restore
  nodeSelector:
    topology.kubernetes.io/zone: us-east-1a
  securityContext:
    runAsUser: 0
    runAsGroup: 0
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
```

If your application volume must be accessed as a non-root user, set the
appropriate `runAsUser`, `runAsGroup`, and `fsGroup` on the `BackupSource` or
`RestoreJob`. If you want ownership in the restored files to remain exactly as
captured, keep the restore writer running as root.

## HTTP API and UI

The manager exposes a read-only live HTTP API on port `8082` by default. If a
built frontend directory is configured with `--frontend-dir` or `KRM_FRONTEND_DIR`,
the same server also serves the UI.

The API and UI should normally be exposed only behind an authentication and
authorization layer, such as an authenticated ingress, SSO-aware reverse proxy,
VPN-only gateway, or another cluster-approved access control layer. Do not put
the live API/UI directly on the public internet. The API exposes rsync machines,
PVC names, run status, restore points, and live run events.

For local administrative access, use port-forwarding:

```sh
kubectl --context <cluster-context> -n kube-rsync-machine-operator port-forward \
  svc/kube-rsync-machine-controller-manager 8082:8082
```

Then open:

```text
http://localhost:8082/
```

Useful API endpoints:

```text
GET /api/v1/machines?namespace=kube-rsync-machine
GET /api/v1/namespaces/kube-rsync-machine/machines
GET /api/v1/namespaces/default/sources
GET /api/v1/namespaces/kube-rsync-machine/machines/app-hourly
GET /api/v1/namespaces/kube-rsync-machine/machines/app-hourly/restorepoints
GET /api/v1/namespaces/kube-rsync-machine/backups
GET /api/v1/namespaces/kube-rsync-machine/backups/app-hourly-manual
GET /api/v1/namespaces/kube-rsync-machine/backups/app-hourly-manual/events
GET /api/v1/namespaces/default/restores
GET /api/v1/namespaces/default/restores/app-data-restore
GET /api/v1/namespaces/default/restores/app-data-restore/events
```

The `/events` endpoints use server-sent events for live progress.

If you need to expose the API/UI through ingress, route only to the service's
`live-api` port and put authentication in front of it:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: kube-rsync-machine
  namespace: kube-rsync-machine-operator
  annotations:
    # Example only. Use the authentication mechanism approved for your cluster.
    nginx.ingress.kubernetes.io/auth-url: https://auth.example.com/oauth2/auth
    nginx.ingress.kubernetes.io/auth-signin: https://auth.example.com/oauth2/start
spec:
  rules:
    - host: krm.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: kube-rsync-machine-controller-manager
                port:
                  name: live-api
```

## Troubleshooting

List custom resources:

```sh
kubectl --context <cluster-context> -n kube-rsync-machine get rsyncmachines,backupjobs
kubectl --context <cluster-context> -n default get backupsources,restorejobs
```

Check generated Jobs and Pods:

```sh
kubectl --context <cluster-context> -n default get jobs,pods \
  -l app.kubernetes.io/name=kube-rsync-machine
kubectl --context <cluster-context> -n kube-rsync-machine get jobs,pods \
  -l app.kubernetes.io/name=kube-rsync-machine
```

Read a failed Job's logs:

```sh
kubectl --context <cluster-context> -n default logs job/<job-name>
```

Check manager logs:

```sh
kubectl --context <cluster-context> -n kube-rsync-machine-operator logs \
  deployment/kube-rsync-machine-controller-manager
```

Common issues:

- `RsyncMachine` is not ready: verify the target PVC exists in the same
  namespace as the `RsyncMachine` and can be mounted by a Job.
- `BackupSource` transfer fails: verify the source PVC exists in the same
  namespace as the `BackupSource` and that `sourcePath` exists.
- `VolumeSnapshot` capture fails: confirm the cluster has the CSI snapshot CRDs,
  snapshot controller, and a usable `VolumeSnapshotClass`; use `capture: Auto`
  or `Direct` if snapshots are unavailable.
- Restore snapshot not found: use a value from
  `RsyncMachine.status.restorePoints[*].snapshot`.
- Out of space on the target PVC: increase target capacity or reduce retention.
  The operator can prune older snapshots for emergency recovery, but retained
  backups still need enough storage for expected change rates.
