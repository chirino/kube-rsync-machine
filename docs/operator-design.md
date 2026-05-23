# kube-rsync-machine Operator Design

This document describes the current implementation for Go developers who need
to maintain or extend `kube-rsync-machine`. It intentionally avoids repeating
the user-facing workflow and the full CRD schema:

- See [user-guide.md](user-guide.md) for installation, resource examples,
  operational workflows, and restore-point semantics from a user's point of
  view.
- See [crd-reference.md](crd-reference.md) for the generated field-by-field CRD
  reference.

## System Overview

`kube-rsync-machine` is a controller-runtime operator plus a small data-plane
binary. Users create `BackupSource`, `RsyncMachine`, `BackupJob`, and
`RestoreJob` resources. The manager validates those resources, creates
Kubernetes `Job`, `Service`, `Secret`, and optional CSI snapshot objects, and
uses a gRPC control channel to coordinate target-side and source-side rsync
processes.

The main package is `cmd/kube-rsync-machine`. The CLI dispatches to these
runtime roles in `internal/cli`:

- `manager`: runs the controller-runtime manager, live API, metrics, health
  checks, and control gRPC server.
- `serve-target`: mounts the target PVC, serves or receives rsync over mTLS,
  scans restore points, recovers target space, and finalizes backups.
- `send-source`: mounts a source PVC or temporary snapshot PVC and sends data to
  the target receiver.
- `restore`: runs the restore writer that copies one source tree out of a
  target snapshot into a destination PVC.

The API types live in `api/v1alpha1`. Generated CRDs live under
`config/crd/bases`. The controller implementation is concentrated in
`internal/controller`; data-plane filesystem and rsync behavior lives in
`internal/dataplane`.

## Resource Model

All custom resources are namespaced. PVCs are also namespaced, so generated
data-plane jobs are created in the namespace where the PVC they mount exists.
Cross-namespace resources are correlated through labels and explicit cleanup
instead of invalid cross-namespace owner references.

`BackupSource` defines a source PVC, an optional path inside that PVC, a
destination path inside target snapshots, default rsync options, and optional
source-side pod scheduling/security settings. It references exactly one
`RsyncMachine`.

`RsyncMachine` defines the target PVC, retention counts, optional schedule,
optional data-plane image override, run-history limits, and target-side pod
scheduling/security settings. One machine can have many sources.

`BackupJob` is one backup execution for a machine. Scheduled runs and manual
runs both use this same resource. Its status stores the durable run phase,
included machine list, per-source transfer summaries, target phase, command
acknowledgement state, and terminal snapshot path.

`RestoreJob` is one restore execution for a single `BackupSource`. It resolves
the source's machine, selects a restore point, starts a target-side restore
server, and starts a destination writer job.

The exact schemas and defaults are in [crd-reference.md](crd-reference.md).

## Manager Composition

`internal/manager.Run` builds the runtime:

- Registers Kubernetes core types and `krm.chirino.github.io/v1alpha1` types.
- Creates the controller-runtime manager and a direct uncached client.
- Resolves the data-plane image from the manager pod unless overridden.
- Registers Prometheus metrics with controller-runtime's metrics registry.
- Ensures a manager-owned TLS secret for the control gRPC server.
- Starts the control gRPC server with client certificate verification.
- Starts the optional live API/frontend server.
- Installs reconcilers for `RsyncMachine`, `BackupSource`, `BackupJob`, and
  `RestoreJob`.
- Adds `ControlEventApplier`, which consumes control-plane events from the
  in-memory event hub and persists compact status changes to Kubernetes.

The manager uses `internal/snapshot.DiscoveryCapabilities` to detect whether
`snapshot.storage.k8s.io/v1` is available. CSI snapshot objects are manipulated
as unstructured objects, so clusters without the snapshot CRDs can still start
the manager and run direct rsync backups.

## Controllers

### RsyncMachine Controller

`RsyncMachineReconciler` validates the target PVC, validates that at least one
`BackupSource` references the machine, validates effective destination paths,
sets `Ready` and `Valid` conditions, and owns scheduled-run creation.

Scheduling is implemented in-process rather than by maintaining Kubernetes
`CronJob` objects. The reconciler parses `spec.schedule` with
`github.com/robfig/cron/v3`, computes missed runs within a bounded lookback, and
creates scheduled `BackupJob` objects with deterministic names. A leader-elected
`RsyncMachineScheduler` periodically enqueues scheduled machines, and
`status.lastScheduledAt` prevents duplicate scheduled runs.

The controller watches:

- `RsyncMachine` objects directly.
- `BackupSource` objects, mapped back through `spec.machineRef`.
- PVCs in the same namespace, mapped to machines that reference the PVC by
  `spec.pvcName`.

On machine deletion, the reconciler deletes scheduled `BackupJob` objects for
that machine and removes the machine finalizer. It does not delete data stored
on the target PVC.

### BackupSource Controller

`BackupSourceReconciler` validates the source PVC reference, machine reference,
and effective target destination path. It sets `Ready` and `Valid` conditions
but does not create data-plane resources by itself.

The controller watches:

- `BackupSource` objects directly.
- `RsyncMachine` objects, mapped to sources that reference the machine.
- PVCs in the same namespace, mapped to sources that reference the PVC by
  `spec.pvc`.

### BackupJob Controller

`BackupJobReconciler` owns the backup state machine. For a new run it:

1. Adds the run finalizer.
2. Resolves the requested machine and all sources for that machine.
3. Coalesces duplicate pending backup jobs for the same target.
4. Holds the run if the target is not ready, another backup is active, or a
   restore is active.
5. Acquires a target guard `coordination.k8s.io/v1 Lease`.
6. Creates per-run mTLS secrets.
7. Creates the target `serve-target` job and target service.
8. Moves the run to `Preparing`.

After the target reports readiness and source capture dependencies are ready,
the reconciler creates one source sender job per source. It watches generated
jobs and services, plus machines and sources that can affect the run. Status is
derived from Kubernetes job state and from accepted data-plane gRPC events.

Source capture is selected per source:

- `Direct` mounts the source PVC directly in the sender job.
- `VolumeSnapshot` creates a CSI `VolumeSnapshot`, waits for readiness, creates
  a temporary PVC from the snapshot, and mounts that temporary PVC in the sender
  job.
- `Auto` uses `VolumeSnapshot` when discovery and source settings allow it,
  otherwise it falls back to direct rsync and records the fallback in status.

Snapshot resources are built by `internal/snapshot` and handled as
unstructured objects. Temporary snapshot PVCs and snapshots are labeled with the
run identity for cleanup.

When all source transfers succeed, the reconciler sends a `FinalizeBackupJob`
command over the target command stream. The target job promotes the staged tree,
creates tier snapshots, refreshes `latest`, applies retention, scans restore
points, and reports completion. The event applier persists restore points onto
the `RsyncMachine` and marks the run succeeded.

If a source sender fails with an out-of-space style error, the backup
reconciler treats it as recoverable target pressure. The detector currently
matches transfer messages containing `no space left on device`, `enospc`, or
`disk full`, plus rsync exit codes `11` and `12`. The reconciler sends a
`RecoverSpace` command to the target stream, waits for the target to acknowledge
it, deletes the failed source job, resets that source transfer to `Preparing`,
and lets normal reconciliation create a replacement sender job.

Target recovery is not a background free-space monitor. It only runs after a
source transfer reports a recoverable out-of-space failure. The default recovery
threshold is 64 MiB of available target space, with a test-only annotation in
`internal/controller/builders.go` for overriding the threshold in integration
tests.

Terminal failed and succeeded runs release target guard Leases, clean temporary
snapshot resources, and participate in run-history pruning. Explicit deletion
uses the finalizer path to delete generated jobs, services, secrets, temporary
PVCs, and optional snapshots by labels.

### RestoreJob Controller

`RestoreJobReconciler` resolves the `BackupSource`, its machine, the selected
snapshot, and the destination PVC namespace. It holds the restore while the
machine is not ready or while an active backup owns the target guard.

When ready, it creates:

- A target-side mTLS secret, restore target job, and restore service in the
  machine namespace.
- A restore-writer mTLS secret and restore writer job in the restore job or
  destination namespace.

The restore target serves the selected snapshot path from the target PVC. The
writer runs rsync into the destination PVC. Restore status currently follows the
generated job conditions and accepted source events; a completed set of restore
jobs marks the run succeeded, and any failed generated job marks it failed.

## Generated Resources and Labels

Generated resources use a common label set from `internal/controller/builders.go`:

```yaml
app.kubernetes.io/name: kube-rsync-machine
krm.chirino.github.io/machine: <machine-name>
krm.chirino.github.io/run-namespace: <run-namespace>
krm.chirino.github.io/run-kind: backup|restore
krm.chirino.github.io/run: <run-name>
krm.chirino.github.io/source: <source-name>
krm.chirino.github.io/resource-role: <role>
```

Important roles include `target-server`, `source-sender`, `restore-writer`, and
`tls-secret`.

Owner references are set only when Kubernetes namespace rules allow them. The
cleanup path therefore always uses labels, because backup and restore runs can
span machine, source, and destination namespaces.

## Target Exclusivity

The implementation uses two layers of target protection:

- A Kubernetes `Lease` named from the target machine key prevents two active
  backup jobs from mutating the same target tree. The Lease holder stores the
  owning `BackupJob`. `Replace` concurrency can claim the guard after canceling
  the previous run; `Forbid` waits.
- The target data plane creates `.krm-run.lock` on the mounted target PVC so
  the filesystem itself rejects concurrent `serve-target` processes for the
  same target.

Restores do not mutate the target tree, but the restore controller waits behind
an active backup guard. The backup controller also waits behind active restores
so the target snapshot tree is not read while it is being finalized or changed.

## Data Plane

The data plane intentionally keeps Kubernetes logic out of the filesystem
operations. It receives all run identity, retention, source, TLS, and control
endpoint information through CLI flags constructed by the controller builders.

`internal/dataplane/rsync.go` wraps the external `rsync` command. It always uses
archive mode, `--numeric-ids`, progress output, and stats parsing. `--delete`
and `--one-file-system` are controlled by API options. Progress and final
summary fields become `TransferStatus` values.

`internal/dataplane/transport.go` serves source and restore streams over mTLS.
The certificates identify the expected run and source/writer identity; server
and client credentials are built in `internal/tlsutil` and
`internal/controlgrpc`.

`internal/dataplane/snapshot.go` owns the target PVC layout:

- `.partial/<run-id>/...` is the staging area for a backup.
- `hourly/<timestamp>/...` is the promoted immutable run snapshot.
- `latest` is a symlink to the latest hourly snapshot.
- `daily/<date>`, `weekly/<week-start>`, and `monthly/<month>` are hardlinked
  tier snapshots created from the hourly snapshot when absent.
- Retention pruning removes older tier directories according to the machine
  retention policy.

`dataplane.RecoverSpace` provides emergency pruning for the controller recovery
path. It checks free bytes with `statfs`, builds a list of removable snapshots
from `hourly`, `daily`, `weekly`, and `monthly`, sorts them by modification time
oldest first, skips protected snapshot names supplied by the command, and
removes candidates until the requested free-space threshold is met. If there are
not enough removable snapshots, it returns an error and the backup run remains
failed with the target recovery diagnostic.

The target finalization path is idempotent. If `.partial/<run-id>` has already
been promoted and the expected hourly snapshot exists, a repeated finalize
command validates and reports the existing snapshot rather than copying again.

## Control Plane Events

`internal/control.Service` is an in-memory command and event broker. Data-plane
processes call the gRPC server to:

- Register a target command stream.
- Report target phase and restore-point summaries.
- Acknowledge target commands.
- Report source/restore transfer progress and terminal state.

Target commands are deduplicated by command ID and replayed to reconnecting
target streams. The command log is in memory, so durable convergence still
depends on Kubernetes resources, run status, target filesystem state, and
recreated reconcile decisions after a manager restart.

`ControlEventApplier` reads events from the event hub and updates the compact
status fields on `BackupJob`, `RestoreJob`, and `RsyncMachine`. High-frequency
progress can also be observed through the live API without every event becoming
a Kubernetes status write.

## TLS Model

The manager maintains a control gRPC serving certificate in a Kubernetes secret.
For each backup or restore run, the controller creates short-lived mTLS
credentials for the target and each sender/writer. The generated jobs mount
those secrets at `/var/run/kube-rsync-machine/tls`.

The control gRPC server verifies client certificates against run identity and
the referenced Kubernetes objects. Data-plane mTLS verifies that the peer is the
expected target/source/writer for the run. When changing identity formats or
certificate TTLs, update `internal/tlsutil`, `internal/controlgrpc`, the
builder tests, and the controller verifier tests together.

## Snapshot Capture

CSI `VolumeSnapshot` support is optional and discovered at runtime. The
operator does not add snapshot API types to the mandatory scheme. It creates,
reads, and deletes snapshot objects as `unstructured.Unstructured` values only
when discovery reports snapshot support.

For snapshot-backed source capture, the backup controller creates the
`VolumeSnapshot` in the source namespace, waits for `readyToUse`, creates a
temporary PVC from that snapshot, waits for the PVC to bind, then points the
sender job at the temporary PVC. The temporary PVC and snapshot are labeled with
the run identity and cleaned up after the run according to the source cleanup
policy and terminal state.

## Status and Metrics

CR status is the durable API surface for users and automation. The important
phase enums and status fields are documented in
[crd-reference.md](crd-reference.md). Maintainers should keep status updates
compact and avoid writing high-frequency progress on every rsync line.

The metrics recorder in `internal/metrics` records run phases, generated job
state, and transfer progress summaries. Controllers call it when phases change
or generated job status is observed.

Kubernetes events are recorded for major lifecycle transitions and blocking
conditions such as missing references, target not ready, active overlaps, and
run failure.

## Extending the Operator

When adding behavior, keep these boundaries intact:

- API shape belongs in `api/v1alpha1`, CRD generation, tests, and
  [crd-reference.md](crd-reference.md).
- User workflow belongs in [user-guide.md](user-guide.md), not in this design
  document.
- Kubernetes desired-state orchestration belongs in `internal/controller`.
- Mounted-filesystem and rsync behavior belongs in `internal/dataplane`.
- TLS identity rules belong in `internal/tlsutil`, `internal/controlgrpc`, and
  the manager's run client verifier.
- Live, non-durable progress belongs in `internal/control` and
  `internal/liveapi`; durable summaries belong in CR status.

The most important invariants are:

- Never run two backups that mutate the same target snapshot tree concurrently.
- Keep generated resources labeled well enough for cross-namespace cleanup.
- Do not make CSI snapshot CRDs mandatory for direct-rsync clusters.
- Keep target finalization idempotent.
- Preserve numeric UID/GID values through rsync.
- Treat the target PVC filesystem as the source of truth for restore data, with
  `RsyncMachine.status.restorePoints` as discovery metadata.
