# CRD Reference

Generated from `config/crd/bases/*.yaml`.

## BackupJob

- API group: `krm.chirino.github.io`
- Scope: `Namespaced`
- Plural: `backupjobs`
- Short names: `bj`

### v1alpha1 (served, storage)

#### Spec

| Field | Type | Required | Default | Enum | Description |
| --- | --- | --- | --- | --- | --- |
| `spec.machineRef` | `object` | yes |  |  | Machine to run for this backup. |
| `spec.machineRef.name` | `string` | yes |  |  | Name of the referenced machine. |
| `spec.machineRef.namespace` | `string` | no |  |  | Namespace of the referenced machine. Defaults to the BackupJob namespace. |
| `spec.trigger` | `string` | no |  | "Manual", "Scheduled" | Reason the backup was created. Defaults to Manual. |

#### Status

| Field | Type | Required | Default | Enum | Description |
| --- | --- | --- | --- | --- | --- |
| `status.completedAt` | `string/date-time` | no |  |  | Time the backup run finished. |
| `status.conditions` | `[]object` | no |  |  | Kubernetes-style conditions for the resource. |
| `status.conditions[].lastTransitionTime` | `string/date-time` | yes |  |  | Time the condition last changed status. |
| `status.conditions[].message` | `string` | yes |  |  | Human-readable condition details. |
| `status.conditions[].observedGeneration` | `integer/int64` | no |  |  | Resource generation observed for the condition. |
| `status.conditions[].reason` | `string` | yes |  |  | Machine-readable reason for the condition. |
| `status.conditions[].status` | `string` | yes |  | "True", "False", "Unknown" | Condition status. |
| `status.conditions[].type` | `string` | yes |  |  | Condition type. |
| `status.includedMachines` | `[]object` | no |  |  | Machines merged into this backup run. |
| `status.includedMachines[].name` | `string` | yes |  |  | Name of a machine included in this run. |
| `status.includedMachines[].namespace` | `string` | no |  |  | Namespace of a machine included in this run. |
| `status.lastCommand` | `object` | no |  |  | Most recent control-plane command sent to the data plane. |
| `status.lastCommand.acknowledgedAt` | `string/date-time` | no |  |  | Time the command was acknowledged. |
| `status.lastCommand.id` | `string` | no |  |  | Command identifier. |
| `status.lastCommand.sentAt` | `string/date-time` | no |  |  | Time the command was sent. |
| `status.lastCommand.type` | `string` | no |  |  | Command type. |
| `status.mergedInto` | `object` | no |  |  | Run that this run was merged into. |
| `status.mergedInto.name` | `string` | yes |  |  | Name of the merged target run. |
| `status.mergedInto.namespace` | `string` | no |  |  | Namespace of the merged target run. |
| `status.phase` | `string` | no |  | "Pending", "Preparing", "Running", "Finalizing", "Succeeded", "Failed", "Canceled" | Current backup lifecycle phase. |
| `status.snapshotPath` | `string` | no |  |  | Snapshot path created by the run. |
| `status.startedAt` | `string/date-time` | no |  |  | Time the backup run started. |
| `status.targetPhase` | `string` | no |  |  | Target-side data-plane phase. |
| `status.transfers` | `[]object` | no |  |  | Per-source transfer summaries. |
| `status.transfers[].bytesReceived` | `integer/int64` | no |  |  | Bytes received by the sender process. |
| `status.transfers[].bytesSent` | `integer/int64` | no |  |  | Bytes sent by the sender process. |
| `status.transfers[].bytesTransferred` | `integer/int64` | no |  |  | Literal file payload bytes transferred by rsync. |
| `status.transfers[].captureMethod` | `string` | no |  | "Direct", "VolumeSnapshot", "Auto" | Capture method used for this transfer. |
| `status.transfers[].captureTime` | `string/date-time` | no |  |  | Time the source capture was taken. |
| `status.transfers[].completedAt` | `string/date-time` | no |  |  | Time this transfer finished. |
| `status.transfers[].exitCode` | `integer/int32` | no |  |  | Transfer process exit code. |
| `status.transfers[].filesTransferred` | `integer/int64` | no |  |  | Regular files transferred by rsync. |
| `status.transfers[].message` | `string` | no |  |  | Human-readable transfer details. |
| `status.transfers[].percent` | `integer/int32` | no |  |  | Approximate transfer completion percentage. |
| `status.transfers[].phase` | `string` | no |  | "Pending", "Preparing", "Running", "Succeeded", "Failed", "Skipped" | Transfer lifecycle phase. |
| `status.transfers[].rateBytesPerSecond` | `integer/int64` | no |  |  | Average transfer rate in bytes per second. |
| `status.transfers[].source` | `string` | yes |  |  | Namespace/name of the source. |
| `status.transfers[].speedup` | `number` | no |  |  | Rsync speedup ratio. |
| `status.transfers[].startedAt` | `string/date-time` | no |  |  | Time this transfer started. |
| `status.transfers[].totalFileSize` | `integer/int64` | no |  |  | Total file size reported by rsync. |
| `status.transfers[].totalFiles` | `integer/int64` | no |  |  | Total files seen by rsync. |
| `status.transfers[].volumeSnapshotName` | `string` | no |  |  | Temporary VolumeSnapshot used for this transfer. |

## BackupSource

- API group: `krm.chirino.github.io`
- Scope: `Namespaced`
- Plural: `backupsources`
- Short names: `bs`

### v1alpha1 (served, storage)

#### Spec

| Field | Type | Required | Default | Enum | Description |
| --- | --- | --- | --- | --- | --- |
| `spec.affinity` | `object` | no |  |  | Pod affinity rules for generated sender jobs. |
| `spec.consistency` | `object` | no |  |  | Source capture behavior before rsync starts. |
| `spec.consistency.capture` | `string` | no | "Auto" | "Direct", "VolumeSnapshot", "Auto" | Capture method for source consistency. |
| `spec.consistency.cleanupPolicy` | `string` | no | "Delete" | "Delete", "RetainOnFailure" | Cleanup behavior for temporary snapshot resources. |
| `spec.consistency.volumeSnapshotClassName` | `string` | no |  |  | VolumeSnapshotClass to use when snapshot capture is selected. |
| `spec.destinationPath` | `string` | no |  |  | Relative target path. Snapshot machines store it under the source namespace in each restore point; Mirror machines store it relative to the target PVC root. |
| `spec.imagePullSecrets` | `[]object` | no |  |  | Image pull secrets for generated sender jobs. |
| `spec.imagePullSecrets[].name` | `string` | yes |  |  | Name of the image pull secret. |
| `spec.machineRef` | `object` | yes |  |  | Machine that stores backups for this source. |
| `spec.machineRef.name` | `string` | yes |  |  | Name of the referenced machine. |
| `spec.machineRef.namespace` | `string` | no |  |  | Namespace of the referenced machine. Defaults to the BackupSource namespace. |
| `spec.nodeSelector` | `map[string]string` | no |  |  | Node labels required for generated sender jobs. |
| `spec.priorityClassName` | `string` | no |  |  | PriorityClass for generated sender jobs. |
| `spec.pvc` | `string` | yes |  |  | Source PVC mounted by the sender job. |
| `spec.resources` | `object` | no |  |  | Container resource requests and limits for generated sender jobs. |
| `spec.rsync` | `object` | no |  |  | Rsync options for this source. |
| `spec.rsync.delete` | `boolean` | no | true |  | Delete destination files that no longer exist in the source. |
| `spec.rsync.oneFileSystem` | `boolean` | no | true |  | Prevent rsync from crossing filesystem mount boundaries. |
| `spec.runtimeClassName` | `string` | no |  |  | RuntimeClass for generated sender jobs. |
| `spec.schedulerName` | `string` | no |  |  | Scheduler name for generated sender jobs. |
| `spec.securityContext` | `object` | no |  |  | Pod security context for generated sender jobs. |
| `spec.sourcePath` | `string` | no |  |  | Path inside the source PVC to back up. Defaults to /. |
| `spec.tolerations` | `[]object` | no |  |  | Tolerations for generated sender jobs. |
| `spec.topologySpreadConstraints` | `[]object` | no |  |  | Topology spread constraints for generated sender jobs. |

#### Status

| Field | Type | Required | Default | Enum | Description |
| --- | --- | --- | --- | --- | --- |
| `status.conditions` | `[]object` | no |  |  | Kubernetes-style conditions for the resource. |
| `status.conditions[].lastTransitionTime` | `string/date-time` | yes |  |  | Time the condition last changed status. |
| `status.conditions[].message` | `string` | yes |  |  | Human-readable condition details. |
| `status.conditions[].observedGeneration` | `integer/int64` | no |  |  | Resource generation observed for the condition. |
| `status.conditions[].reason` | `string` | yes |  |  | Machine-readable reason for the condition. |
| `status.conditions[].status` | `string` | yes |  | "True", "False", "Unknown" | Condition status. |
| `status.conditions[].type` | `string` | yes |  |  | Condition type. |

## RestoreJob

- API group: `krm.chirino.github.io`
- Scope: `Namespaced`
- Plural: `restorejobs`
- Short names: `rj`

### v1alpha1 (served, storage)

#### Spec

| Field | Type | Required | Default | Enum | Description |
| --- | --- | --- | --- | --- | --- |
| `spec.affinity` | `object` | no |  |  | Pod affinity rules for generated restore writer jobs. |
| `spec.imagePullSecrets` | `[]object` | no |  |  | Image pull secrets for generated restore writer jobs. |
| `spec.imagePullSecrets[].name` | `string` | yes |  |  | Name of the image pull secret. |
| `spec.nodeSelector` | `map[string]string` | no |  |  | Node labels required for generated restore writer jobs. |
| `spec.overrides` | `object` | no |  |  | Per-restore destination and rsync overrides. |
| `spec.overrides.destination` | `object` | no |  |  | Destination override for this restore. |
| `spec.overrides.destination.namespace` | `string` | no |  |  | Namespace containing the destination PVC. Defaults to the RestoreJob namespace and cannot name a different namespace. |
| `spec.overrides.destination.path` | `string` | no |  |  | Path inside the destination PVC. |
| `spec.overrides.destination.pvcName` | `string` | no |  |  | Destination PVC to mount and restore into. |
| `spec.overrides.rsync` | `object` | no |  |  | Rsync options for this restore. |
| `spec.overrides.rsync.delete` | `boolean` | no |  |  | Delete destination files that no longer exist in the source for this restore. |
| `spec.overrides.rsync.oneFileSystem` | `boolean` | no |  |  | Prevent restore rsync from crossing filesystem mount boundaries. |
| `spec.priorityClassName` | `string` | no |  |  | PriorityClass for generated restore writer jobs. |
| `spec.resources` | `object` | no |  |  | Container resource requests and limits for generated restore writer jobs. |
| `spec.runtimeClassName` | `string` | no |  |  | RuntimeClass for generated restore writer jobs. |
| `spec.schedulerName` | `string` | no |  |  | Scheduler name for generated restore writer jobs. |
| `spec.securityContext` | `object` | no |  |  | Pod security context for generated restore writer jobs. |
| `spec.snapshot` | `string` | no |  |  | Restore point to restore. Defaults to latest. |
| `spec.sourceRef` | `object` | yes |  |  | BackupSource to restore from. |
| `spec.sourceRef.name` | `string` | yes |  |  | Name of the referenced source. |
| `spec.sourceRef.namespace` | `string` | no |  |  | Namespace of the referenced source. Defaults to the RestoreJob namespace. |
| `spec.tolerations` | `[]object` | no |  |  | Tolerations for generated restore writer jobs. |
| `spec.topologySpreadConstraints` | `[]object` | no |  |  | Topology spread constraints for generated restore writer jobs. |

#### Status

| Field | Type | Required | Default | Enum | Description |
| --- | --- | --- | --- | --- | --- |
| `status.completedAt` | `string/date-time` | no |  |  | Time the restore run finished. |
| `status.conditions` | `[]object` | no |  |  | Kubernetes-style conditions for the resource. |
| `status.conditions[].lastTransitionTime` | `string/date-time` | yes |  |  | Time the condition last changed status. |
| `status.conditions[].message` | `string` | yes |  |  | Human-readable condition details. |
| `status.conditions[].observedGeneration` | `integer/int64` | no |  |  | Resource generation observed for the condition. |
| `status.conditions[].reason` | `string` | yes |  |  | Machine-readable reason for the condition. |
| `status.conditions[].status` | `string` | yes |  | "True", "False", "Unknown" | Condition status. |
| `status.conditions[].type` | `string` | yes |  |  | Condition type. |
| `status.exitCode` | `integer/int32` | no |  |  | Exit code reported by the restore writer process. |
| `status.message` | `string` | no |  |  | Human-readable restore status or error message. |
| `status.phase` | `string` | no |  | "Pending", "Preparing", "Running", "Finalizing", "Succeeded", "Failed", "Canceled" | Current restore lifecycle phase. |
| `status.restoredSnapshot` | `string` | no |  |  | Resolved immutable restore point used by the restore. |
| `status.startedAt` | `string/date-time` | no |  |  | Time the restore run started. |
| `status.transfers` | `[]object` | no |  |  | Per-source transfer summaries. |
| `status.transfers[].bytesReceived` | `integer/int64` | no |  |  | Bytes received by the sender process. |
| `status.transfers[].bytesSent` | `integer/int64` | no |  |  | Bytes sent by the sender process. |
| `status.transfers[].bytesTransferred` | `integer/int64` | no |  |  | Literal file payload bytes transferred by rsync. |
| `status.transfers[].captureMethod` | `string` | no |  | "Direct", "VolumeSnapshot", "Auto" | Capture method used for this transfer. |
| `status.transfers[].captureTime` | `string/date-time` | no |  |  | Time the source capture was taken. |
| `status.transfers[].completedAt` | `string/date-time` | no |  |  | Time this transfer finished. |
| `status.transfers[].exitCode` | `integer/int32` | no |  |  | Transfer process exit code. |
| `status.transfers[].filesTransferred` | `integer/int64` | no |  |  | Regular files transferred by rsync. |
| `status.transfers[].message` | `string` | no |  |  | Human-readable transfer details. |
| `status.transfers[].percent` | `integer/int32` | no |  |  | Approximate transfer completion percentage. |
| `status.transfers[].phase` | `string` | no |  | "Pending", "Preparing", "Running", "Succeeded", "Failed", "Skipped" | Transfer lifecycle phase. |
| `status.transfers[].rateBytesPerSecond` | `integer/int64` | no |  |  | Average transfer rate in bytes per second. |
| `status.transfers[].source` | `string` | yes |  |  | Namespace/name of the source. |
| `status.transfers[].speedup` | `number` | no |  |  | Rsync speedup ratio. |
| `status.transfers[].startedAt` | `string/date-time` | no |  |  | Time this transfer started. |
| `status.transfers[].totalFileSize` | `integer/int64` | no |  |  | Total file size reported by rsync. |
| `status.transfers[].totalFiles` | `integer/int64` | no |  |  | Total files seen by rsync. |
| `status.transfers[].volumeSnapshotName` | `string` | no |  |  | Temporary VolumeSnapshot used for this transfer. |

## RsyncMachine

- API group: `krm.chirino.github.io`
- Scope: `Namespaced`
- Plural: `rsyncmachines`
- Short names: `rm`

### v1alpha1 (served, storage)

#### Spec

| Field | Type | Required | Default | Enum | Description |
| --- | --- | --- | --- | --- | --- |
| `spec.affinity` | `object` | no |  |  | Pod affinity rules for generated target-side jobs. |
| `spec.allowedRestoreNamespaces` | `[]string` | no |  |  | Namespaces allowed to create RestoreJobs against this machine. Defaults to [.] where . means this RsyncMachine namespace; * allows all namespaces. |
| `spec.allowedSourceNamespaces` | `[]string` | no |  |  | Namespaces allowed to attach BackupSources to this machine. Defaults to [.] where . means this RsyncMachine namespace; * allows all namespaces. |
| `spec.concurrencyPolicy` | `string` | no |  | "Forbid", "Replace" | Behavior when a scheduled run overlaps an active run. Defaults to Forbid. |
| `spec.image` | `string` | no |  |  | Data-plane image override for jobs created for this machine. |
| `spec.imagePullSecrets` | `[]object` | no |  |  | Image pull secrets for generated target-side jobs. |
| `spec.imagePullSecrets[].name` | `string` | yes |  |  | Name of the image pull secret. |
| `spec.nodeSelector` | `map[string]string` | no |  |  | Node labels required for generated target-side jobs. |
| `spec.priorityClassName` | `string` | no |  |  | PriorityClass for generated target-side jobs. |
| `spec.pvcName` | `string` | yes |  |  | Target PVC that stores restore points. |
| `spec.resources` | `object` | no |  |  | Container resource requests and limits for generated target-side jobs. |
| `spec.retention` | `object` | no |  |  | Restore point retention counts. |
| `spec.retention.daily` | `integer/int32` | no |  |  | Number of daily restore points to keep. |
| `spec.retention.hourly` | `integer/int32` | no |  |  | Number of hourly restore points to keep. |
| `spec.retention.monthly` | `integer/int32` | no |  |  | Number of monthly restore points to keep. |
| `spec.retention.weekly` | `integer/int32` | no |  |  | Number of weekly restore points to keep. |
| `spec.runHistory` | `object` | no |  |  | Completed BackupJob history retention. |
| `spec.runHistory.count` | `integer/int32` | no |  |  | Total completed BackupJob records to keep across all terminal phases. With count set the successful and failed limits only apply when explicitly configured. |
| `spec.runHistory.failed` | `integer/int32` | no |  |  | Failed BackupJob records to keep. |
| `spec.runHistory.successful` | `integer/int32` | no |  |  | Successful BackupJob records to keep. |
| `spec.runtimeClassName` | `string` | no |  |  | RuntimeClass for generated target-side jobs. |
| `spec.schedule` | `string` | no |  |  | Cron expression for scheduled backups. Omit for manual-only backups. |
| `spec.schedulerName` | `string` | no |  |  | Scheduler name for generated target-side jobs. |
| `spec.securityContext` | `object` | no |  |  | Pod security context for generated target-side jobs. |
| `spec.strategy` | `object` | no |  |  | Backup storage strategy. Defaults to Snapshot. |
| `spec.strategy.type` | `string` | no |  | "Snapshot", "Mirror" | Snapshot keeps timestamped restore points; Mirror keeps only the current target tree. |
| `spec.tolerations` | `[]object` | no |  |  | Tolerations for generated target-side jobs. |
| `spec.topologySpreadConstraints` | `[]object` | no |  |  | Topology spread constraints for generated target-side jobs. |

#### Status

| Field | Type | Required | Default | Enum | Description |
| --- | --- | --- | --- | --- | --- |
| `status.conditions` | `[]object` | no |  |  | Kubernetes-style conditions for the resource. |
| `status.conditions[].lastTransitionTime` | `string/date-time` | yes |  |  | Time the condition last changed status. |
| `status.conditions[].message` | `string` | yes |  |  | Human-readable condition details. |
| `status.conditions[].observedGeneration` | `integer/int64` | no |  |  | Resource generation observed for the condition. |
| `status.conditions[].reason` | `string` | yes |  |  | Machine-readable reason for the condition. |
| `status.conditions[].status` | `string` | yes |  | "True", "False", "Unknown" | Condition status. |
| `status.conditions[].type` | `string` | yes |  |  | Condition type. |
| `status.lastScheduledAt` | `string/date-time` | no |  |  | Most recent time the schedule created a run. |
| `status.restorePointCount` | `integer/int32` | no |  |  | Number of restore points currently reported by the target PVC. |
| `status.restorePoints` | `[]object` | no |  |  | Restore points currently available on the target PVC. |
| `status.restorePoints[].bytesTransferred` | `integer/int64` | no |  |  | Bytes transferred when this restore point was created. |
| `status.restorePoints[].createdAt` | `string/date-time` | no |  |  | Time this restore point was created. |
| `status.restorePoints[].resolvesTo` | `string` | no |  |  | Immutable restore point that this value resolves to. |
| `status.restorePoints[].snapshot` | `string` | yes |  |  | User-facing restore point value. |
| `status.restorePoints[].tier` | `string` | no |  |  | Restore point tier. |
| `status.restorePointsUpdatedAt` | `string/date-time` | no |  |  | Time restore point status was last refreshed. |

