import { useEffect, useMemo, useState } from "react";
import { useQueries } from "@tanstack/react-query";
import {
  Archive,
  ArrowRight,
  Check,
  Copy,
  RefreshCw,
  RotateCcw,
  Search,
  ChevronDown,
  X,
} from "lucide-react";

type KubeObject<TSpec = Record<string, unknown>, TStatus = Record<string, unknown>> = {
  metadata: {
    namespace?: string;
    name: string;
    creationTimestamp?: string;
  };
  spec?: TSpec;
  status?: TStatus;
};

type ObjectList<T> = {
  items?: T[];
};

type Condition = {
  type: string;
  status: string;
  reason?: string;
  message?: string;
};

type BackupJobStatus = {
  phase?: string;
  snapshotPath?: string;
  targetPhase?: string;
  targetZone?: string;
  targetNodeZone?: string;
  target?: TargetPlacementStatus;
  transfers?: TransferStatus[];
  conditions?: Condition[];
  startedAt?: string;
  completedAt?: string;
};

type BackupJobSpec = {
  machineRef?: Ref;
};

type RestoreJobStatus = {
  phase?: string;
  restoredSnapshot?: string;
  message?: string;
  targetZone?: string;
  targetNodeZone?: string;
  target?: TargetPlacementStatus;
  transfers?: TransferStatus[];
  conditions?: Condition[];
  startedAt?: string;
  completedAt?: string;
};

type RestoreJobSpec = {
  machineRef?: Ref;
  sourceRef?: Ref;
  snapshot?: string;
  overrides?: {
    destination?: {
      namespace?: string;
      pvcName?: string;
      path?: string;
    };
  };
};

type TransferStatus = {
  source: string;
  phase?: string;
  message?: string;
  captureMethod?: string;
  volumeSnapshotName?: string;
  captureTime?: string;
  percent?: number;
  bytesTransferred?: number;
  rateBytesPerSecond?: number;
  filesTransferred?: number;
  totalFiles?: number;
  totalFileSize?: number;
  bytesSent?: number;
  bytesReceived?: number;
  speedup?: number;
  startedAt?: string;
  completedAt?: string;
};

type TargetPlacementStatus = {
  nodeName?: string;
  zone?: string;
  zoneLabel?: string;
  nodeLabels?: Record<string, string>;
};

type MachineStatus = {
  restorePointCount?: number;
  restorePoints?: RestorePoint[];
  lastScheduledAt?: string;
  conditions?: Condition[];
};

type RestorePoint = {
  snapshot: string;
  resolvesTo?: string;
  tier?: string;
  createdAt?: string;
  bytesTransferred?: number;
};

type RestorePointGroup = {
  key: string;
  label: string;
  points: RestorePoint[];
};

type RetentionPolicy = {
  hourly?: number;
  daily?: number;
  weekly?: number;
  monthly?: number;
};

type RunHistory = {
  successful?: number;
  failed?: number;
};

type MachineSpec = {
  pvcName?: string;
  strategy?: { type?: string };
  schedule?: string;
  concurrencyPolicy?: string;
  retention?: RetentionPolicy;
  runHistory?: RunHistory;
  schedulerName?: string;
  priorityClassName?: string;
  runtimeClassName?: string;
};

type SourceSpec = {
  machineRef?: Ref;
  pvc?: string;
  sourcePath?: string;
  destinationPath?: string;
  consistency?: { capture?: string };
};

type Ref = {
  namespace?: string;
  name?: string;
};

type ResourceState = {
  machines: KubeObject<MachineSpec, MachineStatus>[];
  backups: KubeObject<BackupJobSpec, BackupJobStatus>[];
  restores: KubeObject<RestoreJobSpec, RestoreJobStatus>[];
  sources: KubeObject<SourceSpec>[];
};

type SourceEvent = {
  sourceNamespace?: string;
  sourceName?: string;
  phase?: string;
  percent?: number;
  bytesTransferred?: number;
  rateBytesPerSecond?: number;
  message?: string;
  captureMethod?: string;
  captureTime?: string;
  volumeSnapshotName?: string;
};

type ControlEvent = {
  Key?: {
    Namespace?: string;
    Name?: string;
    Kind?: string;
  };
  Source?: SourceEvent;
  source?: SourceEvent;
  key?: {
    namespace?: string;
    name?: string;
    kind?: string;
  };
};

type ParsedSseEvent = {
  id?: string;
  event?: string;
  data: string;
};

type RunProgress = Record<string, TransferStatus>;

type ProgressState = Record<string, RunProgress>;

type MachineGroup = {
  machine: KubeObject<MachineSpec, MachineStatus>;
  sources: Array<{ ref: Ref; source?: KubeObject<SourceSpec> }>;
  activeBackups: KubeObject<BackupJobSpec, BackupJobStatus>[];
  activeRestores: KubeObject<RestoreJobSpec, RestoreJobStatus>[];
};

const resourceQueries = [
  ["machines", "machines"],
  ["backups", "backups"],
  ["restores", "restores"],
  ["sources", "sources"],
] as const;

function mockModeEnabled() {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | boolean | undefined> }).env;
  return env?.VITE_KRM_MOCK === "1" || new URLSearchParams(window.location.search).get("mock") === "1";
}

async function fetchResource<T>(namespace: string, resource: string): Promise<T[]> {
  if (mockModeEnabled()) {
    return getMockResource(namespace, resource) as T[];
  }
  const path = namespace
    ? `/api/v1/namespaces/${encodeURIComponent(namespace)}/${resource}`
    : `/api/v1/${resource}`;
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `request failed with ${response.status}`);
  }
  const list = (await response.json()) as ObjectList<T>;
  return list.items ?? [];
}

function phaseClass(phase?: string) {
  switch (phase) {
    case "Ready":
    case "Succeeded":
      return "text-green-700 bg-green-50 border-green-200";
    case "Running":
    case "Preparing":
    case "Finalizing":
      return "text-sky-700 bg-sky-50 border-sky-200";
    case "Waiting for target":
    case "Waiting for status":
      return "text-amber-700 bg-amber-50 border-amber-200";
    case "Not ready":
    case "Failed":
      return "text-red-600 bg-red-50 border-red-200";
    case "Canceled":
      return "text-stone2-600 bg-stone2-50 border-stone2-200";
    case "Pending":
      return "text-amber-700 bg-amber-50 border-amber-200";
    default:
      return "text-stone2-600 bg-stone2-50 border-stone2-200";
  }
}

function phaseDot(phase?: string) {
  switch (phase) {
    case "Ready":
    case "Succeeded":
      return "bg-green-500";
    case "Running":
    case "Preparing":
    case "Finalizing":
      return "bg-sky-500";
    case "Waiting for target":
    case "Waiting for status":
    case "Pending":
      return "bg-amber-500";
    case "Not ready":
    case "Failed":
      return "bg-red-400";
    default:
      return "bg-stone2-400";
  }
}

function refText(ref?: Ref, defaultNamespace?: string) {
  if (!ref?.name) return "-";
  return `${ref.namespace || defaultNamespace || ""}/${ref.name}`.replace(/^\//, "");
}

function refKey(ref?: Ref, defaultNamespace?: string) {
  return ref?.name ? `${ref.namespace || defaultNamespace || ""}/${ref.name}`.replace(/^\//, "") : "";
}

function uniqueValue(values: string[]) {
  const unique = [...new Set(values)];
  return unique.length === 1 ? unique[0] : undefined;
}

function stringValue(value: unknown) {
  return typeof value === "string" ? value : undefined;
}

function numberValue(value: unknown) {
  return typeof value === "number" ? value : undefined;
}

function refsEqual(ref: Ref | undefined, namespace: string | undefined, object: { namespace?: string; name?: string }) {
  return Boolean(ref?.name && object.name && (ref.namespace || namespace || "") === (object.namespace || namespace || "") && ref.name === object.name);
}

function objectByRef<T extends KubeObject>(items: T[], ref: Ref | undefined, defaultNamespace?: string) {
  return items.find((item) => refsEqual(ref, defaultNamespace, item.metadata));
}

function machineStrategy(machine: KubeObject<MachineSpec, MachineStatus>) {
  return machine.spec?.strategy?.type || "Snapshot";
}

function isMirrorMachine(machine: KubeObject<MachineSpec, MachineStatus>) {
  return machineStrategy(machine) === "Mirror";
}

function targetPath(machine: KubeObject<MachineSpec, MachineStatus>, ref: Ref, source: KubeObject<SourceSpec> | undefined, defaultNamespace: string) {
  const destinationPath = source?.spec?.destinationPath || "";
  if (isMirrorMachine(machine)) {
    return destinationPath && destinationPath !== "/" ? destinationPath : "/";
  }
  const sourceNamespace = ref.namespace || source?.metadata.namespace || defaultNamespace;
  return [sourceNamespace, destinationPath].filter(Boolean).join("/") || "-";
}

function restoreJobYaml(
  machine: KubeObject<MachineSpec, MachineStatus>,
  point: RestorePoint,
  sourceItem: { ref: Ref; source?: KubeObject<SourceSpec> },
  defaultNamespace: string,
) {
  const sourceNamespace = sourceItem.ref.namespace || sourceItem.source?.metadata.namespace || defaultNamespace || "default";
  const sourceName = sourceItem.ref.name || sourceItem.source?.metadata.name || "source";
  const sourcePVC = sourceItem.source?.spec?.pvc || "restore-target-pvc";
  const sourcePath = sourceItem.source?.spec?.sourcePath || "/";
  const name = dnsName(`restore-${machine.metadata.name}-${sourceName}-${point.snapshot}`);
  const lines = [
    "apiVersion: krm.chirino.github.io/v1alpha1",
    "kind: RestoreJob",
    "metadata:",
    `  name: ${name}`,
    `  namespace: ${sourceNamespace}`,
    "spec:",
    "  sourceRef:",
    `    name: ${sourceName}`,
    "# Uncomment overrides if restoring to a different destination or rsync behavior.",
    "# overrides:",
    "#   destination:",
    `#     pvcName: ${yamlString(`${sourcePVC}-restore`)}`,
    `#     path: ${yamlString(sourcePath)}`,
    "#   rsync:",
    "#     delete: false",
  ];
  if (point.snapshot !== "current") {
    lines.splice(8, 0, `  snapshot: ${yamlString(point.snapshot)}`);
  }
  return lines.join("\n");
}

function dnsName(value: string) {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return (normalized || "restore-job").slice(0, 63).replace(/-+$/g, "") || "restore-job";
}

function yamlString(value: string) {
  if (/^[a-zA-Z0-9._/-]+$/.test(value)) return value;
  return JSON.stringify(value);
}

function runKey(kind: "backups" | "restores", run: KubeObject) {
  return `${kind}/${run.metadata.namespace || ""}/${run.metadata.name}`;
}

function isActivePhase(phase?: string) {
  return ["Pending", "Preparing", "Running", "Finalizing"].includes(phase || "");
}

function backupDetail(status?: BackupJobStatus) {
  if (status?.targetPhase === "Receiving") return "Running";
  return status?.targetPhase || status?.snapshotPath;
}

function newest<T extends KubeObject>(items: T[]) {
  return [...items].sort((a, b) =>
    (b.metadata.creationTimestamp || "").localeCompare(a.metadata.creationTimestamp || ""),
  );
}

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);
  return debounced;
}

function matchesFilter(filter: string, ...fields: (string | undefined)[]): boolean {
  if (!filter) return true;
  const needle = filter.toLowerCase();
  return fields.some((f) => f?.toLowerCase().includes(needle));
}

export function App() {
  const [filterText, setFilterText] = useState("");
  const filter = useDebouncedValue(filterText, 250);

  const results = useQueries({
    queries: resourceQueries.map(([key, resource]) => ({
      queryKey: ["krm", key],
      queryFn: () => fetchResource<KubeObject>("", resource),
    })),
  });

  const loading = results.some((result) => result.isLoading);
  const fetching = results.some((result) => result.isFetching);
  const error = results.find((result) => result.error)?.error;

  const data = useMemo<ResourceState>(() => {
    const [machines, backups, restores, sources] = results.map((result) => result.data ?? []);
    return {
      machines: machines as KubeObject<MachineSpec, MachineStatus>[],
      backups: backups as KubeObject<BackupJobSpec, BackupJobStatus>[],
      restores: restores as KubeObject<RestoreJobSpec, RestoreJobStatus>[],
      sources: sources as KubeObject<SourceSpec>[],
    };
  }, [results]);

  const activeBackups = data.backups.filter((run) => isActivePhase(run.status?.phase));
  const activeRestores = data.restores.filter((run) => isActivePhase(run.status?.phase));
  const progress = useLiveRunProgress(activeBackups, activeRestores);
  const restorePointCount = data.machines.reduce((sum, machine) => sum + (machine.status?.restorePointCount || 0), 0);
  const allMachineGroups = useMemo(
    () => buildMachineGroups(data.machines, data.sources, activeBackups, activeRestores, ""),
    [activeBackups, activeRestores, data.machines, data.sources],
  );
  const sourceByRef = (ref?: Ref, defaultNamespace?: string) => objectByRef(data.sources, ref, defaultNamespace);

  const machineGroups = useMemo(() => {
    const groups = filter
      ? allMachineGroups.filter((group) =>
          matchesFilter(filter, group.machine.metadata.name, group.machine.metadata.namespace,
            ...group.sources.map((s) => s.source?.metadata.name),
            ...group.sources.map((s) => s.source?.metadata.namespace),
            ...group.activeBackups.map((b) => b.metadata.name),
            ...group.activeRestores.map((r) => r.metadata.name),
          ),
        )
      : allMachineGroups;
    return [...groups].sort((a, b) => a.machine.metadata.name.localeCompare(b.machine.metadata.name));
  }, [allMachineGroups, filter]);

  const filteredActiveRestores = useMemo(() => {
    const runs = filter
      ? activeRestores.filter((run) => matchesFilter(filter, run.metadata.name, run.metadata.namespace))
      : activeRestores;
    return [...runs].sort((a, b) => (a.status?.startedAt || "").localeCompare(b.status?.startedAt || ""));
  }, [activeRestores, filter]);

  const historicalBackups = newest(data.backups.filter((run) => !isActivePhase(run.status?.phase))).slice(0, 6);
  const historicalRestores = newest(data.restores.filter((run) => !isActivePhase(run.status?.phase))).slice(0, 6);

  const historyItems = useMemo(
    () =>
      [
      ...historicalBackups.map((run) => ({ kind: "Backup" as const, run })),
      ...historicalRestores.map((run) => ({ kind: "Restore" as const, run })),
      ].sort((a, b) => (b.run.metadata.creationTimestamp || "").localeCompare(a.run.metadata.creationTimestamp || "")),
    [historicalBackups, historicalRestores],
  );

  const filteredHistory = useMemo(() => {
    if (!filter) return historyItems;
    return historyItems.filter(({ run }) => matchesFilter(filter, run.metadata.name, run.metadata.namespace));
  }, [filter, historyItems]);

  const filteredOutCounts = {
    machines: filter ? allMachineGroups.length - machineGroups.length : 0,
    activeRestores: filter ? activeRestores.length - filteredActiveRestores.length : 0,
    history: filter ? historyItems.length - filteredHistory.length : 0,
  };

  function refresh() {
    results.forEach((result) => void result.refetch());
  }

  return (
    <main className="min-h-screen bg-parchment-50 text-stone2-800 font-body antialiased">
      <header className="sticky top-0 z-40 border-b border-parchment-200 bg-parchment-50/90 backdrop-blur-sm">
        <div className="mx-auto flex max-w-[1080px] flex-col gap-3 px-4 py-4 sm:flex-row sm:items-center sm:justify-between sm:gap-4 sm:px-8 sm:py-5">
          <div className="flex items-center gap-3.5">
            <div className="flex h-9 w-9 items-center justify-center rounded-md bg-sage-800 text-sage-200">
              <Archive size={16} />
            </div>
            <div>
              <h1 className="font-display text-[22px] font-medium text-stone2-900">Kube Rsync Machine</h1>
              <p className="mt-px text-xs font-light tracking-wide text-stone2-400">cluster dashboard</p>
            </div>
          </div>
          <div className="flex items-center gap-2 min-w-0">
            <label className="relative flex-1 sm:flex-none">
              <Search className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-stone2-300" size={14} />
              <input
                value={filterText}
                onChange={(event) => setFilterText(event.target.value)}
                className="h-9 w-full sm:w-56 rounded-md border border-parchment-300 bg-white pl-8 pr-8 text-sm text-stone2-700 outline-none placeholder:text-stone2-300 focus:border-sage-400 focus:ring-1 focus:ring-sage-200 transition"
                placeholder="filter by name or namespace"
              />
              {filterText ? (
                <button
                  type="button"
                  onClick={() => setFilterText("")}
                  className="absolute right-2.5 top-1/2 -translate-y-1/2 text-stone2-300 hover:text-stone2-500 transition"
                >
                  <X size={13} />
                </button>
              ) : null}
            </label>
            <button
              type="button"
              onClick={refresh}
              className="flex h-9 w-9 items-center justify-center rounded-md border border-parchment-300 bg-white text-stone2-400 hover:text-stone2-600 transition"
            >
              <RefreshCw size={14} className={fetching ? "animate-spin" : ""} />
            </button>
          </div>
        </div>
      </header>

      <div className="mx-auto max-w-[1080px] px-4 py-6 sm:px-8 sm:py-10">
        {error ? (
          <div className="mb-6 rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
            {(error as Error).message}
          </div>
        ) : null}

        <section className="grid grid-cols-3 gap-4 border-b border-parchment-200 pb-6 sm:flex sm:items-end sm:justify-between sm:gap-6 sm:pb-8">
          <Stat label="Scheduled" value={data.machines.filter((machine) => Boolean(machine.spec?.schedule)).length} />
          <Stat label="Active Backups" value={activeBackups.length} />
          <Stat label="Active Restores" value={activeRestores.length} />
          <Stat label="Machines" value={data.machines.length} />
          <Stat label="Restore Points" value={restorePointCount} highlight />
        </section>

        <section className="mt-10">
          <SectionHeader title="Rsync Machines" loading={loading} filteredOut={filteredOutCounts.machines} />
          <div className="space-y-3">
            {machineGroups.length === 0 ? <Empty label="No machines found" /> : null}
            {machineGroups.map((group) => (
              <MachineCard key={group.machine.metadata.name} group={group} progress={progress} />
            ))}
          </div>
        </section>

        <section className="mt-10">
          <SectionHeader title="Active Restores" loading={loading} filteredOut={filteredOutCounts.activeRestores} />
          <div className="space-y-3">
            {filteredActiveRestores.length === 0 ? <Empty label="No active restores found" /> : null}
            {newest(filteredActiveRestores).map((run) => (
              <ActiveRunCard
                key={`restore-${run.metadata.namespace}/${run.metadata.name}`}
                kind="Restore"
                run={run}
                progress={progress[runKey("restores", run)]}
                source={sourceByRef(run.spec?.sourceRef, run.metadata.namespace)}
              />
            ))}
          </div>
        </section>

        <section className="mt-10">
          <SectionHeader title="History" loading={loading} filteredOut={filteredOutCounts.history} />
          <div className="space-y-0 overflow-visible rounded-lg border border-parchment-200 bg-white">
            {filteredHistory.length === 0 ? (
              <div className="p-4"><Empty label="No completed runs found" /></div>
            ) : null}
            {filteredHistory.map(({ kind, run }) => (
              <HistoryRunRow key={`${kind.toLowerCase()}-${run.metadata.namespace}/${run.metadata.name}`} kind={kind} run={run} />
            ))}
          </div>
        </section>
      </div>

      <div className="mt-16 pb-8 text-center">
        <div className="mx-auto mb-3 h-[2px] w-6 rounded-full bg-sage-300" />
        <div className="text-[11px] tracking-wide text-stone2-300">krm</div>
      </div>
    </main>
  );
}

function useLiveRunProgress(
  backups: KubeObject<BackupJobSpec, BackupJobStatus>[],
  restores: KubeObject<RestoreJobSpec, RestoreJobStatus>[],
) {
  const [progress, setProgress] = useState<ProgressState>({});
  const mockMode = mockModeEnabled();
  const activeRuns = [
    ...backups.map((run) => ({
      key: runKey("backups", run),
      namespace: run.metadata.namespace || "",
      name: run.metadata.name,
      kind: "backup",
    })),
    ...restores.map((run) => ({
      key: runKey("restores", run),
      namespace: run.metadata.namespace || "",
      name: run.metadata.name,
      kind: "restore",
    })),
  ];
  const activeSignature = activeRuns.map((item) => item.key).join("|");
  const streamNamespace = uniqueValue(activeRuns.map((item) => item.namespace));
  const streamKinds = [...new Set(activeRuns.map((item) => item.kind))].sort();
  const streamSignature = `${streamNamespace ?? ""}|${streamKinds.join("|")}`;

  useEffect(() => {
    if (mockMode) return;
    if (activeRuns.length === 0) return;
    const activeRunKeys = new Map(activeRuns.map((run) => [`${run.kind}/${run.namespace}/${run.name}`, run.key]));
    const params = new URLSearchParams();
    if (streamNamespace) params.set("namespace", streamNamespace);
    streamKinds.forEach((kind) => params.append("kind", kind));
    const controller = new AbortController();
    let lastEventId = "";

    const handleSseEvent = (event: ParsedSseEvent) => {
      if (event.id) lastEventId = event.id;
      if (event.event !== "source" || !event.data) return;
      const envelope = JSON.parse(event.data) as ControlEvent;
      const source = (envelope.source || envelope.Source) as Record<string, unknown> | undefined;
      const key = envelope.key || {
        namespace: envelope.Key?.Namespace,
        name: envelope.Key?.Name,
        kind: envelope.Key?.Kind,
      };
      const progressKey = activeRunKeys.get(`${key.kind}/${key.namespace || ""}/${key.name || ""}`);
      if (!progressKey || !source) return;
      const sourceKey = refKey({
        namespace: stringValue(source.sourceNamespace) || stringValue(source.SourceNamespace),
        name: stringValue(source.sourceName) || stringValue(source.SourceName),
      });
      if (!sourceKey) return;
      setProgress((current) => ({
        ...current,
        [progressKey]: {
          ...(current[progressKey] || {}),
          [sourceKey]: {
            source: sourceKey,
            phase: stringValue(source.phase) || stringValue(source.Phase),
            percent: numberValue(source.percent) ?? numberValue(source.Percent),
            bytesTransferred: numberValue(source.bytesTransferred) ?? numberValue(source.BytesTransferred),
            rateBytesPerSecond: numberValue(source.rateBytesPerSecond) ?? numberValue(source.RateBytesPerSecond),
            totalFileSize: numberValue(source.totalFileSize) ?? numberValue(source.TotalFileSize),
            bytesSent: numberValue(source.bytesSent) ?? numberValue(source.BytesSent),
            bytesReceived: numberValue(source.bytesReceived) ?? numberValue(source.BytesReceived),
            filesTransferred: numberValue(source.filesTransferred) ?? numberValue(source.FilesTransferred),
            totalFiles: numberValue(source.totalFiles) ?? numberValue(source.TotalFiles),
            speedup: numberValue(source.speedup) ?? numberValue(source.Speedup),
            message: stringValue(source.message) || stringValue(source.Message),
            captureMethod: stringValue(source.captureMethod) || stringValue(source.CaptureMethod),
            captureTime: stringValue(source.captureTime) || stringValue(source.CaptureTime),
            volumeSnapshotName: stringValue(source.volumeSnapshotName) || stringValue(source.VolumeSnapshotName),
          },
        },
      }));
    };

    const connect = async () => {
      while (!controller.signal.aborted) {
        try {
          const headers: Record<string, string> = { Accept: "text/event-stream" };
          if (lastEventId) headers["Last-Event-ID"] = lastEventId;
          const response = await fetch(`/api/v1/events?${params.toString()}`, {
            headers,
            signal: controller.signal,
          });
          if (!response.ok || !response.body) {
            await delay(2000, controller.signal);
            continue;
          }
          await readSseStream(response.body, handleSseEvent, controller.signal);
        } catch (error) {
          if (controller.signal.aborted) return;
        }
        await delay(2000, controller.signal);
      }
    };

    void connect();
    return () => controller.abort();
  }, [activeSignature, mockMode, streamSignature]);

  return progress;
}

function getMockResource(namespace: string, resource: string): KubeObject[] {
  const data = buildMockData();
  const items = data[resource as keyof ResourceState] || [];
  if (!namespace) return items;
  return items.filter((item) => item.metadata.namespace === namespace);
}

function isoMinutesAgo(minutes: number) {
  return new Date(Date.now() - minutes * 60 * 1000).toISOString();
}

function buildMockData(): ResourceState {
  const startedBackup = isoMinutesAgo(18);
  const startedRestore = isoMinutesAgo(11);
  return {
    machines: [
      {
        metadata: { namespace: "backup", name: "demo-app", creationTimestamp: isoMinutesAgo(5400) },
        spec: {
          pvcName: "demo-app-archive-east",
          schedule: "0 * * * *",
          concurrencyPolicy: "Forbid",
          retention: { hourly: 24, daily: 7, weekly: 4, monthly: 6 },
          runHistory: { successful: 8, failed: 5 },
          schedulerName: "default-scheduler",
          priorityClassName: "backup-critical",
        },
        status: {
          restorePointCount: 38,
          restorePoints: [
            { snapshot: "hourly/2026-05-22T16-00-00Z", tier: "hourly", createdAt: isoMinutesAgo(58), bytesTransferred: 7858028544 },
            { snapshot: "hourly/2026-05-22T15-00-00Z", tier: "hourly", createdAt: isoMinutesAgo(118), bytesTransferred: 6627000320 },
            { snapshot: "hourly/2026-05-22T14-00-00Z", tier: "hourly", createdAt: isoMinutesAgo(178), bytesTransferred: 4278190080 },
            { snapshot: "hourly/2026-05-22T13-00-00Z", tier: "hourly", createdAt: isoMinutesAgo(238), bytesTransferred: 1226833920 },
            { snapshot: "daily/2026-05-22", tier: "daily", createdAt: isoMinutesAgo(620), bytesTransferred: 0 },
            { snapshot: "daily/2026-05-21", tier: "daily", createdAt: isoMinutesAgo(2060), bytesTransferred: 0 },
            { snapshot: "weekly/2026-05-18", tier: "weekly", createdAt: isoMinutesAgo(6120), bytesTransferred: 0 },
            { snapshot: "monthly/2026-05", tier: "monthly", createdAt: isoMinutesAgo(12520), bytesTransferred: 0 },
          ],
          lastScheduledAt: isoMinutesAgo(58),
          conditions: [{ type: "Ready", status: "True", reason: "ResolvedReferences", message: "RsyncMachine is ready." }],
        },
      },
      {
        metadata: { namespace: "backup", name: "analytics", creationTimestamp: isoMinutesAgo(4100) },
        spec: {
          pvcName: "analytics-archive",
          schedule: "*/30 * * * *",
          concurrencyPolicy: "Replace",
          retention: { hourly: 12, daily: 5, weekly: 2 },
          runHistory: { successful: 5, failed: 10 },
        },
        status: {
          restorePointCount: 12,
          restorePoints: [
            { snapshot: "hourly/2026-05-22T15-30-00Z", tier: "hourly", createdAt: isoMinutesAgo(86), bytesTransferred: 377487360 },
            { snapshot: "daily/2026-05-22", tier: "daily", createdAt: isoMinutesAgo(700), bytesTransferred: 0 },
          ],
          lastScheduledAt: isoMinutesAgo(86),
          conditions: [{ type: "Ready", status: "False", reason: "TargetUnusable", message: "Target storage is unavailable." }],
        },
      },
    ],
    sources: [
      {
        metadata: { namespace: "app-prod", name: "demo-app-files", creationTimestamp: isoMinutesAgo(5300) },
        spec: {
          machineRef: { namespace: "backup", name: "demo-app" },
          pvc: "demo-app-local",
          sourcePath: "/var/lib/demo-app/files",
          destinationPath: "sites/demo-app/files",
          consistency: { capture: "Snapshot" },
        },
      },
      {
        metadata: { namespace: "app-prod", name: "demo-app-db-dumps", creationTimestamp: isoMinutesAgo(5280) },
        spec: {
          machineRef: { namespace: "backup", name: "demo-app" },
          pvc: "demo-db-dumps",
          sourcePath: "/exports",
          destinationPath: "sites/demo-app/db-dumps",
          consistency: { capture: "Direct" },
        },
      },
      {
        metadata: { namespace: "analytics", name: "warehouse", creationTimestamp: isoMinutesAgo(4060) },
        spec: {
          machineRef: { namespace: "backup", name: "analytics" },
          pvc: "warehouse-cache",
          sourcePath: "/warehouse",
          destinationPath: "warehouse/cache",
          consistency: { capture: "Auto" },
        },
      },
    ],
    backups: [
      {
        metadata: { namespace: "backup", name: "demo-app-20260522-160000", creationTimestamp: startedBackup },
        spec: { machineRef: { namespace: "backup", name: "demo-app" } },
        status: {
          phase: "Running",
          targetPhase: "Receiving",
          targetNodeZone: "jea",
          startedAt: startedBackup,
          transfers: [
            {
              source: "app-prod/demo-app-files",
              phase: "Running",
              captureMethod: "VolumeSnapshot",
              percent: 72,
              bytesTransferred: 6442450944,
              rateBytesPerSecond: 18874368,
              filesTransferred: 18240,
              totalFiles: 25100,
              volumeSnapshotName: "krm-vs-backup-demo-app-20260522-app-prod-demo-app-files",
            },
            {
              source: "app-prod/demo-app-db-dumps",
              phase: "Succeeded",
              captureMethod: "Direct",
              percent: 100,
              bytesTransferred: 1415577600,
              rateBytesPerSecond: 9437184,
              filesTransferred: 24,
              totalFiles: 24,
            },
          ],
        },
      },
      {
        metadata: { namespace: "backup", name: "demo-app-20260522-150000", creationTimestamp: isoMinutesAgo(90) },
        spec: { machineRef: { namespace: "backup", name: "demo-app" } },
        status: {
          phase: "Succeeded",
          snapshotPath: "hourly/2026-05-22T15-00-00Z",
          startedAt: isoMinutesAgo(90),
          completedAt: isoMinutesAgo(76),
          transfers: [
            {
              source: "app-prod/demo-app-files",
              phase: "Succeeded",
              captureMethod: "VolumeSnapshot",
              bytesTransferred: 5368709120,
              rateBytesPerSecond: 14680064,
              filesTransferred: 24020,
              totalFiles: 24020,
              speedup: 4.83,
            },
            {
              source: "app-prod/demo-app-db-dumps",
              phase: "Succeeded",
              captureMethod: "Direct",
              bytesTransferred: 1258291200,
              rateBytesPerSecond: 8388608,
              filesTransferred: 24,
              totalFiles: 24,
              speedup: 1.12,
            },
          ],
        },
      },
      {
        metadata: { namespace: "backup", name: "analytics-20260522-153000", creationTimestamp: isoMinutesAgo(86) },
        spec: { machineRef: { namespace: "backup", name: "analytics" } },
        status: {
          phase: "Failed",
          snapshotPath: "hourly/2026-05-22T15-30-00Z",
          startedAt: isoMinutesAgo(86),
          completedAt: isoMinutesAgo(81),
          transfers: [
            {
              source: "analytics/warehouse",
              phase: "Failed",
              captureMethod: "Direct",
              message: "SourceSnapshotFallbackToDirect: VolumeSnapshot API was unavailable; direct rsync failed after target pod restart.",
              bytesTransferred: 377487360,
              rateBytesPerSecond: 3145728,
              filesTransferred: 810,
              totalFiles: 2048,
            },
          ],
        },
      },
    ],
    restores: [
      {
        metadata: { namespace: "app-prod", name: "demo-app-files-restore", creationTimestamp: startedRestore },
        spec: {
          machineRef: { namespace: "backup", name: "demo-app" },
          sourceRef: { namespace: "app-prod", name: "demo-app-files" },
          snapshot: "hourly/2026-05-22T15-00-00Z",
          overrides: { destination: { namespace: "app-prod", pvcName: "demo-app-restore", path: "/restore/files" } },
        },
        status: {
          phase: "Running",
          restoredSnapshot: "hourly/2026-05-22T15-00-00Z",
          startedAt: startedRestore,
          message: "Restoring selected source into temporary PVC.",
          transfers: [
            {
              source: "app-prod/demo-app-files",
              phase: "Running",
              percent: 43,
              bytesTransferred: 2415919104,
              rateBytesPerSecond: 12582912,
            },
          ],
        },
      },
      {
        metadata: { namespace: "app-prod", name: "demo-app-db-restore", creationTimestamp: isoMinutesAgo(280) },
        spec: {
          machineRef: { namespace: "backup", name: "demo-app" },
          sourceRef: { namespace: "app-prod", name: "demo-app-db-dumps" },
          snapshot: "daily/2026-05-22",
          overrides: { destination: { namespace: "app-prod", pvcName: "demo-db-restore", path: "/restore/db" } },
        },
        status: {
          phase: "Succeeded",
          restoredSnapshot: "daily/2026-05-22",
          startedAt: isoMinutesAgo(280),
          completedAt: isoMinutesAgo(268),
          transfers: [
            {
              source: "app-prod/demo-app-db-dumps",
              phase: "Succeeded",
              bytesTransferred: 1308622848,
              rateBytesPerSecond: 5242880,
              filesTransferred: 24,
              totalFiles: 24,
            },
          ],
        },
      },
    ],
  };
}

function buildMachineGroups(
  machines: KubeObject<MachineSpec, MachineStatus>[],
  sources: KubeObject<SourceSpec>[],
  activeBackups: KubeObject<BackupJobSpec, BackupJobStatus>[],
  activeRestores: KubeObject<RestoreJobSpec, RestoreJobStatus>[],
  namespace: string,
): MachineGroup[] {
  const sourceByKey = new Map(sources.map((source) => [refKey(source.metadata, namespace), source]));
  return machines.map((machine) => {
    const sourceRefs = new Map<string, Ref>();
    sources.forEach((source) => {
      if (!refsEqual(source.spec?.machineRef, source.metadata.namespace || namespace, machine.metadata)) {
        return;
      }
      const sourceRef = {
        namespace: source.metadata.namespace || namespace,
        name: source.metadata.name,
      };
      sourceRefs.set(refKey(sourceRef, namespace), sourceRef);
    });
    return {
      machine,
      sources: [...sourceRefs.values()].map((ref) => ({ ref, source: sourceByKey.get(refKey(ref, namespace)) })),
      activeBackups: activeBackups.filter((run) => refsEqual(run.spec?.machineRef, run.metadata.namespace || namespace, machine.metadata)),
      activeRestores: activeRestores.filter((run) => refsEqual(run.spec?.machineRef, run.metadata.namespace || namespace, machine.metadata)),
    };
  });
}

function normalizeSseChunk(chunk: string) {
  return chunk.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

async function readSseStream(stream: ReadableStream<Uint8Array>, onEvent: (event: ParsedSseEvent) => void, signal: AbortSignal) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += normalizeSseChunk(decoder.decode(value, { stream: true }));
      let boundaryIndex: number;
      while ((boundaryIndex = buffer.indexOf("\n\n")) >= 0) {
        const eventChunk = buffer.slice(0, boundaryIndex);
        buffer = buffer.slice(boundaryIndex + 2);
        const event = parseSseEvent(eventChunk);
        if (event) onEvent(event);
      }
    }
    if (buffer.trim()) {
      const event = parseSseEvent(buffer);
      if (event) onEvent(event);
    }
  } finally {
    reader.releaseLock();
  }
}

function parseSseEvent(chunk: string): ParsedSseEvent | undefined {
  const event: ParsedSseEvent = { data: "" };
  const dataLines: string[] = [];
  for (const line of normalizeSseChunk(chunk).split("\n")) {
    if (!line || line.startsWith(":")) continue;
    const separator = line.indexOf(":");
    const field = separator >= 0 ? line.slice(0, separator) : line;
    const rawValue = separator >= 0 ? line.slice(separator + 1) : "";
    const value = rawValue.startsWith(" ") ? rawValue.slice(1) : rawValue;
    switch (field) {
      case "id":
        event.id = value;
        break;
      case "event":
        event.event = value;
        break;
      case "data":
        dataLines.push(value);
        break;
    }
  }
  if (dataLines.length === 0) return undefined;
  event.data = dataLines.join("\n");
  return event;
}

function delay(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = window.setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        window.clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

function Tip({ label, children, className = "" }: { label: string; children: React.ReactNode; className?: string }) {
  return <span className={`tip ${className}`} data-tip={label}>{children}</span>;
}

function StatDots({ stats, className = "" }: { stats: { label: string; value: string; sep?: string }[]; className?: string }) {
  if (stats.length === 0) return null;
  return (
    <span className={`inline-flex flex-wrap items-center gap-1 ${className}`}>
      {stats.map((stat, i) => (
        <span key={stat.label} className="inline-flex items-center gap-1">
          {i > 0 && <span className="text-stone2-300">{stat.sep || "·"}</span>}
          <Tip label={stat.label}>{stat.value}</Tip>
        </span>
      ))}
    </span>
  );
}

function RefName({ value, className = "", nameLabel = "Name" }: { value: string; className?: string; nameLabel?: string }) {
  const slash = value.indexOf("/");
  if (slash < 0) return <Tip label={nameLabel} className={className}>{value}</Tip>;
  const ns = value.slice(0, slash);
  const name = value.slice(slash + 1);
  return (
    <span className={className}>
      <Tip label="Namespace" className="text-stone2-400">{ns}</Tip>
      <span className="text-stone2-300 mx-0.5">/</span>
      <Tip label={nameLabel} className="">{name}</Tip>
    </span>
  );
}

function Stat({ label, value, highlight }: { label: string; value: number; highlight?: boolean }) {
  return (
    <div className="sm:flex-1">
      <div className="mb-1 text-[9px] sm:text-[10px] font-medium uppercase tracking-[0.2em] text-stone2-400">{label}</div>
      <div className={`font-display text-2xl sm:text-4xl font-normal ${highlight ? "text-sage-700" : "text-stone2-900"}`}>{value.toLocaleString()}</div>
    </div>
  );
}

function SectionHeader({ title, loading, filteredOut = 0 }: { title: string; loading: boolean; filteredOut?: number }) {
  return (
    <div className="mb-6 flex items-center gap-3">
      <div className="section-line" />
      <h2 className="font-display text-base font-medium text-stone2-700">{title}</h2>
      {filteredOut > 0 ? <span className="text-xs text-stone2-400">({filteredOut.toLocaleString()} filtered out)</span> : null}
      {loading ? <span className="text-xs text-stone2-300">Loading</span> : null}
    </div>
  );
}

function Badge({ value, title, detail }: { value?: string; title?: string; detail?: string }) {
  const isRunning = value === "Running" || value === "Preparing" || value === "Finalizing";
  const showPopup = Boolean(detail) && value !== "Ready" && value !== "Running" && value !== "Succeeded";
  return (
    <span className={`relative shrink-0 ${showPopup ? "group" : ""}`}>
      <span title={showPopup ? undefined : title} className={`inline-flex items-center gap-1 rounded px-2 py-0.5 text-[11px] font-medium border ${phaseClass(value)} ${showPopup ? "cursor-help" : ""}`}>
        <span className={`h-1 w-1 shrink-0 rounded-full ${phaseDot(value)} ${isRunning ? "live-indicator" : ""}`} />
        {value || "Unknown"}
      </span>
      {showPopup ? (
        <span className="failure-popup">
          {detail}
        </span>
      ) : null}
    </span>
  );
}

function Tag({ value, title }: { value: string; title?: string }) {
  return (
    <span title={title} className="rounded bg-white px-2 py-0.5 text-[11px] font-medium text-stone2-500 border border-parchment-300">
      {value}
    </span>
  );
}

function MachineCard({ group, progress }: { group: MachineGroup; progress: ProgressState }) {
  const condition = group.machine.status?.conditions?.find((item) => item.type === "Ready");
  const readiness = machineReadiness(condition);
  const activeBackups = newest(group.activeBackups);
  return (
    <div className="rounded-lg border border-parchment-200 bg-white hover-lift">
      <div className="p-4 sm:p-6">
        <div className="mb-6">
          <div className="flex items-center justify-between gap-3">
            <h3 className="font-display text-xl font-medium text-stone2-900">{group.machine.metadata.name}</h3>
            <Badge value={readiness.label} title={readiness.detail} detail={readiness.detail} />
          </div>
          <div className="mt-1 flex items-center gap-3 text-xs text-stone2-400">
            <Tip label="Namespace">{group.machine.metadata.namespace || "-"}</Tip>
            <span className="h-0.5 w-0.5 rounded-full bg-stone2-300" />
            <Tip label="PVC">{group.machine.spec?.pvcName || "-"}</Tip>
            <span className="h-0.5 w-0.5 rounded-full bg-stone2-300" />
            <Tip label="Strategy">{machineStrategy(group.machine)}</Tip>
          </div>
        </div>

        {activeBackups.length > 0 ? (
          <div className="mb-6 space-y-3">
            {activeBackups.map((backup) => (
              <ActiveRunCard
                key={`backup-${backup.metadata.namespace}/${backup.metadata.name}`}
                kind="Backup"
                run={backup}
                progress={progress[runKey("backups", backup)]}
                compact
              />
            ))}
          </div>
        ) : null}

        <div className="mb-6">
          <div className="mb-3 flex items-center gap-2">
            <div className="section-line" />
            <span className="text-[10px] font-medium uppercase tracking-[0.2em] text-stone2-400">Sources</span>
          </div>
          <div className="space-y-2">
            {group.sources.length === 0 ? <EmptySmall label="No sources assigned" /> : null}
            {[...group.sources].sort((a, b) => (a.ref.name || "").localeCompare(b.ref.name || "")).map(({ ref, source }) => (
              <div key={refKey(ref)} className="rounded-md border border-parchment-200 bg-parchment-100 p-3">
                <RefName value={refText(ref)} className="text-sm font-medium text-stone2-800" nameLabel="Source" />
                <div className="mt-0.5 flex flex-wrap items-center gap-1 text-xs text-stone2-400">
                  <Tip label="PVC">{source?.spec?.pvc || "outside namespace"}</Tip>
                  <span>&middot;</span>
                  <Tip label="Capture">{source?.spec?.consistency?.capture || "Auto"}</Tip>
                  <span>&middot;</span>
                  <Tip label="Source Path">{source?.spec?.sourcePath || "/"}</Tip>
                  <ArrowRight size={10} className="shrink-0 text-stone2-300" />
                  <Tip label="Target Path" className="break-all">{targetPath(group.machine, ref, source, "")}</Tip>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="mb-6">
          <div className="mb-3 flex items-center gap-2">
            <div className="section-line" />
            <span className="text-[10px] font-medium uppercase tracking-[0.2em] text-stone2-400">Backup Schedule</span>
          </div>
          <ScheduleDetails machine={group.machine} />
        </div>

        <RestorePointsSection
          machine={group.machine}
          sources={group.sources}
          points={group.machine.status?.restorePoints || []}
          count={group.machine.status?.restorePointCount || 0}
        />
      </div>
    </div>
  );
}

function ScheduleDetails({ machine }: { machine: KubeObject<MachineSpec, MachineStatus> }) {
  const spec = machine.spec;
  const concurrency = spec?.concurrencyPolicy || "Forbid";
  const successfulRuns = spec?.runHistory?.successful ?? 5;
  const failedRuns = spec?.runHistory?.failed ?? 5;
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-2 text-sm sm:gap-x-8">
      <div>
        <span className="text-xs text-stone2-400">Cron</span>{" "}
        <Tip label={cronDescription(spec?.schedule)} className="ml-1 text-stone2-700">{spec?.schedule || "manual"}</Tip>
      </div>
      <div>
        <span className="text-xs text-stone2-400">Concurrency</span>{" "}
        <Tip label={concurrencyDescription(concurrency)} className="ml-1 text-stone2-700">{concurrency}</Tip>
      </div>
      <div>
        <span className="text-xs text-stone2-400">Strategy</span>{" "}
        <Tip label={strategyDescription(machineStrategy(machine))} className="ml-1 text-stone2-700">{machineStrategy(machine)}</Tip>
      </div>
      <div>
        <span className="text-xs text-stone2-400">Restore Points</span>{" "}
        {isMirrorMachine(machine) ? <span className="ml-1 text-stone2-700">current mirror</span> : <RetentionDetails retention={spec?.retention} />}
      </div>
      <div>
        <span className="text-xs text-stone2-400">Runs</span>{" "}
        <Tip label={`Retain ${successfulRuns} successful runs`} className="ml-1 text-sage-700">{successfulRuns}</Tip>
        <span className="px-1 text-stone2-300">/</span>
        <Tip label={`Retain ${failedRuns} failed runs`} className="text-red-400">{failedRuns}</Tip>
      </div>
      {spec?.schedulerName ? (
        <div><span className="text-xs text-stone2-400">Scheduler</span> <span className="ml-1 text-stone2-700">{spec.schedulerName}</span></div>
      ) : null}
      {spec?.priorityClassName ? (
        <div><span className="text-xs text-stone2-400">Priority</span> <span className="ml-1 text-stone2-700">{spec.priorityClassName}</span></div>
      ) : null}
      {spec?.runtimeClassName ? (
        <div><span className="text-xs text-stone2-400">Runtime</span> <span className="ml-1 text-stone2-700">{spec.runtimeClassName}</span></div>
      ) : null}
    </div>
  );
}

function RestorePointsSection({
  machine,
  sources,
  points,
  count,
}: {
  machine: KubeObject<MachineSpec, MachineStatus>;
  sources: Array<{ ref: Ref; source?: KubeObject<SourceSpec> }>;
  points: RestorePoint[];
  count: number;
}) {
  const namespace = machine.metadata.namespace || "";
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({
    hourly: false,
    daily: false,
    weekly: false,
    monthly: false,
  });
  const [selectedPoint, setSelectedPoint] = useState<RestorePoint | undefined>();
  const [selectedSource, setSelectedSource] = useState<{ ref: Ref; source?: KubeObject<SourceSpec> } | undefined>();
  const mirror = isMirrorMachine(machine);
  const currentMirrorPoint: RestorePoint = { snapshot: "current" };
  const groups = groupRestorePoints(points);
  const hasPoints = groups.some((group) => group.points.length > 0);
  function toggleGroup(groupKey: string) {
    setExpandedGroups((current) => ({
      ...current,
      [groupKey]: !current[groupKey],
    }));
  }
  return (
    <div>
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="section-line" />
          <span className="text-[10px] font-medium uppercase tracking-[0.2em] text-stone2-400">{mirror ? "Current Mirror" : "Restore Points"}</span>
        </div>
        <span className="text-xs text-stone2-300">{mirror ? "current" : count.toLocaleString()}</span>
      </div>
      {mirror ? (
        <div className="flex items-center justify-between rounded-md border border-parchment-200 bg-parchment-100 px-4 py-3">
          <div>
            <Tip label="Snapshot" className="text-sm text-stone2-700">current</Tip>
            <div className="mt-0.5 text-[11px] text-stone2-400">Target PVC contains the latest mirror state.</div>
          </div>
          <button
            type="button"
            onClick={() => setSelectedPoint(currentMirrorPoint)}
            className="rounded px-2.5 py-1 text-[11px] font-medium text-stone2-600 bg-white border border-parchment-300 hover:border-sage-400 transition"
          >
            Restore
          </button>
        </div>
      ) : (
      <div className="overflow-hidden rounded-md border border-parchment-200">
        {!hasPoints ? <div className="p-3"><EmptySmall label="No restore points reported" /></div> : null}
        <div>
          {groups.map((group) => (
            <RestorePointGroupSection
              key={group.key}
              group={group}
              expanded={expandedGroups[group.key] ?? false}
              onToggle={() => toggleGroup(group.key)}
              onRestore={setSelectedPoint}
            />
          ))}
        </div>
      </div>
      )}
      {selectedPoint && !selectedSource ? (

        <SourcePickerModal
          point={selectedPoint}
          machine={machine}
          sources={sources}
          namespace={namespace}
          onClose={() => setSelectedPoint(undefined)}
          onSelect={(source) => setSelectedSource(source)}
        />
      ) : null}
      {selectedPoint && selectedSource ? (
        <RestoreYamlModal
          yaml={restoreJobYaml(machine, selectedPoint, selectedSource, namespace)}
          onBack={() => setSelectedSource(undefined)}
          onClose={() => {
            setSelectedSource(undefined);
            setSelectedPoint(undefined);
          }}
        />
      ) : null}
    </div>
  );
}

function RestorePointGroupSection({
  group,
  expanded,
  onToggle,
  onRestore,
}: {
  group: RestorePointGroup;
  expanded: boolean;
  onToggle: () => void;
  onRestore: (point: RestorePoint) => void;
}) {
  const count = group.points.length;
  return (
    <div className="border-b border-parchment-200 last:border-b-0">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-center justify-between px-4 py-2.5 text-left bg-white hover:bg-parchment-50 transition"
        aria-expanded={expanded}
      >
        <div className="flex items-center gap-2">
          <ChevronDown size={12} className={`shrink-0 text-stone2-400 transition-transform ${expanded ? "" : "-rotate-90"}`} />
          <span className="text-sm font-medium text-stone2-700">{group.label}</span>
          <span className="text-xs text-stone2-300 ml-1">{count.toLocaleString()}</span>
        </div>
      </button>
      {expanded ? (
        <div className="border-t border-parchment-200 bg-parchment-50">
          {count === 0 ? (
            <div className="p-3"><EmptySmall label={`No ${group.label.toLowerCase()} restore points`} /></div>
          ) : (
            <div className="divide-y divide-parchment-200">
              {group.points.map((point) => (
                <RestorePointRow key={point.snapshot} point={point} onRestore={() => onRestore(point)} />
              ))}
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}

function RestorePointRow({ point, onRestore }: { point: RestorePoint; onRestore: () => void }) {
  return (
    <div className="flex items-center justify-between px-4 py-2.5">
      <div>
        <Tip label="Snapshot" className="text-sm text-stone2-700">{point.snapshot}</Tip>
        <div className="mt-0.5 flex items-center gap-1 text-[11px] text-stone2-400">
          <Tip label="Age">{formatRelativeTime(point.createdAt)}</Tip>
          {point.bytesTransferred ? <><span>&middot;</span><Tip label="Size">{formatOptionalBytes(point.bytesTransferred)}</Tip></> : null}
        </div>
      </div>
      <button
        type="button"
        onClick={onRestore}
        className="rounded px-2.5 py-1 text-[11px] font-medium text-stone2-600 bg-white border border-parchment-300 hover:border-sage-400 transition"
      >
        Restore
      </button>
    </div>
  );
}

function SourcePickerModal({
  point,
  machine,
  sources,
  namespace,
  onClose,
  onSelect,
}: {
  point: RestorePoint;
  machine: KubeObject<MachineSpec, MachineStatus>;
  sources: Array<{ ref: Ref; source?: KubeObject<SourceSpec> }>;
  namespace: string;
  onClose: () => void;
  onSelect: (source: { ref: Ref; source?: KubeObject<SourceSpec> }) => void;
}) {
  return (
    <Modal title="Select Source" onClose={onClose}>
      <div className="mb-3 rounded-md border border-parchment-200 bg-parchment-100 px-3 py-2 text-sm text-stone2-600">
        <Field label="Restore Point" value={point.snapshot} strong />
      </div>
      <div className="grid gap-2 md:grid-cols-2">
        {sources.length === 0 ? <EmptySmall label="No sources assigned" /> : null}
        {sources.map((item) => (
          <button
            key={refKey(item.ref, namespace)}
            type="button"
            onClick={() => onSelect(item)}
            className="rounded-md border border-parchment-200 bg-white p-3 text-left hover:border-sage-400 hover:bg-parchment-50 transition"
          >
            <div>
              <div className="text-[10px] font-medium uppercase tracking-[0.15em] text-stone2-400">Source</div>
              <RefName value={refText(item.ref, namespace)} className="mt-0.5 text-sm font-semibold text-stone2-800" />
            </div>
            <div className="mt-2 grid gap-2 sm:grid-cols-2">
              <Field label="PVC" value={item.source?.spec?.pvc || "-"} />
              <Field label="Capture" value={item.source?.spec?.consistency?.capture || "Auto"} />
              <Field label="Source Path" value={item.source?.spec?.sourcePath || "/"} />
              <Field label="Target Path" value={targetPath(machine, item.ref, item.source, namespace)} />
            </div>
          </button>
        ))}
      </div>
    </Modal>
  );
}

function RestoreYamlModal({
  yaml,
  onBack,
  onClose,
}: {
  yaml: string;
  onBack: () => void;
  onClose: () => void;
}) {
  const [copied, setCopied] = useState(false);
  async function copyYaml() {
    try {
      await navigator.clipboard.writeText(yaml);
    } catch {
      copyTextFallback(yaml);
    }
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1800);
  }
  return (
    <Modal title="RestoreJob YAML" onClose={onClose}>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <button
          type="button"
          onClick={onBack}
          className="h-9 rounded-md border border-parchment-300 bg-white px-3 text-sm font-medium text-stone2-700 hover:bg-parchment-50 transition"
        >
          Back
        </button>
        <button
          type="button"
          onClick={() => void copyYaml()}
          className="inline-flex h-9 items-center gap-2 rounded-md bg-stone2-800 px-3 text-sm font-medium text-white hover:bg-stone2-700 transition"
        >
          {copied ? <Check size={16} /> : <Copy size={16} />}
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <div className="overflow-hidden rounded-md border border-stone2-200 bg-stone2-900">
        <div className="border-b border-stone2-700 px-3 py-2 text-xs font-medium text-stone2-400">yaml</div>
        <pre className="max-h-[60vh] overflow-auto p-3 text-sm leading-6 text-stone2-100">
          <code>{highlightYaml(yaml)}</code>
        </pre>
      </div>
    </Modal>
  );
}

function copyTextFallback(value: string) {
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  document.execCommand("copy");
  textarea.remove();
}

function Modal({ title, children, onClose }: { title: string; children: React.ReactNode; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-stone2-900/30 p-4">
      <div className="max-h-[90vh] w-full max-w-4xl overflow-hidden rounded-lg border border-parchment-200 bg-white shadow-xl">
        <div className="flex items-center justify-between gap-3 border-b border-parchment-200 px-4 py-3">
          <h3 className="font-display text-base font-medium text-stone2-900">{title}</h3>
          <button
            type="button"
            onClick={onClose}
            className="flex h-8 w-8 items-center justify-center rounded-md text-stone2-400 hover:bg-parchment-100 hover:text-stone2-700 transition"
            aria-label="Close"
          >
            <X size={18} />
          </button>
        </div>
        <div className="max-h-[calc(90vh-57px)] overflow-auto p-4">{children}</div>
      </div>
    </div>
  );
}

function ActiveRunCard({
  kind,
  run,
  progress,
  compact = false,
  source,
}: {
  kind: "Backup" | "Restore";
  run: KubeObject<BackupJobSpec & RestoreJobSpec, BackupJobStatus & RestoreJobStatus>;
  progress?: RunProgress;
  compact?: boolean;
  source?: KubeObject<SourceSpec>;
}) {
  const isBackup = kind === "Backup";
  const KindIcon = isBackup ? Archive : RotateCcw;
  const phase = run.status?.phase || "Unknown";
  const transfers = mergedTransfers(run, progress);
  const percent = aggregatePercent(transfers, phase);
  const transferRate = aggregateTransferRate(transfers);
  const [transfersOpen, setTransfersOpen] = useState(false);
  const condition = primaryRunCondition(run.status?.conditions);
  const zone = targetZone(run.status);
  const detail = runStatusDetail(kind, run, transfers, condition);
  return (
    <div className={`flex rounded-md border ${compact ? "border-parchment-200 bg-parchment-100" : "border-parchment-200 bg-white hover-lift"}`}>
      <div className={`w-1 shrink-0 rounded-r-full my-3 ${isBackup ? "bg-sage-400" : "bg-sky-400"}`} />
      <div className="flex-1 min-w-0 p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium tracking-wide ${isBackup ? "bg-sage-100 text-sage-700" : "bg-sky-100 text-sky-700"}`}>
            <KindIcon size={10} />
            {kind}
          </span>
          <div className="mt-1"><RefName value={`${run.metadata.namespace || ""}/${run.metadata.name}`} className="text-sm font-medium text-stone2-800" nameLabel={isBackup ? "Backup Job" : "Restore Job"} /></div>
          <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-stone2-400">
            <Tip label="Status">{detail || condition?.message || "Waiting for progress"}</Tip>
            {zone ? <Tip label="Zone">zone:{zone}</Tip> : null}
            <RunElapsed startedAt={run.status?.startedAt} />
          </div>
        </div>
        <Badge value={phase} detail={runBadgeDetail(kind, run, transfers, condition)} />
      </div>
      {kind === "Restore" ? <RestoreDetails run={run} source={source} /> : null}
      <div className="mt-4">
        <button
          type="button"
          onClick={() => setTransfersOpen((open) => !open)}
          className="mb-1 flex w-full items-center justify-between gap-3 text-left text-[11px] text-stone2-400"
        >
          <span className="flex items-center gap-1.5">
            <ChevronDown size={12} className={`transition-transform ${transfersOpen ? "" : "-rotate-90"}`} />
            {transfers.length || kind === "Restore" ? "Transfer progress" : "Waiting for source transfers"}
          </span>
          <span>{progressSummary(percent, transferRate)}</span>
        </button>
        <ProgressBar percent={percent} />
      </div>
      {transfersOpen ? (
        <div className={`mt-3 grid gap-2 ${compact ? "lg:grid-cols-2" : ""}`}>
          {transfers.length === 0 ? <EmptySmall label="No transfer events yet" /> : null}
          {transfers.map((transfer) => (
            <TransferRow key={transfer.source} transfer={transfer} />
          ))}
        </div>
      ) : null}
      </div>
    </div>
  );
}

function RestoreDetails({
  run,
  source,
}: {
  run: KubeObject<RestoreJobSpec, RestoreJobStatus>;
  source?: KubeObject<SourceSpec>;
}) {
  const snapshot = run.status?.restoredSnapshot || run.spec?.snapshot || "latest";
  const machineSourcePath = source?.spec?.destinationPath ? `${snapshot}/${source.spec.destinationPath}` : snapshot;
  const destinationNamespace = run.spec?.overrides?.destination?.namespace || run.metadata.namespace || "";
  const destinationPVC = run.spec?.overrides?.destination?.pvcName || source?.spec?.pvc || "-";
  const destinationPath = run.spec?.overrides?.destination?.path || source?.spec?.sourcePath || "/";
  return (
    <div className="mt-4 grid gap-3 md:grid-cols-2">
      <div className="rounded-md border border-parchment-200 bg-parchment-100 p-3">
        <div className="mb-2 text-[10px] font-medium uppercase tracking-[0.15em] text-stone2-400">From</div>
        <RefName value={refText(run.spec?.machineRef, run.metadata.namespace)} className="block text-sm text-stone2-700" nameLabel="Machine" />
        <Tip label="Snapshot" className="mt-0.5 block text-xs text-stone2-400">{snapshot}</Tip>
        {source?.spec?.destinationPath ? <Tip label="Path" className="mt-0.5 block text-xs text-stone2-400">{source.spec.destinationPath}</Tip> : null}
      </div>
      <div className="rounded-md border border-parchment-200 bg-parchment-100 p-3">
        <div className="mb-2 text-[10px] font-medium uppercase tracking-[0.15em] text-stone2-400">To</div>
        <div className="text-sm text-stone2-700"><Tip label="Namespace">{destinationNamespace || "-"}</Tip> / <Tip label="PVC">{destinationPVC}</Tip></div>
        <Tip label="Path" className="mt-0.5 block text-xs text-stone2-400">{destinationPath}</Tip>
      </div>
    </div>
  );
}

function RunElapsed({ startedAt }: { startedAt?: string }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  if (!startedAt) return null;
  const started = new Date(startedAt);
  if (Number.isNaN(started.getTime())) return null;
  const elapsedMs = Math.max(0, now - started.getTime());
  return (
    <Tip label={`Elapsed · ${started.toLocaleString()}`} className="shrink-0 text-stone2-300">
      {formatDuration(elapsedMs)}
    </Tip>
  );
}

function HistoryRunRow({
  kind,
  run,
}: {
  kind: "Backup" | "Restore";
  run: KubeObject<BackupJobSpec & RestoreJobSpec, BackupJobStatus & RestoreJobStatus>;
}) {
  const phase = run.status?.phase || "Unknown";
  const transfers = mergedTransfers(run);
  const startedAt = parseTime(run.status?.startedAt);
  const completedAt = parseTime(run.status?.completedAt);
  const duration = startedAt && completedAt ? completedAt.getTime() - startedAt.getTime() : undefined;
  const transferredBytes = sumOptional(transfers.map((transfer) => transfer.bytesTransferred));
  const filesTransferred = sumOptional(transfers.map((transfer) => transfer.filesTransferred));
  const totalFiles = sumOptional(transfers.map((transfer) => transfer.totalFiles));
  const aggregateRate = aggregateCompletedRate(transfers);
  const aggregateSpeedup = aggregateRsyncSpeedup(transfers);
  const detail =
    kind === "Backup"
      ? run.status?.snapshotPath || `${run.status?.transfers?.length || 0} transfers`
      : run.status?.restoredSnapshot || run.status?.message || "No restored snapshot";
  const summaryStats: { label: string; value: string; sep?: string }[] = [];
  if (typeof duration === "number") summaryStats.push({ label: "Duration", value: formatDuration(duration) });
  if (typeof transferredBytes === "number") summaryStats.push({ label: "Transferred", value: formatBytes(transferredBytes) });
  if (typeof aggregateSpeedup === "number") summaryStats.push({ label: "Speedup", value: `${aggregateSpeedup.toFixed(2)}x` });
  else if (typeof aggregateRate === "number") summaryStats.push({ label: "Rate", value: `${formatBytes(aggregateRate)}/s` });
  if (typeof filesTransferred === "number" && typeof totalFiles === "number") {
    summaryStats.push({ label: "Files Transferred", value: filesTransferred.toLocaleString() });
    summaryStats.push({ label: "Total Files", value: `${totalFiles.toLocaleString()} files`, sep: "/" });
  }

  const isBackup = kind === "Backup";
  const KindIcon = isBackup ? Archive : RotateCcw;

  return (
    <div className={`border-b border-parchment-200 last:border-b-0 pl-0 pr-5 py-3.5 transition flex ${isBackup ? "hover:bg-sage-50/40" : "hover:bg-sky-50/40"}`}>
      <div className={`w-1 shrink-0 rounded-r-full ${isBackup ? "bg-sage-400" : "bg-sky-400"}`} />
      <div className="flex-1 min-w-0 pl-4">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0 flex-1">
            <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium tracking-wide ${isBackup ? "bg-sage-100 text-sage-700" : "bg-sky-100 text-sky-700"}`}>
              <KindIcon size={10} />
              {kind}
            </span>
            <div className="mt-1"><RefName value={`${run.metadata.namespace || ""}/${run.metadata.name}`} className="text-sm font-medium text-stone2-800" nameLabel="Job" /></div>
            <div className="mt-1 text-[11px] text-stone2-400">
              <Tip label={isBackup ? "Snapshot" : "Restored Snapshot"}>{detail}</Tip>
              <span className="ml-2 text-stone2-300">{formatRelativeTime(run.status?.completedAt)}</span>
            </div>
            {summaryStats.length > 0 ? (
              <StatDots stats={summaryStats} className="mt-1 text-xs text-stone2-500" />
            ) : null}
          </div>
          <Badge value={phase} detail={failureDetail(run)} />
        </div>
        {transfers.length > 0 ? (
          <div className="mt-2 space-y-1.5">
            {transfers.map((transfer) => (
              <HistoryTransferRow key={transfer.source} transfer={transfer} />
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function HistoryTransferRow({ transfer }: { transfer: TransferStatus }) {
  const startedAt = parseTime(transfer.startedAt);
  const completedAt = parseTime(transfer.completedAt);
  const duration = startedAt && completedAt ? completedAt.getTime() - startedAt.getTime() : undefined;
  const notice = transferNotice(transfer);
  const stats: { label: string; value: string; sep?: string }[] = [{ label: "Method", value: transfer.captureMethod || "rsync" }];
  if (typeof duration === "number") stats.push({ label: "Duration", value: formatDuration(duration) });
  if (typeof transfer.bytesTransferred === "number") stats.push({ label: "Transferred", value: formatBytes(transfer.bytesTransferred) });
  if (typeof transfer.rateBytesPerSecond === "number") stats.push({ label: "Rate", value: `${formatBytes(transfer.rateBytesPerSecond)}/s` });
  if (typeof transfer.speedup === "number") stats.push({ label: "Speedup", value: `${transfer.speedup.toFixed(2)}x` });
  return (
    <div className="rounded-md border border-parchment-200 bg-parchment-100 px-3 py-2">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div><RefName value={transfer.source} className="text-sm font-medium text-stone2-700" nameLabel="Source" /></div>
          <StatDots stats={stats} className="mt-0.5 text-xs text-stone2-400" />
          {!notice && transfer.message ? <div className="mt-1 text-xs text-stone2-400">{transfer.message}</div> : null}
        </div>
        <div className="shrink-0 flex items-center gap-1.5">
          {notice ? (
            <span className="relative group">
              <span className="inline-flex items-center justify-center h-5 w-5 rounded-full bg-amber-100 border border-amber-300 text-amber-600 text-[11px] font-bold cursor-help">!</span>
              <span className="notice-popup"><span className="font-medium">{notice.title}</span> &mdash; {notice.summary}</span>
            </span>
          ) : null}
          <Badge value={transfer.phase} detail={transfer.phase === "Failed" ? transfer.message : undefined} />
        </div>
      </div>
    </div>
  );
}

function TransferRow({ transfer }: { transfer: TransferStatus }) {
  const percent = normalizedPercent(transfer.percent, transfer.phase);
  const notice = transferNotice(transfer);
  return (
    <div className="rounded-md border border-parchment-200 bg-parchment-100 px-3 py-2">
      <div className="flex items-center justify-between gap-3 text-[11px] mb-1">
        <RefName value={transfer.source} className="font-medium text-stone2-600" nameLabel="Source" />
        <Tip label="Progress" className="text-stone2-400">{percentLabel(percent)}</Tip>
      </div>
      <ProgressBar percent={percent} />
      <StatDots stats={[
        { label: "Method", value: transfer.captureMethod || "rsync" },
        ...(typeof transfer.bytesTransferred === "number" ? [{ label: "Transferred", value: formatBytes(transfer.bytesTransferred) }] : []),
        ...(typeof transfer.rateBytesPerSecond === "number" ? [{ label: "Rate", value: `${formatBytes(transfer.rateBytesPerSecond)}/s` }] : []),
      ]} className="mt-1.5 text-[10px] text-stone2-400" />
      {notice ? <TransferNotice notice={notice} /> : null}
      {!notice && transfer.message ? <div className="mt-1 text-[10px] text-stone2-400">{transfer.message}</div> : null}
    </div>
  );
}

function TransferNotice({ notice }: { notice: { title: string; summary: string; detail: string } }) {
  return (
    <div title={notice.detail} className="mt-2 rounded bg-amber-50 border border-amber-200 px-2.5 py-1.5 text-[11px] text-amber-800">
      <span className="font-medium">{notice.title}</span> &mdash; {notice.summary}
    </div>
  );
}

function Field({ label, value, strong, className = "" }: { label: string; value: string; strong?: boolean; className?: string }) {
  return (
    <div className={className}>
      <div className="text-[10px] font-medium uppercase tracking-[0.15em] text-stone2-400">{label}</div>
      <div className={`mt-0.5 truncate ${strong ? "text-sm font-semibold text-stone2-800" : "text-sm text-stone2-600"}`}>{value}</div>
    </div>
  );
}

function ProgressBar({ percent }: { percent?: number }) {
  const value = typeof percent === "number" ? Math.max(0, Math.min(100, percent)) : 0;
  return (
    <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-parchment-200">
      <div className="h-full rounded-full bg-sage-500 progress-grow" style={{ width: `${value}%` }} />
    </div>
  );
}

function Empty({ label }: { label: string }) {
  return (
    <div className="rounded-md border border-dashed border-parchment-300 bg-white px-4 py-8 text-center text-sm text-stone2-400">
      {label}
    </div>
  );
}

function EmptySmall({ label }: { label: string }) {
  return <div className="rounded-md border border-dashed border-parchment-200 bg-white px-3 py-3 text-sm text-stone2-400">{label}</div>;
}

function mergedTransfers(
  run: KubeObject<BackupJobSpec & RestoreJobSpec, BackupJobStatus & RestoreJobStatus>,
  live?: RunProgress,
): TransferStatus[] {
  const bySource = new Map<string, TransferStatus>();
  (run.status?.transfers || []).forEach((transfer) => bySource.set(transfer.source, transfer));
  Object.values(live || {}).forEach((transfer) => bySource.set(transfer.source, { ...(bySource.get(transfer.source) || {}), ...transfer }));
  if (bySource.size === 0 && run.spec?.sourceRef?.name) {
    const source = refKey(run.spec.sourceRef, run.metadata.namespace);
    bySource.set(source, { source, phase: run.status?.phase, message: run.status?.message });
  }
  return [...bySource.values()].sort((a, b) => a.source.localeCompare(b.source));
}

function aggregatePercent(transfers: TransferStatus[], phase?: string) {
  if (phase === "Succeeded") return 100;
  if (phase === "Failed" || phase === "Canceled") return undefined;
  if (transfers.length === 0) return undefined;
  const values = transfers.map((transfer) => normalizedPercent(transfer.percent, transfer.phase)).filter((value) => typeof value === "number");
  if (values.length === 0) return undefined;
  return Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
}

function aggregateTransferRate(transfers: TransferStatus[]) {
  return sumOptional(transfers.map((transfer) => transfer.rateBytesPerSecond));
}

function aggregateCompletedRate(transfers: TransferStatus[]) {
  const wireBytes = aggregateWireBytes(transfers);
  const transferredBytes = sumOptional(transfers.map((transfer) => transfer.bytesTransferred));
  const totalBytes = typeof wireBytes === "number" ? wireBytes : typeof transferredBytes === "number" && transferredBytes > 0 ? transferredBytes : undefined;
  const started = transfers.map((transfer) => parseTime(transfer.startedAt)?.getTime()).filter((value): value is number => typeof value === "number");
  const completed = transfers.map((transfer) => parseTime(transfer.completedAt)?.getTime()).filter((value): value is number => typeof value === "number");
  if (typeof totalBytes === "number" && started.length > 0 && completed.length > 0) {
    const durationSeconds = (Math.max(...completed) - Math.min(...started)) / 1000;
    if (durationSeconds > 0) return totalBytes / durationSeconds;
  }
  return averageOptional(transfers.map((transfer) => transfer.rateBytesPerSecond));
}

function aggregateRsyncSpeedup(transfers: TransferStatus[]) {
  const totalFileSize = sumOptional(transfers.map((transfer) => transfer.totalFileSize));
  const wireBytes = aggregateWireBytes(transfers);
  if (typeof totalFileSize === "number" && typeof wireBytes === "number" && wireBytes > 0) {
    return totalFileSize / wireBytes;
  }
  return averageOptional(transfers.map((transfer) => transfer.speedup));
}

function aggregateWireBytes(transfers: TransferStatus[]) {
  return sumOptional(
    transfers.map((transfer) =>
      typeof transfer.bytesSent === "number" || typeof transfer.bytesReceived === "number"
        ? (transfer.bytesSent || 0) + (transfer.bytesReceived || 0)
        : undefined,
    ),
  );
}

function machineReadiness(condition?: Condition) {
  if (!condition) {
    return {
      label: "Waiting for status",
      detail: "The operator has not reported a Ready condition for this machine yet.",
    };
  }
  const detail = conditionDetail(condition);
  if (condition.status === "True") return { label: "Ready", detail };
  if (condition.status === "False") return { label: "Not ready", detail };
  return { label: "Waiting for status", detail };
}

function conditionDetail(condition: Condition) {
  return [condition.reason, condition.message].filter(Boolean).join(": ");
}

function primaryRunCondition(conditions?: Condition[]) {
  if (!conditions?.length) return undefined;
  return (
    conditions.find((condition) => condition.type === "Failed" && condition.status === "True") ||
    conditions.find((condition) => condition.type === "TargetOverlap" && condition.status === "True") ||
    conditions.find((condition) => (condition.status === "False" || condition.status === "Unknown") && !isInformationalCondition(condition)) ||
    conditions.find((condition) => condition.type !== "Valid" && condition.message && !isInformationalCondition(condition)) ||
    conditions.find((condition) => condition.message && !isInformationalCondition(condition))
  );
}

function isInformationalCondition(condition: Condition) {
  return (
    (condition.type === "TargetOverlap" && condition.status === "False") ||
    (condition.type === "Failed" && condition.status === "False") ||
    (condition.type === "Valid" && condition.status === "True") ||
    (condition.type === "TargetReady" && condition.status === "True") ||
    (condition.type === "Transport" && condition.status === "True") ||
    (condition.type === "SnapshotCapture" && condition.status === "True")
  );
}

function runStatusDetail(
  kind: "Backup" | "Restore",
  run: KubeObject<BackupJobSpec & RestoreJobSpec, BackupJobStatus & RestoreJobStatus>,
  transfers: TransferStatus[],
  condition?: Condition,
) {
  const phase = run.status?.phase;
  if (phase === "Failed" || phase === "Canceled") return failureDetail(run);
  const conditionText = condition?.message ? conditionDetail(condition) : undefined;
  const transferWait = transferWaitingDetail(transfers);
  if (phase === "Pending") {
    return conditionText || `Waiting for the operator to start ${runRef(run)}.`;
  }
  if (phase === "Preparing") {
    if (conditionText && condition?.type !== "Valid") return conditionText;
    if (kind === "Backup") {
      if (!run.status?.targetPhase) {
        return `Waiting for pod ${run.metadata.namespace || ""}/${targetJobName(run)} to start up.`;
      }
      return transferWait || "Waiting for source pod to contact operator.";
    }
    return transferWait || "Waiting for restore pod to contact operator.";
  }
  if (phase === "Running") {
    return transferWait || conditionText || (kind === "Backup" ? backupDetail(run.status) : run.status?.message) || `${kind} is running.`;
  }
  if (phase === "Finalizing") {
    return kind === "Backup" ? "Waiting for rsync machine to send backup summary." : conditionText || "Waiting for restore final status.";
  }
  return kind === "Backup"
    ? backupDetail(run.status) || conditionText || "Waiting for progress."
    : run.status?.message || conditionText || "Restore in progress.";
}

function runBadgeDetail(
  kind: "Backup" | "Restore",
  run: KubeObject<BackupJobSpec & RestoreJobSpec, BackupJobStatus & RestoreJobStatus>,
  transfers: TransferStatus[],
  condition?: Condition,
) {
  const phase = run.status?.phase;
  if (!phase || phase === "Succeeded") return undefined;
  return runStatusDetail(kind, run, transfers, condition);
}

function transferWaitingDetail(transfers: TransferStatus[]) {
  const waiting = transfers.find((transfer) => transfer.phase === "Pending" || transfer.phase === "Preparing");
  if (waiting?.message) return `${waiting.source}: ${waiting.message}`;
  if (waiting) return `Waiting for source ${waiting.source} to contact operator.`;
  return undefined;
}

function targetJobName(run: KubeObject) {
  return `krm-target-${run.metadata.name}`;
}

function runRef(run: KubeObject) {
  return `${run.metadata.namespace || ""}/${run.metadata.name}`;
}

function failureDetail(run: KubeObject<Record<string, unknown>, BackupJobStatus & RestoreJobStatus>) {
  const phase = run.status?.phase;
  if (phase !== "Failed" && phase !== "Canceled") return undefined;
  const parts: string[] = [];
  const condition = run.status?.conditions?.find((c) => c.message);
  if (condition) parts.push(conditionDetail(condition));
  if (run.status?.message) parts.push(run.status.message);
  run.status?.transfers?.forEach((t) => {
    if (t.phase === "Failed" && t.message) parts.push(`${t.source}: ${t.message}`);
  });
  return parts.join("\n") || undefined;
}

function transferNotice(transfer: TransferStatus) {
  if (!transfer.message) return undefined;
  if (transfer.message.startsWith("SourceSnapshotFallbackToDirect")) {
    return {
      title: "Direct transfer fallback",
      summary: "VolumeSnapshot support was not detected, so this backup is continuing with direct PVC rsync.",
      detail:
        `${transfer.message}\n\n` +
        "This backup source requested automatic snapshot capture, but the cluster does not expose the Kubernetes VolumeSnapshot API. " +
        "The backup is continuing with direct PVC rsync.\n\n" +
        "To avoid this notice, set the BackupSource to use direct capture explicitly: spec.consistency.capture: Direct. " +
        "If you want snapshot-based capture instead, install the VolumeSnapshot CRDs/controller and use a CSI driver that supports snapshots.",
    };
  }
  return undefined;
}

function normalizedPercent(percent?: number, phase?: string) {
  if (phase === "Succeeded") return 100;
  if (phase === "Failed" || phase === "Canceled") return undefined;
  if (typeof percent !== "number") return undefined;
  return Math.max(0, Math.min(100, percent));
}

function percentLabel(percent?: number) {
  return typeof percent === "number" ? `${Math.round(percent)}%` : "-";
}

function progressSummary(percent?: number, rateBytesPerSecond?: number) {
  const percentText = percentLabel(percent);
  if (typeof rateBytesPerSecond !== "number") return percentText;
  return `${percentText} · ${formatBytes(rateBytesPerSecond)}/s`;
}

function targetZone(status?: BackupJobStatus & RestoreJobStatus) {
  return (
    status?.targetZone ||
    status?.targetNodeZone ||
    status?.target?.zone ||
    status?.target?.zoneLabel ||
    status?.target?.nodeLabels?.["topology.kubernetes.io/zone"] ||
    status?.target?.nodeLabels?.["failure-domain.beta.kubernetes.io/zone"]
  );
}

function highlightYaml(yaml: string) {
  return yaml.split("\n").map((line, index) => {
    const match = line.match(/^(\s*)([A-Za-z0-9_-]+):(.*)$/);
    if (!match) {
      return (
        <span key={index}>
          {line}
          {"\n"}
        </span>
      );
    }
    return (
      <span key={index}>
        {match[1]}
        <span className="text-sage-300">{match[2]}</span>
        <span className="text-stone2-500">:</span>
        <span className="text-sage-200">{match[3]}</span>
        {"\n"}
      </span>
    );
  });
}

function RetentionDetails({ retention }: { retention?: RetentionPolicy }) {
  const parts = [
    retention?.hourly ? `${retention.hourly} hourly` : undefined,
    retention?.daily ? `${retention.daily} daily` : undefined,
    retention?.weekly ? `${retention.weekly} weekly` : undefined,
    retention?.monthly ? `${retention.monthly} monthly` : undefined,
  ].filter((part): part is string => Boolean(part));
  if (parts.length === 0) return <span className="ml-1 text-stone2-700">-</span>;
  return (
    <span className="ml-1 text-stone2-700">
      {parts.map((part, index) => (
        <span key={part}>
          {index > 0 ? ", " : null}
          <Tip label="Restore Points Retention">{part}</Tip>
        </span>
      ))}
    </span>
  );
}

function concurrencyDescription(policy: string) {
  switch (policy) {
    case "Forbid":
      return "Do not start a new scheduled backup while another run for this machine is active.";
    case "Replace":
      return "Cancel the active scheduled backup and replace it with the new scheduled run.";
    default:
      return `Concurrency policy: ${policy}`;
  }
}

function strategyDescription(strategy: string) {
  switch (strategy) {
    case "Mirror":
      return "Keep only the current target PVC tree; no timestamped restore points are retained.";
    case "Snapshot":
      return "Keep timestamped restore points with retention tiers.";
    default:
      return `Backup strategy: ${strategy}`;
  }
}

function cronDescription(schedule?: string) {
  if (!schedule) return "No schedule; backups are started manually.";
  const fields = schedule.trim().split(/\s+/);
  if (fields.length !== 5) return "Custom cron schedule.";
  const [minute, hour, dayOfMonth, month, dayOfWeek] = fields;
  if (minute === "0" && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    return "Runs every hour at minute 0.";
  }
  if (hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    return `Runs every hour at minute ${minute}.`;
  }
  if (dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    return `Runs every day at ${padCronTime(hour)}:${padCronTime(minute)}.`;
  }
  if (dayOfMonth === "*" && month === "*" && dayOfWeek !== "*") {
    return `Runs on day-of-week ${dayOfWeek} at ${padCronTime(hour)}:${padCronTime(minute)}.`;
  }
  return `Cron schedule: ${schedule}`;
}

function padCronTime(value: string) {
  return /^\d+$/.test(value) ? value.padStart(2, "0") : value;
}

function formatRunHistory(runHistory?: RunHistory) {
  const successful = runHistory?.successful ?? 5;
  const failed = runHistory?.failed ?? 5;
  return `${successful} successful, ${failed} failed`;
}

function formatDateTime(value?: string) {
  const parsed = parseTime(value);
  return parsed ? parsed.toLocaleString() : "-";
}

function formatRelativeTime(value?: string) {
  const parsed = parseTime(value);
  if (!parsed) return "-";
  const seconds = Math.max(0, Math.floor((Date.now() - parsed.getTime()) / 1000));
  const units = [
    { name: "day", seconds: 86400 },
    { name: "hour", seconds: 3600 },
    { name: "minute", seconds: 60 },
  ];
  for (const unit of units) {
    const count = Math.floor(seconds / unit.seconds);
    if (count > 0) return `${count} ${unit.name}${count === 1 ? "" : "s"} ago`;
  }
  return "just now";
}

function newestRestorePoints(points: RestorePoint[]) {
  return [...points].sort((a, b) => {
    const aTime = parseTime(a.createdAt)?.getTime() || 0;
    const bTime = parseTime(b.createdAt)?.getTime() || 0;
    if (aTime !== bTime) return bTime - aTime;
    return b.snapshot.localeCompare(a.snapshot);
  });
}

function groupRestorePoints(points: RestorePoint[]): RestorePointGroup[] {
  const groups = [
    { key: "hourly", label: "Hourly", points: [] as RestorePoint[] },
    { key: "daily", label: "Daily", points: [] as RestorePoint[] },
    { key: "weekly", label: "Weekly", points: [] as RestorePoint[] },
    { key: "monthly", label: "Monthly", points: [] as RestorePoint[] },
  ];
  const byKey = new Map(groups.map((group) => [group.key, group.points]));
  const other: RestorePoint[] = [];

  newestRestorePoints(points).forEach((point) => {
    const groupKey = restorePointGroupKey(point);
    const group = byKey.get(groupKey);
    if (group) {
      group.push(point);
    } else {
      other.push(point);
    }
  });

  if (other.length > 0) {
    groups.push({ key: "other", label: "Other", points: other });
  }
  return groups;
}

function restorePointGroupKey(point: RestorePoint) {
  const tier = point.tier?.toLowerCase();
  if (tier) return tier;
  const separator = point.snapshot.indexOf("/");
  return separator > 0 ? point.snapshot.slice(0, separator).toLowerCase() : "other";
}

function formatFileCount(transferred?: number, total?: number) {
  if (typeof transferred === "number" && typeof total === "number") return `${transferred.toLocaleString()} / ${total.toLocaleString()}`;
  if (typeof transferred === "number") return transferred.toLocaleString();
  if (typeof total === "number") return total.toLocaleString();
  return "-";
}

function formatBytes(value: number) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size >= 10 || unit === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[unit]}`;
}

function formatOptionalBytes(value?: number) {
  return typeof value === "number" && value > 0 ? formatBytes(value) : "-";
}

function parseTime(value?: string) {
  if (!value) return undefined;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed;
}

function sumOptional(values: Array<number | undefined>) {
  const present = values.filter((value): value is number => typeof value === "number");
  if (present.length === 0) return undefined;
  return present.reduce((sum, value) => sum + value, 0);
}

function averageOptional(values: Array<number | undefined>) {
  const present = values.filter((value): value is number => typeof value === "number");
  if (present.length === 0) return undefined;
  return present.reduce((sum, value) => sum + value, 0) / present.length;
}

function formatDuration(ms: number) {
  const totalSeconds = Math.floor(ms / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}
