package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chirino/kube-rsync-machine/internal/control"
	"github.com/chirino/kube-rsync-machine/internal/dataplane"
	"github.com/chirino/kube-rsync-machine/internal/manager"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
)

const defaultTLSMountPath = "/var/run/kube-rsync-machine/tls"

type App struct {
	version string
	commit  string
	date    string
	out     io.Writer
	errOut  io.Writer
}

func New(version, commit, date string) *App {
	return &App{
		version: version,
		commit:  commit,
		date:    date,
		out:     os.Stdout,
		errOut:  os.Stderr,
	}
}

func (a *App) Run(ctx context.Context, args []string) error {
	if len(args) < 2 {
		a.usage()
		return errors.New("missing command")
	}
	switch args[1] {
	case "version":
		fmt.Fprintf(a.out, "kube-rsync-machine %s commit=%s date=%s\n", a.version, a.commit, a.date)
		return nil
	case "manager":
		return a.manager(args[2:])
	case "serve-target":
		return a.serveTarget(args[2:])
	case "send-source":
		return a.sendSource(ctx, args[2:])
	case "restore":
		return a.restore(ctx, args[2:])
	case "help", "-h", "--help":
		a.usage()
		return nil
	default:
		a.usage()
		return fmt.Errorf("unknown command %q", args[1])
	}
}

func (a *App) usage() {
	fmt.Fprintln(a.errOut, `Usage:
  kube-rsync-machine manager
  kube-rsync-machine serve-target --target PATH --run-id ID --timestamp TS --source ns:path [--strategy snapshot|mirror] [--retention-hourly N]
  kube-rsync-machine serve-target --target PATH --restore-snapshot SNAPSHOT --restore-source PATH --restore-writer ns/name --tls-dir DIR
  kube-rsync-machine send-source --source PATH --target PATH [--delete] [--one-file-system] [--dry-run]
  kube-rsync-machine restore --snapshot PATH --destination PATH [--target-endpoint HOST:PORT] [--delete] [--one-file-system] [--dry-run]
  kube-rsync-machine version`)
}

func (a *App) manager(args []string) error {
	fs := flag.NewFlagSet("manager", flag.ContinueOnError)
	fs.SetOutput(a.errOut)
	metricsAddr := fs.String("metrics-bind-address", ":8080", "metrics bind address")
	healthAddr := fs.String("health-probe-bind-address", ":8081", "health probe bind address")
	leaderElect := fs.Bool("leader-elect", false, "enable leader election")
	leaderElectionID := fs.String("leader-election-id", "kube-rsync-machine.krm.chirino.github.io", "leader election id")
	image := fs.String("image", "ghcr.io/chirino/kube-rsync-machine:latest", "data-plane image used by generated Jobs")
	liveAPIAddr := fs.String("live-api-bind-address", ":8082", "read-only live API bind address; empty disables it")
	frontendDir := fs.String("frontend-dir", os.Getenv("KRM_FRONTEND_DIR"), "directory containing built frontend files served by the live API")
	controlGRPCAddr := fs.String("control-grpc-bind-address", ":8083", "operator gRPC control API bind address")
	namespace := fs.String("namespace", manager.DefaultManagerNamespace(), "operator namespace for manager-owned Secrets")
	controlGRPCTLSSecret := fs.String("control-grpc-tls-secret", manager.DefaultControlGRPCTLSSecretName, "Secret name for the operator gRPC serving certificate")
	controlGRPCTLSTTL := fs.Duration("control-grpc-tls-ttl", manager.DefaultControlGRPCTLSCertificateTTL, "operator gRPC serving certificate lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return manager.Run(context.Background(), manager.Options{
		MetricsBindAddress:     *metricsAddr,
		HealthProbeBindAddress: *healthAddr,
		LeaderElect:            *leaderElect,
		LeaderElectionID:       *leaderElectionID,
		Image:                  *image,
		LiveAPIAddress:         *liveAPIAddr,
		FrontendDir:            *frontendDir,
		ControlGRPCAddress:     *controlGRPCAddr,
		Namespace:              *namespace,
		ControlGRPCTLSSecret:   *controlGRPCTLSSecret,
		ControlGRPCTLSTTL:      *controlGRPCTLSTTL,
	})
}

func (a *App) serveTarget(args []string) error {
	fs := flag.NewFlagSet("serve-target", flag.ContinueOnError)
	fs.SetOutput(a.errOut)
	target := fs.String("target", "", "mounted target root")
	runID := fs.String("run-id", "", "backup job id")
	tlsDir := fs.String("tls-dir", "", "directory containing ca.crt, tls.crt, and tls.key")
	controlGRPCEndpoint := fs.String("control-grpc-endpoint", "", "operator gRPC control API endpoint")
	controlGRPCNamespace := fs.String("control-grpc-namespace", manager.DefaultNamespace, "operator namespace used in the gRPC server certificate identity")
	runNamespace := fs.String("run-namespace", "", "BackupJob namespace for control events")
	runName := fs.String("run-name", "", "BackupJob name for control events")
	targetNamespace := fs.String("target-namespace", "", "RsyncMachine namespace for control events")
	targetName := fs.String("target-name", "", "RsyncMachine name for control events")
	listen := fs.String("listen", ":873", "mTLS receiver listen address when --tls-dir is set")
	timestamp := fs.String("timestamp", time.Now().UTC().Format(dataplane.SnapshotTimestampLayout), "snapshot timestamp")
	strategy := fs.String("strategy", "snapshot", "backup strategy: snapshot or mirror")
	restoreSnapshot := fs.String("restore-snapshot", "", "restore snapshot to serve from the target root")
	restoreSource := fs.String("restore-source", "", "restore source path to serve below the snapshot")
	restoreWriter := fs.String("restore-writer", "", "expected restore writer identity as namespace/name")
	retentionHourly := fs.Int("retention-hourly", 24, "hourly snapshots to retain")
	retentionDaily := fs.Int("retention-daily", 7, "daily snapshots to retain")
	retentionWeekly := fs.Int("retention-weekly", 8, "weekly snapshots to retain")
	retentionMonthly := fs.Int("retention-monthly", 12, "monthly snapshots to retain")
	var sources multiFlag
	fs.Var(&sources, "source", "expected source as namespace/path; repeatable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *target == "" {
		return errors.New("--target is required")
	}
	a.log("serve-target starting", "target", *target, "runNamespace", *runNamespace, "runName", *runName, "targetNamespace", *targetNamespace, "targetName", *targetName, "controlEndpoint", *controlGRPCEndpoint)
	var tlsBundle tlsutil.Bundle
	if *tlsDir != "" {
		a.log("loading TLS bundle", "tlsDir", *tlsDir)
		loaded, err := tlsutil.LoadBundle(*tlsDir)
		if err != nil {
			return err
		}
		tlsBundle = loaded
	}
	controlClient, err := dataPlaneControlClient(*controlGRPCEndpoint, *controlGRPCNamespace, *tlsDir, tlsBundle)
	if err != nil {
		return err
	}
	if *restoreSnapshot != "" || *restoreSource != "" || *restoreWriter != "" {
		if *restoreSnapshot == "" || *restoreSource == "" || *restoreWriter == "" {
			return errors.New("--restore-snapshot, --restore-source, and --restore-writer are required for restore serving")
		}
		if *tlsDir == "" {
			return errors.New("--tls-dir is required for restore serving")
		}
		writerNamespace, writerName, ok := strings.Cut(*restoreWriter, "/")
		if !ok || writerNamespace == "" || writerName == "" {
			return errors.New("--restore-writer must be namespace/name")
		}
		a.log("restore target waiting for writer", "listen", *listen, "writer", *restoreWriter, "snapshot", *restoreSnapshot, "source", *restoreSource)
		listener, err := net.Listen("tcp", *listen)
		if err != nil {
			return fmt.Errorf("listen for restore transfers: %w", err)
		}
		a.reportTarget(context.Background(), controlClient, control.TargetEvent{
			RunNamespace:    *runNamespace,
			RunName:         *runName,
			RunKind:         control.RunKindRestore,
			TargetNamespace: *targetNamespace,
			TargetName:      *targetName,
			Phase:           "Ready",
		})
		err = dataplane.ServeRestoreTarget(context.Background(), listener, dataplane.RestoreTargetOptions{
			TargetRoot: *target,
			TLSBundle:  tlsBundle,
			Writer: dataplane.ExpectedRestoreWriter{
				Identity: tlsutil.SourceIdentity(*runNamespace, *runName, writerNamespace, writerName),
				Snapshot: *restoreSnapshot,
				Source:   *restoreSource,
			},
			Log: a.out,
		})
		phase := "Completed"
		message := ""
		if err != nil {
			phase = "Failed"
			message = err.Error()
		}
		a.reportTarget(context.Background(), controlClient, control.TargetEvent{
			RunNamespace:    *runNamespace,
			RunName:         *runName,
			RunKind:         control.RunKindRestore,
			TargetNamespace: *targetNamespace,
			TargetName:      *targetName,
			Phase:           phase,
			Message:         message,
		})
		return err
	}
	if *runID == "" {
		return errors.New("--run-id is required")
	}
	mirrorStrategy := strings.EqualFold(*strategy, "mirror")
	if !mirrorStrategy && !strings.EqualFold(*strategy, "snapshot") {
		return fmt.Errorf("unsupported strategy %q", *strategy)
	}
	receiverSources, finalizeSources, err := dataplane.ParseExpectedTransferSourcesWithStrategy(*runNamespace, *runName, *runID, mirrorStrategy, sources)
	if err != nil {
		return err
	}
	a.log("parsed expected sources", "runID", *runID, "sourceCount", fmt.Sprintf("%d", len(receiverSources)), "finalizeSourceCount", fmt.Sprintf("%d", len(finalizeSources)))
	var receiverDone chan error
	var receiverListener net.Listener
	if *tlsDir != "" && len(receiverSources) > 0 {
		a.log("target receiver preparing listener", "listen", *listen)
		listener, err := net.Listen("tcp", *listen)
		if err != nil {
			return fmt.Errorf("listen for source transfers: %w", err)
		}
		receiverListener = listener
		receiverDone = make(chan error, 1)
		go func() {
			receiverDone <- dataplane.ServeTargetReceiver(context.Background(), listener, dataplane.TargetReceiverOptions{
				TargetRoot:              *target,
				RunID:                   *runID,
				TLSBundle:               tlsBundle,
				Sources:                 receiverSources,
				Mirror:                  mirrorStrategy,
				ContinueOnTransferError: *controlGRPCEndpoint != "",
				Log:                     a.out,
			})
		}()
		if *controlGRPCEndpoint == "" {
			if err := <-receiverDone; err != nil {
				a.reportTarget(context.Background(), controlClient, control.TargetEvent{
					RunNamespace:    *runNamespace,
					RunName:         *runName,
					RunKind:         control.RunKindBackup,
					TargetNamespace: *targetNamespace,
					TargetName:      *targetName,
					Phase:           "Failed",
					Message:         err.Error(),
				})
				return err
			}
			receiverDone = nil
		}
	}
	a.reportTarget(context.Background(), controlClient, control.TargetEvent{
		RunNamespace:    *runNamespace,
		RunName:         *runName,
		RunKind:         control.RunKindBackup,
		TargetNamespace: *targetNamespace,
		TargetName:      *targetName,
		Phase:           "Ready",
	})
	finalizeCommandID := ""
	var bytesTransferred uint64
	if *controlGRPCEndpoint != "" {
		waitReq := control.RegisterTargetRequest{
			RunNamespace:    *runNamespace,
			RunName:         *runName,
			RunKind:         control.RunKindBackup,
			TargetNamespace: *targetNamespace,
			TargetName:      *targetName,
		}
		a.log("waiting for target command", "run", namespacedName(*runNamespace, *runName), "target", namespacedName(*targetNamespace, *targetName), "command", control.TargetCommandFinalizeBackupJob)
		command, err := controlClient.WaitForFinalizeBackupJobWithRecovery(context.Background(), waitReq, func(ctx context.Context, command control.TargetCommand) error {
			a.log("received recovery command", "commandID", command.CommandID, "minAvailableBytes", fmt.Sprintf("%d", command.RecoverSpace.MinAvailableBytes), "protectedSnapshots", strings.Join(command.RecoverSpace.ProtectedSnapshots, ","))
			before, _ := dataplane.AvailableBytes(*target)
			removed, recoverErr := dataplane.RecoverSpace(*target, command.RecoverSpace.MinAvailableBytes, command.RecoverSpace.ProtectedSnapshots)
			after, _ := dataplane.AvailableBytes(*target)
			bytesFreed := uint64(0)
			if after > before {
				bytesFreed = after - before
			}
			phase := "Ready"
			message := fmt.Sprintf("recovered target space; removed %d snapshots", len(removed))
			if recoverErr != nil {
				phase = "Failed"
				message = recoverErr.Error()
			}
			a.reportTarget(ctx, controlClient, control.TargetEvent{
				RunNamespace:    *runNamespace,
				RunName:         *runName,
				RunKind:         control.RunKindBackup,
				TargetNamespace: *targetNamespace,
				TargetName:      *targetName,
				Phase:           phase,
				Message:         message,
				BytesFreed:      bytesFreed,
				Paths:           removed,
				AvailableBytes:  after,
			})
			if recoverErr != nil {
				return recoverErr
			}
			return a.ackTargetCommand(ctx, controlClient, control.TargetCommandAckEvent{
				RunNamespace:    *runNamespace,
				RunName:         *runName,
				RunKind:         control.RunKindBackup,
				TargetNamespace: *targetNamespace,
				TargetName:      *targetName,
				CommandID:       command.CommandID,
				CommandType:     command.Type,
			})
		})
		if err != nil {
			if receiverListener != nil {
				_ = receiverListener.Close()
			}
			a.reportTarget(context.Background(), controlClient, control.TargetEvent{
				RunNamespace:    *runNamespace,
				RunName:         *runName,
				RunKind:         control.RunKindBackup,
				TargetNamespace: *targetNamespace,
				TargetName:      *targetName,
				Phase:           "Failed",
				Message:         err.Error(),
			})
			return err
		}
		finalizeCommandID = command.CommandID
		bytesTransferred = command.Finalize.BytesTransferred
		a.log("received finalize command", "commandID", command.CommandID, "timestamp", command.Finalize.Timestamp, "sourceCount", fmt.Sprintf("%d", len(command.Finalize.Sources)), "bytesTransferred", fmt.Sprintf("%d", bytesTransferred))
		if command.Finalize.Timestamp != "" {
			*timestamp = command.Finalize.Timestamp
		}
		if len(command.Finalize.Sources) > 0 {
			finalizeSources = finalizeCommandSources(command.Finalize.Sources)
		}
	}
	if receiverDone != nil {
		// The controller only sends finalize after all source jobs report success,
		// but wait for the receiver goroutine so the promoted snapshot sees the
		// final on-disk state.
		a.log("waiting for receiver goroutine to finish before finalize", "run", namespacedName(*runNamespace, *runName))
		if err := <-receiverDone; err != nil {
			a.reportTarget(context.Background(), controlClient, control.TargetEvent{
				RunNamespace:    *runNamespace,
				RunName:         *runName,
				RunKind:         control.RunKindBackup,
				TargetNamespace: *targetNamespace,
				TargetName:      *targetName,
				Phase:           "Failed",
				Message:         err.Error(),
			})
			return err
		}
	}
	if mirrorStrategy {
		a.log("completing mirror backup", "target", *target, "runID", *runID, "sources", strings.Join(finalizeSources, ","))
		a.reportTarget(context.Background(), controlClient, control.TargetEvent{
			RunNamespace:    *runNamespace,
			RunName:         *runName,
			RunKind:         control.RunKindBackup,
			TargetNamespace: *targetNamespace,
			TargetName:      *targetName,
			Phase:           "Completed",
			Snapshot:        "current",
			BytesFreed:      0,
			RestorePoints:   nil,
			RestoreScanned:  true,
		})
		if finalizeCommandID != "" {
			_ = a.ackTargetCommand(context.Background(), controlClient, control.TargetCommandAckEvent{
				RunNamespace:    *runNamespace,
				RunName:         *runName,
				RunKind:         control.RunKindBackup,
				TargetNamespace: *targetNamespace,
				TargetName:      *targetName,
				CommandID:       finalizeCommandID,
				CommandType:     control.TargetCommandFinalizeBackupJob,
			})
		}
		return nil
	}
	opts := dataplane.FinalizeOptions{
		TargetRoot: *target,
		RunID:      *runID,
		Timestamp:  *timestamp,
		Sources:    finalizeSources,
		Retention: dataplane.RetentionPolicy{
			Hourly:  *retentionHourly,
			Daily:   *retentionDaily,
			Weekly:  *retentionWeekly,
			Monthly: *retentionMonthly,
		},
	}
	a.log("finalizing backup", "target", *target, "runID", *runID, "timestamp", *timestamp, "sources", strings.Join(finalizeSources, ","), "retention", fmt.Sprintf("hourly=%d daily=%d weekly=%d monthly=%d", *retentionHourly, *retentionDaily, *retentionWeekly, *retentionMonthly))
	points, err := dataplane.FinalizeBackup(opts)
	if err != nil {
		a.reportTarget(context.Background(), controlClient, control.TargetEvent{
			RunNamespace:    *runNamespace,
			RunName:         *runName,
			RunKind:         control.RunKindBackup,
			TargetNamespace: *targetNamespace,
			TargetName:      *targetName,
			Phase:           "Failed",
			Message:         err.Error(),
		})
		return err
	}
	restorePoints, scanErr := dataplane.ScanRestorePoints(*target)
	message := ""
	if scanErr != nil {
		message = scanErr.Error()
	}
	a.log("scanned restore points", "count", fmt.Sprintf("%d", len(restorePoints)), "scanError", message)
	a.reportTarget(context.Background(), controlClient, control.TargetEvent{
		RunNamespace:    *runNamespace,
		RunName:         *runName,
		RunKind:         control.RunKindBackup,
		TargetNamespace: *targetNamespace,
		TargetName:      *targetName,
		Phase:           "Completed",
		Snapshot:        "hourly/" + *timestamp,
		Message:         message,
		RestorePoints:   controlRestorePoints(restorePoints, "hourly/"+*timestamp, bytesTransferred),
		RestoreScanned:  scanErr == nil,
	})
	if finalizeCommandID != "" {
		_ = a.ackTargetCommand(context.Background(), controlClient, control.TargetCommandAckEvent{
			RunNamespace:    *runNamespace,
			RunName:         *runName,
			RunKind:         control.RunKindBackup,
			TargetNamespace: *targetNamespace,
			TargetName:      *targetName,
			CommandID:       finalizeCommandID,
			CommandType:     control.TargetCommandFinalizeBackupJob,
		})
	}
	a.logFinalizedSnapshotPoints(points)
	return nil
}

func (a *App) logFinalizedSnapshotPoints(points []string) {
	for _, point := range points {
		alias, target, ok := strings.Cut(point, " -> ")
		if ok {
			a.log("snapshot alias updated", "alias", alias, "resolvesTo", target)
			continue
		}
		a.log("snapshot created", "snapshot", point)
	}
}

func finalizeCommandSources(sources []control.ExpectedSource) []string {
	out := make([]string, 0, len(sources))
	for _, source := range sources {
		if source.DestinationPath != "" {
			out = append(out, source.DestinationPath)
		}
	}
	sort.Strings(out)
	return out
}

func controlRestorePoints(points []dataplane.RestorePoint, snapshot string, bytesTransferred uint64) []control.RestorePoint {
	out := make([]control.RestorePoint, 0, len(points))
	for _, point := range points {
		createdAt := ""
		if !point.CreatedAt.IsZero() {
			createdAt = point.CreatedAt.UTC().Format(time.RFC3339)
		}
		restorePointBytes := point.BytesTransferred
		if point.Snapshot == snapshot {
			restorePointBytes = restorePointBytesTransferred(bytesTransferred)
		}
		out = append(out, control.RestorePoint{
			Snapshot:         point.Snapshot,
			ResolvesTo:       point.ResolvesTo,
			Tier:             point.Tier,
			CreatedAt:        createdAt,
			BytesTransferred: restorePointBytes,
		})
	}
	return out
}

func restorePointBytesTransferred(bytesTransferred uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if bytesTransferred > maxInt64 {
		return int64(maxInt64)
	}
	return int64(bytesTransferred)
}

func dataPlaneControlClient(grpcEndpoint, grpcNamespace, tlsDir string, tlsBundle tlsutil.Bundle) (dataplane.ControlClient, error) {
	client := dataplane.ControlClient{}
	if grpcEndpoint == "" {
		return client, nil
	}
	if tlsDir == "" {
		return dataplane.ControlClient{}, errors.New("--tls-dir is required with --control-grpc-endpoint")
	}
	controlCA, err := os.ReadFile(filepath.Join(tlsDir, tlsutil.SecretControlCAFile))
	if err != nil {
		return dataplane.ControlClient{}, fmt.Errorf("read %s: %w", tlsutil.SecretControlCAFile, err)
	}
	client.GRPCEndpoint = grpcEndpoint
	client.TLSBundle = tlsBundle
	client.ControlCAPEM = controlCA
	client.ExpectedControl = manager.ControlGRPCServerIdentity(grpcNamespace)
	return client, nil
}

func (a *App) sendSource(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("send-source", flag.ContinueOnError)
	fs.SetOutput(a.errOut)
	source := fs.String("source", "", "source directory")
	target := fs.String("target", "", "target partial directory")
	targetEndpoint := fs.String("target-endpoint", "", "target receiver host:port")
	tlsDir := fs.String("tls-dir", "", "directory containing ca.crt, tls.crt, and tls.key")
	controlGRPCEndpoint := fs.String("control-grpc-endpoint", "", "operator gRPC control API endpoint")
	controlGRPCNamespace := fs.String("control-grpc-namespace", manager.DefaultNamespace, "operator namespace used in the gRPC server certificate identity")
	runNamespace := fs.String("run-namespace", "", "BackupJob namespace for control events")
	runName := fs.String("run-name", "", "BackupJob name for control events")
	sourceNamespace := fs.String("source-namespace", "", "BackupSource namespace for control events")
	sourceName := fs.String("source-name", "", "BackupSource name for control events")
	targetNamespace := fs.String("target-namespace", "", "RsyncMachine namespace for TLS identity verification")
	targetName := fs.String("target-name", "", "RsyncMachine name for TLS identity verification")
	deleteExtra := fs.Bool("delete", false, "delete files absent from source")
	oneFileSystem := fs.Bool("one-file-system", false, "do not cross filesystem boundaries")
	dryRun := fs.Bool("dry-run", false, "print rsync command without executing it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *source == "" || *target == "" {
		return errors.New("--source and --target are required")
	}
	a.log("send-source starting", "source", *source, "target", *target, "targetEndpoint", *targetEndpoint, "run", namespacedName(*runNamespace, *runName), "sourceRef", namespacedName(*sourceNamespace, *sourceName), "delete", fmt.Sprintf("%t", *deleteExtra), "oneFileSystem", fmt.Sprintf("%t", *oneFileSystem), "dryRun", fmt.Sprintf("%t", *dryRun))
	var tlsBundle tlsutil.Bundle
	if *tlsDir != "" {
		a.log("loading TLS bundle", "tlsDir", *tlsDir)
		loaded, err := tlsutil.LoadBundle(*tlsDir)
		if err != nil {
			return err
		}
		tlsBundle = loaded
	}
	controlClient, err := dataPlaneControlClient(*controlGRPCEndpoint, *controlGRPCNamespace, *tlsDir, tlsBundle)
	if err != nil {
		return err
	}
	a.reportSource(ctx, controlClient, control.SourceEvent{
		RunNamespace:    *runNamespace,
		RunName:         *runName,
		RunKind:         control.RunKindBackup,
		SourceNamespace: *sourceNamespace,
		SourceName:      *sourceName,
		Phase:           "Running",
		StartedAt:       time.Now().UTC().Format(time.RFC3339),
	})
	var transferStats dataplane.TransferStats
	reportProgress := func(stats dataplane.TransferStats) {
		transferStats = mergeTransferStats(transferStats, stats)
		if !stats.Summary && stats.Percent == 0 && stats.BytesTransferred == 0 && stats.RateBytesPerSecond == 0 {
			return
		}
		a.reportSource(ctx, controlClient, sourceEventWithStats(control.SourceEvent{
			RunNamespace:    *runNamespace,
			RunName:         *runName,
			RunKind:         control.RunKindBackup,
			SourceNamespace: *sourceNamespace,
			SourceName:      *sourceName,
			Phase:           "Running",
		}, stats))
	}
	var runErr error
	if *targetEndpoint != "" {
		if *targetNamespace == "" || *targetName == "" {
			return errors.New("--target-namespace and --target-name are required with --target-endpoint")
		}
		a.log("sending source over mTLS tunnel", "targetEndpoint", *targetEndpoint, "expectedTarget", tlsutil.TargetIdentity(*runNamespace, *runName, *targetNamespace, *targetName).URI())
		runErr = dataplane.SendSource(ctx, dataplane.SourceSenderOptions{
			Source:         *source,
			Destination:    *target,
			TargetEndpoint: *targetEndpoint,
			TLSBundle:      tlsBundle,
			TLSDir:         *tlsDir,
			ExpectedTarget: tlsutil.TargetIdentity(*runNamespace, *runName, *targetNamespace, *targetName),
			Delete:         *deleteExtra,
			OneFileSystem:  *oneFileSystem,
			DryRun:         *dryRun,
			Stdout:         a.out,
			Stderr:         a.errOut,
			Progress:       reportProgress,
		})
	} else {
		var stats dataplane.TransferStats
		a.log("sending source with local rsync", "source", *source, "target", *target)
		stats, runErr = dataplane.Rsync(ctx, dataplane.RsyncOptions{
			Source:        *source,
			Destination:   *target,
			Delete:        *deleteExtra,
			OneFileSystem: *oneFileSystem,
			DryRun:        *dryRun,
			Stdout:        a.out,
			Stderr:        a.errOut,
			Progress:      reportProgress,
		})
		transferStats = mergeTransferStats(transferStats, stats)
	}
	phase := "Succeeded"
	message := ""
	exitCode := int32(0)
	completedAt := time.Now().UTC().Format(time.RFC3339)
	if runErr != nil {
		phase = "Failed"
		message = runErr.Error()
		exitCode = rsyncExitCode(runErr)
	}
	a.log("send-source completed", "phase", phase, "exitCode", fmt.Sprintf("%d", exitCode), "stats", formatTransferStats(transferStats), "message", message)
	a.reportSource(ctx, controlClient, sourceEventWithStats(control.SourceEvent{
		RunNamespace:    *runNamespace,
		RunName:         *runName,
		RunKind:         control.RunKindBackup,
		SourceNamespace: *sourceNamespace,
		SourceName:      *sourceName,
		Phase:           phase,
		Message:         message,
		CompletedAt:     completedAt,
		RsyncExitCode:   exitCode,
	}, transferStats))
	return runErr
}

func (a *App) restore(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	fs.SetOutput(a.errOut)
	snapshot := fs.String("snapshot", "", "snapshot source directory")
	destination := fs.String("destination", "", "restore destination directory")
	tlsDir := fs.String("tls-dir", "", "directory containing ca.crt, tls.crt, and tls.key")
	controlGRPCEndpoint := fs.String("control-grpc-endpoint", "", "operator gRPC control API endpoint")
	controlGRPCNamespace := fs.String("control-grpc-namespace", manager.DefaultNamespace, "operator namespace used in the gRPC server certificate identity")
	runNamespace := fs.String("run-namespace", "", "RestoreJob namespace for control events")
	runName := fs.String("run-name", "", "RestoreJob name for control events")
	sourceNamespace := fs.String("source-namespace", "", "BackupSource namespace for control events")
	sourceName := fs.String("source-name", "", "BackupSource name for control events")
	snapshotSource := fs.String("snapshot-source", "", "source path below the selected snapshot when using --target-endpoint")
	targetEndpoint := fs.String("target-endpoint", "", "target restore receiver host:port")
	targetNamespace := fs.String("target-namespace", "", "RsyncMachine namespace for TLS identity verification")
	targetName := fs.String("target-name", "", "RsyncMachine name for TLS identity verification")
	deleteExtra := fs.Bool("delete", false, "delete files absent from snapshot")
	oneFileSystem := fs.Bool("one-file-system", false, "do not cross filesystem boundaries")
	dryRun := fs.Bool("dry-run", false, "print rsync command without executing it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *snapshot == "" || *destination == "" {
		return errors.New("--snapshot and --destination are required")
	}
	a.log("restore writer starting", "snapshot", *snapshot, "destination", *destination, "targetEndpoint", *targetEndpoint, "run", namespacedName(*runNamespace, *runName), "sourceRef", namespacedName(*sourceNamespace, *sourceName), "delete", fmt.Sprintf("%t", *deleteExtra), "oneFileSystem", fmt.Sprintf("%t", *oneFileSystem), "dryRun", fmt.Sprintf("%t", *dryRun))
	remoteRestore := *targetEndpoint != ""
	if !remoteRestore {
		if info, err := os.Stat(*snapshot); err != nil {
			return fmt.Errorf("snapshot path %q is not accessible: %w", *snapshot, err)
		} else if !info.IsDir() {
			return fmt.Errorf("snapshot path %q is not a directory", *snapshot)
		}
	} else {
		if *snapshotSource == "" {
			return errors.New("--snapshot-source is required with --target-endpoint")
		}
		if *targetNamespace == "" || *targetName == "" {
			return errors.New("--target-namespace and --target-name are required with --target-endpoint")
		}
		if *tlsDir == "" {
			return errors.New("--tls-dir is required with --target-endpoint")
		}
	}
	var tlsBundle tlsutil.Bundle
	if *tlsDir != "" {
		a.log("loading TLS bundle", "tlsDir", *tlsDir)
		loaded, err := tlsutil.LoadBundle(*tlsDir)
		if err != nil {
			return err
		}
		tlsBundle = loaded
	}
	controlClient, err := dataPlaneControlClient(*controlGRPCEndpoint, *controlGRPCNamespace, *tlsDir, tlsBundle)
	if err != nil {
		return err
	}
	a.reportSource(ctx, controlClient, control.SourceEvent{
		RunNamespace:    *runNamespace,
		RunName:         *runName,
		RunKind:         control.RunKindRestore,
		SourceNamespace: *sourceNamespace,
		SourceName:      *sourceName,
		Phase:           "Running",
		StartedAt:       time.Now().UTC().Format(time.RFC3339),
	})
	var transferStats dataplane.TransferStats
	reportProgress := func(stats dataplane.TransferStats) {
		transferStats = mergeTransferStats(transferStats, stats)
		if !stats.Summary && stats.Percent == 0 && stats.BytesTransferred == 0 && stats.RateBytesPerSecond == 0 {
			return
		}
		a.reportSource(ctx, controlClient, sourceEventWithStats(control.SourceEvent{
			RunNamespace:    *runNamespace,
			RunName:         *runName,
			RunKind:         control.RunKindRestore,
			SourceNamespace: *sourceNamespace,
			SourceName:      *sourceName,
			Phase:           "Running",
		}, stats))
	}
	var runErr error
	if remoteRestore {
		a.log("receiving restore over mTLS tunnel", "targetEndpoint", *targetEndpoint, "expectedTarget", tlsutil.TargetIdentity(*runNamespace, *runName, *targetNamespace, *targetName).URI(), "snapshotSource", *snapshotSource)
		runErr = dataplane.ReceiveRestore(ctx, dataplane.RestoreWriterOptions{
			Destination:    *destination,
			Snapshot:       *snapshot,
			Source:         *snapshotSource,
			TargetEndpoint: *targetEndpoint,
			TLSBundle:      tlsBundle,
			ExpectedTarget: tlsutil.TargetIdentity(*runNamespace, *runName, *targetNamespace, *targetName),
			Delete:         *deleteExtra,
			OneFileSystem:  *oneFileSystem,
			DryRun:         *dryRun,
			Stdout:         a.out,
			Stderr:         a.errOut,
			Progress:       reportProgress,
		})
	} else {
		var stats dataplane.TransferStats
		a.log("restoring with local rsync", "snapshot", *snapshot, "destination", *destination)
		stats, runErr = dataplane.Rsync(ctx, dataplane.RsyncOptions{
			Source:        *snapshot,
			Destination:   *destination,
			Delete:        *deleteExtra,
			OneFileSystem: *oneFileSystem,
			DryRun:        *dryRun,
			Stdout:        a.out,
			Stderr:        a.errOut,
			Progress:      reportProgress,
		})
		transferStats = mergeTransferStats(transferStats, stats)
	}
	phase := "Succeeded"
	message := ""
	exitCode := int32(0)
	completedAt := time.Now().UTC().Format(time.RFC3339)
	if runErr != nil {
		phase = "Failed"
		message = runErr.Error()
		exitCode = rsyncExitCode(runErr)
	}
	a.log("restore writer completed", "phase", phase, "exitCode", fmt.Sprintf("%d", exitCode), "stats", formatTransferStats(transferStats), "message", message)
	a.reportSource(ctx, controlClient, sourceEventWithStats(control.SourceEvent{
		RunNamespace:    *runNamespace,
		RunName:         *runName,
		RunKind:         control.RunKindRestore,
		SourceNamespace: *sourceNamespace,
		SourceName:      *sourceName,
		Phase:           phase,
		Message:         message,
		CompletedAt:     completedAt,
		RsyncExitCode:   exitCode,
	}, transferStats))
	return runErr
}

func sourceEventWithStats(event control.SourceEvent, stats dataplane.TransferStats) control.SourceEvent {
	event.Percent = stats.Percent
	event.BytesTransferred = stats.BytesTransferred
	event.RateBytesPerSecond = stats.RateBytesPerSecond
	event.FilesTransferred = stats.FilesTransferred
	event.TotalFiles = stats.TotalFiles
	event.TotalFileSize = stats.TotalFileSize
	event.BytesSent = stats.BytesSent
	event.BytesReceived = stats.BytesReceived
	event.Speedup = stats.Speedup
	event.StatsComplete = stats.Summary
	return event
}

func (a *App) log(message string, fields ...string) {
	if a == nil || a.out == nil {
		return
	}
	var b strings.Builder
	b.WriteString(time.Now().UTC().Format(time.RFC3339))
	b.WriteString(" krm ")
	b.WriteString(message)
	for i := 0; i+1 < len(fields); i += 2 {
		key := strings.TrimSpace(fields[i])
		if key == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(fields[i+1]))
	}
	b.WriteByte('\n')
	_, _ = io.WriteString(a.out, b.String())
}

func (a *App) reportTarget(ctx context.Context, client dataplane.ControlClient, event control.TargetEvent) {
	a.log("reporting target event", "run", namespacedName(event.RunNamespace, event.RunName), "kind", event.RunKind, "target", namespacedName(event.TargetNamespace, event.TargetName), "phase", event.Phase, "snapshot", event.Snapshot, "message", event.Message, "restorePoints", fmt.Sprintf("%d", len(event.RestorePoints)), "bytesFreed", fmt.Sprintf("%d", event.BytesFreed), "availableBytes", fmt.Sprintf("%d", event.AvailableBytes))
	if err := client.ReportTarget(ctx, event); err != nil {
		a.log("failed to report target event", "run", namespacedName(event.RunNamespace, event.RunName), "phase", event.Phase, "error", err.Error())
	}
}

func (a *App) reportSource(ctx context.Context, client dataplane.ControlClient, event control.SourceEvent) {
	a.log("reporting source event", "run", namespacedName(event.RunNamespace, event.RunName), "kind", event.RunKind, "source", namespacedName(event.SourceNamespace, event.SourceName), "phase", event.Phase, "percent", fmt.Sprintf("%d", event.Percent), "bytes", fmt.Sprintf("%d", event.BytesTransferred), "rateBytesPerSecond", fmt.Sprintf("%d", event.RateBytesPerSecond), "files", fmt.Sprintf("%d/%d", event.FilesTransferred, event.TotalFiles), "captureMethod", event.CaptureMethod, "snapshot", event.VolumeSnapshotName, "exitCode", fmt.Sprintf("%d", event.RsyncExitCode), "message", event.Message)
	if err := client.ReportSource(ctx, event); err != nil {
		a.log("failed to report source event", "run", namespacedName(event.RunNamespace, event.RunName), "source", namespacedName(event.SourceNamespace, event.SourceName), "phase", event.Phase, "error", err.Error())
	}
}

func (a *App) ackTargetCommand(ctx context.Context, client dataplane.ControlClient, event control.TargetCommandAckEvent) error {
	a.log("acknowledging target command", "run", namespacedName(event.RunNamespace, event.RunName), "kind", event.RunKind, "target", namespacedName(event.TargetNamespace, event.TargetName), "commandID", event.CommandID, "commandType", event.CommandType)
	if err := client.AcknowledgeTargetCommand(ctx, event); err != nil {
		a.log("failed to acknowledge target command", "commandID", event.CommandID, "commandType", event.CommandType, "error", err.Error())
		return err
	}
	return nil
}

func namespacedName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

func formatTransferStats(stats dataplane.TransferStats) string {
	return fmt.Sprintf("percent=%d bytes=%d rateBytesPerSecond=%d files=%d totalFiles=%d totalFileSize=%d bytesSent=%d bytesReceived=%d speedup=%.2f", stats.Percent, stats.BytesTransferred, stats.RateBytesPerSecond, stats.FilesTransferred, stats.TotalFiles, stats.TotalFileSize, stats.BytesSent, stats.BytesReceived, stats.Speedup)
}

func mergeTransferStats(existing, update dataplane.TransferStats) dataplane.TransferStats {
	if update.Summary {
		return update
	}
	if update.Percent != 0 {
		existing.Percent = update.Percent
	}
	if update.BytesTransferred != 0 {
		existing.BytesTransferred = update.BytesTransferred
	}
	if update.RateBytesPerSecond != 0 {
		existing.RateBytesPerSecond = update.RateBytesPerSecond
	}
	if update.FilesTransferred != 0 {
		existing.FilesTransferred = update.FilesTransferred
	}
	if update.TotalFiles != 0 {
		existing.TotalFiles = update.TotalFiles
	}
	if update.TotalFileSize != 0 {
		existing.TotalFileSize = update.TotalFileSize
	}
	if update.BytesSent != 0 {
		existing.BytesSent = update.BytesSent
	}
	if update.BytesReceived != 0 {
		existing.BytesReceived = update.BytesReceived
	}
	if update.Speedup != 0 {
		existing.Speedup = update.Speedup
	}
	return existing
}

func rsyncExitCode(err error) int32 {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return int32(exitErr.ExitCode())
	}
	if err != nil {
		return 1
	}
	return 0
}

type multiFlag []string

func (m *multiFlag) String() string {
	return fmt.Sprint([]string(*m))
}

func (m *multiFlag) Set(value string) error {
	*m = append(*m, value)
	return nil
}
