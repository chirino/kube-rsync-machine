package dataplane

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/chirino/kube-rsync-machine/internal/control"
	"github.com/chirino/kube-rsync-machine/internal/controlgrpc"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	"google.golang.org/grpc"
)

func TestControlClientNoopWithoutGRPCEndpoint(t *testing.T) {
	client := ControlClient{}
	if err := client.ReportSource(context.Background(), control.SourceEvent{}); err != nil {
		t.Fatal(err)
	}
}

func TestControlClientReportSourceGRPC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ca, err := tlsutil.NewRunCA("operator", "control-grpc", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	serverIdentity := tlsutil.TargetIdentity("operator", "control-grpc", "kube-rsync-machine-operator", "kube-rsync-machine-manager")
	serverBundle, err := ca.Mint(serverIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity := tlsutil.SourceIdentity("backup", "run-1", "app", "files")
	clientBundle, err := ca.Mint(clientIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity := tlsutil.TargetIdentity("backup", "run-1", "backup", "archive")
	targetBundle, err := ca.Mint(targetIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	serverCreds, err := controlgrpc.ServerCredentials(serverBundle, clientBundle.CACertPEM)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	controlService := control.NewService(control.NewEventHub(4))
	grpcServer := grpc.NewServer(grpc.Creds(serverCreds))
	controlgrpc.Register(grpcServer, controlService)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	client := ControlClient{
		GRPCEndpoint:    listener.Addr().String(),
		TLSBundle:       clientBundle,
		ControlCAPEM:    serverBundle.CACertPEM,
		ExpectedControl: serverIdentity,
	}
	if err := client.ReportSource(ctx, control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Succeeded",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := controlService.EnqueueRecover(control.RunKey{Namespace: "backup", Name: "run-1", Kind: control.RunKindBackup}, "recover-run-1", control.RecoverSpace{
		FailedSourceNamespace: "app",
		FailedSourceName:      "files",
		Reason:                "TargetOutOfSpace",
		MinAvailableBytes:     64 * 1024 * 1024,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := controlService.EnqueueFinalize(control.RunKey{Namespace: "backup", Name: "run-1", Kind: control.RunKindBackup}, "finalize-run-1", control.FinalizeBackupJob{
		Timestamp: "2026-05-20T12-00-00Z",
		Sources: []control.ExpectedSource{{
			Namespace:       "app",
			Name:            "files",
			DestinationPath: "app/files",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	targetClient := ControlClient{
		GRPCEndpoint:    listener.Addr().String(),
		TLSBundle:       targetBundle,
		ControlCAPEM:    serverBundle.CACertPEM,
		ExpectedControl: serverIdentity,
	}
	recovered := false
	command, err := targetClient.WaitForFinalizeBackupJobWithRecovery(ctx, control.RegisterTargetRequest{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
	}, func(ctx context.Context, command control.TargetCommand) error {
		recovered = command.Type == control.TargetCommandRecoverSpace && command.RecoverSpace.MinAvailableBytes > 0
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !recovered {
		t.Fatal("expected recovery callback before finalize")
	}
	if command.CommandID != "finalize-run-1" || command.Finalize == nil {
		t.Fatalf("unexpected command: %#v", command)
	}
	if err := targetClient.AcknowledgeTargetCommand(ctx, control.TargetCommandAckEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
		CommandID:       command.CommandID,
		CommandType:     command.Type,
	}); err != nil {
		t.Fatal(err)
	}
}
