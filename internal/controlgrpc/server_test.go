package controlgrpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/chirino/kube-rsync-machine/internal/control"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	"google.golang.org/grpc"
)

func TestGRPCControlServiceStreamsCommandsAndReportsEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ca, err := tlsutil.NewRunCA("operator", "control", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	serverIdentity := tlsutil.TargetIdentity("operator", "control", "kube-rsync-machine-operator", "manager")
	serverBundle, err := ca.Mint(serverIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity := tlsutil.TargetIdentity("backup", "run-1", "backup", "archive")
	clientBundle, err := ca.Mint(clientIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sourceIdentity := tlsutil.SourceIdentity("backup", "run-1", "app", "files")
	sourceBundle, err := ca.Mint(sourceIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	serverCreds, err := ServerCredentials(serverBundle, clientBundle.CACertPEM)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	controlService := control.NewService(control.NewEventHub(8))
	grpcServer := grpc.NewServer(grpc.Creds(serverCreds))
	Register(grpcServer, controlService)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	clientCreds, err := ClientCredentials(clientBundle, serverIdentity)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := NewClient(conn)

	key := control.RunKey{Namespace: "backup", Name: "run-1", Kind: control.RunKindBackup}
	if _, err := controlService.EnqueueFinalize(key, "finalize-run-1", control.FinalizeBackupJob{
		Timestamp: "2026-05-20T12-00-00Z",
		Sources: []control.ExpectedSource{{
			Namespace:       "app",
			Name:            "files",
			DestinationPath: "app/files",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	commands, errs, err := client.RegisterTarget(ctx, control.RegisterTargetRequest{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case command := <-commands:
		if command.CommandID != "finalize-run-1" || command.Finalize == nil {
			t.Fatalf("unexpected command: %#v", command)
		}
	case err := <-errs:
		t.Fatalf("stream failed: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	targetAck, err := client.ReportTarget(ctx, control.TargetEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
		Phase:           "Ready",
	})
	if err != nil {
		t.Fatal(err)
	}
	if targetAck.LastSequence == 0 {
		t.Fatal("expected target event sequence")
	}
	commandAck, err := client.AcknowledgeTargetCommand(ctx, control.TargetCommandAckEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
		CommandID:       "finalize-run-1",
		CommandType:     control.TargetCommandFinalizeBackupJob,
	})
	if err != nil {
		t.Fatal(err)
	}
	if commandAck.LastSequence <= targetAck.LastSequence {
		t.Fatal("expected command ack to advance event sequence")
	}
	sourceCreds, err := ClientCredentials(sourceBundle, serverIdentity)
	if err != nil {
		t.Fatal(err)
	}
	sourceConn, err := grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(sourceCreds))
	if err != nil {
		t.Fatal(err)
	}
	defer sourceConn.Close()
	sourceClient := NewClient(sourceConn)
	sourceAck, err := sourceClient.ReportSource(ctx, control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Succeeded",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sourceAck.LastSequence <= commandAck.LastSequence {
		t.Fatal("expected source event to advance event sequence")
	}
}

func TestGRPCPeerIdentityFromMTLSContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ca, err := tlsutil.NewRunCA("operator", "control", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	serverIdentity := tlsutil.TargetIdentity("operator", "control", "kube-rsync-machine-operator", "manager")
	serverBundle, err := ca.Mint(serverIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity := tlsutil.SourceIdentity("backup", "run-1", "app", "files")
	clientBundle, err := ca.Mint(clientIdentity, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	serverCreds, err := ServerCredentials(serverBundle, clientBundle.CACertPEM)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var observed tlsutil.Identity
	grpcServer := grpc.NewServer(grpc.Creds(serverCreds), grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var ok bool
		observed, ok = PeerIdentity(ctx)
		if !ok {
			t.Fatal("expected peer identity")
		}
		return handler(ctx, req)
	}))
	Register(grpcServer, control.NewService(control.NewEventHub(4)))
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	clientCreds, err := ClientCredentials(clientBundle, serverIdentity)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := NewClient(conn)
	if _, err := client.ReportSource(ctx, control.SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         control.RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
	}); err != nil {
		t.Fatal(err)
	}
	if observed != clientIdentity {
		t.Fatalf("expected peer identity %#v, got %#v", clientIdentity, observed)
	}
}
