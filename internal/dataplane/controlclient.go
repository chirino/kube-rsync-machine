package dataplane

import (
	"context"
	"fmt"

	"github.com/chirino/kube-rsync-machine/internal/control"
	"github.com/chirino/kube-rsync-machine/internal/controlgrpc"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	"google.golang.org/grpc"
)

type ControlClient struct {
	GRPCEndpoint    string
	TLSBundle       tlsutil.Bundle
	ControlCAPEM    []byte
	ExpectedControl tlsutil.Identity
}

func (c ControlClient) ReportTarget(ctx context.Context, event control.TargetEvent) error {
	if c.GRPCEndpoint == "" {
		return nil
	}
	return c.reportTargetGRPC(ctx, event)
}

func (c ControlClient) ReportSource(ctx context.Context, event control.SourceEvent) error {
	if c.GRPCEndpoint == "" {
		return nil
	}
	return c.reportSourceGRPC(ctx, event)
}

func (c ControlClient) WaitForFinalizeBackupJob(ctx context.Context, req control.RegisterTargetRequest) (control.TargetCommand, error) {
	return c.WaitForFinalizeBackupJobWithRecovery(ctx, req, nil)
}

func (c ControlClient) WaitForFinalizeBackupJobWithRecovery(ctx context.Context, req control.RegisterTargetRequest, recover func(context.Context, control.TargetCommand) error) (control.TargetCommand, error) {
	client, close, err := c.grpcClient(ctx)
	if err != nil {
		return control.TargetCommand{}, err
	}
	defer close()
	commands, errs, err := client.RegisterTarget(ctx, req)
	if err != nil {
		return control.TargetCommand{}, err
	}
	for {
		select {
		case command, ok := <-commands:
			if !ok {
				return control.TargetCommand{}, fmt.Errorf("target command stream closed before finalize command")
			}
			switch command.Type {
			case control.TargetCommandFinalizeBackupJob:
				if command.Finalize == nil {
					return control.TargetCommand{}, fmt.Errorf("finalize command %q missing payload", command.CommandID)
				}
				return command, nil
			case control.TargetCommandRecoverSpace:
				if command.RecoverSpace == nil {
					return control.TargetCommand{}, fmt.Errorf("recover command %q missing payload", command.CommandID)
				}
				if recover != nil {
					if err := recover(ctx, command); err != nil {
						return control.TargetCommand{}, err
					}
				}
			case control.TargetCommandAbortRun:
				if command.Abort != nil && command.Abort.Reason != "" {
					return control.TargetCommand{}, fmt.Errorf("target run aborted: %s", command.Abort.Reason)
				}
				return control.TargetCommand{}, fmt.Errorf("target run aborted")
			}
		case err, ok := <-errs:
			if ok && err != nil {
				return control.TargetCommand{}, err
			}
		case <-ctx.Done():
			return control.TargetCommand{}, ctx.Err()
		}
	}
}

func (c ControlClient) AcknowledgeTargetCommand(ctx context.Context, event control.TargetCommandAckEvent) error {
	client, close, err := c.grpcClient(ctx)
	if err != nil {
		return err
	}
	defer close()
	_, err = client.AcknowledgeTargetCommand(ctx, event)
	return err
}

func (c ControlClient) reportTargetGRPC(ctx context.Context, event control.TargetEvent) error {
	client, close, err := c.grpcClient(ctx)
	if err != nil {
		return err
	}
	defer close()
	_, err = client.ReportTarget(ctx, event)
	return err
}

func (c ControlClient) reportSourceGRPC(ctx context.Context, event control.SourceEvent) error {
	client, close, err := c.grpcClient(ctx)
	if err != nil {
		return err
	}
	defer close()
	_, err = client.ReportSource(ctx, event)
	return err
}

func (c ControlClient) grpcClient(ctx context.Context) (controlgrpc.Client, func(), error) {
	if c.GRPCEndpoint == "" {
		return controlgrpc.Client{}, func() {}, fmt.Errorf("control grpc endpoint is not configured")
	}
	creds, err := controlgrpc.ClientCredentialsWithServerCA(c.TLSBundle, c.ControlCAPEM, c.ExpectedControl)
	if err != nil {
		return controlgrpc.Client{}, func() {}, err
	}
	conn, err := grpc.DialContext(ctx, c.GRPCEndpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return controlgrpc.Client{}, func() {}, err
	}
	return controlgrpc.NewClient(conn), func() { _ = conn.Close() }, nil
}
