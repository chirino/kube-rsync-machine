package controlgrpc

import (
	"context"
	"io"

	"github.com/chirino/kube-rsync-machine/internal/control"
	"google.golang.org/grpc"
)

type Client struct {
	conn grpc.ClientConnInterface
}

func NewClient(conn grpc.ClientConnInterface) Client {
	return Client{conn: conn}
}

func (c Client) RegisterTarget(ctx context.Context, req control.RegisterTargetRequest, opts ...grpc.CallOption) (<-chan control.TargetCommand, <-chan error, error) {
	stream, err := c.conn.NewStream(ctx, &serviceDesc.Streams[0], MethodRegisterTarget, callOptions(opts)...)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.SendMsg(&req); err != nil {
		return nil, nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, nil, err
	}
	commands := make(chan control.TargetCommand)
	errs := make(chan error, 1)
	go func() {
		defer close(commands)
		defer close(errs)
		for {
			var command control.TargetCommand
			if err := stream.RecvMsg(&command); err != nil {
				if err != io.EOF {
					errs <- err
				}
				return
			}
			select {
			case commands <- command:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}
	}()
	return commands, errs, nil
}

func (c Client) ReportTarget(ctx context.Context, event control.TargetEvent, opts ...grpc.CallOption) (control.TargetEventAck, error) {
	var ack control.TargetEventAck
	err := c.conn.Invoke(ctx, MethodReportTarget, &event, &ack, callOptions(opts)...)
	return ack, err
}

func (c Client) AcknowledgeTargetCommand(ctx context.Context, event control.TargetCommandAckEvent, opts ...grpc.CallOption) (control.TargetEventAck, error) {
	var ack control.TargetEventAck
	err := c.conn.Invoke(ctx, MethodAcknowledgeTargetCommand, &event, &ack, callOptions(opts)...)
	return ack, err
}

func (c Client) ReportSource(ctx context.Context, event control.SourceEvent, opts ...grpc.CallOption) (control.SourceEventAck, error) {
	var ack control.SourceEventAck
	err := c.conn.Invoke(ctx, MethodReportSource, &event, &ack, callOptions(opts)...)
	return ack, err
}

func callOptions(opts []grpc.CallOption) []grpc.CallOption {
	out := []grpc.CallOption{grpc.ForceCodec(jsonCodec{})}
	out = append(out, opts...)
	return out
}
