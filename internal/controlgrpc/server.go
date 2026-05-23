package controlgrpc

import (
	"context"
	"fmt"

	"github.com/chirino/kube-rsync-machine/internal/control"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ServiceName = "krm.control.v1.Control"

	MethodRegisterTarget           = "/" + ServiceName + "/RegisterTarget"
	MethodReportTarget             = "/" + ServiceName + "/ReportTarget"
	MethodAcknowledgeTargetCommand = "/" + ServiceName + "/AcknowledgeTargetCommand"
	MethodReportSource             = "/" + ServiceName + "/ReportSource"
)

type Server struct {
	Control *control.Service
}

type serviceServer interface {
	RegisterTarget(control.RegisterTargetRequest, grpc.ServerStream) error
	ReportTarget(context.Context, control.TargetEvent) (control.TargetEventAck, error)
	AcknowledgeTargetCommand(context.Context, control.TargetCommandAckEvent) (control.TargetEventAck, error)
	ReportSource(context.Context, control.SourceEvent) (control.SourceEventAck, error)
}

func Register(server grpc.ServiceRegistrar, controlService *control.Service) {
	server.RegisterService(&serviceDesc, &Server{Control: controlService})
}

func (s *Server) service() (*control.Service, error) {
	if s == nil || s.Control == nil {
		return nil, fmt.Errorf("control service is required")
	}
	return s.Control, nil
}

func (s *Server) RegisterTarget(req control.RegisterTargetRequest, stream grpc.ServerStream) error {
	service, err := s.service()
	if err != nil {
		return err
	}
	if err := requirePeerIdentity(stream.Context(), tlsutil.TargetIdentity(req.RunNamespace, req.RunName, req.TargetNamespace, req.TargetName)); err != nil {
		return err
	}
	commands, err := service.RegisterTarget(stream.Context(), req)
	if err != nil {
		return err
	}
	for {
		select {
		case command, ok := <-commands:
			if !ok {
				return nil
			}
			if err := stream.SendMsg(&command); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *Server) ReportTarget(ctx context.Context, event control.TargetEvent) (control.TargetEventAck, error) {
	service, err := s.service()
	if err != nil {
		return control.TargetEventAck{}, err
	}
	if err := requirePeerIdentity(ctx, tlsutil.TargetIdentity(event.RunNamespace, event.RunName, event.TargetNamespace, event.TargetName)); err != nil {
		return control.TargetEventAck{}, err
	}
	return service.ReportTarget(event)
}

func (s *Server) AcknowledgeTargetCommand(ctx context.Context, event control.TargetCommandAckEvent) (control.TargetEventAck, error) {
	service, err := s.service()
	if err != nil {
		return control.TargetEventAck{}, err
	}
	if err := requirePeerIdentity(ctx, tlsutil.TargetIdentity(event.RunNamespace, event.RunName, event.TargetNamespace, event.TargetName)); err != nil {
		return control.TargetEventAck{}, err
	}
	return service.AcknowledgeTargetCommand(event)
}

func (s *Server) ReportSource(ctx context.Context, event control.SourceEvent) (control.SourceEventAck, error) {
	service, err := s.service()
	if err != nil {
		return control.SourceEventAck{}, err
	}
	if err := requirePeerIdentity(ctx, tlsutil.SourceIdentity(event.RunNamespace, event.RunName, event.SourceNamespace, event.SourceName)); err != nil {
		return control.SourceEventAck{}, err
	}
	return service.ReportSource(event)
}

func requirePeerIdentity(ctx context.Context, expected tlsutil.Identity) error {
	identity, ok := PeerIdentity(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "client certificate identity is required")
	}
	if identity != expected {
		return status.Errorf(codes.PermissionDenied, "client certificate identity %s cannot act as %s", identity.URI(), expected.URI())
	}
	return nil
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: ServiceName,
	HandlerType: (*serviceServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "ReportTarget",
		Handler:    unaryHandler((*Server).ReportTarget),
	}, {
		MethodName: "AcknowledgeTargetCommand",
		Handler:    unaryHandler((*Server).AcknowledgeTargetCommand),
	}, {
		MethodName: "ReportSource",
		Handler:    unaryHandler((*Server).ReportSource),
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "RegisterTarget",
		Handler:       registerTargetHandler,
		ServerStreams: true,
	}},
}

func registerTargetHandler(srv interface{}, stream grpc.ServerStream) error {
	var req control.RegisterTargetRequest
	if err := stream.RecvMsg(&req); err != nil {
		return err
	}
	return srv.(*Server).RegisterTarget(req, stream)
}

func unaryHandler[Req any, Resp any](method func(*Server, context.Context, Req) (Resp, error)) grpc.MethodHandler {
	return func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
		var req Req
		if err := dec(&req); err != nil {
			var zero Resp
			return zero, err
		}
		if interceptor == nil {
			return method(srv.(*Server), ctx, req)
		}
		info := &grpc.UnaryServerInfo{
			Server:     srv,
			FullMethod: fullMethodFor[Req](),
		}
		handler := func(ctx context.Context, request interface{}) (interface{}, error) {
			return method(srv.(*Server), ctx, request.(Req))
		}
		return interceptor(ctx, req, info, handler)
	}
}

func fullMethodFor[Req any]() string {
	var req Req
	switch any(req).(type) {
	case control.TargetEvent:
		return MethodReportTarget
	case control.TargetCommandAckEvent:
		return MethodAcknowledgeTargetCommand
	case control.SourceEvent:
		return MethodReportSource
	default:
		return "/" + ServiceName
	}
}
