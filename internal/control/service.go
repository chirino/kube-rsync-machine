package control

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

const defaultTargetCommandBuffer = 64

type Service struct {
	hub *EventHub

	mu     sync.Mutex
	buffer int
	runs   map[RunKey]*targetCommandLog
}

type targetCommandLog struct {
	commands []TargetCommand
	seen     map[string]struct{}
	streams  map[*targetCommandStream]struct{}
}

type targetCommandStream struct {
	ch chan TargetCommand
}

func NewService(hub *EventHub) *Service {
	if hub == nil {
		hub = NewEventHub(256)
	}
	return &Service{
		hub:    hub,
		buffer: defaultTargetCommandBuffer,
		runs:   map[RunKey]*targetCommandLog{},
	}
}

func (s *Service) Hub() *EventHub {
	return s.hub
}

func (s *Service) RegisterTarget(ctx context.Context, req RegisterTargetRequest) (<-chan TargetCommand, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	key := req.Key()
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if err := validateNamespaceName("target namespace", req.TargetNamespace); err != nil {
		return nil, err
	}
	if err := validateObjectName("target name", req.TargetName); err != nil {
		return nil, err
	}

	stream := &targetCommandStream{ch: make(chan TargetCommand, s.buffer)}

	s.mu.Lock()
	run := s.runLocked(key)
	replay := cloneTargetCommands(run.commands)
	run.streams[stream] = struct{}{}
	s.mu.Unlock()

	out := make(chan TargetCommand, s.buffer)
	go func() {
		defer close(out)
		defer s.unregisterTarget(key, stream)

		for _, command := range replay {
			select {
			case out <- command:
			case <-ctx.Done():
				return
			}
		}
		for {
			select {
			case command := <-stream.ch:
				select {
				case out <- command:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (s *Service) ReportTarget(event TargetEvent) (TargetEventAck, error) {
	if err := validateTargetEvent(event); err != nil {
		return TargetEventAck{}, err
	}
	published, err := s.hub.PublishTarget(event)
	if err != nil {
		return TargetEventAck{}, err
	}
	return TargetEventAck{LastSequence: published.Sequence}, nil
}

func (s *Service) AcknowledgeTargetCommand(event TargetCommandAckEvent) (TargetEventAck, error) {
	if event.AcknowledgedAt == "" {
		event.AcknowledgedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if err := validateTargetCommandAckEvent(event); err != nil {
		return TargetEventAck{}, err
	}
	published, err := s.hub.PublishTargetCommandAck(event)
	if err != nil {
		return TargetEventAck{}, err
	}
	return TargetEventAck{LastSequence: published.Sequence}, nil
}

func (s *Service) ReportSource(event SourceEvent) (SourceEventAck, error) {
	if err := validateSourceEvent(event); err != nil {
		return SourceEventAck{}, err
	}
	published, err := s.hub.PublishSource(event)
	if err != nil {
		return SourceEventAck{}, err
	}
	return SourceEventAck{LastSequence: published.Sequence}, nil
}

func (s *Service) EnqueueFinalize(key RunKey, commandID string, finalize FinalizeBackupJob) (bool, error) {
	return s.EnqueueTargetCommand(key, NewFinalizeBackupJobCommand(commandID, finalize))
}

func (s *Service) EnqueueRecover(key RunKey, commandID string, recover RecoverSpace) (bool, error) {
	return s.EnqueueTargetCommand(key, NewRecoverSpaceCommand(commandID, recover))
}

func (s *Service) EnqueueAbort(key RunKey, commandID string, abort AbortRun) (bool, error) {
	return s.EnqueueTargetCommand(key, NewAbortRunCommand(commandID, abort))
}

func (s *Service) EnqueueTargetCommand(key RunKey, command TargetCommand) (bool, error) {
	if err := key.Validate(); err != nil {
		return false, err
	}
	if err := validateTargetCommand(command); err != nil {
		return false, err
	}

	command = cloneTargetCommand(command)

	s.mu.Lock()
	run := s.runLocked(key)
	if _, exists := run.seen[command.CommandID]; exists {
		s.mu.Unlock()
		return false, nil
	}
	run.seen[command.CommandID] = struct{}{}
	run.commands = append(run.commands, command)
	streams := make([]*targetCommandStream, 0, len(run.streams))
	for stream := range run.streams {
		streams = append(streams, stream)
	}
	s.mu.Unlock()

	for _, stream := range streams {
		stream.ch <- cloneTargetCommand(command)
	}
	return true, nil
}

func (s *Service) runLocked(key RunKey) *targetCommandLog {
	run := s.runs[key]
	if run == nil {
		run = &targetCommandLog{
			seen:    map[string]struct{}{},
			streams: map[*targetCommandStream]struct{}{},
		}
		s.runs[key] = run
	}
	return run
}

func (s *Service) unregisterTarget(key RunKey, stream *targetCommandStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run := s.runs[key]
	if run == nil {
		return
	}
	delete(run.streams, stream)
}

func validateTargetEvent(event TargetEvent) error {
	if err := event.Key().Validate(); err != nil {
		return err
	}
	if err := validateNamespaceName("target namespace", event.TargetNamespace); err != nil {
		return err
	}
	if err := validateObjectName("target name", event.TargetName); err != nil {
		return err
	}
	if event.Phase == "" {
		return fmt.Errorf("target phase is required")
	}
	return nil
}

func validateTargetCommandAckEvent(event TargetCommandAckEvent) error {
	if err := event.Key().Validate(); err != nil {
		return err
	}
	if err := validateNamespaceName("target namespace", event.TargetNamespace); err != nil {
		return err
	}
	if err := validateObjectName("target name", event.TargetName); err != nil {
		return err
	}
	if event.CommandID == "" {
		return fmt.Errorf("command id is required")
	}
	if errs := validation.IsDNS1123Subdomain(event.CommandID); len(errs) > 0 {
		return fmt.Errorf("command id is invalid: %s", errs[0])
	}
	switch event.CommandType {
	case TargetCommandFinalizeBackupJob, TargetCommandRecoverSpace, TargetCommandAbortRun:
	case "":
		return fmt.Errorf("command type is required")
	default:
		return fmt.Errorf("unsupported command type %q", event.CommandType)
	}
	if _, err := time.Parse(time.RFC3339, event.AcknowledgedAt); err != nil {
		return fmt.Errorf("acknowledgedAt is invalid: %w", err)
	}
	return nil
}

func validateSourceEvent(event SourceEvent) error {
	if err := event.Key().Validate(); err != nil {
		return err
	}
	if err := validateNamespaceName("source namespace", event.SourceNamespace); err != nil {
		return err
	}
	if err := validateObjectName("source name", event.SourceName); err != nil {
		return err
	}
	if event.Phase == "" {
		return fmt.Errorf("source phase is required")
	}
	return nil
}

func validateTargetCommand(command TargetCommand) error {
	if command.CommandID == "" {
		return fmt.Errorf("command id is required")
	}
	if errs := validation.IsDNS1123Subdomain(command.CommandID); len(errs) > 0 {
		return fmt.Errorf("command id is invalid: %s", errs[0])
	}
	switch command.Type {
	case TargetCommandFinalizeBackupJob:
		if command.Finalize == nil {
			return fmt.Errorf("finalize command payload is required")
		}
		if command.RecoverSpace != nil || command.Abort != nil {
			return fmt.Errorf("finalize command must not include other payloads")
		}
		return validateFinalizeBackupJob(*command.Finalize)
	case TargetCommandRecoverSpace:
		if command.RecoverSpace == nil {
			return fmt.Errorf("recover command payload is required")
		}
		if command.Finalize != nil || command.Abort != nil {
			return fmt.Errorf("recover command must not include other payloads")
		}
		return validateRecoverSpace(*command.RecoverSpace)
	case TargetCommandAbortRun:
		if command.Abort == nil {
			return fmt.Errorf("abort command payload is required")
		}
		if command.Finalize != nil || command.RecoverSpace != nil {
			return fmt.Errorf("abort command must not include other payloads")
		}
		return nil
	case "":
		return fmt.Errorf("command type is required")
	default:
		return fmt.Errorf("unsupported command type %q", command.Type)
	}
}

func validateFinalizeBackupJob(finalize FinalizeBackupJob) error {
	if finalize.Timestamp == "" {
		return fmt.Errorf("finalize timestamp is required")
	}
	for i, source := range finalize.Sources {
		if err := validateNamespaceName("expected source namespace", source.Namespace); err != nil {
			return fmt.Errorf("expected source %d: %w", i, err)
		}
		if err := validateObjectName("expected source name", source.Name); err != nil {
			return fmt.Errorf("expected source %d: %w", i, err)
		}
		if source.DestinationPath == "" {
			return fmt.Errorf("expected source %d: destination path is required", i)
		}
	}
	return nil
}

func validateRecoverSpace(recover RecoverSpace) error {
	if err := validateNamespaceName("failed source namespace", recover.FailedSourceNamespace); err != nil {
		return err
	}
	if err := validateObjectName("failed source name", recover.FailedSourceName); err != nil {
		return err
	}
	if recover.Reason == "" {
		return fmt.Errorf("recover reason is required")
	}
	if recover.MinAvailableBytes == 0 {
		return fmt.Errorf("recover min available bytes is required")
	}
	for i, snapshot := range recover.ProtectedSnapshots {
		if snapshot == "" {
			return fmt.Errorf("recover protected snapshot %d is required", i)
		}
	}
	return nil
}

func validateNamespaceName(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if errs := validation.IsDNS1123Label(value); len(errs) > 0 {
		return fmt.Errorf("%s is invalid: %s", field, errs[0])
	}
	return nil
}

func validateObjectName(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if errs := validation.IsDNS1123Subdomain(value); len(errs) > 0 {
		return fmt.Errorf("%s is invalid: %s", field, errs[0])
	}
	return nil
}

func cloneTargetCommands(commands []TargetCommand) []TargetCommand {
	cloned := make([]TargetCommand, len(commands))
	for i := range commands {
		cloned[i] = cloneTargetCommand(commands[i])
	}
	return cloned
}

func cloneTargetCommand(command TargetCommand) TargetCommand {
	cloned := command
	if command.Finalize != nil {
		finalize := *command.Finalize
		finalize.Sources = append([]ExpectedSource(nil), command.Finalize.Sources...)
		cloned.Finalize = &finalize
	}
	if command.RecoverSpace != nil {
		recover := *command.RecoverSpace
		recover.ProtectedSnapshots = append([]string(nil), command.RecoverSpace.ProtectedSnapshots...)
		cloned.RecoverSpace = &recover
	}
	if command.Abort != nil {
		abort := *command.Abort
		cloned.Abort = &abort
	}
	return cloned
}
