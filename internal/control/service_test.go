package control

import (
	"context"
	"testing"
	"time"
)

func TestServiceRegisterTargetValidatesIdentity(t *testing.T) {
	service := NewService(NewEventHub(4))

	if _, err := service.RegisterTarget(context.Background(), RegisterTargetRequest{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
	}); err != nil {
		t.Fatalf("expected valid target registration, got %v", err)
	}

	if _, err := service.RegisterTarget(context.Background(), RegisterTargetRequest{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "Archive",
	}); err == nil {
		t.Fatal("expected invalid target name error")
	}

	if _, err := service.RegisterTarget(nil, RegisterTargetRequest{}); err == nil {
		t.Fatal("expected missing context error")
	}
}

func TestServiceReportTargetAndSourcePublishEvents(t *testing.T) {
	hub := NewEventHub(10)
	service := NewService(hub)
	key := RunKey{Namespace: "backup", Name: "run-1", Kind: RunKindBackup}

	targetAck, err := service.ReportTarget(TargetEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		TargetNamespace: "backup",
		TargetName:      "archive",
		Phase:           "Ready",
	})
	if err != nil {
		t.Fatal(err)
	}
	if targetAck.LastSequence == 0 {
		t.Fatal("expected target ack sequence")
	}

	commandAck, err := service.AcknowledgeTargetCommand(TargetCommandAckEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		TargetNamespace: "backup",
		TargetName:      "archive",
		CommandID:       "cmd-1",
		CommandType:     TargetCommandFinalizeBackupJob,
		AcknowledgedAt:  "2026-05-20T12:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if commandAck.LastSequence <= targetAck.LastSequence {
		t.Fatalf("expected command ack sequence after target sequence, got target=%d command=%d", targetAck.LastSequence, commandAck.LastSequence)
	}

	sourceAck, err := service.ReportSource(SourceEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
		Percent:         42,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sourceAck.LastSequence <= commandAck.LastSequence {
		t.Fatalf("expected source sequence after command ack sequence, got command=%d source=%d", commandAck.LastSequence, sourceAck.LastSequence)
	}

	events, err := hub.Snapshot(key)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 published events, got %d", len(events))
	}
	if events[0].Target == nil || events[0].Target.Phase != "Ready" {
		t.Fatalf("expected target Ready event, got %#v", events[0])
	}
	if events[1].CommandAck == nil || events[1].CommandAck.CommandID != "cmd-1" {
		t.Fatalf("expected command ack event, got %#v", events[1])
	}
	if events[2].Source == nil || events[2].Source.Percent != 42 {
		t.Fatalf("expected source progress event, got %#v", events[2])
	}
}

func TestServiceRejectsInvalidReportIdentities(t *testing.T) {
	service := NewService(NewEventHub(4))

	if _, err := service.ReportTarget(TargetEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
	}); err == nil {
		t.Fatal("expected missing target phase error")
	}
	if _, err := service.ReportSource(SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files_1",
		Phase:           "Running",
	}); err == nil {
		t.Fatal("expected invalid source name error")
	}
	if _, err := service.AcknowledgeTargetCommand(TargetCommandAckEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
		CommandID:       "cmd-1",
		CommandType:     "unsupported",
	}); err == nil {
		t.Fatal("expected invalid command acknowledgement error")
	}
}

func TestServiceTargetCommandQueueOrderingAndIdempotentCommandIDs(t *testing.T) {
	service := NewService(NewEventHub(4))
	key := RunKey{Namespace: "backup", Name: "run-1", Kind: RunKindBackup}

	enqueued, err := service.EnqueueFinalize(key, "cmd-1", FinalizeBackupJob{
		Timestamp: "2026-05-20T12-00-00Z",
		Sources: []ExpectedSource{{
			Namespace:       "app",
			Name:            "files",
			DestinationPath: "sites/demo/files",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued {
		t.Fatal("expected first command to enqueue")
	}
	enqueued, err = service.EnqueueRecover(key, "cmd-2", RecoverSpace{
		FailedSourceNamespace: "app",
		FailedSourceName:      "files",
		Reason:                "TargetOutOfSpace",
		MinAvailableBytes:     64 * 1024 * 1024,
		ProtectedSnapshots:    []string{"latest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued {
		t.Fatal("expected second command to enqueue")
	}
	enqueued, err = service.EnqueueAbort(key, "cmd-2", AbortRun{Reason: "duplicate"})
	if err != nil {
		t.Fatal(err)
	}
	if enqueued {
		t.Fatal("expected duplicate command id to be ignored")
	}
	enqueued, err = service.EnqueueAbort(key, "cmd-3", AbortRun{Reason: "operator canceled"})
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued {
		t.Fatal("expected third unique command to enqueue")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commands, err := service.RegisterTarget(ctx, RegisterTargetRequest{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		TargetNamespace: "backup",
		TargetName:      "archive",
	})
	if err != nil {
		t.Fatal(err)
	}

	assertCommand(t, readCommand(t, commands), "cmd-1", TargetCommandFinalizeBackupJob)
	assertCommand(t, readCommand(t, commands), "cmd-2", TargetCommandRecoverSpace)
	assertCommand(t, readCommand(t, commands), "cmd-3", TargetCommandAbortRun)
	assertNoCommand(t, commands)
}

func TestServiceTargetCommandQueueStreamsLiveCommands(t *testing.T) {
	service := NewService(NewEventHub(4))
	key := RunKey{Namespace: "backup", Name: "run-1", Kind: RunKindBackup}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commands, err := service.RegisterTarget(ctx, RegisterTargetRequest{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		TargetNamespace: "backup",
		TargetName:      "archive",
	})
	if err != nil {
		t.Fatal(err)
	}

	enqueued, err := service.EnqueueAbort(key, "cmd-live", AbortRun{Reason: "operator canceled"})
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued {
		t.Fatal("expected live command to enqueue")
	}
	assertCommand(t, readCommand(t, commands), "cmd-live", TargetCommandAbortRun)
}

func readCommand(t *testing.T, commands <-chan TargetCommand) TargetCommand {
	t.Helper()
	select {
	case command, ok := <-commands:
		if !ok {
			t.Fatal("command stream closed before command")
		}
		return command
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command")
	}
	return TargetCommand{}
}

func assertCommand(t *testing.T, command TargetCommand, commandID, commandType string) {
	t.Helper()
	if command.CommandID != commandID || command.Type != commandType {
		t.Fatalf("expected %s/%s, got %#v", commandID, commandType, command)
	}
}

func assertNoCommand(t *testing.T, commands <-chan TargetCommand) {
	t.Helper()
	select {
	case command := <-commands:
		t.Fatalf("expected no command, got %#v", command)
	case <-time.After(50 * time.Millisecond):
	}
}
