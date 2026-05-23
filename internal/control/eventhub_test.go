package control

import (
	"context"
	"testing"
	"time"
)

func TestEventHubSnapshotsBoundedReplayByRunKey(t *testing.T) {
	hub := NewEventHub(2)
	key := RunKey{Namespace: "backup", Name: "run-1", Kind: RunKindBackup}
	otherKey := RunKey{Namespace: "backup", Name: "run-2", Kind: RunKindBackup}

	mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Preparing",
	})
	mustPublishTarget(t, hub, TargetEvent{
		RunNamespace:    otherKey.Namespace,
		RunName:         otherKey.Name,
		RunKind:         otherKey.Kind,
		TargetNamespace: "backup",
		TargetName:      "archive",
		Phase:           "Ready",
	})
	running := mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
		Percent:         42,
	})
	succeeded := mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Succeeded",
		Percent:         100,
	})

	snapshot, err := hub.Snapshot(key)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 2 {
		t.Fatalf("expected bounded replay of 2 events, got %d", len(snapshot))
	}
	if snapshot[0].Sequence != running.Sequence || snapshot[1].Sequence != succeeded.Sequence {
		t.Fatalf("unexpected replay sequence: %#v", snapshot)
	}
	if snapshot[0].Source.Phase != "Running" || snapshot[1].Source.Phase != "Succeeded" {
		t.Fatalf("unexpected replay phases: %#v", snapshot)
	}

	otherSnapshot, err := hub.Snapshot(otherKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(otherSnapshot) != 1 || otherSnapshot[0].Target.Phase != "Ready" {
		t.Fatalf("unexpected other run snapshot: %#v", otherSnapshot)
	}
}

func TestEventHubSubscribeReplaysThenStreamsUntilContextCancel(t *testing.T) {
	hub := NewEventHub(4)
	key := RunKey{Namespace: "backup", Name: "run-1", Kind: RunKindBackup}

	mustPublishTarget(t, hub, TargetEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		TargetNamespace: "backup",
		TargetName:      "archive",
		Phase:           "Preparing",
	})

	ctx, cancel := context.WithCancel(context.Background())
	events, err := hub.Subscribe(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	first := readEvent(t, events)
	if first.Target == nil || first.Target.Phase != "Preparing" {
		t.Fatalf("expected replayed target preparing event, got %#v", first)
	}

	mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
	})

	second := readEvent(t, events)
	if second.Source == nil || second.Source.Phase != "Running" {
		t.Fatalf("expected live source running event, got %#v", second)
	}

	cancel()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("expected subscription channel to close after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscription channel to close")
	}
}

func TestEventHubSubscribeAfterReplaysOnlyNewerEvents(t *testing.T) {
	hub := NewEventHub(4)
	key := RunKey{Namespace: "backup", Name: "run-1", Kind: RunKindBackup}

	first := mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Preparing",
	})
	second := mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
	})

	snapshot, err := hub.SnapshotAfter(key, first.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot[0].Sequence != second.Sequence {
		t.Fatalf("expected snapshot after first sequence to contain second event, got %#v", snapshot)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := hub.SubscribeAfter(ctx, key, first.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	replayed := readEvent(t, events)
	if replayed.Sequence != second.Sequence || replayed.Source.Phase != "Running" {
		t.Fatalf("unexpected replayed event: %#v", replayed)
	}
}

func TestEventHubSubscribeAllStreamsEventsAcrossRuns(t *testing.T) {
	hub := NewEventHub(4)
	mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    "backup",
		RunName:         "already-published",
		RunKind:         RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := hub.SubscribeAll(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mustPublishTarget(t, hub, TargetEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         RunKindBackup,
		TargetNamespace: "backup",
		TargetName:      "archive",
		Phase:           "Ready",
	})
	mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    "backup",
		RunName:         "restore-1",
		RunKind:         RunKindRestore,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Succeeded",
	})

	replayed := readEvent(t, events)
	if replayed.Source == nil || replayed.Key.Name != "already-published" {
		t.Fatalf("unexpected replayed event: %#v", replayed)
	}
	first := readEvent(t, events)
	if first.Target == nil || first.Key.Name != "run-1" {
		t.Fatalf("unexpected first event: %#v", first)
	}
	second := readEvent(t, events)
	if second.Source == nil || second.Key.Name != "restore-1" {
		t.Fatalf("unexpected second event: %#v", second)
	}
}

func TestEventHubSubscribeAllAfterReplaysOnlyNewerEvents(t *testing.T) {
	hub := NewEventHub(4)
	first := mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    "backup",
		RunName:         "run-1",
		RunKind:         RunKindBackup,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Preparing",
	})
	second := mustPublishSource(t, hub, SourceEvent{
		RunNamespace:    "restore",
		RunName:         "run-2",
		RunKind:         RunKindRestore,
		SourceNamespace: "app",
		SourceName:      "files",
		Phase:           "Running",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := hub.SubscribeAllAfter(ctx, first.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	replayed := readEvent(t, events)
	if replayed.Sequence != second.Sequence || replayed.Key.Name != "run-2" {
		t.Fatalf("unexpected replayed event: %#v", replayed)
	}
}

func TestEventHubReturnsDefensiveCopies(t *testing.T) {
	hub := NewEventHub(1)
	key := RunKey{Namespace: "backup", Name: "run-1", Kind: RunKindBackup}

	published, err := hub.PublishTarget(TargetEvent{
		RunNamespace:    key.Namespace,
		RunName:         key.Name,
		RunKind:         key.Kind,
		TargetNamespace: "backup",
		TargetName:      "archive",
		Phase:           "Completed",
		Paths:           []string{"hourly/2026-05-20T02-03-04Z"},
		RestorePoints: []RestorePoint{{
			Snapshot: "latest",
		}},
		Conditions: []TargetCondition{{
			Type:   "Ready",
			Status: "True",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	published.Target.Paths[0] = "mutated"
	published.Target.RestorePoints[0].Snapshot = "mutated"
	published.Target.Conditions[0].Status = "False"

	snapshot, err := hub.Snapshot(key)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot[0].Target.Paths[0] != "hourly/2026-05-20T02-03-04Z" {
		t.Fatalf("paths were not defensively copied: %#v", snapshot[0].Target.Paths)
	}
	if snapshot[0].Target.RestorePoints[0].Snapshot != "latest" {
		t.Fatalf("restore points were not defensively copied: %#v", snapshot[0].Target.RestorePoints)
	}
	if snapshot[0].Target.Conditions[0].Status != "True" {
		t.Fatalf("conditions were not defensively copied: %#v", snapshot[0].Target.Conditions)
	}
}

func TestEventHubRejectsInvalidRunKeys(t *testing.T) {
	hub := NewEventHub(1)
	if _, err := hub.PublishSource(SourceEvent{RunNamespace: "backup", RunName: "run-1", RunKind: "invalid"}); err == nil {
		t.Fatal("expected invalid run kind error")
	}
	if _, err := hub.Snapshot(RunKey{Namespace: "backup", Name: "run-1"}); err == nil {
		t.Fatal("expected missing run kind error")
	}
	if _, err := hub.Subscribe(context.Background(), RunKey{Namespace: "backup", Name: "run-1"}); err == nil {
		t.Fatal("expected missing run kind error")
	}
}

func readEvent(t *testing.T, events <-chan ControlEvent) ControlEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("subscription closed before event")
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
	return ControlEvent{}
}

func mustPublishTarget(t *testing.T, hub *EventHub, event TargetEvent) ControlEvent {
	t.Helper()
	published, err := hub.PublishTarget(event)
	if err != nil {
		t.Fatal(err)
	}
	return published
}

func mustPublishSource(t *testing.T, hub *EventHub, event SourceEvent) ControlEvent {
	t.Helper()
	published, err := hub.PublishSource(event)
	if err != nil {
		t.Fatal(err)
	}
	return published
}
