package control

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	EventKindTarget           = "target"
	EventKindTargetCommandAck = "target_command_ack"
	EventKindSource           = "source"
)

type ControlEvent struct {
	Sequence    uint64
	PublishedAt time.Time
	Kind        string
	Key         RunKey

	Target     *TargetEvent
	CommandAck *TargetCommandAckEvent
	Source     *SourceEvent
}

type EventHub struct {
	mu               sync.Mutex
	replayLimit      int
	subscriberBuffer int
	nextSequence     uint64
	runs             map[RunKey]*runEvents
	allSubscribers   map[*subscriber]struct{}
}

type runEvents struct {
	replay      []ControlEvent
	subscribers map[*subscriber]struct{}
}

type subscriber struct {
	ch chan ControlEvent
}

func NewEventHub(replayLimit int) *EventHub {
	return NewEventHubWithBuffer(replayLimit, 64)
}

func NewEventHubWithBuffer(replayLimit, subscriberBuffer int) *EventHub {
	if replayLimit < 0 {
		replayLimit = 0
	}
	if subscriberBuffer < 1 {
		subscriberBuffer = 1
	}
	return &EventHub{
		replayLimit:      replayLimit,
		subscriberBuffer: subscriberBuffer,
		runs:             map[RunKey]*runEvents{},
		allSubscribers:   map[*subscriber]struct{}{},
	}
}

func (h *EventHub) PublishTarget(event TargetEvent) (ControlEvent, error) {
	return h.publish(ControlEvent{
		Kind:   EventKindTarget,
		Key:    event.Key(),
		Target: cloneTargetEvent(event),
	})
}

func (h *EventHub) PublishTargetCommandAck(event TargetCommandAckEvent) (ControlEvent, error) {
	return h.publish(ControlEvent{
		Kind:       EventKindTargetCommandAck,
		Key:        event.Key(),
		CommandAck: cloneTargetCommandAckEvent(event),
	})
}

func (h *EventHub) PublishSource(event SourceEvent) (ControlEvent, error) {
	return h.publish(ControlEvent{
		Kind:   EventKindSource,
		Key:    event.Key(),
		Source: cloneSourceEvent(event),
	})
}

func (h *EventHub) Snapshot(key RunKey) ([]ControlEvent, error) {
	return h.SnapshotAfter(key, 0)
}

func (h *EventHub) SnapshotAfter(key RunKey, afterSequence uint64) ([]ControlEvent, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	run := h.runs[key]
	if run == nil || len(run.replay) == 0 {
		return []ControlEvent{}, nil
	}
	return cloneEventsAfter(run.replay, afterSequence), nil
}

func (h *EventHub) Subscribe(ctx context.Context, key RunKey) (<-chan ControlEvent, error) {
	return h.SubscribeAfter(ctx, key, 0)
}

func (h *EventHub) SubscribeAfter(ctx context.Context, key RunKey, afterSequence uint64) (<-chan ControlEvent, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}

	out := make(chan ControlEvent, h.subscriberBuffer)
	sub := &subscriber{ch: make(chan ControlEvent, h.subscriberBuffer)}

	h.mu.Lock()
	run := h.runLocked(key)
	replay := cloneEventsAfter(run.replay, afterSequence)
	run.subscribers[sub] = struct{}{}
	h.mu.Unlock()

	go func() {
		defer close(out)
		defer h.unsubscribe(key, sub)

		for _, event := range replay {
			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
		for {
			select {
			case event := <-sub.ch:
				select {
				case out <- event:
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

func (h *EventHub) SubscribeAll(ctx context.Context) (<-chan ControlEvent, error) {
	return h.SubscribeAllAfter(ctx, 0)
}

func (h *EventHub) SubscribeAllAfter(ctx context.Context, afterSequence uint64) (<-chan ControlEvent, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}

	out := make(chan ControlEvent, h.subscriberBuffer)
	sub := &subscriber{ch: make(chan ControlEvent, h.subscriberBuffer)}

	h.mu.Lock()
	replay := make([]ControlEvent, 0)
	for _, run := range h.runs {
		replay = append(replay, cloneEventsAfter(run.replay, afterSequence)...)
	}
	sort.Slice(replay, func(i, j int) bool {
		return replay[i].Sequence < replay[j].Sequence
	})
	h.allSubscribers[sub] = struct{}{}
	h.mu.Unlock()

	go func() {
		defer close(out)
		defer h.unsubscribeAll(sub)

		for _, event := range replay {
			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
		for {
			select {
			case event := <-sub.ch:
				select {
				case out <- event:
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

func (h *EventHub) publish(event ControlEvent) (ControlEvent, error) {
	if event.Kind != EventKindTarget && event.Kind != EventKindTargetCommandAck && event.Kind != EventKindSource {
		return ControlEvent{}, fmt.Errorf("unsupported event kind %q", event.Kind)
	}
	if err := event.Key.Validate(); err != nil {
		return ControlEvent{}, err
	}
	if event.Kind == EventKindTarget && event.Target == nil {
		return ControlEvent{}, fmt.Errorf("target event payload is required")
	}
	if event.Kind == EventKindTargetCommandAck && event.CommandAck == nil {
		return ControlEvent{}, fmt.Errorf("target command acknowledgement payload is required")
	}
	if event.Kind == EventKindSource && event.Source == nil {
		return ControlEvent{}, fmt.Errorf("source event payload is required")
	}

	h.mu.Lock()
	h.nextSequence++
	event.Sequence = h.nextSequence
	event.PublishedAt = time.Now().UTC()
	event = cloneEvent(event)

	run := h.runLocked(event.Key)
	if h.replayLimit > 0 {
		run.replay = append(run.replay, event)
		if overflow := len(run.replay) - h.replayLimit; overflow > 0 {
			run.replay = append([]ControlEvent(nil), run.replay[overflow:]...)
		}
	}
	subscribers := make([]*subscriber, 0, len(run.subscribers))
	for sub := range run.subscribers {
		subscribers = append(subscribers, sub)
	}
	allSubscribers := make([]*subscriber, 0, len(h.allSubscribers))
	for sub := range h.allSubscribers {
		allSubscribers = append(allSubscribers, sub)
	}
	h.mu.Unlock()

	for _, sub := range subscribers {
		deliverLatest(sub.ch, event)
	}
	for _, sub := range allSubscribers {
		deliverLatest(sub.ch, event)
	}
	return cloneEvent(event), nil
}

func (h *EventHub) runLocked(key RunKey) *runEvents {
	run := h.runs[key]
	if run == nil {
		run = &runEvents{subscribers: map[*subscriber]struct{}{}}
		h.runs[key] = run
	}
	return run
}

func (h *EventHub) unsubscribe(key RunKey, sub *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()

	run := h.runs[key]
	if run == nil {
		return
	}
	delete(run.subscribers, sub)
	if len(run.subscribers) == 0 && len(run.replay) == 0 {
		delete(h.runs, key)
	}
}

func (h *EventHub) unsubscribeAll(sub *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.allSubscribers, sub)
}

func deliverLatest(ch chan ControlEvent, event ControlEvent) {
	for {
		select {
		case ch <- event:
			return
		default:
			select {
			case <-ch:
			default:
			}
		}
	}
}

func cloneEvents(events []ControlEvent) []ControlEvent {
	cloned := make([]ControlEvent, len(events))
	for i := range events {
		cloned[i] = cloneEvent(events[i])
	}
	return cloned
}

func cloneEventsAfter(events []ControlEvent, afterSequence uint64) []ControlEvent {
	if afterSequence == 0 {
		return cloneEvents(events)
	}
	start := len(events)
	for i, event := range events {
		if event.Sequence > afterSequence {
			start = i
			break
		}
	}
	return cloneEvents(events[start:])
}

func cloneEvent(event ControlEvent) ControlEvent {
	cloned := event
	if event.Target != nil {
		cloned.Target = cloneTargetEvent(*event.Target)
	}
	if event.CommandAck != nil {
		cloned.CommandAck = cloneTargetCommandAckEvent(*event.CommandAck)
	}
	if event.Source != nil {
		cloned.Source = cloneSourceEvent(*event.Source)
	}
	return cloned
}

func cloneTargetEvent(event TargetEvent) *TargetEvent {
	cloned := event
	cloned.Paths = append([]string(nil), event.Paths...)
	cloned.RestorePoints = append([]RestorePoint(nil), event.RestorePoints...)
	cloned.Conditions = append([]TargetCondition(nil), event.Conditions...)
	return &cloned
}

func cloneTargetCommandAckEvent(event TargetCommandAckEvent) *TargetCommandAckEvent {
	cloned := event
	return &cloned
}

func cloneSourceEvent(event SourceEvent) *SourceEvent {
	cloned := event
	return &cloned
}
