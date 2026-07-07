package engine

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/mateusfdl/gentis/internal/namespace"
)

func wildcardRegistry() *namespace.Registry {
	return mustRegistry(namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
		Namespaces: map[string]namespace.Settings{
			"metrics": {AllowPublish: true, AllowWildcard: true},
			"logs":    {AllowPublish: true},
		},
	}))
}

func TestSubscribePatternDeliversMatchingPublish(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}

	r := e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	if r.Delivered != 1 {
		t.Fatalf("Delivered = %d, want 1", r.Delivered)
	}
	if r.Offset != 1 {
		t.Fatalf("Offset = %d, want 1", r.Offset)
	}
	if got := rec.counts()[1]; got != 1 {
		t.Fatalf("subscriber 1 received %d deliveries, want 1", got)
	}
}

func TestSubscribePatternIgnoresNonMatchingPublish(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}

	r := e.Publish("logs:app", []byte("v"), 0, rec.deliver)
	if r.Delivered != 0 {
		t.Fatalf("Delivered = %d, want 0", r.Delivered)
	}
	if got := rec.counts()[1]; got != 0 {
		t.Fatalf("subscriber 1 received %d deliveries, want 0", got)
	}
}

func TestPatternOverlapWithExactNoDoubleDelivery(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.Subscribe(1, "metrics:cpu"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}

	r := e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	if r.Delivered != 1 {
		t.Fatalf("Delivered = %d, want exactly 1", r.Delivered)
	}
	if got := rec.counts()[1]; got != 1 {
		t.Fatalf("subscriber 1 received %d deliveries, want exactly 1", got)
	}
}

func TestPatternBroadcastToMultipleSubscribers(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern 1: %v", err)
	}
	if err := e.SubscribePattern(2, "metrics:cpu*"); err != nil {
		t.Fatalf("SubscribePattern 2: %v", err)
	}

	r := e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	if r.Delivered != 2 {
		t.Fatalf("Delivered = %d, want 2", r.Delivered)
	}
	counts := rec.counts()
	if counts[1] != 1 || counts[2] != 1 {
		t.Fatalf("counts = %v, want 1 each", counts)
	}
}

func TestPatternPublishExcludesPublisher(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}

	r := e.Publish("metrics:cpu", []byte("v"), 1, rec.deliver)
	if r.Delivered != 0 {
		t.Fatalf("Delivered = %d, want 0", r.Delivered)
	}
	if got := rec.counts()[1]; got != 0 {
		t.Fatalf("excluded subscriber received %d deliveries, want 0", got)
	}
}

func TestSubscribePatternDuplicate(t *testing.T) {
	e := New()
	defer e.Stop()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}
	if err := e.SubscribePattern(1, "metrics:*"); !errors.Is(err, ErrAlreadySubscribed) {
		t.Fatalf("duplicate SubscribePattern err = %v, want ErrAlreadySubscribed", err)
	}
}

func TestUnsubscribePattern(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}
	if !e.UnsubscribePattern(1, "metrics:*") {
		t.Fatal("UnsubscribePattern returned false for active subscription")
	}
	if e.UnsubscribePattern(1, "metrics:*") {
		t.Fatal("UnsubscribePattern returned true for removed subscription")
	}

	r := e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	if r.Delivered != 0 {
		t.Fatalf("Delivered = %d after unsubscribe, want 0", r.Delivered)
	}
}

func TestUnsubscribeAllRemovesPatterns(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.Subscribe(1, "logs:app"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}

	e.UnsubscribeAll(1)

	r := e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	if r.Delivered != 0 {
		t.Fatalf("Delivered = %d after UnsubscribeAll, want 0", r.Delivered)
	}
	if int(e.Stats().TotalSubscribers) != 0 {
		t.Fatalf("TotalSubscriptions = %d, want 0", int(e.Stats().TotalSubscribers))
	}
}

func BenchmarkPatternChurnWithPopulation(b *testing.B) {
	e := New()
	defer e.Stop()
	for i := range 1000 {
		if err := e.SubscribePattern(SubscriberID(i+10), fmt.Sprintf("metrics:p%d*", i)); err != nil {
			b.Fatalf("SubscribePattern(%d): %v", i, err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := e.SubscribePattern(5, "metrics:churn*"); err != nil {
			b.Fatalf("SubscribePattern: %v", err)
		}
		if !e.UnsubscribePattern(5, "metrics:churn*") {
			b.Fatal("UnsubscribePattern returned false")
		}
	}
}

func publishFrames(t *testing.T, exact, patterns []SubscriberID) []*EncodedFrame {
	t.Helper()
	e := New()
	t.Cleanup(e.Stop)
	for _, id := range exact {
		if err := e.Subscribe(id, "metrics:cpu"); err != nil {
			t.Fatalf("Subscribe(%d): %v", id, err)
		}
	}
	for _, id := range patterns {
		if err := e.SubscribePattern(id, "metrics:*"); err != nil {
			t.Fatalf("SubscribePattern(%d): %v", id, err)
		}
	}
	var frames []*EncodedFrame
	e.Publish("metrics:cpu", []byte("v"), 0, func(id SubscriberID, d Delivery) bool {
		frames = append(frames, d.Frame)
		return true
	})
	return frames
}

func TestFrameSharedAcrossAllRecipients(t *testing.T) {
	tests := []struct {
		name      string
		exact     []SubscriberID
		patterns  []SubscriberID
		wantFrame bool
	}{
		{name: "single exact recipient pays no frame", exact: []SubscriberID{1}, wantFrame: false},
		{name: "two exact recipients share one frame", exact: []SubscriberID{1, 2}, wantFrame: true},
		{name: "two pattern recipients share one frame", patterns: []SubscriberID{1, 2}, wantFrame: true},
		{name: "one exact plus one pattern share one frame", exact: []SubscriberID{1}, patterns: []SubscriberID{2}, wantFrame: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frames := publishFrames(t, tt.exact, tt.patterns)
			if len(frames) != len(tt.exact)+len(tt.patterns) {
				t.Fatalf("deliveries = %d, want %d", len(frames), len(tt.exact)+len(tt.patterns))
			}
			for i, f := range frames {
				if (f != nil) != tt.wantFrame {
					t.Fatalf("delivery %d frame presence = %v, want %v", i, f != nil, tt.wantFrame)
				}
				if f != frames[0] {
					t.Fatalf("delivery %d does not share delivery 0's frame", i)
				}
			}
		})
	}
}

func TestPatternScopedToItsNamespaceUnderRegistry(t *testing.T) {
	reg := mustRegistry(namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true, AllowWildcard: true},
		Namespaces: map[string]namespace.Settings{
			"jobs": {AllowPublish: true, Fanout: namespace.RoundRobin},
		},
	}))
	e := New(WithNamespaces(reg))
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "jobs*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}
	if err := e.Subscribe(2, "jobs:q"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if r := e.Publish("jobs:q", []byte("task"), 0, rec.deliver); r.Delivered != 1 {
		t.Fatalf("Delivered = %d, want 1: the round-robin worker only", r.Delivered)
	}
	if got := rec.counts()[1]; got != 0 {
		t.Fatalf("default-namespace wildcard received %d deliveries from the jobs namespace, want 0", got)
	}

	if r := e.Publish("jobsboard", []byte("v"), 0, rec.deliver); r.Delivered != 1 {
		t.Fatalf("Delivered = %d on default-namespace channel, want 1", r.Delivered)
	}
	if got := rec.counts()[1]; got != 1 {
		t.Fatalf("pattern subscriber received %d default-namespace deliveries, want 1", got)
	}
}

func TestSubscribePatternDeniedByNamespace(t *testing.T) {
	e := New(WithNamespaces(wildcardRegistry()))
	defer e.Stop()

	if err := e.SubscribePattern(1, "logs:*"); !errors.Is(err, ErrWildcardDenied) {
		t.Fatalf("SubscribePattern on logs err = %v, want ErrWildcardDenied", err)
	}
	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern on metrics: %v", err)
	}
}

func TestSubscribePatternWildcardNamespaceDenied(t *testing.T) {
	e := New(WithNamespaces(wildcardRegistry()))
	defer e.Stop()

	if err := e.SubscribePattern(1, "*:cpu"); !errors.Is(err, ErrWildcardDenied) {
		t.Fatalf("wildcard namespace err = %v, want ErrWildcardDenied", err)
	}
}

func TestSubscribePatternStrictUnknownNamespace(t *testing.T) {
	reg := mustRegistry(namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
		Strict:  true,
	}))
	e := New(WithNamespaces(reg))
	defer e.Stop()

	if err := e.SubscribePattern(1, "nope:*"); !errors.Is(err, ErrUnknownNamespace) {
		t.Fatalf("strict unknown namespace err = %v, want ErrUnknownNamespace", err)
	}
}

func TestPatternChannelReapedAfterLastPatternUnsubscribe(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}

	e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	e.Publish("metrics:mem", []byte("v"), 0, rec.deliver)
	if int(e.Stats().Channels) != 2 {
		t.Fatalf("ChannelCount = %d after pattern publishes, want 2", int(e.Stats().Channels))
	}

	e.UnsubscribePattern(1, "metrics:*")
	if int(e.Stats().Channels) != 0 {
		t.Fatalf("ChannelCount = %d after last pattern unsubscribe, want 0", int(e.Stats().Channels))
	}
}

func TestUnsubscribePatternAfterCachedPublish(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}
	if r := e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver); r.Delivered != 1 {
		t.Fatalf("Delivered = %d before unsubscribe, want 1", r.Delivered)
	}

	e.UnsubscribePattern(1, "metrics:*")

	if r := e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver); r.Delivered != 0 {
		t.Fatalf("Delivered = %d after unsubscribe on cached channel, want 0", r.Delivered)
	}
}

func TestPatternChurnUnderConcurrentPublish(t *testing.T) {
	e := New()
	defer e.Stop()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sink := func(SubscriberID, Delivery) bool { return true }
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				e.Publish(fmt.Sprintf("metrics:cpu%d", (w*8+i)%16), []byte("v"), 0, sink)
			}
		}(w)
	}

	for i := range 200 {
		id := SubscriberID(i%4 + 1)
		if err := e.SubscribePattern(id, "metrics:*"); err != nil && !errors.Is(err, ErrAlreadySubscribed) {
			t.Errorf("SubscribePattern: %v", err)
		}
		e.UnsubscribePattern(id, "metrics:*")
	}
	close(stop)
	wg.Wait()

	rec := newDeliveryRecorder()
	if r := e.Publish("metrics:cpu1", []byte("v"), 0, rec.deliver); r.Delivered != 0 {
		t.Fatalf("Delivered = %d after churn drained all patterns, want 0", r.Delivered)
	}
	if int(e.Stats().TotalSubscribers) != 0 {
		t.Fatalf("TotalSubscriptions = %d, want 0", int(e.Stats().TotalSubscribers))
	}
}

func TestPatternMaterializedChannelKeepsOffsetContinuity(t *testing.T) {
	e := New()
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "metrics:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}

	r1 := e.Publish("metrics:cpu", []byte("a"), 0, rec.deliver)
	r2 := e.Publish("metrics:cpu", []byte("b"), 0, rec.deliver)
	if r1.Offset != 1 || r2.Offset != 2 {
		t.Fatalf("offsets = %d, %d, want 1, 2", r1.Offset, r2.Offset)
	}
	if r1.Epoch != r2.Epoch {
		t.Fatalf("epochs differ: %d vs %d", r1.Epoch, r2.Epoch)
	}
}
