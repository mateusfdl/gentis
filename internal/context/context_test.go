package context

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMetadata_SetAndGet(t *testing.T) {
	m := NewMetadata()

	m.Set("key1", "value1")
	m.Set("key2", 42)
	m.Set("key3", true)

	v, ok := m.Get("key1")
	if !ok || v != "value1" {
		t.Errorf("expected value1, got %v", v)
	}

	v, ok = m.Get("key2")
	if !ok || v != 42 {
		t.Errorf("expected 42, got %v", v)
	}

	v, ok = m.Get("key3")
	if !ok || v != true {
		t.Errorf("expected true, got %v", v)
	}

	_, ok = m.Get("nonexistent")
	if ok {
		t.Error("expected not found for nonexistent key")
	}
}

func TestMetadata_GetString(t *testing.T) {
	m := NewMetadata()

	m.Set("str", "hello")
	m.Set("num", 123)

	if s := m.GetString("str"); s != "hello" {
		t.Errorf("expected hello, got %s", s)
	}

	if s := m.GetString("num"); s != "" {
		t.Errorf("expected empty string for non-string, got %s", s)
	}

	if s := m.GetString("missing"); s != "" {
		t.Errorf("expected empty string for missing, got %s", s)
	}
}

func TestMetadata_GetInt(t *testing.T) {
	m := NewMetadata()

	m.Set("num", 42)
	m.Set("str", "hello")

	if n := m.GetInt("num"); n != 42 {
		t.Errorf("expected 42, got %d", n)
	}

	if n := m.GetInt("str"); n != 0 {
		t.Errorf("expected 0 for non-int, got %d", n)
	}

	if n := m.GetInt("missing"); n != 0 {
		t.Errorf("expected 0 for missing, got %d", n)
	}
}

func TestMetadata_Delete(t *testing.T) {
	m := NewMetadata()

	m.Set("key", "value")
	if _, ok := m.Get("key"); !ok {
		t.Error("expected key to exist")
	}

	m.Delete("key")
	if _, ok := m.Get("key"); ok {
		t.Error("expected key to be deleted")
	}
}

func TestMetadata_Keys(t *testing.T) {
	m := NewMetadata()

	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	keys := m.Keys()
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	for _, expected := range []string{"a", "b", "c"} {
		if !keySet[expected] {
			t.Errorf("expected key %s not found", expected)
		}
	}
}

func TestMetadata_Clone(t *testing.T) {
	m := NewMetadata()
	m.Set("key", "original")

	clone := m.Clone()

	// Verify clone has the value
	if v := clone.GetString("key"); v != "original" {
		t.Errorf("expected original in clone, got %s", v)
	}

	// Modify original
	m.Set("key", "modified")

	// Clone should not be affected
	if v := clone.GetString("key"); v != "original" {
		t.Errorf("clone should not be affected, got %s", v)
	}
}

func TestMetadata_Len(t *testing.T) {
	m := NewMetadata()

	if m.Len() != 0 {
		t.Errorf("expected 0, got %d", m.Len())
	}

	m.Set("a", 1)
	m.Set("b", 2)

	if m.Len() != 2 {
		t.Errorf("expected 2, got %d", m.Len())
	}
}

func TestMetadata_ConcurrentAccess(t *testing.T) {
	m := NewMetadata()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set("key", i)
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Get("key")
		}()
	}

	wg.Wait()

	// Should not panic and should have a value
	if _, ok := m.Get("key"); !ok {
		t.Error("expected key to exist after concurrent access")
	}
}

func TestContext_New(t *testing.T) {
	c := New(nil)
	if c == nil {
		t.Fatal("expected non-nil context")
	}
	if c.metadata == nil {
		t.Error("expected metadata to be initialized")
	}
}

func TestContext_NewWithParent(t *testing.T) {
	parent := context.Background()
	c := New(parent)

	if c.Context != parent {
		t.Error("expected parent context to be set")
	}
}

func TestContext_SetAndGet(t *testing.T) {
	c := New(nil)

	c.Set("key", "value")

	v, ok := c.Get("key")
	if !ok || v != "value" {
		t.Errorf("expected value, got %v", v)
	}
}

func TestContext_Chaining(t *testing.T) {
	c := New(nil).
		WithTraceID("trace-123").
		WithRequestID("req-456").
		WithSessionID(1).
		WithClientID("client-789").
		WithOperation("subscribe")

	if c.TraceID() != "trace-123" {
		t.Errorf("expected trace-123, got %s", c.TraceID())
	}
	if c.RequestID() != "req-456" {
		t.Errorf("expected req-456, got %s", c.RequestID())
	}
	if c.SessionID() != 1 {
		t.Errorf("expected 1, got %d", c.SessionID())
	}
	if c.ClientID() != "client-789" {
		t.Errorf("expected client-789, got %s", c.ClientID())
	}
	if c.Operation() != "subscribe" {
		t.Errorf("expected subscribe, got %s", c.Operation())
	}
}

func TestContext_Value(t *testing.T) {
	c := New(nil)
	c.Set("mykey", "myvalue")

	// Should be retrievable via Value method
	v := c.Value("mykey")
	if v != "myvalue" {
		t.Errorf("expected myvalue via Value(), got %v", v)
	}

	// Non-existent key should return nil
	v = c.Value("nonexistent")
	if v != nil {
		t.Errorf("expected nil for nonexistent, got %v", v)
	}
}

func TestContext_ValueFallsBackToParent(t *testing.T) {
	type parentKey struct{}
	parent := context.WithValue(context.Background(), parentKey{}, "parentvalue")

	c := New(parent)

	// Should fall back to parent context
	v := c.Value(parentKey{})
	if v != "parentvalue" {
		t.Errorf("expected parentvalue, got %v", v)
	}
}

func TestContext_WithCancel(t *testing.T) {
	c := New(nil)
	c.Set("preserved", "value")

	child, cancel := c.WithCancel()
	defer cancel()

	// Metadata should be shared
	if child.metadata.GetString("preserved") != "value" {
		t.Error("metadata should be preserved")
	}

	// Cancel should work
	cancel()

	select {
	case <-child.Done():
		// Expected
	case <-time.After(time.Second):
		t.Error("context should be canceled")
	}
}

func TestContext_WithTimeout(t *testing.T) {
	c := New(nil)
	c.Set("preserved", "value")

	child, cancel := c.WithTimeout(50 * time.Millisecond)
	defer cancel()

	// Metadata should be shared
	if child.metadata.GetString("preserved") != "value" {
		t.Error("metadata should be preserved")
	}

	// Should timeout
	select {
	case <-child.Done():
		// Expected
	case <-time.After(time.Second):
		t.Error("context should timeout")
	}
}

func TestContext_WithDeadline(t *testing.T) {
	c := New(nil)
	c.Set("preserved", "value")

	deadline := time.Now().Add(50 * time.Millisecond)
	child, cancel := c.WithDeadline(deadline)
	defer cancel()

	// Metadata should be shared
	if child.metadata.GetString("preserved") != "value" {
		t.Error("metadata should be preserved")
	}

	// Should reach deadline
	select {
	case <-child.Done():
		// Expected
	case <-time.After(time.Second):
		t.Error("context should reach deadline")
	}
}

func TestContext_FromContext(t *testing.T) {
	// From nil
	c := FromContext(nil)
	if c == nil {
		t.Fatal("expected non-nil context from nil")
	}

	// From *Context
	original := New(nil).WithTraceID("trace-123")
	extracted := FromContext(original)
	if extracted.TraceID() != "trace-123" {
		t.Error("expected to get same context")
	}

	// From standard context.Context
	stdCtx := context.Background()
	c = FromContext(stdCtx)
	if c == nil {
		t.Fatal("expected non-nil context from standard context")
	}
}

func TestContext_WrapAndExtract(t *testing.T) {
	c := New(nil).WithTraceID("trace-123")

	// Wrap into standard context
	wrapped := c.Wrap()

	// Extract back
	extracted := FromContext(wrapped)
	if extracted.TraceID() != "trace-123" {
		t.Errorf("expected trace-123 after wrap/extract, got %s", extracted.TraceID())
	}
}

func TestContext_WithMetadata(t *testing.T) {
	md := NewMetadata()
	md.Set("key", "value")

	c := WithMetadata(context.Background(), md)

	// Should have the metadata
	if c.metadata.GetString("key") != "value" {
		t.Error("expected metadata to be copied")
	}

	// Should be a clone
	md.Set("key", "modified")
	if c.metadata.GetString("key") != "value" {
		t.Error("context should have cloned metadata")
	}
}

func TestContext_TimeHelpers(t *testing.T) {
	c := New(nil)

	// Without start time
	if !c.StartTime().IsZero() {
		t.Error("expected zero start time")
	}
	if c.Elapsed() != 0 {
		t.Error("expected zero elapsed without start time")
	}

	// With start time
	start := time.Now().Add(-100 * time.Millisecond)
	c.WithStartTime(start)

	if c.StartTime() != start {
		t.Error("expected start time to be set")
	}

	elapsed := c.Elapsed()
	if elapsed < 100*time.Millisecond {
		t.Errorf("expected elapsed >= 100ms, got %v", elapsed)
	}
}

func TestContext_AllHelpers(t *testing.T) {
	now := time.Now()
	c := New(nil).
		WithTraceID("trace").
		WithRequestID("request").
		WithSessionID(42).
		WithClientID("client").
		WithClientIP("192.168.1.1").
		WithOperation("publish").
		WithChannel("chat").
		WithStartTime(now).
		WithUserID("user123").
		WithTenantID("tenant456")

	tests := []struct {
		name     string
		got      any
		expected any
	}{
		{"TraceID", c.TraceID(), "trace"},
		{"RequestID", c.RequestID(), "request"},
		{"SessionID", c.SessionID(), 42},
		{"ClientID", c.ClientID(), "client"},
		{"ClientIP", c.ClientIP(), "192.168.1.1"},
		{"Operation", c.Operation(), "publish"},
		{"Channel", c.Channel(), "chat"},
		{"StartTime", c.StartTime(), now},
		{"UserID", c.UserID(), "user123"},
		{"TenantID", c.TenantID(), "tenant456"},
	}

	for _, tt := range tests {
		if tt.got != tt.expected {
			t.Errorf("%s: expected %v, got %v", tt.name, tt.expected, tt.got)
		}
	}
}

func TestContext_ImplementsContextInterface(t *testing.T) {
	var _ context.Context = New(nil)
}

func TestContext_DeadlineFromParent(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	parent, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	c := New(parent)

	d, ok := c.Deadline()
	if !ok {
		t.Error("expected deadline to be set")
	}
	if !d.Equal(deadline) {
		t.Errorf("expected deadline %v, got %v", deadline, d)
	}
}

func TestContext_DoneFromParent(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())

	c := New(parent)

	// Should not be done yet
	select {
	case <-c.Done():
		t.Error("context should not be done yet")
	default:
		// Expected
	}

	cancel()

	// Should be done now
	select {
	case <-c.Done():
		// Expected
	case <-time.After(time.Second):
		t.Error("context should be done after cancel")
	}
}

func TestContext_ErrFromParent(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())

	c := New(parent)

	if c.Err() != nil {
		t.Error("expected no error before cancel")
	}

	cancel()

	if c.Err() != context.Canceled {
		t.Errorf("expected Canceled error, got %v", c.Err())
	}
}
