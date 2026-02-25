package context

import (
	"context"
	"log/slog"
	"maps"
	"sync"
	"time"
)

const (
	KeyTraceID  = "trace_id"
	KeySpanID   = "span_id"
	KeyParentID = "parent_id"

	KeyRequestID = "request_id"
	KeyOperation = "operation"
	KeyChannel   = "channel"
	KeyStartTime = "start_time"

	KeySessionID = "session_id"
	KeyClientID  = "client_id"
	KeyClientIP  = "client_ip"
	KeyUserAgent = "user_agent"

	KeyUserID   = "user_id"
	KeyTenantID = "tenant_id"
	KeyAuthTime = "auth_time"
)

type Metadata struct {
	mu     sync.RWMutex
	values map[string]any
}

func NewMetadata() *Metadata {
	return &Metadata{
		values: make(map[string]any),
	}
}

func (m *Metadata) Set(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = value
}

func (m *Metadata) Get(key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.values[key]
	return v, ok
}

func (m *Metadata) GetString(key string) string {
	v, ok := m.Get(key)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func (m *Metadata) GetInt(key string) int {
	v, ok := m.Get(key)
	if !ok {
		return 0
	}
	i, _ := v.(int)
	return i
}

func (m *Metadata) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.values, key)
}

func (m *Metadata) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.values))
	for k := range m.values {
		keys = append(keys, k)
	}
	return keys
}

func (m *Metadata) Clone() *Metadata {
	m.mu.RLock()
	defer m.mu.RUnlock()
	clone := NewMetadata()

	maps.Copy(clone.values, m.values)

	return clone
}

func (m *Metadata) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.values)
}

type contextKey struct{}

type Context struct {
	context.Context
	metadata *Metadata
	logger   *slog.Logger
}

func New(parent context.Context) *Context {
	if parent == nil {
		parent = context.Background()
	}
	return &Context{
		Context:  parent,
		metadata: NewMetadata(),
	}
}

func WithMetadata(parent context.Context, md *Metadata) *Context {
	ctx := New(parent)
	if md != nil {
		ctx.metadata = md.Clone()
	}
	return ctx
}

func (c *Context) Metadata() *Metadata {
	return c.metadata
}

func (c *Context) Set(key string, value any) *Context {
	c.metadata.Set(key, value)
	return c
}

func (c *Context) Get(key string) (any, bool) {
	return c.metadata.Get(key)
}

func (c *Context) Value(key any) any {
	if _, ok := key.(contextKey); ok {
		return c
	}

	if k, ok := key.(string); ok {
		if v, exists := c.metadata.Get(k); exists {
			return v
		}
	}

	return c.Context.Value(key)
}

func (c *Context) WithLogger(l *slog.Logger) *Context {
	c.logger = l
	return c
}

func (c *Context) Logger() *slog.Logger {
	l := c.logger
	if l == nil {
		l = slog.Default()
	}

	attrs := make([]any, 0, 16)

	if v := c.metadata.GetString(KeyTraceID); v != "" {
		attrs = append(attrs, slog.String(KeyTraceID, v))
	}
	if v := c.metadata.GetInt(KeySessionID); v != 0 {
		attrs = append(attrs, slog.Int(KeySessionID, v))
	}
	if v := c.metadata.GetString(KeyClientID); v != "" {
		attrs = append(attrs, slog.String(KeyClientID, v))
	}
	if v := c.metadata.GetString(KeyClientIP); v != "" {
		attrs = append(attrs, slog.String(KeyClientIP, v))
	}
	if v := c.metadata.GetString(KeyChannel); v != "" {
		attrs = append(attrs, slog.String(KeyChannel, v))
	}
	if v := c.metadata.GetString(KeyOperation); v != "" {
		attrs = append(attrs, slog.String(KeyOperation, v))
	}

	if len(attrs) == 0 {
		return l
	}

	return l.With(attrs...)
}

func (c *Context) WithCancel() (*Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.Context)
	return &Context{
		Context:  ctx,
		metadata: c.metadata,
		logger:   c.logger,
	}, cancel
}

func (c *Context) WithTimeout(timeout time.Duration) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.Context, timeout)
	return &Context{
		Context:  ctx,
		metadata: c.metadata,
		logger:   c.logger,
	}, cancel
}

func (c *Context) WithDeadline(d time.Time) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(c.Context, d)
	return &Context{
		Context:  ctx,
		metadata: c.metadata,
		logger:   c.logger,
	}, cancel
}

func FromContext(ctx context.Context) *Context {
	if ctx == nil {
		return New(context.TODO())
	}

	if c, ok := ctx.(*Context); ok {
		return c
	}

	if c, ok := ctx.Value(contextKey{}).(*Context); ok {
		return c
	}

	return New(ctx)
}

func (c *Context) Wrap() context.Context {
	return context.WithValue(c.Context, contextKey{}, c)
}

func (c *Context) WithTraceID(traceID string) *Context {
	return c.Set(KeyTraceID, traceID)
}

func (c *Context) TraceID() string {
	return c.metadata.GetString(KeyTraceID)
}

func (c *Context) WithRequestID(requestID string) *Context {
	return c.Set(KeyRequestID, requestID)
}

func (c *Context) RequestID() string {
	return c.metadata.GetString(KeyRequestID)
}

func (c *Context) WithSessionID(sessionID int) *Context {
	return c.Set(KeySessionID, sessionID)
}

func (c *Context) SessionID() int {
	return c.metadata.GetInt(KeySessionID)
}

func (c *Context) WithClientID(clientID string) *Context {
	return c.Set(KeyClientID, clientID)
}

func (c *Context) ClientID() string {
	return c.metadata.GetString(KeyClientID)
}

func (c *Context) WithClientIP(clientIP string) *Context {
	return c.Set(KeyClientIP, clientIP)
}

func (c *Context) ClientIP() string {
	return c.metadata.GetString(KeyClientIP)
}

func (c *Context) WithOperation(operation string) *Context {
	return c.Set(KeyOperation, operation)
}

func (c *Context) Operation() string {
	return c.metadata.GetString(KeyOperation)
}

func (c *Context) WithChannel(channel string) *Context {
	return c.Set(KeyChannel, channel)
}

func (c *Context) Channel() string {
	return c.metadata.GetString(KeyChannel)
}

func (c *Context) WithStartTime(t time.Time) *Context {
	return c.Set(KeyStartTime, t)
}

func (c *Context) StartTime() time.Time {
	v, ok := c.metadata.Get(KeyStartTime)
	if !ok {
		return time.Time{}
	}
	t, _ := v.(time.Time)
	return t
}

func (c *Context) Elapsed() time.Duration {
	start := c.StartTime()
	if start.IsZero() {
		return 0
	}
	return time.Since(start)
}

func (c *Context) WithUserID(userID string) *Context {
	return c.Set(KeyUserID, userID)
}

func (c *Context) UserID() string {
	return c.metadata.GetString(KeyUserID)
}

func (c *Context) WithTenantID(tenantID string) *Context {
	return c.Set(KeyTenantID, tenantID)
}

func (c *Context) TenantID() string {
	return c.metadata.GetString(KeyTenantID)
}
