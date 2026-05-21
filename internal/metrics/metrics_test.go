package metrics

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
)

type mockStatsProvider struct {
	stats engine.EngineStats
}

func (m *mockStatsProvider) Stats() engine.EngineStats {
	return m.stats
}

type mockConnectionCounter struct {
	count          int64
	total          int64
	disconnections int64
}

func (m *mockConnectionCounter) ConnectionCount() int64 {
	return m.count
}

func (m *mockConnectionCounter) ConnectionsTotal() int64 {
	return m.total
}

func (m *mockConnectionCounter) DisconnectionsTotal() int64 {
	return m.disconnections
}

func TestCollector(t *testing.T) {
	provider := &mockStatsProvider{
		stats: engine.EngineStats{
			Channels:          5,
			TotalSubscribers:  100,
			MessagesPublished: 1000,
			MessagesDelivered: 950,
			MessagesDropped:   50,
		},
	}
	connCounter := &mockConnectionCounter{count: 10}

	collector := NewCollector(provider, connCounter, "server")

	if collector == nil {
		t.Fatal("expected collector to be non-nil")
	}

	if collector.mode != "server" {
		t.Errorf("expected mode to be 'server', got %s", collector.mode)
	}
}

func TestCollectorWithNilConnectionCounter(t *testing.T) {
	provider := &mockStatsProvider{
		stats: engine.EngineStats{
			Channels:          3,
			TotalSubscribers:  50,
			MessagesPublished: 500,
			MessagesDelivered: 480,
			MessagesDropped:   20,
		},
	}

	collector := NewCollector(provider, nil, "relay")

	if collector == nil {
		t.Fatal("expected collector to be non-nil")
	}
}

func TestServerStartStop(t *testing.T) {
	provider := &mockStatsProvider{
		stats: engine.EngineStats{
			Channels:          1,
			TotalSubscribers:  10,
			MessagesPublished: 100,
			MessagesDelivered: 95,
			MessagesDropped:   5,
		},
	}
	connCounter := &mockConnectionCounter{count: 2}

	collector := NewCollector(provider, connCounter, "server")
	server := NewServer("127.0.0.1:0", collector)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := server.Stop(); err != nil {
		t.Errorf("failed to stop server: %v", err)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	provider := &mockStatsProvider{
		stats: engine.EngineStats{
			Channels:          5,
			TotalSubscribers:  100,
			MessagesPublished: 1000,
			MessagesDelivered: 950,
			MessagesDropped:   50,
		},
	}
	connCounter := &mockConnectionCounter{count: 10}

	collector := NewCollector(provider, connCounter, "server")
	server := NewServer("127.0.0.1:19090", collector)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19090/metrics")
	if err != nil {
		t.Fatalf("failed to fetch metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	content := string(body)

	expectedMetrics := []string{
		"gentis_channels_active",
		"gentis_subscriptions_total",
		"gentis_messages_published_total",
		"gentis_messages_delivered_total",
		"gentis_messages_dropped_total",
		"gentis_connections_active",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(content, metric) {
			t.Errorf("expected metric %s not found in output", metric)
		}
	}

	if !strings.Contains(content, `mode="server"`) {
		t.Error("expected mode label not found")
	}

	if !strings.Contains(content, "gentis_channels_active{mode=\"server\"} 5") {
		t.Error("expected channels_active value not found")
	}
	if !strings.Contains(content, "gentis_connections_active{mode=\"server\"} 10") {
		t.Error("expected connections_active value not found")
	}
}

func TestDeliveryLatencyHistogram(t *testing.T) {
	provider := &mockStatsProvider{}
	connCounter := &mockConnectionCounter{count: 1}

	obs := NewObserver("server")
	obs.ObserveDeliveryLatency(0.003)
	obs.ObserveDeliveryLatency(0.42)

	collector := NewCollector(provider, connCounter, "server")
	collector.SetObserver(obs)
	server := NewServer("127.0.0.1:19092", collector)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19092/metrics")
	if err != nil {
		t.Fatalf("failed to fetch metrics: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	content := string(body)

	expected := []string{
		`gentis_delivery_latency_seconds_count{mode="server"} 2`,
		`gentis_delivery_latency_seconds_sum{mode="server"} 0.423`,
		`gentis_delivery_latency_seconds_bucket{mode="server",le="0.5"} 2`,
		`gentis_delivery_latency_seconds_bucket{mode="server",le="0.005"} 1`,
	}
	for _, want := range expected {
		if !strings.Contains(content, want) {
			t.Errorf("expected %q in metrics output", want)
		}
	}
}

func TestHealthEndpoint(t *testing.T) {
	provider := &mockStatsProvider{}
	collector := NewCollector(provider, nil, "test")
	server := NewServer("127.0.0.1:19091", collector)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19091/health")
	if err != nil {
		t.Fatalf("failed to fetch health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	if string(body) != "ok" {
		t.Errorf("expected body 'ok', got %s", string(body))
	}
}
