package metrics

import (
	"context"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mateusfdl/gentis/internal/engine"
)

const namespace = "gentis"

type StatsProvider interface {
	Stats() engine.EngineStats
}

type ConnectionCounter interface {
	ConnectionCount() int64
}

type Collector struct {
	statsProvider StatsProvider
	connCounter   ConnectionCounter
	mode          string

	channelsDesc    *prometheus.Desc
	subscribersDesc *prometheus.Desc
	publishedDesc   *prometheus.Desc
	deliveredDesc   *prometheus.Desc
	droppedDesc     *prometheus.Desc
	connectionsDesc *prometheus.Desc
}

func NewCollector(provider StatsProvider, connCounter ConnectionCounter, mode string) *Collector {
	labels := prometheus.Labels{"mode": mode}

	return &Collector{
		statsProvider: provider,
		connCounter:   connCounter,
		mode:          mode,
		channelsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "channels_active"),
			"Number of active channels",
			nil, labels,
		),
		subscribersDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "subscriptions_total"),
			"Total number of active subscriptions",
			nil, labels,
		),
		publishedDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "messages_published_total"),
			"Total number of messages published",
			nil, labels,
		),
		deliveredDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "messages_delivered_total"),
			"Total number of messages successfully delivered",
			nil, labels,
		),
		droppedDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "messages_dropped_total"),
			"Total number of messages dropped due to buffer overflow",
			nil, labels,
		),
		connectionsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "connections_active"),
			"Number of active client connections",
			nil, labels,
		),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.channelsDesc
	ch <- c.subscribersDesc
	ch <- c.publishedDesc
	ch <- c.deliveredDesc
	ch <- c.droppedDesc
	ch <- c.connectionsDesc
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	stats := c.statsProvider.Stats()

	ch <- prometheus.MustNewConstMetric(
		c.channelsDesc, prometheus.GaugeValue, float64(stats.Channels),
	)
	ch <- prometheus.MustNewConstMetric(
		c.subscribersDesc, prometheus.GaugeValue, float64(stats.TotalSubscribers),
	)
	ch <- prometheus.MustNewConstMetric(
		c.publishedDesc, prometheus.CounterValue, float64(stats.MessagesPublished),
	)
	ch <- prometheus.MustNewConstMetric(
		c.deliveredDesc, prometheus.CounterValue, float64(stats.MessagesDelivered),
	)
	ch <- prometheus.MustNewConstMetric(
		c.droppedDesc, prometheus.CounterValue, float64(stats.MessagesDropped),
	)

	if c.connCounter != nil {
		ch <- prometheus.MustNewConstMetric(
			c.connectionsDesc, prometheus.GaugeValue, float64(c.connCounter.ConnectionCount()),
		)
	}
}

type Server struct {
	addr       string
	httpServer *http.Server
	registry   *prometheus.Registry
	running    atomic.Bool
}

func NewServer(addr string, collector *Collector) *Server {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	return &Server{
		addr:     addr,
		registry: registry,
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.running.Store(true)

	go func() {
		if err := s.httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	if !s.running.Load() {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.running.Store(false)
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) Addr() string {
	return s.addr
}
