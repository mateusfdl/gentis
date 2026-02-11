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
	ConnectionsTotal() int64
	DisconnectionsTotal() int64
}

type DedupStats interface {
	DedupHits() int64
	DedupMisses() int64
}

type UpstreamStatus interface {
	IsUpstreamConnected() bool
}

type Observer struct {
	publishDuration prometheus.Histogram
	publishFanout   prometheus.Histogram
}

func NewObserver(mode string) *Observer {
	return &Observer{
		publishDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   namespace,
			Name:        "publish_duration_seconds",
			Help:        "Time spent in the publish fan-out loop",
			ConstLabels: prometheus.Labels{"mode": mode},
			Buckets:     []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		}),
		publishFanout: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   namespace,
			Name:        "publish_fanout",
			Help:        "Number of subscribers reached per publish operation",
			ConstLabels: prometheus.Labels{"mode": mode},
			Buckets:     []float64{0, 1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
	}
}

func (o *Observer) ObservePublishDuration(seconds float64) {
	o.publishDuration.Observe(seconds)
}

func (o *Observer) ObservePublishFanout(count float64) {
	o.publishFanout.Observe(count)
}

type Collector struct {
	statsProvider  StatsProvider
	connCounter    ConnectionCounter
	dedupStats     DedupStats
	upstreamStatus UpstreamStatus
	observer       *Observer
	mode           string

	channelsDesc    *prometheus.Desc
	subscribersDesc *prometheus.Desc
	publishedDesc   *prometheus.Desc
	deliveredDesc   *prometheus.Desc
	droppedDesc     *prometheus.Desc
	connectionsDesc *prometheus.Desc

	subscribeOpsDesc        *prometheus.Desc
	unsubscribeOpsDesc      *prometheus.Desc
	messageBytesDesc        *prometheus.Desc
	connectionsTotalDesc    *prometheus.Desc
	disconnectionsTotalDesc *prometheus.Desc

	dedupHitsDesc         *prometheus.Desc
	dedupMissesDesc       *prometheus.Desc
	upstreamConnectedDesc *prometheus.Desc
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
		subscribeOpsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "subscribe_ops_total"),
			"Total number of subscribe operations",
			nil, labels,
		),
		unsubscribeOpsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "unsubscribe_ops_total"),
			"Total number of unsubscribe operations",
			nil, labels,
		),
		messageBytesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "message_bytes_total"),
			"Total bytes of published message payloads",
			nil, labels,
		),
		connectionsTotalDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "connections_total"),
			"Total number of connections established",
			nil, labels,
		),
		disconnectionsTotalDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "disconnections_total"),
			"Total number of disconnections",
			nil, labels,
		),
		dedupHitsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "relay_dedup_hits_total"),
			"Total deduplicated (dropped) messages in relay",
			nil, labels,
		),
		dedupMissesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "relay_dedup_passes_total"),
			"Total messages that passed deduplication in relay",
			nil, labels,
		),
		upstreamConnectedDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "relay_upstream_connected"),
			"Whether the relay is connected to upstream (1=yes, 0=no)",
			nil, labels,
		),
	}
}

func (c *Collector) SetObserver(obs *Observer) {
	c.observer = obs
}

func (c *Collector) SetDedupStats(d DedupStats) {
	c.dedupStats = d
}

func (c *Collector) SetUpstreamStatus(u UpstreamStatus) {
	c.upstreamStatus = u
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.channelsDesc
	ch <- c.subscribersDesc
	ch <- c.publishedDesc
	ch <- c.deliveredDesc
	ch <- c.droppedDesc
	ch <- c.connectionsDesc
	ch <- c.subscribeOpsDesc
	ch <- c.unsubscribeOpsDesc
	ch <- c.messageBytesDesc
	ch <- c.connectionsTotalDesc
	ch <- c.disconnectionsTotalDesc

	if c.dedupStats != nil {
		ch <- c.dedupHitsDesc
		ch <- c.dedupMissesDesc
	}
	if c.upstreamStatus != nil {
		ch <- c.upstreamConnectedDesc
	}
	if c.observer != nil {
		c.observer.publishDuration.Describe(ch)
		c.observer.publishFanout.Describe(ch)
	}
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
	ch <- prometheus.MustNewConstMetric(
		c.subscribeOpsDesc, prometheus.CounterValue, float64(stats.SubscribeOps),
	)
	ch <- prometheus.MustNewConstMetric(
		c.unsubscribeOpsDesc, prometheus.CounterValue, float64(stats.UnsubscribeOps),
	)
	ch <- prometheus.MustNewConstMetric(
		c.messageBytesDesc, prometheus.CounterValue, float64(stats.MessageBytes),
	)

	if c.connCounter != nil {
		ch <- prometheus.MustNewConstMetric(
			c.connectionsDesc, prometheus.GaugeValue, float64(c.connCounter.ConnectionCount()),
		)
		ch <- prometheus.MustNewConstMetric(
			c.connectionsTotalDesc, prometheus.CounterValue, float64(c.connCounter.ConnectionsTotal()),
		)
		ch <- prometheus.MustNewConstMetric(
			c.disconnectionsTotalDesc, prometheus.CounterValue, float64(c.connCounter.DisconnectionsTotal()),
		)
	}

	if c.dedupStats != nil {
		ch <- prometheus.MustNewConstMetric(
			c.dedupHitsDesc, prometheus.CounterValue, float64(c.dedupStats.DedupHits()),
		)
		ch <- prometheus.MustNewConstMetric(
			c.dedupMissesDesc, prometheus.CounterValue, float64(c.dedupStats.DedupMisses()),
		)
	}

	if c.upstreamStatus != nil {
		val := float64(0)
		if c.upstreamStatus.IsUpstreamConnected() {
			val = 1
		}
		ch <- prometheus.MustNewConstMetric(
			c.upstreamConnectedDesc, prometheus.GaugeValue, val,
		)
	}

	if c.observer != nil {
		c.observer.publishDuration.Collect(ch)
		c.observer.publishFanout.Collect(ch)
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
