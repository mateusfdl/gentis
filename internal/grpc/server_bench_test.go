package grpc

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/arena"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Phase F benchmarks. Each bench runs under `heap` and `arena` sub-
// benchmarks so the results are directly comparable.
//
//   - BenchmarkGRPCConnectionMemory: per-session heap & sys footprint at
//     idle, plus goroutines/conn.
//   - BenchmarkGRPCConnectionChurn: connect/auth/disconnect cycles per second.
//   - BenchmarkGRPCMessageLatency: round-trip latency percentiles.
//   - BenchmarkGRPCMessageThroughput: fanout throughput.
//   - BenchmarkGRPCGCPause: forced GC timing at idle.
//   - BenchmarkGRPCGCPauseUnderTraffic: forced GC timing under traffic load.
//
// The arena path only matters on linux; on darwin/etc. the `arena` sub-
// benchmarks skip via arena.New returning ErrUnsupported.

// benchMode selects which per-session state backend the server uses.
type benchMode int

const (
	modeHeap  benchMode = iota // counter IDs + heap *client.State
	modeArena                  // arena slot IDs + mmap SessionSlot
)

func (m benchMode) String() string {
	switch m {
	case modeHeap:
		return "heap"
	case modeArena:
		return "arena"
	}
	return "unknown"
}

const benchMaxSessions = 16384

// suppressBenchLogs hides the session created/closed logs emitted by
// the gRPC server so they don't swamp bench output.
func init() {
	log.SetOutput(io.Discard)
}

func startBenchServer(b *testing.B, mode benchMode) (string, func()) {
	b.Helper()

	if mode == modeArena {
		a, err := arena.New(1, 1)
		if err != nil {
			b.Skipf("arena not supported on this platform: %v", err)
		}
		a.Close()
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	eng := engine.New()

	// Match the production wiring: flat store only for arena mode where
	// session IDs come from slot indices and land in [1, benchMaxSessions].
	// Heap mode uses counter IDs outside that range so a flat array would
	// be pure GC-scan overhead.
	var store *transport.SessionStore
	if mode == modeArena {
		store = transport.NewFlatSessionStore(engine.SubscriberID(1), benchMaxSessions)
	} else {
		store = transport.NewSessionStore()
	}

	opts := []Option{
		WithEngine(eng),
		WithSessionStore(store),
	}
	if mode == modeArena {
		opts = append(opts, WithArena(), WithMaxSessions(benchMaxSessions))
	}

	srv := New(addr, opts...)
	if err := srv.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}

	var once sync.Once
	stop := func() {
		once.Do(func() {
			srv.Stop()
			eng.Stop()
		})
	}
	b.Cleanup(stop)
	return addr, stop
}

func benchEachMode(b *testing.B, fn func(b *testing.B, mode benchMode)) {
	b.Helper()
	for _, m := range []benchMode{modeHeap, modeArena} {
		b.Run(m.String(), func(b *testing.B) {
			fn(b, m)
		})
	}
}

// dialBench returns a grpc.ClientConn. Caller must Close() it.
func dialBench(b *testing.B, addr string) *grpc.ClientConn {
	b.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("grpc.NewClient: %v", err)
	}
	return cc
}

// openStreamBench opens a Stream and authenticates. Returns the stream
// plus a cleanup that closes send and the underlying conn.
func openStreamBench(b *testing.B, addr string) (gentisv1.GentisService_StreamClient, func()) {
	b.Helper()
	cc := dialBench(b, addr)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := gentisv1.NewGentisServiceClient(cc).Stream(ctx)
	if err != nil {
		cancel()
		cc.Close()
		b.Fatalf("Stream: %v", err)
	}

	// Authenticate.
	if err := stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Connect{
			Connect: &gentisv1.ConnectRequest{AuthToken: "bench"},
		},
	}); err != nil {
		cancel()
		cc.Close()
		b.Fatalf("Send Connect: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		cancel()
		cc.Close()
		b.Fatalf("Recv Connected: %v", err)
	}
	return stream, func() {
		_ = stream.CloseSend()
		cancel()
		cc.Close()
	}
}

// ---------- Family 1: Connection ----------

// BenchmarkGRPCConnectionMemory reports per-session heap bytes, sys bytes,
// and goroutine count at N ∈ {100, 1000}. Higher Ns gated behind
// GENTIS_BENCH_LARGE=1 on the env if ever needed.
func BenchmarkGRPCConnectionMemory(b *testing.B) {
	benchEachMode(b, func(b *testing.B, mode benchMode) {
		for _, n := range []int{100, 1000} {
			b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
				addr, stop := startBenchServer(b, mode)
				defer stop()

				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				goroBefore := runtime.NumGoroutine()

				streams := make([]gentisv1.GentisService_StreamClient, n)
				closes := make([]func(), n)
				for i := 0; i < n; i++ {
					s, c := openStreamBench(b, addr)
					streams[i] = s
					closes[i] = c
				}
				// Let the server finish registering all sessions.
				time.Sleep(100 * time.Millisecond)

				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				goroAfter := runtime.NumGoroutine()

				heapDelta := int64(after.HeapInuse) - int64(before.HeapInuse)
				sysDelta := int64(after.Sys) - int64(before.Sys)
				goroDelta := goroAfter - goroBefore

				b.ReportMetric(float64(heapDelta)/float64(n), "heapB/conn")
				b.ReportMetric(float64(sysDelta)/float64(n), "sysB/conn")
				b.ReportMetric(float64(goroDelta)/float64(n), "goro/conn")

				for _, c := range closes {
					c()
				}
			})
		}
	})
}

// BenchmarkGRPCConnectionChurn measures one full connect/auth/disconnect
// cycle per iteration.
func BenchmarkGRPCConnectionChurn(b *testing.B) {
	benchEachMode(b, func(b *testing.B, mode benchMode) {
		addr, stop := startBenchServer(b, mode)
		defer stop()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, closeStream := openStreamBench(b, addr)
			closeStream()
		}
	})
}

// ---------- Family 2: Message ----------

// BenchmarkGRPCMessageLatency measures round-trip Ping/Pong latency and
// reports p50/p99/p999.
func BenchmarkGRPCMessageLatency(b *testing.B) {
	benchEachMode(b, func(b *testing.B, mode benchMode) {
		addr, stop := startBenchServer(b, mode)
		defer stop()

		stream, closeStream := openStreamBench(b, addr)
		defer closeStream()

		samples := make([]time.Duration, 0, b.N)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t0 := time.Now()
			if err := stream.Send(&gentisv1.ClientMessage{
				Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
			}); err != nil {
				b.Fatalf("Send Ping: %v", err)
			}
			if _, err := stream.Recv(); err != nil {
				b.Fatalf("Recv Pong: %v", err)
			}
			samples = append(samples, time.Since(t0))
		}
		b.StopTimer()

		if len(samples) > 0 {
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			b.ReportMetric(float64(samples[len(samples)*50/100].Nanoseconds()), "p50-ns")
			b.ReportMetric(float64(samples[len(samples)*99/100].Nanoseconds()), "p99-ns")
			if len(samples) >= 1000 {
				b.ReportMetric(float64(samples[len(samples)*999/1000].Nanoseconds()), "p999-ns")
			}
		}
	})
}

// BenchmarkGRPCMessageThroughput measures fanout: 1 publisher → N subs
// receiving the same channel. Reports deliveries/sec.
func BenchmarkGRPCMessageThroughput(b *testing.B) {
	benchEachMode(b, func(b *testing.B, mode benchMode) {
		for _, subs := range []int{10, 100} {
			b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
				addr, stop := startBenchServer(b, mode)
				defer stop()

				subStreams := make([]gentisv1.GentisService_StreamClient, subs)
				subCloses := make([]func(), subs)
				for i := 0; i < subs; i++ {
					s, c := openStreamBench(b, addr)
					subStreams[i] = s
					subCloses[i] = c

					if err := s.Send(&gentisv1.ClientMessage{
						Message: &gentisv1.ClientMessage_Subscribe{
							Subscribe: &gentisv1.SubscribeRequest{Channel: "tp"},
						},
					}); err != nil {
						b.Fatalf("sub Send: %v", err)
					}
					if _, err := s.Recv(); err != nil {
						b.Fatalf("sub Recv Subscribed: %v", err)
					}
				}
				defer func() {
					for _, c := range subCloses {
						c()
					}
				}()

				pub, closePub := openStreamBench(b, addr)
				defer closePub()

				var delivered int64
				// Each subscriber recvs in a goroutine, counting received
				// messages. Stops when the bench context is cancelled.
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var wg sync.WaitGroup
				for _, s := range subStreams {
					wg.Add(1)
					go func(stream gentisv1.GentisService_StreamClient) {
						defer wg.Done()
						for {
							select {
							case <-ctx.Done():
								return
							default:
							}
							if _, err := stream.Recv(); err != nil {
								return
							}
							atomic.AddInt64(&delivered, 1)
						}
					}(s)
				}

				data := make([]byte, 128)
				b.ResetTimer()
				start := time.Now()
				for i := 0; i < b.N; i++ {
					if err := pub.Send(&gentisv1.ClientMessage{
						Message: &gentisv1.ClientMessage_Publish{
							Publish: &gentisv1.PublishRequest{
								Channel: "tp",
								Data:    data,
							},
						},
					}); err != nil {
						b.Fatalf("pub Send: %v", err)
					}
				}
				elapsed := time.Since(start)
				b.StopTimer()

				// Give the fanout a moment to drain in-flight deliveries.
				time.Sleep(50 * time.Millisecond)
				cancel()
				// Drain the subscribers so wg.Wait returns.
				for _, c := range subCloses {
					c()
				}
				wg.Wait()

				rate := float64(atomic.LoadInt64(&delivered)) / elapsed.Seconds()
				b.ReportMetric(rate, "deliveries/s")
			})
		}
	})
}

// ---------- Family 3: GC ----------

// BenchmarkGRPCGCPause holds N idle sessions open and forces GC, reporting
// ns/gc, gc_heap_MB, and gc_sys_MB. This is the arena's natural win axis:
// arena slots aren't in Go's heap so they don't add GC scan work.
func BenchmarkGRPCGCPause(b *testing.B) {
	benchEachMode(b, func(b *testing.B, mode benchMode) {
		for _, n := range []int{1000} {
			b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
				addr, stop := startBenchServer(b, mode)
				defer stop()

				closes := make([]func(), n)
				for i := 0; i < n; i++ {
					_, c := openStreamBench(b, addr)
					closes[i] = c
				}
				defer func() {
					for _, c := range closes {
						c()
					}
				}()

				// Give the server a moment to register all sessions.
				time.Sleep(200 * time.Millisecond)

				const gcRuns = 10
				samples := make([]time.Duration, 0, gcRuns)
				var mem runtime.MemStats

				b.ResetTimer()
				for i := 0; i < gcRuns; i++ {
					t0 := time.Now()
					runtime.GC()
					samples = append(samples, time.Since(t0))
				}
				b.StopTimer()
				runtime.ReadMemStats(&mem)

				var total time.Duration
				for _, s := range samples {
					total += s
				}
				avgNs := float64(total.Nanoseconds()) / float64(len(samples))
				b.ReportMetric(avgNs, "ns/gc")
				b.ReportMetric(float64(mem.HeapInuse)/(1<<20), "gc_heap_MB")
				b.ReportMetric(float64(mem.Sys)/(1<<20), "gc_sys_MB")
			})
		}
	})
}

// BenchmarkGRPCGCPauseUnderTraffic holds N subscribers on a channel and
// bursts M publishes through, forcing GC mid-drain so scan work covers
// buffer contents. This is a closer model of production GC behaviour.
func BenchmarkGRPCGCPauseUnderTraffic(b *testing.B) {
	benchEachMode(b, func(b *testing.B, mode benchMode) {
		for _, n := range []int{1000} {
			b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
				addr, stop := startBenchServer(b, mode)
				defer stop()

				// N subs each receive in a goroutine.
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				subs := make([]gentisv1.GentisService_StreamClient, n)
				subCloses := make([]func(), n)
				var wg sync.WaitGroup
				for i := 0; i < n; i++ {
					s, c := openStreamBench(b, addr)
					subs[i] = s
					subCloses[i] = c

					if err := s.Send(&gentisv1.ClientMessage{
						Message: &gentisv1.ClientMessage_Subscribe{
							Subscribe: &gentisv1.SubscribeRequest{Channel: "gc"},
						},
					}); err != nil {
						b.Fatalf("sub Send: %v", err)
					}
					if _, err := s.Recv(); err != nil {
						b.Fatalf("sub Recv Subscribed: %v", err)
					}

					wg.Add(1)
					go func(stream gentisv1.GentisService_StreamClient) {
						defer wg.Done()
						for {
							select {
							case <-ctx.Done():
								return
							default:
							}
							if _, err := stream.Recv(); err != nil {
								return
							}
						}
					}(s)
				}
				defer func() {
					cancel()
					for _, c := range subCloses {
						c()
					}
					wg.Wait()
				}()

				pub, closePub := openStreamBench(b, addr)
				defer closePub()

				// Burst a handful of publishes to fill buffers.
				data := make([]byte, 512)
				const burst = 50
				for i := 0; i < burst; i++ {
					if err := pub.Send(&gentisv1.ClientMessage{
						Message: &gentisv1.ClientMessage_Publish{
							Publish: &gentisv1.PublishRequest{
								Channel: "gc",
								Data:    data,
							},
						},
					}); err != nil {
						b.Fatalf("pub Send: %v", err)
					}
				}

				// Let delivery be mid-flight — don't fully drain.
				time.Sleep(20 * time.Millisecond)

				const gcRuns = 5
				samples := make([]time.Duration, 0, gcRuns)
				var mem runtime.MemStats

				b.ResetTimer()
				for i := 0; i < gcRuns; i++ {
					t0 := time.Now()
					runtime.GC()
					samples = append(samples, time.Since(t0))
				}
				b.StopTimer()
				runtime.ReadMemStats(&mem)

				var total time.Duration
				for _, s := range samples {
					total += s
				}
				avgNs := float64(total.Nanoseconds()) / float64(len(samples))
				b.ReportMetric(avgNs, "ns/gc")
				b.ReportMetric(float64(mem.HeapInuse)/(1<<20), "gc_heap_MB")
				b.ReportMetric(float64(mem.Sys)/(1<<20), "gc_sys_MB")
			})
		}
	})
}
