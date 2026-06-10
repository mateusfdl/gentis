package relay

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	// upstreamKeepaliveTime/Timeout bound how long a black-holed upstream
	// TCP connection can wedge sendLoop in Send before it errors out and
	// the reconnect loop takes over.
	upstreamKeepaliveTime    = 30 * time.Second
	upstreamKeepaliveTimeout = 10 * time.Second
)

type MessageHandler func(channel string, data []byte, offset, epoch uint64)

type Upstream struct {
	config   UpstreamConfig
	policy   ReconnectPolicy
	handler  MessageHandler
	logger   *slog.Logger
	conn     *grpc.ClientConn
	client   gentisv1.GentisServiceClient
	stream   gentisv1.GentisService_StreamClient
	streamMu sync.Mutex

	subscriptions sync.Map
	subsMu        sync.Mutex

	// lastSeen tracks the newest {offset, epoch} observed per channel so
	// a reconnect can ask the upstream to replay exactly the gap instead
	// of silently losing it.
	lastSeen sync.Map

	sendCh chan *gentisv1.ClientMessage
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	connected    atomic.Bool
	reconnecting atomic.Bool
}

type subscriptionRef struct {
	count int
}

type recoverPoint struct {
	offset uint64
	epoch  uint64
}

func NewUpstream(config UpstreamConfig, policy ReconnectPolicy, handler MessageHandler, logger *slog.Logger) *Upstream {
	ctx, cancel := context.WithCancel(context.Background())
	return &Upstream{
		config:  config,
		policy:  policy,
		handler: handler,
		logger:  logger.With("upstream", config.Address),
		sendCh:  make(chan *gentisv1.ClientMessage, 256),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (u *Upstream) Start() error {
	if err := u.connect(); err != nil {
		return err
	}

	u.wg.Add(2)
	go u.receiveLoop()
	go u.sendLoop()

	return nil
}

func (u *Upstream) Stop() {
	u.cancel()
	if u.conn != nil {
		u.conn.Close()
	}
	u.wg.Wait()
}

func (u *Upstream) Subscribe(channel string) error {
	u.subsMu.Lock()
	defer u.subsMu.Unlock()

	if val, ok := u.subscriptions.Load(channel); ok {
		ref := val.(*subscriptionRef)
		ref.count++
		return nil
	}

	u.subscriptions.Store(channel, &subscriptionRef{count: 1})

	if u.connected.Load() {
		u.enqueue(&gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Subscribe{
				Subscribe: &gentisv1.SubscribeRequest{Channel: channel},
			},
		})
	}

	return nil
}

func (u *Upstream) Unsubscribe(channel string) error {
	u.subsMu.Lock()
	defer u.subsMu.Unlock()

	val, ok := u.subscriptions.Load(channel)
	if !ok {
		return nil
	}

	ref := val.(*subscriptionRef)
	ref.count--

	if ref.count <= 0 {
		u.subscriptions.Delete(channel)
		u.lastSeen.Delete(channel)
		if u.connected.Load() {
			u.enqueue(&gentisv1.ClientMessage{
				Message: &gentisv1.ClientMessage_Unsubscribe{
					Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: channel},
				},
			})
		}
	}

	return nil
}

func (u *Upstream) Publish(channel string, data []byte) error {
	if !u.connected.Load() {
		return fmt.Errorf("not connected to upstream")
	}

	select {
	case u.sendCh <- &gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{
				Channel: channel,
				Data:    data,
			},
		},
	}:
		return nil
	case <-u.ctx.Done():
		return u.ctx.Err()
	}
}

func (u *Upstream) IsConnected() bool {
	return u.connected.Load()
}

// enqueue queues a control message without ever wedging the caller: a full
// send buffer with a stuck upstream must not block dispatch loops holding
// subsMu.
func (u *Upstream) enqueue(msg *gentisv1.ClientMessage) {
	select {
	case u.sendCh <- msg:
	case <-u.ctx.Done():
	}
}

func (u *Upstream) transportCredentials() (credentials.TransportCredentials, error) {
	if !u.config.TLS {
		return insecure.NewCredentials(), nil
	}
	if u.config.CAFile == "" {
		return credentials.NewClientTLSFromCert(nil, ""), nil
	}
	return credentials.NewClientTLSFromFile(u.config.CAFile, "")
}

func (u *Upstream) connect() error {
	creds, err := u.transportCredentials()
	if err != nil {
		return fmt.Errorf("failed to load upstream TLS credentials: %w", err)
	}
	conn, err := grpc.NewClient(
		u.config.Address,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                upstreamKeepaliveTime,
			Timeout:             upstreamKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to upstream: %w", err)
	}

	u.conn = conn
	u.client = gentisv1.NewGentisServiceClient(conn)

	if err := u.establishStream(); err != nil {
		conn.Close()
		return err
	}

	return nil
}

func (u *Upstream) establishStream() error {
	u.streamMu.Lock()
	defer u.streamMu.Unlock()

	stream, err := u.client.Stream(u.ctx)
	if err != nil {
		return fmt.Errorf("failed to establish stream: %w", err)
	}

	u.stream = stream

	if u.config.AuthToken != "" {
		u.sendCh <- &gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Connect{
				Connect: &gentisv1.ConnectRequest{AuthToken: u.config.AuthToken},
			},
		}
	}

	// subsMu serializes the resubscribe sweep against Subscribe: a
	// subscription registered mid-sweep would see connected=false and
	// never reach the upstream until the next reconnect.
	u.subsMu.Lock()
	u.subscriptions.Range(func(key, _ any) bool {
		channel := key.(string)
		sub := &gentisv1.SubscribeRequest{Channel: channel}
		if val, ok := u.lastSeen.Load(channel); ok {
			rp := val.(recoverPoint)
			sub.Recover = &gentisv1.RecoverPoint{Offset: rp.offset, Epoch: rp.epoch}
		}
		u.enqueue(&gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Subscribe{
				Subscribe: sub,
			},
		})
		return true
	})
	u.connected.Store(true)
	u.subsMu.Unlock()

	return nil
}

func (u *Upstream) receiveLoop() {
	defer u.wg.Done()

	for {
		select {
		case <-u.ctx.Done():
			return
		default:
		}

		u.streamMu.Lock()
		stream := u.stream
		u.streamMu.Unlock()

		if stream == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		msg, err := stream.Recv()
		if err != nil {
			if u.ctx.Err() != nil {
				return
			}
			// EOF included: a server-side close is a disconnect to
			// recover from, not a reason to stop receiving forever.
			u.logger.Warn("upstream receive error, reconnecting", "err", err)
			u.handleDisconnect()
			continue
		}

		u.handleMessage(msg)
	}
}

func (u *Upstream) sendLoop() {
	defer u.wg.Done()

	for {
		select {
		case <-u.ctx.Done():
			return
		case msg := <-u.sendCh:
			u.streamMu.Lock()
			stream := u.stream
			u.streamMu.Unlock()

			if stream == nil {
				continue
			}

			if err := stream.Send(msg); err != nil {
				u.logger.Warn("upstream send error, reconnecting", "err", err)
				u.handleDisconnect()
			}
		}
	}
}

func (u *Upstream) handleMessage(msg *gentisv1.ServerMessage) {
	switch m := msg.Message.(type) {
	case *gentisv1.ServerMessage_ChannelMessage:
		cm := m.ChannelMessage
		// Track recover points only for channels with an exact upstream
		// subscription: channels observed through a pattern are replayless
		// and would otherwise accumulate entries forever.
		if cm.Offset != 0 {
			if _, subscribed := u.subscriptions.Load(cm.Channel); subscribed {
				u.lastSeen.Store(cm.Channel, recoverPoint{offset: cm.Offset, epoch: cm.Epoch})
			}
		}
		if u.handler != nil {
			u.handler(cm.Channel, cm.Data, cm.Offset, cm.Epoch)
		}
	case *gentisv1.ServerMessage_Subscribed:
		if m.Subscribed.Recovered {
			u.logger.Info("upstream gap recovered", "channel", m.Subscribed.Channel)
		}
	case *gentisv1.ServerMessage_Connected:
		u.logger.Info("connected to upstream", "connection_id", m.Connected.ConnectionId)
	case *gentisv1.ServerMessage_Error:
		u.logger.Warn("upstream returned error", "message", m.Error.Message, "code", m.Error.Code)
	}
}

func (u *Upstream) handleDisconnect() {
	u.connected.Store(false)

	u.streamMu.Lock()
	u.stream = nil
	u.streamMu.Unlock()

	if u.reconnecting.CompareAndSwap(false, true) {
		go u.reconnect()
	}
}

func (u *Upstream) reconnect() {
	defer u.reconnecting.Store(false)

	delay := u.policy.InitialDelay
	attempts := 0

	for {
		select {
		case <-u.ctx.Done():
			return
		case <-time.After(delay):
		}

		attempts++
		if u.policy.MaxRetries > 0 && attempts > u.policy.MaxRetries {
			u.logger.Error("upstream reconnect giving up, max attempts reached", "attempts", attempts-1)
			return
		}

		u.logger.Info("reconnecting to upstream", "attempt", attempts)

		if err := u.establishStream(); err != nil {
			u.logger.Warn("upstream reconnect failed", "attempt", attempts, "err", err)
			delay = min(time.Duration(float64(delay)*u.policy.Multiplier), u.policy.MaxDelay)
			continue
		}

		u.logger.Info("reconnected to upstream", "attempts", attempts)
		return
	}
}
