package relay

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MessageHandler func(channel string, data []byte)

type Upstream struct {
	config   UpstreamConfig
	policy   ReconnectPolicy
	handler  MessageHandler
	conn     *grpc.ClientConn
	client   gentisv1.GentisServiceClient
	stream   gentisv1.GentisService_StreamClient
	streamMu sync.Mutex

	subscriptions sync.Map
	subsMu        sync.Mutex

	sendCh chan *gentisv1.ClientMessage
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	connected atomic.Bool
}

type subscriptionRef struct {
	count int
}

func NewUpstream(config UpstreamConfig, policy ReconnectPolicy, handler MessageHandler) *Upstream {
	ctx, cancel := context.WithCancel(context.Background())
	return &Upstream{
		config:  config,
		policy:  policy,
		handler: handler,
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
		u.sendCh <- &gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Subscribe{
				Subscribe: &gentisv1.SubscribeRequest{Channel: channel},
			},
		}
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
		if u.connected.Load() {
			u.sendCh <- &gentisv1.ClientMessage{
				Message: &gentisv1.ClientMessage_Unsubscribe{
					Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: channel},
				},
			}
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

func (u *Upstream) connect() error {
	conn, err := grpc.NewClient(
		u.config.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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

	u.subscriptions.Range(func(key, _ any) bool {
		channel := key.(string)
		u.sendCh <- &gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Subscribe{
				Subscribe: &gentisv1.SubscribeRequest{Channel: channel},
			},
		}
		return true
	})

	u.connected.Store(true)
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
			if err == io.EOF || u.ctx.Err() != nil {
				return
			}
			log.Printf("upstream receive error: %v", err)
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
				log.Printf("upstream send error: %v", err)
				u.handleDisconnect()
			}
		}
	}
}

func (u *Upstream) handleMessage(msg *gentisv1.ServerMessage) {
	switch m := msg.Message.(type) {
	case *gentisv1.ServerMessage_ChannelMessage:
		if u.handler != nil {
			u.handler(m.ChannelMessage.Channel, m.ChannelMessage.Data)
		}
	case *gentisv1.ServerMessage_Connected:
		log.Printf("connected to upstream: %s", m.Connected.ConnectionId)
	case *gentisv1.ServerMessage_Error:
		log.Printf("upstream error: %s (%v)", m.Error.Message, m.Error.Code)
	}
}

func (u *Upstream) handleDisconnect() {
	u.connected.Store(false)

	u.streamMu.Lock()
	u.stream = nil
	u.streamMu.Unlock()

	go u.reconnect()
}

func (u *Upstream) reconnect() {
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
			log.Printf("max reconnect attempts reached")
			return
		}

		log.Printf("reconnecting to upstream (attempt %d)", attempts)

		if err := u.establishStream(); err != nil {
			log.Printf("reconnect failed: %v", err)
			delay = min(time.Duration(float64(delay)*u.policy.Multiplier), u.policy.MaxDelay)
			continue
		}

		log.Printf("reconnected to upstream")
		return
	}
}
