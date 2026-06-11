package grpc

import (
	"fmt"
	"io"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/protocol"
	"github.com/mateusfdl/gentis/internal/protocol/pbcode"
)

const (
	// maxBatchSize caps how many deliveries one BatchMessage frame packs.
	maxBatchSize = 64

	// maxBatchBytes caps the payload bytes one BatchMessage accumulates:
	// grpc-go clients reject frames above 4MiB by default, so the batch
	// must stay well under it regardless of message count.
	maxBatchBytes = 1 << 20
)

func (s *Server) Stream(stream gentisv1.GentisService_StreamServer) error {
	// Session contexts are rooted in the server context, not the stream
	// context: client cancellation must surface through Recv so in-flight
	// messages drain in order, while ctx.Done stays reserved for
	// server-initiated closes (credential expiry, shutdown).
	sess := s.createSession(s.ctx)
	if sess == nil {
		return fmt.Errorf("failed to create session")
	}
	defer s.cleanupSession(sess)

	go sess.runSender(stream)

	// Recv runs in its own goroutine so the dispatch loop can also exit
	// on session cancellation (credential expiry). Returning from this
	// handler cancels the stream context, which unblocks Recv.
	recvCh := make(chan *gentisv1.ClientMessage)
	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case recvCh <- msg:
			case <-sess.ctx.Done():
				return
			}
		}
	}()

	for {
		// Drain pending traffic before honoring cancellation so a client
		// that publishes and immediately closes doesn't lose its last
		// writes to the select race.
		select {
		case msg := <-recvCh:
			sess.handleMessage(msg)
			continue
		default:
		}

		select {
		case <-sess.ctx.Done():
			for {
				select {
				case msg := <-recvCh:
					sess.handleMessage(msg)
				default:
					return nil
				}
			}
		case err := <-recvErr:
			if err == io.EOF {
				return nil
			}
			return err
		case msg := <-recvCh:
			sess.handleMessage(msg)
		}
	}
}

func (s *Session) runSender(stream gentisv1.GentisService_StreamServer) {
	// senderDone unblocks any send() waiting on ring drain: after the
	// outbound side dies nobody consumes the ring, so a blocked send in
	// the dispatch loop would wedge the handler forever. The session
	// itself stays alive to drain inbound traffic until Recv errors.
	defer close(s.senderDone)
	defer s.drainSendRing()
	var pending []*gentisv1.ServerMessage
	var pendingBytes int
	for {
		batching := s.protoVersion.Load() >= 2
		for {
			msg, ok := s.sendRing.TryConsume()
			if !ok {
				break
			}
			if batching && msg.GetChannelMessage() != nil && msg.Id == "" {
				size := len(msg.GetChannelMessage().Data)
				if len(pending) > 0 && pendingBytes+size > maxBatchBytes {
					if !s.flushPending(stream, &pending) {
						putServerMsgIfPooled(msg)
						return
					}
					pendingBytes = 0
				}
				pending = append(pending, msg)
				pendingBytes += size
				if len(pending) >= maxBatchSize || pendingBytes >= maxBatchBytes {
					if !s.flushPending(stream, &pending) {
						return
					}
					pendingBytes = 0
				}
				continue
			}
			if !s.flushPending(stream, &pending) {
				putServerMsgIfPooled(msg)
				return
			}
			pendingBytes = 0
			if err := stream.Send(msg); err != nil {
				putServerMsgIfPooled(msg)
				return
			}
			putServerMsgIfPooled(msg)
			s.signalDrain()
		}
		if !s.flushPending(stream, &pending) {
			return
		}
		pendingBytes = 0
		select {
		case <-s.ctx.Done():
			return
		case <-s.wakeCh:
		}
	}
}

// flushPending sends accumulated consecutive deliveries: as-is when there
// is one, packed into a single BatchMessage frame when there are more.
func (s *Session) flushPending(stream gentisv1.GentisService_StreamServer, pending *[]*gentisv1.ServerMessage) bool {
	msgs := *pending
	switch len(msgs) {
	case 0:
		return true
	case 1:
		err := stream.Send(msgs[0])
		putServerMsgIfPooled(msgs[0])
		*pending = msgs[:0]
		if err != nil {
			return false
		}
		s.signalDrain()
		return true
	}

	env := getBatchMsg()
	batch := env.GetBatch()
	for _, m := range msgs {
		batch.Messages = append(batch.Messages, m.GetChannelMessage())
	}
	err := stream.Send(env)
	for _, m := range msgs {
		putServerMsgIfPooled(m)
		s.signalDrain()
	}
	putBatchMsg(env)
	*pending = msgs[:0]
	return err == nil
}

func (s *Session) drainSendRing() {
	for {
		msg, ok := s.sendRing.TryConsume()
		if !ok {
			return
		}
		putServerMsgIfPooled(msg)
	}
}

func (s *Session) handleMessage(msg *gentisv1.ClientMessage) {
	reqID := msg.Id
	switch m := msg.Message.(type) {
	case *gentisv1.ClientMessage_Connect:
		protocol.Connect(s, protocol.ConnectRequest{
			AuthToken:       m.Connect.AuthToken,
			ProtocolVersion: m.Connect.ProtocolVersion,
		}, reqID)
	case *gentisv1.ClientMessage_Ping:
		protocol.Ping(s, reqID)
	case *gentisv1.ClientMessage_Refresh:
		protocol.Refresh(s, protocol.RefreshRequest{AuthToken: m.Refresh.AuthToken}, reqID)
	case *gentisv1.ClientMessage_Confirm:
		protocol.Confirm(s, m.Confirm.Channel, m.Confirm.Offset, reqID)
	case *gentisv1.ClientMessage_Subscribe:
		protocol.Subscribe(s, pbcode.ToSubscribe(m.Subscribe), reqID)
	case *gentisv1.ClientMessage_Unsubscribe:
		protocol.Unsubscribe(s, m.Unsubscribe.Channel, reqID)
	case *gentisv1.ClientMessage_Publish:
		protocol.Publish(s, protocol.PublishRequest{Channel: m.Publish.Channel, Data: m.Publish.Data}, reqID)
	default:
		protocol.Unknown(s, reqID)
	}
}
