package ws

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/protocol"
	"github.com/mateusfdl/gentis/internal/transport"
)

// Session implements protocol.Session so the shared core can drive it.
func (s *Session) State() transport.SessionState     { return s.state }
func (s *Session) Engine() protocol.Engine           { return s.engine }
func (s *Session) QoS() protocol.Consumer            { return s.qosc }
func (s *Session) SubscriberID() engine.SubscriberID { return engine.SubscriberID(s.id) }
func (s *Session) Verifier() auth.Verifier           { return s.server.config.Verifier }
func (s *Session) Logger() *slog.Logger              { return s.logger }
func (s *Session) MaxMessageSize() int               { return s.server.config.MaxMessageSize }
func (s *Session) MaxSubscriptions() int             { return s.server.config.MaxSubscriptions }
func (s *Session) DeliverFunc() engine.DeliveryFunc  { return s.deliverFn }
func (s *Session) SetProtocolVersion(v uint32)       { s.protoVersion.Store(v) }
func (s *Session) Hooks() *protocol.Hooks            { return nil }

func (s *Session) SendError(code protocol.ErrorCode, message, reqID string) {
	s.sendError(wsErrorCode(code), message, reqID)
}

func (s *Session) SendConnected(reqID string, version uint32) {
	s.send(&ServerMessage{
		ID: reqID,
		Connected: &ConnectedResponse{
			ConnectionID:    fmt.Sprintf("ws-conn-%d", s.id),
			ProtocolVersion: version,
		},
	})
}

func (s *Session) SendRefreshed(reqID string, expiresAt uint64) {
	s.send(&ServerMessage{
		ID:        reqID,
		Refreshed: &RefreshResponse{ExpiresAt: expiresAt},
	})
}

func (s *Session) SendSubscribed(reqID, channel string, recovered, didRecover bool) {
	resp := &SubscribedResponse{Channel: channel}
	if didRecover {
		r := recovered
		resp.Recovered = &r
	}
	s.send(&ServerMessage{ID: reqID, Subscribed: resp})
}

func (s *Session) SendUnsubscribed(reqID, channel string) {
	s.send(&ServerMessage{
		ID:           reqID,
		Unsubscribed: &UnsubscribedResponse{Channel: channel},
	})
}

func (s *Session) SendPublished(reqID, channel string, r engine.PublishResult) {
	s.send(&ServerMessage{
		ID: reqID,
		Published: &PublishResponse{
			Channel:   channel,
			Offset:    r.Offset,
			Epoch:     r.Epoch,
			Delivered: uint32(r.Delivered),
			Dropped:   uint32(r.Dropped),
		},
	})
}

func (s *Session) SendPong(reqID string) {
	s.send(&ServerMessage{ID: reqID, Pong: &PongResponse{}})
}

// ScheduleExpiry arms (or re-arms) the timer that force-closes the session
// when its credentials lapse. Only the dispatch goroutine calls this, so no
// locking is needed. A zero expiry disables enforcement.
func (s *Session) ScheduleExpiry(exp time.Time) {
	if s.expiryTimer != nil {
		s.expiryTimer.Stop()
		s.expiryTimer = nil
	}
	if exp.IsZero() {
		return
	}
	s.expiryTimer = time.AfterFunc(time.Until(exp), func() {
		s.server.logger.Debug("session credentials expired", "session_id", s.id)
		s.cancel()
	})
}

var _ protocol.Session = (*Session)(nil)

// liveConn stamps the session's lastRecv on every successful read. Any
// inbound bytes (data, pong replies, close frames) count as liveness, so
// quiet-but-healthy subscribers answering protocol pings are never reaped.
type liveConn struct {
	net.Conn
	sess *Session
}

func (c *liveConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.sess.lastRecv.Store(time.Now().UnixNano())
	}
	return n, err
}

// runReader blocks in Read with no deadline: cancellation surfaces through
// runWriter, which exits on ctx.Done and closes the connection, failing the
// read immediately. Deadline polling would cost two timer updates per read
// and a wakeup per idle second per connection, and a frame trickling across
// the deadline would fail mid-parse and desync the stream on the retry.
func (s *Server) runReader(sess *Session, rawConn net.Conn) {
	conn := &liveConn{Conn: rawConn, sess: sess}
	for {
		data, _, err := wsutil.ReadClientData(conn)
		if err != nil {
			if sess.ctx.Err() != nil {
				return
			}

			var closeErr wsutil.ClosedError
			if errors.As(err, &closeErr) {
				if closeErr.Code == ws.StatusNormalClosure || closeErr.Code == ws.StatusGoingAway {
					return
				}
			}

			s.logger.Debug("websocket read error", "session_id", sess.id, "err", err)
			return
		}

		DispatchMessage(sess, data, s.config.ReadLimit)
	}
}

// batchFor decides how a dequeued message is framed. Deliveries on a v2+
// session coalesce with other queued deliveries into one array frame; v1
// sessions and control responses ship one message per frame. The trailing
// message, when present, is a control response that stopped the drain and
// must keep its own frame in order.
func (sess *Session) batchFor(msg *ServerMessage) (batch []*ServerMessage, trailing *ServerMessage) {
	if sess.protoVersion.Load() >= 2 && msg.ChannelMessage != nil && msg.ID == "" {
		return drainBatch(sess, msg)
	}
	return []*ServerMessage{msg}, nil
}

// drainBatch opportunistically collects more deliveries already queued in
// the send channel so they ship as one array frame. A non-delivery message
// stops the drain and is returned separately so control responses keep
// their own frame, in order.
func drainBatch(sess *Session, first *ServerMessage) (batch []*ServerMessage, trailing *ServerMessage) {
	batch = []*ServerMessage{first}
	bytes := len(first.ChannelMessage.Data)
	for len(batch) < maxBatchSize && bytes < maxBatchBytes {
		select {
		case next := <-sess.sendCh:
			if next.ChannelMessage == nil || next.ID != "" {
				return batch, next
			}
			batch = append(batch, next)
			bytes += len(next.ChannelMessage.Data)
		default:
			return batch, nil
		}
	}
	return batch, nil
}

// messageBytes returns the wire encoding of one message. When the message
// carries a shared fan-out frame, the first writer to reach it encodes and
// stores the bytes and every other subscriber reuses them, so a broadcast
// payload is marshaled once rather than once per subscriber. The encoding of
// the per-session message is byte-identical across subscribers, so reusing
// the first writer's result is sound.
func messageBytes(m *ServerMessage) ([]byte, error) {
	if m.frame == nil {
		return json.Marshal(m)
	}
	if b, ok := m.frame.Load(); ok {
		return b, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	m.frame.Store(b)
	return b, nil
}

// writeFrame encodes one or more messages (an array frame when 2+) and
// writes them as a single websocket text frame into w. The caller owns the
// write deadline and the flush, so many frames can share one syscall.
func (s *Server) writeFrame(sess *Session, w io.Writer, batch []*ServerMessage) bool {
	data, err := s.encodeBatch(sess, batch)

	enqueuedAt := batch[0].enqueuedAt
	deliveries := 0
	for _, m := range batch {
		if m.ChannelMessage != nil {
			deliveries++
			putWSMsg(m)
		}
	}
	if err != nil {
		s.logger.Error("websocket marshal error", "session_id", sess.id, "err", err)
		return true
	}

	if err := wsutil.WriteServerMessage(w, ws.OpText, data); err != nil {
		sess.cancel()
		return false
	}

	if deliveries > 0 && s.config.OnDeliveryLatency != nil && !enqueuedAt.IsZero() {
		s.config.OnDeliveryLatency(time.Since(enqueuedAt))
	}
	return true
}

// encodeBatch produces the wire bytes for a frame. A single message ships
// as its own object; 2+ messages are concatenated into one array frame.
// Either way each message's bytes come from messageBytes, so shared
// fan-out frames are reused and an unmarshalable payload is dropped with a
// warning without poisoning its batch neighbors.
func (s *Server) encodeBatch(sess *Session, batch []*ServerMessage) ([]byte, error) {
	if len(batch) == 1 {
		return messageBytes(batch[0])
	}

	parts := make([][]byte, 0, len(batch))
	size := 1
	for _, m := range batch {
		p, err := messageBytes(m)
		if err != nil {
			s.logger.Warn("message dropped, unmarshalable payload",
				"session_id", sess.id, "channel", m.ChannelMessage.Channel, "offset", m.ChannelMessage.Offset)
			continue
		}
		parts = append(parts, p)
		size += len(p) + 1
	}
	if len(parts) == 0 {
		return nil, errors.New("ws: no marshalable messages in batch")
	}
	data := make([]byte, 0, size)
	data = append(data, '[')
	for i, p := range parts {
		if i > 0 {
			data = append(data, ',')
		}
		data = append(data, p...)
	}
	return append(data, ']'), nil
}

// drainWrites buffers the dequeued message and every other message already
// queued into one pass, then flushes a single time, so a spike of queued
// deliveries costs one write syscall per buffer instead of one per frame. The
// whole pass shares one write deadline, which also bounds the bufio
// auto-flushes a large drain triggers. v2 sessions coalesce consecutive
// deliveries into array frames; v1 ships one frame per message but shares the
// flush. The frame cap yields back to the run loop so a relentless producer
// cannot starve keepalive pings or cancellation.
func (s *Server) drainWrites(sess *Session, conn net.Conn, bw *bufio.Writer, first *ServerMessage) bool {
	conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	defer conn.SetWriteDeadline(time.Time{})

	msg := first
	frames := 0
	for {
		batch, trailing := sess.batchFor(msg)
		if !s.writeFrame(sess, bw, batch) {
			return false
		}
		frames++
		if trailing != nil {
			if !s.writeFrame(sess, bw, []*ServerMessage{trailing}) {
				return false
			}
			frames++
		}

		if frames >= maxDrainFrames {
			break
		}
		next, ok := tryRecv(sess.sendCh)
		if !ok {
			break
		}
		msg = next
	}

	if err := bw.Flush(); err != nil {
		sess.cancel()
		return false
	}
	return true
}

// tryRecv pulls the next queued message without blocking; ok is false when
// the send channel is momentarily empty.
func tryRecv(ch <-chan *ServerMessage) (msg *ServerMessage, ok bool) {
	select {
	case msg = <-ch:
		return msg, true
	default:
		return nil, false
	}
}

func (s *Server) runWriter(sess *Session, conn net.Conn) {
	defer conn.Close()
	bw := bufio.NewWriterSize(conn, writeBufferSize)
	var pingCh <-chan time.Time
	if s.config.PingInterval > 0 {
		ticker := time.NewTicker(s.config.PingInterval)
		defer ticker.Stop()
		pingCh = ticker.C
	}

	for {
		select {
		case <-pingCh:
			idle := time.Since(time.Unix(0, sess.lastRecv.Load()))
			if idle >= 3*s.config.PingInterval {
				s.logger.Warn("session reaped, keepalive timeout", "session_id", sess.id, "idle", idle)
				sess.cancel()
				continue
			}
			if idle < s.config.PingInterval {
				continue
			}
			conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			err := ws.WriteFrame(conn, ws.NewPingFrame(nil))
			conn.SetWriteDeadline(time.Time{})
			if err != nil {
				sess.cancel()
			}
		case <-sess.ctx.Done():
			closeBody := ws.NewCloseFrameBody(ws.StatusGoingAway, "server shutting down")
			frame := ws.NewCloseFrame(closeBody)
			ws.WriteFrame(conn, frame)
			return
		case msg := <-sess.sendCh:
			if !s.drainWrites(sess, conn, bw, msg) {
				return
			}
		}
	}
}
