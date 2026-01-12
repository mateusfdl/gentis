// Package server implements the Gentis pub/sub server.
package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/protocol"
	"github.com/mateusfdl/gentis/internal/pubsub"
)

// ErrorCode represents protocol error codes.
type ErrorCode uint8

const (
	ErrUnknownMessage   ErrorCode = 1
	ErrInvalidPayload   ErrorCode = 2
	ErrNotAuthenticated ErrorCode = 3
	ErrChannelNotFound  ErrorCode = 4
	ErrAlreadySubscribed ErrorCode = 5
	ErrNotSubscribed    ErrorCode = 6
	ErrInternalError    ErrorCode = 7
)

// Server represents the pub/sub server.
type Server struct {
	address     string
	listener    net.Listener
	pubsub      *pubsub.PubSub
	clients     sync.Map // map[int]*client.State
	connections sync.Map // map[int]net.Conn
	nextConnID  atomic.Int32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new server instance.
func New(address string) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		address: address,
		pubsub:  pubsub.New(),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start starts the server and begins accepting connections.
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}

	s.listener = listener
	log.Printf("Server listening on %s", s.address)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop gracefully stops the server.
func (s *Server) Stop() error {
	log.Println("Shutting down server...")
	s.cancel()

	if s.listener != nil {
		s.listener.Close()
	}

	s.wg.Wait()
	log.Println("Server stopped")
	return nil
}

// Wait blocks until the server stops.
func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		connID := int(s.nextConnID.Add(1))
		log.Printf("New connection from %s (id: %d)", conn.RemoteAddr(), connID)

		s.wg.Add(1)
		go s.handleConnection(conn, connID)
	}
}

func (s *Server) handleConnection(conn net.Conn, connID int) {
	defer s.wg.Done()
	defer conn.Close()

	clientState := client.NewState(connID)
	s.clients.Store(connID, clientState)
	s.connections.Store(connID, conn)
	defer func() {
		s.clients.Delete(connID)
		s.connections.Delete(connID)
		s.pubsub.RemoveConnection(pubsub.ConnectionID(connID))
		log.Printf("Connection %d closed", connID)
	}()

	reader := bufio.NewReader(conn)
	builder := protocol.NewBuilder()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Read message
		msg, err := protocol.ReadMessage(reader)
		if err != nil {
			if err.Error() != "EOF" {
				log.Printf("Read error from conn %d: %v", connID, err)
			}
			return
		}

		// Handle message
		if err := s.handleMessage(conn, connID, clientState, builder, msg); err != nil {
			log.Printf("Error handling message from conn %d: %v", connID, err)

			// Send error response for protocol errors
			if err == protocol.ErrInvalidPayload {
				s.sendError(conn, builder, ErrInvalidPayload, "Invalid payload format")
			}
		}
	}
}

func (s *Server) handleMessage(conn net.Conn, connID int, state *client.State, builder *protocol.Builder, msg *protocol.Message) error {
	log.Printf("Conn %d: Received %s", connID, msg.Type)

	switch msg.Type {
	case protocol.TypeConnect:
		return s.handleConnect(conn, state, builder, msg.Payload)
	case protocol.TypeSubscribe:
		return s.handleSubscribe(conn, connID, state, builder, msg.Payload)
	case protocol.TypeUnsubscribe:
		return s.handleUnsubscribe(conn, connID, state, builder, msg.Payload)
	case protocol.TypePublish:
		return s.handlePublish(conn, connID, state, builder, msg.Payload)
	case protocol.TypePing:
		return s.handlePing(conn, builder)
	default:
		log.Printf("Conn %d: Unhandled message type: %s", connID, msg.Type)
		return s.sendError(conn, builder, ErrUnknownMessage, "Unknown message type")
	}
}

func (s *Server) handleConnect(conn net.Conn, state *client.State, builder *protocol.Builder, payload []byte) error {
	token, err := protocol.ParseConnect(payload)
	if err != nil {
		return s.sendError(conn, builder, ErrInvalidPayload, "Invalid CONNECT format")
	}

	if err := state.Authenticate(token); err != nil {
		return s.sendError(conn, builder, ErrInternalError, "Authentication failed")
	}

	connID := fmt.Sprintf("conn-%d", state.ID())
	msg, err := builder.BuildConnected(connID)
	if err != nil {
		return err
	}

	_, err = conn.Write(msg)
	if err != nil {
		return err
	}

	log.Printf("Conn %d: Authenticated with token '%s'", state.ID(), token)
	return nil
}

func (s *Server) handleSubscribe(conn net.Conn, connID int, state *client.State, builder *protocol.Builder, payload []byte) error {
	channel, err := protocol.ParseSubscribe(payload)
	if err != nil {
		return s.sendError(conn, builder, ErrInvalidPayload, "Invalid SUBSCRIBE format")
	}

	newlySubscribed := s.pubsub.Subscribe(pubsub.ConnectionID(connID), channel)

	if newlySubscribed {
		state.AddSubscription(channel)

		msg, err := builder.BuildSubscribed(channel)
		if err != nil {
			return err
		}

		_, err = conn.Write(msg)
		if err != nil {
			return err
		}

		log.Printf("Conn %d: Subscribed to '%s'", connID, channel)
	} else {
		log.Printf("Conn %d: Already subscribed to '%s'", connID, channel)
	}

	return nil
}

func (s *Server) handleUnsubscribe(conn net.Conn, connID int, state *client.State, builder *protocol.Builder, payload []byte) error {
	channel, err := protocol.ParseUnsubscribe(payload)
	if err != nil {
		return s.sendError(conn, builder, ErrInvalidPayload, "Invalid UNSUBSCRIBE format")
	}

	wasSubscribed := s.pubsub.Unsubscribe(pubsub.ConnectionID(connID), channel)

	if wasSubscribed {
		state.RemoveSubscription(channel)

		msg, err := builder.BuildUnsubscribed(channel)
		if err != nil {
			return err
		}

		_, err = conn.Write(msg)
		if err != nil {
			return err
		}

		log.Printf("Conn %d: Unsubscribed from '%s'", connID, channel)
	} else {
		return s.sendError(conn, builder, ErrNotSubscribed, "Not subscribed to channel")
	}

	return nil
}

func (s *Server) handlePublish(conn net.Conn, connID int, state *client.State, builder *protocol.Builder, payload []byte) error {
	pub, err := protocol.ParsePublish(payload)
	if err != nil {
		return s.sendError(conn, builder, ErrInvalidPayload, "Invalid PUBLISH format")
	}

	// Get subscribers (excluding sender)
	subscribers := s.pubsub.GetSubscribers(pub.Channel, pubsub.ConnectionID(connID))

	// Build message once
	msg, err := builder.BuildMessage(pub.Channel, pub.Data)
	if err != nil {
		return err
	}

	// Broadcast to subscribers
	sent := s.broadcastToSubscribers(subscribers, msg)

	log.Printf("Conn %d: Published to '%s' (%d recipients)", connID, pub.Channel, sent)
	return nil
}

func (s *Server) broadcastToSubscribers(subscribers []pubsub.ConnectionID, msg []byte) int {
	sent := 0
	for _, subID := range subscribers {
		if connValue, ok := s.connections.Load(int(subID)); ok {
			if subConn, ok := connValue.(net.Conn); ok {
				if _, err := subConn.Write(msg); err != nil {
					log.Printf("Failed to send to conn %d: %v", subID, err)
				} else {
					sent++
				}
			}
		}
	}
	return sent
}

func (s *Server) handlePing(conn net.Conn, builder *protocol.Builder) error {
	msg, err := builder.BuildPong()
	if err != nil {
		return err
	}

	_, err = conn.Write(msg)
	return err
}

func (s *Server) sendError(conn net.Conn, builder *protocol.Builder, code ErrorCode, message string) error {
	msg, err := builder.BuildError(uint8(code), message)
	if err != nil {
		return err
	}

	_, err = conn.Write(msg)
	return err
}
