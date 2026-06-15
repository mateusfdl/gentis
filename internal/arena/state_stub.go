//go:build !linux

package arena

import (
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/transport"
)

type ArenaState struct{}

func NewArenaState(id int, a *Arena) (*ArenaState, error) {
	return nil, ErrUnsupported
}

func NewArenaStateAuto(a *Arena, baseID int) (*ArenaState, error) {
	return nil, ErrUnsupported
}

func (s *ArenaState) ID() int                            { return 0 }
func (s *ArenaState) IsAuthenticated() bool              { return false }
func (s *ArenaState) Authenticate(c auth.Claims) error   { return ErrUnsupported }
func (s *ArenaState) Subject() string                    { return "" }
func (s *ArenaState) ExpiresAt() time.Time               { return time.Time{} }
func (s *ArenaState) CanSubscribe(channel string) bool   { return false }
func (s *ArenaState) CanPublish(channel string) bool     { return false }
func (s *ArenaState) AddSubscription(channel string) transport.AddSubscriptionResult {
	return transport.SubscriptionAdded
}
func (s *ArenaState) RemoveSubscription(channel string)  {}
func (s *ArenaState) IsSubscribedTo(channel string) bool { return false }
func (s *ArenaState) SubscriptionCount() int             { return 0 }
func (s *ArenaState) Close()                             {}
