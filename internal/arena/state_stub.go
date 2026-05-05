//go:build !linux

package arena

type ArenaState struct{}

func NewArenaState(id int, a *Arena) (*ArenaState, error) {
	return nil, ErrUnsupported
}

func NewArenaStateAuto(a *Arena, baseID int) (*ArenaState, error) {
	return nil, ErrUnsupported
}

func (s *ArenaState) ID() int                           { return 0 }
func (s *ArenaState) IsAuthenticated() bool             { return false }
func (s *ArenaState) Authenticate(token string) error   { return ErrUnsupported }
func (s *ArenaState) AuthToken() string                 { return "" }
func (s *ArenaState) AddSubscription(channel string)    {}
func (s *ArenaState) RemoveSubscription(channel string) {}
func (s *ArenaState) IsSubscribedTo(channel string) bool { return false }
func (s *ArenaState) SubscriptionCount() int            { return 0 }
func (s *ArenaState) Close()                            {}
