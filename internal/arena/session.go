package arena

const (
	MaxSubjectLen    = 128
	MaxSubscriptions = 16
	MaxChanNameLen   = 256
)

// SessionSlot is a fixed-layout, pointer-free representation of per-connection
// session state.
type SessionSlot struct {
	ID            uint64
	Authenticated uint32
	_             uint32 // alignment padding
	ExpiresAt     int64
	Subject       [MaxSubjectLen]byte
	SubjectLen    uint32
	SubCount      uint32
	Subscriptions [MaxSubscriptions][MaxChanNameLen]byte
	SubLens       [MaxSubscriptions]uint32
}

func (s *SessionSlot) SetSubject(subject string) {
	n := copy(s.Subject[:], subject)
	s.SubjectLen = uint32(n)
}

// GetSubject returns the stored subject as a string.
//
// Note: this allocates a heap string. Use only on the auth path, not the
// hot message delivery path.
func (s *SessionSlot) GetSubject() string {
	return string(s.Subject[:s.SubjectLen])
}

func (s *SessionSlot) AddSubscription(channel string) bool {
	// Check for duplicate.
	for i := range s.SubCount {
		if s.matchSub(i, channel) {
			return false
		}
	}

	if s.SubCount >= MaxSubscriptions {
		return false
	}

	idx := s.SubCount
	n := copy(s.Subscriptions[idx][:], channel)
	s.SubLens[idx] = uint32(n)
	s.SubCount++
	return true
}

func (s *SessionSlot) RemoveSubscription(channel string) bool {
	for i := range s.SubCount {
		if s.matchSub(i, channel) {
			last := s.SubCount - 1
			if i != last {
				// swap with last element.
				s.Subscriptions[i] = s.Subscriptions[last]
				s.SubLens[i] = s.SubLens[last]
			}
			// clear last slot.
			s.Subscriptions[last] = [MaxChanNameLen]byte{}
			s.SubLens[last] = 0
			s.SubCount--
			return true
		}
	}
	return false
}

func (s *SessionSlot) IsSubscribed(channel string) bool {
	for i := range s.SubCount {
		if s.matchSub(i, channel) {
			return true
		}
	}
	return false
}

func (s *SessionSlot) Clear() {
	*s = SessionSlot{}
}

func (s *SessionSlot) matchSub(idx uint32, channel string) bool {
	n := s.SubLens[idx]
	if uint32(len(channel)) != n {
		return false
	}
	return string(s.Subscriptions[idx][:n]) == channel
}
