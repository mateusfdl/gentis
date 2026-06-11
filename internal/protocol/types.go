package protocol

// The request types are plain value structs so converting from proto or
// JSON wire types is a stack copy: optional wire sub-messages become
// inline values plus Has booleans, never pointers.

type ConnectRequest struct {
	AuthToken       string
	ProtocolVersion uint32
}

type RefreshRequest struct {
	AuthToken string
}

type RecoverPoint struct {
	Offset uint64
	Epoch  uint64
}

type Window struct {
	Count uint32
	Bytes uint64
}

type SubscribeRequest struct {
	Channel    string
	Priority   int32
	Window     Window
	HasWindow  bool
	Recover    RecoverPoint
	HasRecover bool
}

type PublishRequest struct {
	Channel string
	Data    []byte
}
