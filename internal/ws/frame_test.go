package ws

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws/wsutil"

	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
)

func legacyEnvelope(t *testing.T, d engine.Delivery) []byte {
	t.Helper()
	b, err := json.Marshal(&ServerMessage{
		ChannelMessage: &ChannelMessagePayload{
			Channel: d.Channel,
			Data:    json.RawMessage(d.Data),
			Offset:  d.Offset,
			Epoch:   d.Epoch,
		},
	})
	if err != nil {
		t.Fatalf("legacy marshal: %v", err)
	}
	return b
}

func TestMessageBytesMatchesLegacyEnvelope(t *testing.T) {
	d := engine.Delivery{Channel: "room", Data: []byte(`{"x":1}`), Offset: 9, Epoch: 7}
	msg := getWSMsg(d)

	got, err := messageBytes(msg)
	if err != nil {
		t.Fatalf("messageBytes: %v", err)
	}
	if want := legacyEnvelope(t, d); string(got) != string(want) {
		t.Fatalf("frame bytes = %q, want %q", got, want)
	}
}

func TestMessageBytesSharesEncodingAcrossSubscribers(t *testing.T) {
	d := engine.Delivery{Channel: "room", Data: []byte(`"hi"`), Offset: 1, Epoch: 7, Frame: &engine.EncodedFrame{}}
	want := legacyEnvelope(t, d)

	first := getWSMsg(d)
	second := getWSMsg(d)

	b1, err := messageBytes(first)
	if err != nil {
		t.Fatalf("messageBytes first: %v", err)
	}
	b2, err := messageBytes(second)
	if err != nil {
		t.Fatalf("messageBytes second: %v", err)
	}

	if string(b1) != string(want) || string(b2) != string(want) {
		t.Fatalf("encodings = (%q, %q), want %q", b1, b2, want)
	}
	if &b1[0] != &b2[0] {
		t.Fatal("subscribers got distinct encodings; shared frame not reused")
	}
}

func TestMessageBytesWithoutFrameDoesNotShare(t *testing.T) {
	d := engine.Delivery{Channel: "room", Data: []byte(`"hi"`), Offset: 1, Epoch: 7}

	b1, err := messageBytes(getWSMsg(d))
	if err != nil {
		t.Fatalf("messageBytes first: %v", err)
	}
	b2, err := messageBytes(getWSMsg(d))
	if err != nil {
		t.Fatalf("messageBytes second: %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("single-recipient encodings differ: %q vs %q", b1, b2)
	}
	if &b1[0] == &b2[0] {
		t.Fatal("frame-less messages aliased; each must marshal independently")
	}
}

func TestWriteFrameSingleSharedFrameOnWire(t *testing.T) {
	srv := &Server{config: &Config{WriteTimeout: time.Second}, logger: gentislog.Nop()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &Session{id: 1, ctx: ctx, cancel: cancel}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	d := engine.Delivery{Channel: "c", Data: []byte(`"raw"`), Offset: 1, Epoch: 7, Frame: &engine.EncodedFrame{}}
	want := legacyEnvelope(t, d)

	done := make(chan bool, 1)
	go func() { done <- srv.writeFrame(sess, server, []*ServerMessage{getWSMsg(d)}) }()

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := wsutil.ReadServerText(client)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !<-done {
		t.Fatal("writeFrame = false, want true")
	}
	if string(data) != string(want) {
		t.Fatalf("wire bytes = %q, want %q", data, want)
	}
}

func TestEncodeBatchReusesSharedFrameParts(t *testing.T) {
	srv := &Server{config: &Config{}, logger: gentislog.Nop()}

	deliveries := []engine.Delivery{
		{Channel: "c", Data: []byte(`"a"`), Offset: 1, Epoch: 7, Frame: &engine.EncodedFrame{}},
		{Channel: "c", Data: []byte(`"b"`), Offset: 2, Epoch: 7, Frame: &engine.EncodedFrame{}},
	}
	batch := make([]*ServerMessage, len(deliveries))
	legacy := make([]*ServerMessage, len(deliveries))
	for i, d := range deliveries {
		batch[i] = getWSMsg(d)
		legacy[i] = &ServerMessage{ChannelMessage: &ChannelMessagePayload{
			Channel: d.Channel, Data: json.RawMessage(d.Data), Offset: d.Offset, Epoch: d.Epoch,
		}}
	}

	got, err := srv.encodeBatch(&Session{id: 1}, batch)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	want, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("legacy batch marshal: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("batch frame = %q, want %q", got, want)
	}
}
