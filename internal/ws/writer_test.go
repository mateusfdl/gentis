package ws

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws/wsutil"

	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
)

type countingConn struct {
	net.Conn
	writes int
	buf    bytes.Buffer
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes++
	return c.buf.Write(p)
}

func (c *countingConn) SetWriteDeadline(time.Time) error { return nil }

type frameReader struct{ *bytes.Reader }

func (frameReader) Write(p []byte) (int, error) { return len(p), nil }

func readServerFrames(t *testing.T, raw []byte) [][]byte {
	t.Helper()
	r := frameReader{bytes.NewReader(raw)}
	var frames [][]byte
	for r.Len() > 0 {
		data, err := wsutil.ReadServerText(r)
		if err != nil {
			t.Fatalf("read frame %d: %v", len(frames), err)
		}
		frames = append(frames, data)
	}
	return frames
}

func TestDrainWritesBatchesV1IntoSingleSyscall(t *testing.T) {
	srv := &Server{config: &Config{WriteTimeout: time.Second}, logger: gentislog.Nop()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &Session{id: 1, ctx: ctx, cancel: cancel, sendCh: make(chan *ServerMessage, maxBatchSize*2)}
	sess.protoVersion.Store(1)

	const total = 32
	for i := 1; i <= total; i++ {
		sess.sendCh <- getWSMsg(engine.Delivery{Channel: "c", Data: fmt.Appendf(nil, `"m-%d"`, i), Offset: uint64(i), Epoch: 7})
	}

	conn := &countingConn{}
	bw := bufio.NewWriterSize(conn, writeBufferSize)
	first := <-sess.sendCh
	if !srv.drainWrites(sess, conn, bw, first) {
		t.Fatal("drainWrites = false, want true")
	}

	if conn.writes != 1 {
		t.Fatalf("drain issued %d write syscalls for %d v1 messages, want 1", conn.writes, total)
	}

	frames := readServerFrames(t, conn.buf.Bytes())
	if len(frames) != total {
		t.Fatalf("drain wrote %d frames, want %d", len(frames), total)
	}
	for i, f := range frames {
		var m ServerMessage
		if err := json.Unmarshal(f, &m); err != nil {
			t.Fatalf("frame %d is not a JSON object: %v (%q)", i, err, f)
		}
		if m.ChannelMessage == nil {
			t.Fatalf("frame %d is not a delivery: %+v", i, m)
		}
		if m.ChannelMessage.Offset != uint64(i+1) {
			t.Fatalf("frame %d offset = %d, want %d", i, m.ChannelMessage.Offset, i+1)
		}
	}
}

func TestDrainWritesFlushesV2ArrayFrameOnce(t *testing.T) {
	srv := &Server{config: &Config{WriteTimeout: time.Second}, logger: gentislog.Nop()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &Session{id: 1, ctx: ctx, cancel: cancel, sendCh: make(chan *ServerMessage, maxBatchSize*2)}
	sess.protoVersion.Store(2)

	const total = 8
	for i := 1; i <= total; i++ {
		sess.sendCh <- getWSMsg(engine.Delivery{Channel: "c", Data: fmt.Appendf(nil, `"m-%d"`, i), Offset: uint64(i), Epoch: 7})
	}

	conn := &countingConn{}
	bw := bufio.NewWriterSize(conn, writeBufferSize)
	first := <-sess.sendCh
	if !srv.drainWrites(sess, conn, bw, first) {
		t.Fatal("drainWrites = false, want true")
	}

	if conn.writes != 1 {
		t.Fatalf("v2 drain issued %d write syscalls, want 1", conn.writes)
	}

	frames := readServerFrames(t, conn.buf.Bytes())
	if len(frames) != 1 {
		t.Fatalf("v2 drain wrote %d frames, want 1 coalesced array", len(frames))
	}
	if frames[0][0] != '[' {
		t.Fatalf("v2 frame is not an array: %q", frames[0])
	}
	var got []ServerMessage
	if err := json.Unmarshal(frames[0], &got); err != nil {
		t.Fatalf("unmarshal array frame: %v", err)
	}
	if len(got) != total {
		t.Fatalf("array frame carries %d messages, want %d", len(got), total)
	}
	for i, m := range got {
		if m.ChannelMessage == nil || m.ChannelMessage.Offset != uint64(i+1) {
			t.Fatalf("array entry %d out of order or non-delivery: %+v", i, m)
		}
	}
}

func TestDrainWritesCapBoundsFramesPerPass(t *testing.T) {
	srv := &Server{config: &Config{WriteTimeout: time.Second}, logger: gentislog.Nop()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const total = maxDrainFrames + 16
	sess := &Session{id: 1, ctx: ctx, cancel: cancel, sendCh: make(chan *ServerMessage, total+1)}
	sess.protoVersion.Store(1)

	for i := 1; i <= total; i++ {
		sess.sendCh <- getWSMsg(engine.Delivery{Channel: "c", Data: []byte(`"x"`), Offset: uint64(i), Epoch: 7})
	}

	conn := &countingConn{}
	bw := bufio.NewWriterSize(conn, writeBufferSize)
	first := <-sess.sendCh
	if !srv.drainWrites(sess, conn, bw, first) {
		t.Fatal("drainWrites = false, want true")
	}

	frames := readServerFrames(t, conn.buf.Bytes())
	if len(frames) != maxDrainFrames {
		t.Fatalf("one drain pass wrote %d frames, want cap %d", len(frames), maxDrainFrames)
	}
	if left := len(sess.sendCh); left != total-maxDrainFrames {
		t.Fatalf("after capped pass %d messages left queued, want %d", left, total-maxDrainFrames)
	}
}
