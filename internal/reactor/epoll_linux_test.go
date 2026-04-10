//go:build linux

package reactor

import (
	"syscall"
	"testing"
)

// epollet is EPOLLET as a uint32 constant. syscall.EPOLLET is a signed
// constant that overflows uint32 in direct conversion, so we define it here
const epollet uint32 = 1 << 31

func socketPair(t *testing.T) (r, w int) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	return fds[0], fds[1]
}

func TestCreateAndClose(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if epfd < 0 {
		t.Fatalf("expected valid epfd, got %d", epfd)
	}
	if err := syscall.Close(epfd); err != nil {
		t.Fatalf("Close epfd: %v", err)
	}
}

func TestAddAndWaitReadable(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	rfd, wfd := socketPair(t)
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)

	if err := Add(epfd, rfd, syscall.EPOLLIN); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// make rfd readable.
	msg := []byte("foo")
	if _, err := syscall.Write(wfd, msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	events := make([]syscall.EpollEvent, 4)
	n, err := Wait(epfd, events, 100)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 ready event, got %d", n)
	}
	if int(events[0].Fd) != rfd {
		t.Fatalf("expected fd %d, got %d", rfd, events[0].Fd)
	}
	if events[0].Events&syscall.EPOLLIN == 0 {
		t.Fatalf("expected EPOLLIN, got events %d", events[0].Events)
	}
}

func TestWaitTimeout(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	rfd, wfd := socketPair(t)
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)

	if err := Add(epfd, rfd, syscall.EPOLLIN); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// No data => returns 0 after timeout.
	events := make([]syscall.EpollEvent, 4)
	n, err := Wait(epfd, events, 1)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 events on timeout, got %d", n)
	}
}

func TestMod(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	rfd, wfd := socketPair(t)
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)

	if err := Add(epfd, rfd, syscall.EPOLLIN); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// modify to also monitor EPOLLOUT
	if err := Mod(epfd, rfd, syscall.EPOLLIN|syscall.EPOLLOUT); err != nil {
		t.Fatalf("Mod: %v", err)
	}

	// socket is writable, so we get EPOLLOUT
	events := make([]syscall.EpollEvent, 4)
	n, err := Wait(epfd, events, 100)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n < 1 {
		t.Fatalf("expected at least 1 event, got %d", n)
	}
	if events[0].Events&syscall.EPOLLOUT == 0 {
		t.Fatalf("expected EPOLLOUT, got events %d", events[0].Events)
	}
}

func TestDel(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	rfd, wfd := socketPair(t)
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)

	if err := Add(epfd, rfd, syscall.EPOLLIN); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := Del(epfd, rfd); err != nil {
		t.Fatalf("Del: %v", err)
	}

	// write data => NOT trigger any event since the fd was removed.
	if _, err := syscall.Write(wfd, []byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	events := make([]syscall.EpollEvent, 4)
	n, err := Wait(epfd, events, 1)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 events after Del, got %d", n)
	}
}

func TestAddInvalidFd(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	if err := Add(epfd, -1, syscall.EPOLLIN); err == nil {
		t.Fatal("expected error adding invalid fd")
	}
}

func TestAddDuplicate(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	rfd, wfd := socketPair(t)
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)

	if err := Add(epfd, rfd, syscall.EPOLLIN); err != nil {
		t.Fatalf("Add: %v", err)
	}

	err = Add(epfd, rfd, syscall.EPOLLIN)
	if err == nil {
		t.Fatal("expected error on duplicate Add")
	}
	if err != syscall.EEXIST {
		t.Fatalf("expected EEXIST, got %v", err)
	}
}

func TestDelUnregistered(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	rfd, wfd := socketPair(t)
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)

	err = Del(epfd, rfd)
	if err == nil {
		t.Fatal("expected error deleting unregistered fd")
	}
	if err != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %v", err)
	}
}

func TestMultipleFds(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	r1, w1 := socketPair(t)
	defer syscall.Close(r1)
	defer syscall.Close(w1)

	r2, w2 := socketPair(t)
	defer syscall.Close(r2)
	defer syscall.Close(w2)

	if err := Add(epfd, r1, syscall.EPOLLIN); err != nil {
		t.Fatalf("Add r1: %v", err)
	}
	if err := Add(epfd, r2, syscall.EPOLLIN); err != nil {
		t.Fatalf("Add r2: %v", err)
	}

	if _, err := syscall.Write(w1, []byte("a")); err != nil {
		t.Fatalf("Write w1: %v", err)
	}
	if _, err := syscall.Write(w2, []byte("b")); err != nil {
		t.Fatalf("Write w2: %v", err)
	}

	events := make([]syscall.EpollEvent, 4)
	n, err := Wait(epfd, events, 100)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 ready events, got %d", n)
	}

	fds := map[int]bool{int(events[0].Fd): true, int(events[1].Fd): true}
	if !fds[r1] || !fds[r2] {
		t.Fatalf("expected fds %d and %d, got %v", r1, r2, fds)
	}
}

func TestEdgeTriggered(t *testing.T) {
	epfd, err := Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer syscall.Close(epfd)

	rfd, wfd := socketPair(t)
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)

	// edge-triggered mode.
	if err := Add(epfd, rfd, syscall.EPOLLIN|epollet); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if _, err := syscall.Write(wfd, []byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	events := make([]syscall.EpollEvent, 4)

	// report the event
	n, err := Wait(epfd, events, 100)
	if err != nil {
		t.Fatalf("Wait 1: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 event, got %d", n)
	}

	// without draining: edge-triggered will NOT re-report
	n, err = Wait(epfd, events, 1)
	if err != nil {
		t.Fatalf("Wait 2: %v", err)
	}
	if n != 0 {
		t.Fatalf("edge-triggered: expected 0 events on second wait without drain, got %d", n)
	}
}
