//go:build !linux

package reactor

import (
	"errors"
	"syscall"
)

var ErrUnsupported = errors.New("not epoll available huh?")

func Create() (int, error)                  { return -1, ErrUnsupported }
func Add(epfd, fd int, events uint32) error { return ErrUnsupported }
func Mod(epfd, fd int, events uint32) error { return ErrUnsupported }
func Del(epfd, fd int) error                { return ErrUnsupported }
func Wait(epfd int, events []syscall.EpollEvent, timeoutMs int) (int, error) {
	return 0, ErrUnsupported
}
