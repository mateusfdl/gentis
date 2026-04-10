//go:build linux

package reactor

import "syscall"

func Create() (epfd int, err error) {
	return syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
}

func Add(epfd, fd int, events uint32) error {
	return syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
}

func Mod(epfd, fd int, events uint32) error {
	return syscall.EpollCtl(epfd, syscall.EPOLL_CTL_MOD, fd, &syscall.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
}

func Del(epfd, fd int) error {
	return syscall.EpollCtl(epfd, syscall.EPOLL_CTL_DEL, fd, nil)
}

func Wait(epfd int, events []syscall.EpollEvent, timeoutMs int) (n int, err error) {
	for {
		n, err = syscall.EpollWait(epfd, events, timeoutMs)
		if err == syscall.EINTR {
			continue
		}
		return n, err
	}
}
