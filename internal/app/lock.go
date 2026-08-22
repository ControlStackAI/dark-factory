package app

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/ControlStackAI/dark-factory/internal/ports"
	"golang.org/x/sys/unix"
)

type InstanceLock struct {
	file *os.File
}

func AcquireInstanceLock(path string) (*InstanceLock, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open factoryd instance lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		_ = file.Close()
		return nil, errors.New("factoryd instance lock is not a private regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: factoryd already owns this state database", ports.ErrBusy)
		}
		return nil, fmt.Errorf("lock factoryd instance: %w", err)
	}
	return &InstanceLock{file: file}, nil
}

func (l *InstanceLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	err := l.file.Close()
	l.file = nil
	return err
}
