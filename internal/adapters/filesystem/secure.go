//go:build linux

package filesystem

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type secureDirectory struct {
	path string
	file *os.File
	uid  int
}

func openSecureDirectory(path string, expectedUID int) (*secureDirectory, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	fd, err := openDirectoryNoSymlinks(abs)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), abs)
	directory := &secureDirectory{path: abs, file: file, uid: expectedUID}
	if err := directory.validateSelf(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return directory, nil
}

func openDirectoryNoSymlinks(abs string) (int, error) {
	if !filepath.IsAbs(abs) {
		return -1, errors.New("directory path must be absolute")
	}
	parts := strings.Split(strings.TrimPrefix(abs, string(filepath.Separator)), string(filepath.Separator))
	if len(parts) == 1 && parts[0] == "" {
		return unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	}
	current, err := unix.Open(string(filepath.Separator), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for index, part := range parts {
		if part == "" || part == "." || part == ".." {
			_ = unix.Close(current)
			return -1, errors.New("directory path contains an unsafe component")
		}
		flags := unix.O_PATH | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if index == len(parts)-1 {
			flags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		}
		next, openErr := unix.Openat(current, part, flags, 0)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, openErr
		}
		current = next
	}
	return current, nil
}

func openSecureDirectoryAt(parent *secureDirectory, name string, expectedUID int) (*secureDirectory, error) {
	if !safeDirectoryEntry(name) {
		return nil, fmt.Errorf("%w: unsafe directory entry %q", ErrMalformedPacket, name)
	}
	fd, err := unix.Openat(parent.FD(), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	directory := &secureDirectory{path: filepath.Join(parent.path, name), file: file, uid: expectedUID}
	if err := directory.validateSelf(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return directory, nil
}

func (d *secureDirectory) Close() error { return d.file.Close() }
func (d *secureDirectory) FD() int      { return int(d.file.Fd()) }

func (d *secureDirectory) validateSelf() error {
	stat, err := d.stat()
	if err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || int(stat.Uid) != d.uid || stat.Mode&0o077 != 0 || stat.Mode&(unix.S_ISUID|unix.S_ISGID|unix.S_ISVTX) != 0 {
		return fmt.Errorf("%w: directory is not private and owned by uid %d", ErrMalformedPacket, d.uid)
	}
	return nil
}

func (d *secureDirectory) stat() (unix.Stat_t, error) {
	var stat unix.Stat_t
	err := unix.Fstat(d.FD(), &stat)
	return stat, err
}

func (d *secureDirectory) names(max int) ([]string, error) {
	if _, err := d.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	names, err := d.file.Readdirnames(max + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(names) > max {
		return nil, fmt.Errorf("%w: member count exceeds %d", ErrMalformedPacket, max)
	}
	sort.Strings(names)
	return names, nil
}

func (d *secureDirectory) readRegular(name string, maxBytes int64) ([]byte, unix.Stat_t, error) {
	if !safeMember(name) {
		return nil, unix.Stat_t{}, fmt.Errorf("%w: unsafe member name %q", ErrMalformedPacket, name)
	}
	fd, err := unix.Openat(d.FD(), name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, unix.Stat_t{}, os.ErrNotExist
		}
		return nil, unix.Stat_t{}, fmt.Errorf("%w: open member %q: %v", ErrMalformedPacket, name, err)
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	var before, after unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return nil, unix.Stat_t{}, err
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG || int(before.Uid) != d.uid || before.Mode&0o077 != 0 || before.Mode&(unix.S_ISUID|unix.S_ISGID|unix.S_ISVTX) != 0 || before.Nlink != 1 {
		return nil, unix.Stat_t{}, fmt.Errorf("%w: member %q is not a private, singly linked regular file owned by uid %d", ErrMalformedPacket, name, d.uid)
	}
	if before.Size < 0 || before.Size > maxBytes {
		return nil, unix.Stat_t{}, fmt.Errorf("%w: member %q exceeds size limit", ErrMalformedPacket, name)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, unix.Stat_t{}, fmt.Errorf("%w: read member %q: %v", ErrMalformedPacket, name, err)
	}
	if int64(len(data)) > maxBytes {
		return nil, unix.Stat_t{}, fmt.Errorf("%w: member %q exceeds size limit", ErrMalformedPacket, name)
	}
	if err := unix.Fstat(fd, &after); err != nil {
		return nil, unix.Stat_t{}, err
	}
	if !sameStat(before, after) || int64(len(data)) != after.Size {
		return nil, unix.Stat_t{}, fmt.Errorf("%w: member %q changed while it was read", ErrMalformedPacket, name)
	}
	return data, after, nil
}

func sameStat(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid && a.Nlink == b.Nlink && a.Size == b.Size &&
		a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func ensurePrivateRoot(path string, uid int, create bool) error {
	if create {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	directory, err := openSecureDirectory(path, uid)
	if err != nil {
		return err
	}
	return directory.Close()
}

func writeTreeAtomic(parent, finalName string, files map[string][]byte, uid int, beforeCommit func() error) error {
	if !safeMember(finalName) || !strings.HasPrefix(finalName, "packet-") {
		return errors.New("unsafe final snapshot name")
	}
	root, err := openSecureDirectory(parent, uid)
	if err != nil {
		return err
	}
	defer root.Close()
	tmpName, err := createTemporaryDirectory(root)
	if err != nil {
		return err
	}
	tmp, err := openSecureDirectoryAt(root, tmpName, uid)
	if err != nil {
		_ = unix.Unlinkat(root.FD(), tmpName, unix.AT_REMOVEDIR)
		return err
	}
	if err := unix.Fchmod(tmp.FD(), 0o700); err != nil {
		_ = tmp.Close()
		_ = unix.Unlinkat(root.FD(), tmpName, unix.AT_REMOVEDIR)
		return err
	}
	installed := false
	created := make([]string, 0, len(files))
	defer func() {
		if !installed {
			for _, name := range created {
				_ = unix.Unlinkat(tmp.FD(), name, 0)
			}
		}
		_ = tmp.Close()
		if !installed {
			_ = unix.Unlinkat(root.FD(), tmpName, unix.AT_REMOVEDIR)
		}
	}()
	names := make([]string, 0, len(files))
	for name := range files {
		if !safeMember(name) {
			return errors.New("unsafe snapshot member")
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fd, openErr := unix.Openat(tmp.FD(), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o400)
		if openErr != nil {
			return openErr
		}
		file := os.NewFile(uintptr(fd), name)
		created = append(created, name)
		if err := file.Chmod(0o400); err != nil {
			file.Close()
			return err
		}
		if _, err := file.Write(files[name]); err != nil {
			file.Close()
			return err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	actualNames, err := tmp.names(len(names))
	if err != nil || !equalStrings(actualNames, names) {
		if err == nil {
			err = fmt.Errorf("%w: snapshot staging membership changed", ErrMalformedPacket)
		}
		return err
	}
	if err := tmp.validateSelf(); err != nil {
		return err
	}
	if err := unix.Fsync(tmp.FD()); err != nil {
		return err
	}
	if err := root.validateSelf(); err != nil {
		return err
	}
	if beforeCommit != nil {
		if err := beforeCommit(); err != nil {
			return err
		}
	}
	if err := unix.Renameat2(root.FD(), tmpName, root.FD(), finalName, unix.RENAME_NOREPLACE); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return os.ErrExist
		}
		return err
	}
	installed = true
	if err := unix.Fsync(root.FD()); err != nil {
		return err
	}
	return nil
}

func writeAtomicFile(parent, finalName string, contents []byte, uid int) error {
	if !safeMember(finalName) {
		return errors.New("unsafe receipt name")
	}
	root, err := openSecureDirectory(parent, uid)
	if err != nil {
		return err
	}
	defer root.Close()
	tmpName, file, err := createTemporaryFile(root)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		_ = file.Close()
		if !installed {
			_ = unix.Unlinkat(root.FD(), tmpName, 0)
		}
	}()
	if err := file.Chmod(0o400); err != nil {
		return err
	}
	if _, err := file.Write(contents); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := root.validateSelf(); err != nil {
		return err
	}
	if err := unix.Renameat2(root.FD(), tmpName, root.FD(), finalName, unix.RENAME_NOREPLACE); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return os.ErrExist
		}
		return err
	}
	installed = true
	return unix.Fsync(root.FD())
}

func removeAtomicFile(parent, name string, uid int) error {
	if !safeMember(name) {
		return errors.New("unsafe file name")
	}
	root, err := openSecureDirectory(parent, uid)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := unix.Unlinkat(root.FD(), name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	return unix.Fsync(root.FD())
}

func createTemporaryDirectory(parent *secureDirectory) (string, error) {
	for attempts := 0; attempts < 128; attempts++ {
		name, err := temporaryName()
		if err != nil {
			return "", err
		}
		if err := unix.Mkdirat(parent.FD(), name, 0o700); err == nil {
			return name, nil
		} else if !errors.Is(err, unix.EEXIST) {
			return "", err
		}
	}
	return "", errors.New("could not allocate temporary directory")
}

func createTemporaryFile(parent *secureDirectory) (string, *os.File, error) {
	for attempts := 0; attempts < 128; attempts++ {
		name, err := temporaryName()
		if err != nil {
			return "", nil, err
		}
		fd, err := unix.Openat(parent.FD(), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o400)
		if err == nil {
			return name, os.NewFile(uintptr(fd), name), nil
		}
		if !errors.Is(err, unix.EEXIST) {
			return "", nil, err
		}
	}
	return "", nil, errors.New("could not allocate temporary file")
}

func temporaryName() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return ".pending-" + hex.EncodeToString(nonce[:]), nil
}

func safeDirectoryEntry(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && !strings.ContainsAny(name, "/\\\x00")
}

func canonicalConsumption(receipt ConsumptionReceipt) ([]byte, error) {
	if receipt.ReceiptVersion != ReceiptVersion || strings.TrimSpace(receipt.ReviewID) == "" || strings.TrimSpace(receipt.ReviewID) != receipt.ReviewID || strings.ContainsRune(receipt.ReviewID, 0) || !validDigest(receipt.PacketDigest) || !validDigest(receipt.SourceDigest) || strings.TrimSpace(receipt.RunID) == "" || strings.TrimSpace(receipt.RunID) != receipt.RunID || strings.ContainsRune(receipt.RunID, 0) || receipt.Fence == 0 || (receipt.Outcome != "pending_consumption" && receipt.Outcome != "approved_and_consumed") {
		return nil, errors.New("invalid consumption receipt")
	}
	parsed, err := time.Parse(time.RFC3339Nano, receipt.ConsumedAt)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(time.RFC3339Nano) != receipt.ConsumedAt {
		return nil, errors.New("invalid consumption time")
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}
