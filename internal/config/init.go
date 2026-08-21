package config

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

type anchoredRoot struct {
	path string
	root *os.Root
	dir  *os.File
}

func (r *anchoredRoot) close() {
	_ = r.root.Close()
	_ = r.dir.Close()
}

func WriteNew(path string, cfg Config) error {
	// Config is accepted by value, but slices retain shared backing arrays. Initialization
	// canonicalizes paths, so clone mutable slices before validation to keep concurrent calls
	// independent and race-free.
	cfg.Paths.AllowedRoots = append([]string(nil), cfg.Paths.AllowedRoots...)
	cfg.Scope.IssueAllowlist = append([]string(nil), cfg.Scope.IssueAllowlist...)
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(absolute)
	if err := mkdirConfigDir(dir); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	configRoot, err := openAnchoredRoot(dir)
	if err != nil {
		return fmt.Errorf("anchor config directory: %w", err)
	}
	defer configRoot.close()
	if err := cfg.Validate(dir); err != nil {
		return err
	}
	roots := make([]*anchoredRoot, 0, len(cfg.Paths.AllowedRoots))
	defer func() {
		for _, root := range roots {
			root.close()
		}
	}()
	for _, path := range cfg.Paths.AllowedRoots {
		root, err := openAnchoredRoot(path)
		if err != nil {
			return fmt.Errorf("anchor allowed root %q: %w", path, err)
		}
		roots = append(roots, root)
	}
	for _, root := range []string{cfg.Paths.StateRoot, cfg.Paths.ArtifactRoot, cfg.Paths.ReviewRoot} {
		if err := mkdirPrivateAnchored(root, roots); err != nil {
			return fmt.Errorf("create private root %q: %w", root, err)
		}
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp, tmpName, err := createTempAnchored(configRoot.root)
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	defer func() { _ = configRoot.root.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(b)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	// A same-filesystem hard link is an atomic no-replace install: link(2) returns EEXIST
	// if any final path entry already exists, including a dangling symlink.
	finalName := filepath.Base(absolute)
	if finalName == "." || finalName == string(filepath.Separator) {
		return errors.New("config path must name a file")
	}
	if err := unix.Linkat(int(configRoot.dir.Fd()), tmpName, int(configRoot.dir.Fd()), finalName, 0); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("config %q already exists", absolute)
		}
		return fmt.Errorf("install config without replacement: %w", err)
	}
	if err := configRoot.root.Remove(tmpName); err != nil {
		return fmt.Errorf("remove temporary config: %w", err)
	}
	if runtime.GOOS != "windows" {
		d, err := configRoot.root.Open(".")
		if err != nil {
			return err
		}
		defer d.Close()
		if err := d.Sync(); err != nil {
			return fmt.Errorf("sync config directory: %w", err)
		}
	}
	return nil
}

func mkdirConfigDir(path string) error {
	current := filepath.Clean(path)
	existed := true
	for {
		_, err := os.Lstat(current)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		existed = false
		parent := filepath.Dir(current)
		if parent == current {
			return errors.New("no existing directory ancestor")
		}
		current = parent
	}
	ancestor, err := openAnchoredRoot(current)
	if err != nil {
		return err
	}
	defer ancestor.close()
	rel, err := filepath.Rel(current, path)
	if err != nil {
		return err
	}
	if rel != "." {
		if err := mkdirAllAnchored(ancestor.root, rel); err != nil {
			return err
		}
	}
	info, err := ancestor.root.Lstat(rel)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("path is not a directory")
	}
	if err := secureInfo(info); err != nil {
		return err
	}
	if !existed && info.Mode().Perm() != 0o700 {
		return fmt.Errorf("created directory mode is %04o; require 0700", info.Mode().Perm())
	}
	return nil
}

func openAnchoredRoot(path string) (*anchoredRoot, error) {
	if err := ensureNoSymlinkComponents(path); err != nil {
		return nil, err
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return nil, errors.New("path is not a directory")
	}
	if err := secureInfo(before); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	dir := os.NewFile(uintptr(fd), path)
	if dir == nil {
		_ = syscall.Close(fd)
		return nil, errors.New("open anchored directory")
	}
	dirInfo, err := dir.Stat()
	if err != nil {
		dir.Close()
		return nil, err
	}
	if !os.SameFile(before, dirInfo) {
		dir.Close()
		return nil, errors.New("directory changed while it was being anchored")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		dir.Close()
		return nil, err
	}
	rootInfo, err := root.Lstat(".")
	if err != nil {
		root.Close()
		dir.Close()
		return nil, err
	}
	if !os.SameFile(before, rootInfo) {
		root.Close()
		dir.Close()
		return nil, errors.New("directory changed while it was being anchored")
	}
	if err := secureInfo(rootInfo); err != nil {
		root.Close()
		dir.Close()
		return nil, err
	}
	return &anchoredRoot{path: filepath.Clean(path), root: root, dir: dir}, nil
}

func mkdirPrivateAnchored(path string, roots []*anchoredRoot) error {
	var selected *anchoredRoot
	for _, root := range roots {
		if pathWithin(path, root.path) && (selected == nil || len(root.path) > len(selected.path)) {
			selected = root
		}
	}
	if selected == nil {
		return errors.New("path is outside anchored allowed roots")
	}
	rel, err := filepath.Rel(selected.path, path)
	if err != nil {
		return err
	}
	if rel != "." {
		if err := mkdirAllAnchored(selected.root, rel); err != nil {
			return err
		}
	}
	info, err := selected.root.Lstat(rel)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("path is not a directory")
	}
	if err := secureInfo(info); err != nil {
		return err
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf("existing directory mode is %04o; require 0700", info.Mode().Perm())
	}
	return nil
}

func mkdirAllAnchored(root *os.Root, rel string) error {
	clean := filepath.Clean(rel)
	if clean == "." || !filepath.IsLocal(clean) {
		return errors.New("directory path is not local to its anchored root")
	}
	prefix := ""
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		prefix = filepath.Join(prefix, component)
		if err := root.Mkdir(prefix, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err := root.Lstat(prefix)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("directory path contains a non-directory or symbolic link")
		}
		if err := secureInfo(info); err != nil {
			return err
		}
	}
	return nil
}

func createTempAnchored(root *os.Root) (*os.File, string, error) {
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", err
		}
		name := fmt.Sprintf(".factory-config-%x.tmp", random[:])
		file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", errors.New("could not allocate a unique temporary config name")
}

func DefaultPath() string {
	if value := os.Getenv("DARK_FACTORY_CONFIG"); value != "" {
		return value
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "factory.json"
	}
	return filepath.Join(dir, "dark-factory", "factory.json")
}
