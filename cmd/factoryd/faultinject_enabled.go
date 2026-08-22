//go:build darkfactory_faultinject

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/app"
	"golang.org/x/sys/unix"
)

const (
	faultPointEnvironment  = "DARK_FACTORY_M5_FAULT_POINT"
	faultMarkerEnvironment = "DARK_FACTORY_M5_FAULT_MARKER"
)

func productionHooks() app.ProductionHooks {
	hit := processFaultInjector(os.Getenv(faultPointEnvironment), os.Getenv(faultMarkerEnvironment))
	return app.ProductionHooks{
		SQLiteBefore: func(phase string) error { return hit("before_" + phase) },
		SQLiteAfter:  func(phase string) error { return hit("after_" + phase) },
		Filesystem:   hit,
		Linear:       hit,
		OpenClaw:     hit,
	}
}

func processFaultInjector(wanted, marker string) func(string) error {
	return func(observed string) error {
		if wanted == "" || observed != wanted {
			return nil
		}
		if !strings.HasPrefix(wanted, "before_") && !strings.HasPrefix(wanted, "after_") {
			return fmt.Errorf("invalid M5 fault point %q", wanted)
		}
		if !filepath.IsAbs(marker) {
			return errors.New("M5 fault marker must be an absolute path")
		}
		file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("create M5 fault marker: %w", err)
		}
		record := map[string]any{"phase": observed, "pid": os.Getpid(), "observed_at": time.Now().UTC().Format(time.RFC3339Nano)}
		encodeErr := json.NewEncoder(file).Encode(record)
		if encodeErr == nil {
			encodeErr = file.Sync()
		}
		if closeErr := file.Close(); encodeErr == nil {
			encodeErr = closeErr
		}
		if encodeErr != nil {
			return fmt.Errorf("persist M5 fault marker: %w", encodeErr)
		}
		directory, err := os.Open(filepath.Dir(marker))
		if err != nil {
			return err
		}
		err = directory.Sync()
		_ = directory.Close()
		if err != nil {
			return err
		}
		if err := unix.Kill(os.Getpid(), unix.SIGSTOP); err != nil {
			return err
		}
		return errors.New("M5 fault-injected process resumed without replacement")
	}
}
