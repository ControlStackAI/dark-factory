package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ControlStackAI/dark-factory/internal/config"
)

func TestTwoKeyInterlock(t *testing.T) {
	base := validOperatorConfig(t)
	cases := []struct {
		name, mode string
		apply      bool
		want       string
		success    bool
	}{
		{"dry-no-apply", "dry-run", false, "", true},
		{"dry-apply", "dry-run", true, "--apply requires config mode live", false},
		{"live-no-apply", "live", false, "config mode live requires --apply", false},
		{"live-apply", "live", true, "requires the factoryd foreground supervisor", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Mode = tc.mode
			op := Operator{Config: cfg}
			_, err := op.Once(context.Background(), tc.apply)
			if tc.success && err != nil {
				t.Fatal(err)
			}
			if !tc.success && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("error=%v want %q", err, tc.want)
			}
		})
	}
}

func TestDoctorHasSeparateHonestReadiness(t *testing.T) {
	cfg := validOperatorConfig(t)
	cfg.OpenClaw.Executable = filepath.Join(t.TempDir(), "missing-openclaw")
	t.Setenv("LINEAR_API_KEY", "")
	report := (&Operator{Config: cfg}).Doctor()
	want := map[string]bool{"config": true, "state-db": true, "linear": true, "openclaw": true, "review-root": true, "artifact-root": true, "service-environment": true}
	for _, check := range report.Checks {
		delete(want, check.Name)
		if (check.Name == "linear" || check.Name == "openclaw") && check.Status == "ready" {
			t.Errorf("%s was dishonestly ready", check.Name)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing checks: %v", want)
	}
	if report.Status != "not-ready" {
		t.Fatalf("overall=%q", report.Status)
	}
}

func TestStatusDoesNotCreateState(t *testing.T) {
	cfg := validOperatorConfig(t)
	op := &Operator{Config: cfg}
	if got := op.Status(); got.Status != "not-started" {
		t.Fatalf("status=%+v", got)
	}
	if _, err := os.Stat(cfg.Paths.StateDB); !os.IsNotExist(err) {
		t.Fatalf("state changed: %v", err)
	}
}

func validOperatorConfig(t *testing.T) config.Config {
	t.Helper()
	root := t.TempDir()
	_ = os.Chmod(root, 0o700)
	cfg, err := config.Default(filepath.Join(root, "factory.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{cfg.Paths.StateRoot, cfg.Paths.ArtifactRoot, cfg.Paths.ReviewRoot} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return cfg
}
