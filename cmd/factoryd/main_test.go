package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ControlStackAI/dark-factory/internal/config"
	"github.com/ControlStackAI/dark-factory/internal/domain"
)

func TestDaemonTwoKeyInterlockAndNoAdapterCall(t *testing.T) {
	path, cfg, sentinel := daemonConfig(t)
	cases := []struct {
		name, mode string
		args       []string
		want       string
		success    bool
	}{
		{"dry-no-apply", "dry-run", []string{"--once", "--config", path}, "", true},
		{"dry-apply", "dry-run", []string{"--once", "--apply", "--config", path}, "--apply requires config mode live", false},
		{"live-no-apply", "live", []string{"--once", "--config", path}, "config mode live requires --apply", false},
		{"live-apply", "live", []string{"--once", "--apply", "--config", path}, "not implemented until M2/M3", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg.Mode = tc.mode
			writeDaemonConfig(t, path, cfg)
			var out, stderr bytes.Buffer
			err := run(tc.args, &out, &stderr)
			if tc.success && err != nil {
				t.Fatal(err)
			}
			if !tc.success && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("error=%v want %q", err, tc.want)
			}
			if _, statErr := os.Stat(sentinel); !os.IsNotExist(statErr) {
				t.Fatalf("adapter called: %v", statErr)
			}
		})
	}
}

func TestDaemonHelpAndExitContract(t *testing.T) {
	for _, arg := range []string{"-h", "--help"} {
		var out, stderr bytes.Buffer
		if err := run([]string{arg}, &out, &stderr); err != nil || !strings.Contains(out.String(), "usage: factoryd") {
			t.Fatalf("arg=%s out=%q err=%v", arg, out.String(), err)
		}
	}
	for _, args := range [][]string{nil, {"--once", "extra"}, {"--apply"}} {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err == nil {
			t.Fatalf("%v unexpectedly succeeded", args)
		}
	}
}

func TestDaemonDryRunJSONGolden(t *testing.T) {
	path, _, _ := daemonConfig(t)
	var out, stderr bytes.Buffer
	if err := run([]string{"--once", "--config", path}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/dry-run.golden")
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != string(want) {
		t.Fatalf("golden mismatch\nwant=%s\ngot=%s", want, out.String())
	}
}

func TestDaemonDurableStateCompatibility(t *testing.T) {
	state := filepath.Join(t.TempDir(), "factory.db")
	var first, second domain.Run
	for index, target := range []*domain.Run{&first, &second} {
		var out, stderr bytes.Buffer
		if err := run([]string{"--once", "--state", state}, &out, &stderr); err != nil {
			t.Fatalf("run %d: %v", index+1, err)
		}
		if err := json.Unmarshal(out.Bytes(), target); err != nil {
			t.Fatalf("decode run %d: %v", index+1, err)
		}
	}
	if first.Status != domain.RunComplete || first.Attempts != 1 {
		t.Fatalf("first run status=%s attempts=%d", first.Status, first.Attempts)
	}
	if second.Status != first.Status || second.Attempts != first.Attempts || second.Version != first.Version {
		t.Fatalf("durable replay changed the run: first=%+v second=%+v", first, second)
	}
	for _, args := range [][]string{
		{"--once", "--state", state, "--apply"},
		{"--once", "--state", state, "--config", filepath.Join(t.TempDir(), "factory.json")},
	} {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err == nil {
			t.Fatalf("unsafe compatibility combination succeeded: %v", args)
		}
	}
}

func daemonConfig(t *testing.T) (string, config.Config, string) {
	t.Helper()
	root := t.TempDir()
	_ = os.Chmod(root, 0o700)
	path := filepath.Join(root, "factory.json")
	cfg, err := config.Default(path)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(root, "CALLED")
	executable := filepath.Join(root, "fake-openclaw")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nprintf called > \""+sentinel+"\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg.OpenClaw.Executable = executable
	cfg.Linear.Endpoint = "https://127.0.0.1:1/graphql"
	if err := config.WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	return path, cfg, sentinel
}
func writeDaemonConfig(t *testing.T, path string, cfg config.Config) {
	t.Helper()
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
