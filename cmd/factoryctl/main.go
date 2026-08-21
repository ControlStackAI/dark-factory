package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ControlStackAI/dark-factory/internal/app"
	"github.com/ControlStackAI/dark-factory/internal/buildinfo"
	"github.com/ControlStackAI/dark-factory/internal/config"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "factoryctl:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageError()
	}
	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return nil
	case "version":
		if len(args) != 1 {
			return usageError()
		}
		fmt.Fprintln(stdout, buildinfo.Version)
		return nil
	case "init":
		fs := newFlagSet("init", stderr)
		path := fs.String("config", config.DefaultPath(), "configuration path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return usageError()
		}
		cfg, err := config.Default(*path)
		if err != nil {
			return err
		}
		if err := config.WriteNew(*path, cfg); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "created", *path)
		return nil
	case "validate":
		path, jsonOutput, err := configFlags("validate", args[1:], stderr)
		if err != nil {
			return err
		}
		op, err := app.Compose(path, true)
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(stdout, op.Validation())
		}
		fmt.Fprintf(stdout, "valid config_version=%d mode=%s\n", op.Config.ConfigVersion, op.Config.Mode)
		return nil
	case "doctor":
		path, jsonOutput, err := configFlags("doctor", args[1:], stderr)
		if err != nil {
			return err
		}
		op, err := app.Compose(path, false)
		if err != nil {
			return err
		}
		report := op.Doctor()
		if jsonOutput {
			return writeJSON(stdout, report)
		}
		fmt.Fprint(stdout, app.FormatDoctor(report))
		return nil
	case "dry-run":
		path, jsonOutput, err := configFlags("dry-run", args[1:], stderr)
		if err != nil {
			return err
		}
		op, err := app.Compose(path, false)
		if err != nil {
			return err
		}
		run, err := op.DryRun(context.Background())
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(stdout, app.SummarizeRun(run))
		}
		fmt.Fprintf(stdout, "dry-run complete: %s\n", run.ID)
		return nil
	case "status":
		path, jsonOutput, err := configFlags("status", args[1:], stderr)
		if err != nil {
			return err
		}
		op, err := app.Compose(path, false)
		if err != nil {
			return err
		}
		status := op.Status()
		if jsonOutput {
			return writeJSON(stdout, status)
		}
		fmt.Fprintf(stdout, "%s: %s\n", status.Status, status.Message)
		return nil
	case "recover":
		if len(args) != 2 {
			return usageError()
		}
		run, err := app.DurableDryRun(context.Background(), args[1])
		if err != nil {
			return err
		}
		return writeJSON(stdout, run)
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usageText)
	}
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func configFlags(name string, args []string, stderr io.Writer) (string, bool, error) {
	fs := newFlagSet(name, stderr)
	path := fs.String("config", config.DefaultPath(), "configuration path")
	jsonOutput := fs.Bool("json", false, "emit stable JSON")
	if err := fs.Parse(args); err != nil {
		return "", false, err
	}
	if fs.NArg() != 0 {
		return "", false, usageError()
	}
	return *path, *jsonOutput, nil
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func usageError() error { return errors.New(usageText) }

const usageText = `usage: factoryctl <command> [options]

Commands:
  init       create a private config and roots without replacement
  validate   strictly validate config, paths, permissions, and env references
  doctor     report offline component readiness (never contacts adapters)
  dry-run    execute the credential-free in-memory composition
  status     inspect state presence without opening or mutating the database
  recover    execute or resume the M0 credential-free SQLite recovery fixture
  version    print the version
  help       show this help

Common options: --config PATH, --json
`
