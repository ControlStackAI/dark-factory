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
		fmt.Fprintln(os.Stderr, "factoryd:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprint(stdout, daemonUsage)
		return nil
	}
	fs := flag.NewFlagSet("factoryd", flag.ContinueOnError)
	fs.SetOutput(stderr)
	once := fs.Bool("once", false, "run the configured composition once")
	apply := fs.Bool("apply", false, "second key required with config mode live")
	path := fs.String("config", config.DefaultPath(), "configuration path")
	state := fs.String("state", "", "SQLite state path for the M0 durable credential-free slice")
	version := fs.Bool("version", false, "print version")
	help := fs.Bool("help", false, "show help")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *help {
		fmt.Fprint(stdout, daemonUsage)
		return nil
	}
	if *version {
		fmt.Fprintln(stdout, buildinfo.Version)
		return nil
	}
	if !*once {
		return errors.New("continuous daemon is not implemented until M3; use --once")
	}
	configSet := false
	fs.Visit(func(f *flag.Flag) { configSet = configSet || f.Name == "config" })
	if *state != "" {
		if *apply {
			return errors.New("--state is a credential-free compatibility mode and cannot be combined with --apply")
		}
		if configSet {
			return errors.New("--state and --config are mutually exclusive")
		}
		result, err := app.DurableDryRun(context.Background(), *state)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(result)
	}
	op, err := app.Compose(*path, false)
	if err != nil {
		return err
	}
	result, err := op.Once(context.Background(), *apply)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(app.SummarizeRun(result))
}

const daemonUsage = `usage: factoryd --once --config PATH [--apply]
       factoryd --once --state STATE_DB

M2 supports credential-free --once execution. Live execution requires both
config mode live and --apply, then fails closed until the M3 OpenClaw side exists.
The --state form preserves the M0 restart-safe credential-free recovery fixture.
`
