package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/ControlStackAI/dark-factory/internal/app"
	"github.com/ControlStackAI/dark-factory/internal/buildinfo"
	"github.com/ControlStackAI/dark-factory/internal/config"
	"github.com/ControlStackAI/dark-factory/internal/domain"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runContext(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "factoryd:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	return runContext(context.Background(), args, stdout, stderr)
}

var runLiveSupervisor = func(ctx context.Context, cfg config.Config) (domain.Run, error) {
	supervisor, err := app.NewProductionSupervisor(cfg)
	if err != nil {
		return domain.Run{}, err
	}
	defer supervisor.Close()
	return supervisor.Run(ctx)
}

func runContext(ctx context.Context, args []string, stdout, stderr io.Writer) error {
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
	configSet := false
	fs.Visit(func(f *flag.Flag) { configSet = configSet || f.Name == "config" })
	if *state != "" {
		if !*once {
			return errors.New("--state is available only with --once")
		}
		if *apply {
			return errors.New("--state is a credential-free compatibility mode and cannot be combined with --apply")
		}
		if configSet {
			return errors.New("--state and --config are mutually exclusive")
		}
		result, err := app.DurableDryRun(ctx, *state)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(result)
	}
	op, err := app.Compose(*path, false)
	if err != nil {
		return err
	}
	if op.Config.Mode != "live" {
		if *apply {
			return errors.New("--apply requires config mode live; no external action was taken")
		}
		if !*once {
			return errors.New("continuous factoryd requires config mode live and --apply; no external action was taken")
		}
		result, err := op.DryRun(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(app.SummarizeRun(result))
	}
	if !*apply {
		return errors.New("config mode live requires --apply; no external action was taken")
	}
	result, err := runLiveSupervisor(ctx, op.Config)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(app.SummarizeRun(result))
}

const daemonUsage = `usage: factoryd --config PATH --apply
       factoryd --once --config PATH [--apply]
       factoryd --once --state STATE_DB

factoryd runs as a foreground continuous supervisor and never daemonizes. Live
execution requires both config mode live and --apply. SIGTERM performs a bounded
clean shutdown. The --state form preserves the M0 restart-safe recovery fixture.
`
