package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ControlStackAI/dark-factory/internal/app"
	"github.com/ControlStackAI/dark-factory/internal/buildinfo"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "factoryd:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("factoryd", flag.ContinueOnError)
	once := flags.Bool("once", false, "run the credential-free vertical slice once")
	version := flags.Bool("version", false, "print version")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *version {
		fmt.Println(buildinfo.Version)
		return nil
	}
	if !*once {
		return fmt.Errorf("no persistent daemon is configured in this bootstrap; use --once")
	}
	run, err := app.DryRun(context.Background())
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(run)
}
