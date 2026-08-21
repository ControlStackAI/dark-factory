package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/ControlStackAI/dark-factory/internal/app"
	"github.com/ControlStackAI/dark-factory/internal/buildinfo"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "factoryctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 2 && args[0] == "recover" {
		run, err := app.DurableDryRun(context.Background(), args[1])
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(run)
	}
	if len(args) != 1 {
		return fmt.Errorf("usage: factoryctl <version|dry-run|recover STATE_PATH>")
	}
	switch args[0] {
	case "version":
		fmt.Println(buildinfo.Version)
		return nil
	case "dry-run":
		run, err := app.DryRun(context.Background())
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(run)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
