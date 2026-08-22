//go:build !darkfactory_faultinject

package main

import "github.com/ControlStackAI/dark-factory/internal/app"

func productionHooks() app.ProductionHooks { return app.ProductionHooks{} }
