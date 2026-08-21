.PHONY: build check fmt fmt-check lint smoke test test-race

build:
	go build ./...

fmt:
	gofmt -w $$(find cmd internal -name '*.go' -type f)

fmt-check:
	@test -z "$$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }

lint:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

smoke:
	go run ./cmd/factoryctl version
	go run ./cmd/factoryctl dry-run >/dev/null
	go run ./cmd/factoryd --once >/dev/null

check: fmt-check test lint build smoke
