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
	@tmp=$$(mktemp -d); chmod 700 "$$tmp"; trap 'rm -rf "$$tmp"' EXIT; \
		go run ./cmd/factoryctl init --config "$$tmp/factory.json" >/dev/null; \
		LINEAR_API_KEY=smoke-only go run ./cmd/factoryctl validate --config "$$tmp/factory.json" --json >/dev/null; \
		env -u LINEAR_API_KEY go run ./cmd/factoryctl doctor --config "$$tmp/factory.json" --json >/dev/null; \
		go run ./cmd/factoryctl dry-run --config "$$tmp/factory.json" --json >/dev/null; \
		go run ./cmd/factoryctl status --config "$$tmp/factory.json" --json >/dev/null; \
		go run ./cmd/factoryd --once --config "$$tmp/factory.json" >/dev/null; \
		go run ./cmd/factoryd --once --state "$$tmp/daemon-recovery.db" >/dev/null; \
		go run ./cmd/factoryctl recover "$$tmp/recovery.db" >/dev/null

check: fmt-check test lint build smoke
