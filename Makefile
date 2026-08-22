.PHONY: build check fmt fmt-check lint m5 smoke test test-race

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

m5:
	@mkdir -p test-results/m5
	@chmod 700 test-results/m5
	@printf '%s\n' '51001,51002,51003,51004,51005,51006,51007,51008,51009,51010,51011,51012,51013,51014,51015,51016,51017,51018,51019,51020' > test-results/m5/seeds.txt
	@DARK_FACTORY_M5_RUN=1 \
		DARK_FACTORY_M5_SEEDS="$$(cat test-results/m5/seeds.txt)" \
		DARK_FACTORY_M5_RECEIPT="$$(pwd)/test-results/m5/matrix.json" \
		go test -race ./internal/e2e -run '^TestM5ForcedRestartEndToEnd$$' -count=1 -v > test-results/m5/race.log 2>&1 || { cat test-results/m5/race.log; exit 1; }
	@sha256sum test-results/m5/seeds.txt test-results/m5/matrix.json test-results/m5/race.log > test-results/m5/sha256sums.txt
	@cat test-results/m5/race.log

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
