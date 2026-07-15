.PHONY: install test lint mocks

install:
	go install .
	@bin="$$(go env GOBIN)"; \
	if [ -z "$$bin" ]; then bin="$$(go env GOPATH)/bin"; fi; \
	mkdir -p "$$bin"; \
	install -m 0755 scripts/vev-bar-top-right scripts/vev-bar-bottom-right "$$bin"

test:
	go test ./... -race

lint:
	@test -z "$$(goimports -l .)"
	go vet ./...

mocks:
	@version="$$(mockery version 2>/dev/null || true)"; \
	if [ "$$version" != "v3.7.1" ]; then \
		echo "mockery v3.7.1 required; install with: go install github.com/vektra/mockery/v3@v3.7.1" >&2; \
		exit 1; \
	fi
	mockery
