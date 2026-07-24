.PHONY: install test test-installer lint mocks demo

install:
	go install .
	@bin="$$(go env GOBIN)"; \
	if [ -z "$$bin" ]; then bin="$$(go env GOPATH)/bin"; fi; \
	mkdir -p "$$bin"; \
	install -m 0755 scripts/vev-bar-top-right scripts/vev-bar-bottom-right "$$bin"

test:
	go test ./... -race

test-installer:
	sh scripts/install_platform_test.sh

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

demo:
	docker build -f scripts/demo/Dockerfile -t vev-demo .
	./scripts/demo/run.sh demo.tape
	mkdir -p docs/assets
	cp scripts/demo/out/demo.gif docs/assets/demo.gif
