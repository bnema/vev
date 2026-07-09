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
	mockery
