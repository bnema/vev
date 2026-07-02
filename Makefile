.PHONY: install test lint mocks

install:
	go install .

test:
	go test ./... -race

lint:
	@test -z "$$(goimports -l .)"
	go vet ./...

mocks:
	mockery
