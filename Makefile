.PHONY: help test test-unit test-integration lint fmt build

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: test-unit ## Run all tests

test-unit: ## Run unit tests
	go test -v -race -coverprofile=coverage.out ./...

test-integration: ## Run integration tests (uses testcontainers; requires Docker)
	go test -p 1 -v -tags=integration ./...

lint: ## Run linter
	golangci-lint run --timeout=5m

fmt: ## Format code
	gofmt -w -s .
	goimports -w -local github.com/eventsalsa/store .

build: ## Build all packages
	go build -v ./...
