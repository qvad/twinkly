# Twinkly Makefile

.PHONY: build test clean install run help test-integration docker-test

# Build the proxy
build:
	go build -o twinkly .

# Run all tests
test:
	go test ./...

# Run tests with verbose output
test-verbose:
	go test -v ./...

# Run specific test suites
test-config:
	go test -v -run TestConfigAdapter

test-validator:
	go test -v -run TestResultValidator

test-security:
	go test -v -run TestSecurity

# Run integration tests (requires databases)
test-integration:
	go test -v ./test/integration/...

# Run integration tests with Docker
docker-test:
	docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit
	docker-compose -f docker-compose.test.yml down

# Start test databases only
test-db-up:
	docker-compose -f docker-compose.test.yml up -d postgres yugabytedb

# Stop test databases
test-db-down:
	docker-compose -f docker-compose.test.yml down

# Run the proxy
run: build
	./twinkly

# Run proxy with test configuration
run-test: build
	TWINKLY_CONFIG=test/config/integration-test.conf ./twinkly

# Clean build artifacts
clean:
	rm -f twinkly
	go clean
	docker-compose -f docker-compose.test.yml down -v

# Development workflow: start databases and run tests
dev-test: test-db-up
	sleep 10  # Wait for databases to be ready
	make test-integration
	make test-db-down

# Install dependencies
install:
	go mod tidy
	go mod download

# Format code
fmt:
	go fmt ./...

# Run linter
lint:
	golangci-lint run

# Build for multiple platforms
build-all:
	GOOS=linux GOARCH=amd64 go build -o twinkly-linux-amd64 .
	GOOS=darwin GOARCH=amd64 go build -o twinkly-darwin-amd64 .
	GOOS=windows GOARCH=amd64 go build -o twinkly-windows-amd64.exe .

# Show help
help:
	@echo "Available targets:"
	@echo "  build            - Build the proxy"
	@echo "  test             - Run all unit tests"
	@echo "  test-verbose     - Run tests with verbose output"
	@echo "  test-integration - Run integration tests (requires databases)"
	@echo "  test-config      - Run configuration tests"
	@echo "  test-validator   - Run result validator tests"
	@echo "  test-security    - Run security tests"
	@echo "  docker-test      - Run full integration tests with Docker"
	@echo "  test-db-up       - Start test databases only"
	@echo "  test-db-down     - Stop test databases"
	@echo "  dev-test         - Full development test cycle"
	@echo "  run              - Build and run the proxy"
	@echo "  run-test         - Run proxy with test configuration"
	@echo "  clean            - Clean build artifacts and containers"
	@echo "  install          - Install dependencies"
	@echo "  fmt              - Format code"
	@echo "  lint             - Run linter"
	@echo "  build-all        - Build for multiple platforms"
	@echo "  help             - Show this help message"