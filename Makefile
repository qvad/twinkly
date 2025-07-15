# Twinkly Makefile

.PHONY: build test clean install run help

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

# Run the proxy
run: build
	./twinkly

# Clean build artifacts
clean:
	rm -f twinkly
	go clean

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
	@echo "  build       - Build the proxy"
	@echo "  test        - Run all tests"
	@echo "  test-verbose - Run tests with verbose output"
	@echo "  test-config - Run configuration tests"
	@echo "  test-validator - Run result validator tests"
	@echo "  test-security - Run security tests"
	@echo "  run         - Build and run the proxy"
	@echo "  clean       - Clean build artifacts"
	@echo "  install     - Install dependencies"
	@echo "  fmt         - Format code"
	@echo "  lint        - Run linter"
	@echo "  build-all   - Build for multiple platforms"
	@echo "  help        - Show this help message"