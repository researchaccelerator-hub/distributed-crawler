#!/bin/bash

# Build script for telegram-scraper
# Runs tests first, then builds the project

set -e  # Exit on any error

echo "🔧 Telegram Scraper Build Script"
echo "================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Set up environment variables for macOS (from CLAUDE.md)
if [[ "$OSTYPE" == "darwin"* ]]; then
    print_status "Setting up macOS environment variables..."
    export CGO_CFLAGS=-I/opt/homebrew/include
    export CGO_LDFLAGS="-L/opt/homebrew/lib -lssl -lcrypto"
fi

# Check if go is installed
if ! command -v go &> /dev/null; then
    print_error "Go is not installed or not in PATH"
    exit 1
fi

print_status "Go version: $(go version)"

# Clean previous builds
print_status "Cleaning previous builds..."
rm -f telegram-scraper
rm -rf ./bin/app

# Run tests
print_status "Running tests..."
echo "----------------------------------------"

# Run all tests with verbose output
if go test -v ./...; then
    print_success "All tests passed!"
else
    print_error "Tests failed! Aborting build."
    exit 1
fi

echo "----------------------------------------"

# Run tests with coverage (optional)
print_status "Running tests with coverage..."
if go test -coverprofile=coverage.out ./...; then
    print_status "Coverage report generated: coverage.out"
    # Show coverage summary
    go tool cover -func=coverage.out | tail -1
else
    print_warning "Coverage analysis failed, but continuing with build..."
fi

# Build the project
print_status "Building telegram-scraper..."
echo "----------------------------------------"

# Standard build
if go build -o telegram-scraper; then
    print_success "Standard build completed: telegram-scraper"
else
    print_error "Standard build failed!"
    exit 1
fi

# Debug build
print_status "Building debug version..."
mkdir -p ./bin
if go build -gcflags "all=-N -l" -o ./bin/app; then
    print_success "Debug build completed: ./bin/app"
else
    print_warning "Debug build failed, but standard build succeeded"
fi

# Docker build (optional)
if command -v docker &> /dev/null; then
    print_status "Docker found. Building Docker image..."
    if docker build -t telegram-scraper .; then
        print_success "Docker image built: telegram-scraper"
    else
        print_warning "Docker build failed, but binary builds succeeded"
    fi
else
    print_warning "Docker not found, skipping Docker build"
fi

# Final status
echo "========================================"
print_success "Build completed successfully!"
echo ""
print_status "Available executables:"
if [ -f "telegram-scraper" ]; then
    echo "  • telegram-scraper (standard build)"
    ls -lh telegram-scraper
fi
if [ -f "./bin/app" ]; then
    echo "  • ./bin/app (debug build)"
    ls -lh ./bin/app
fi

echo ""
print_status "Usage examples:"
echo "  ./telegram-scraper --help"
echo "  ./telegram-scraper --mode=dapr-standalone --urls=\"https://t.me/example\""
echo "  ./telegram-scraper --mode=dapr-job --dapr-port=3000"

echo ""
print_success "🎉 Build script completed successfully!"