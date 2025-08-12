#!/bin/bash

# Simple build script for telegram-scraper
set -e

echo "🧪 Running tests..."
go test -v ./...

echo ""
echo "🔧 Building telegram-scraper..."
go build -o telegram-scraper

echo ""
echo "✅ Build completed successfully!"
echo "   Executable: ./telegram-scraper"