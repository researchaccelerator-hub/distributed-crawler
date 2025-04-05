#!/bin/bash
# Check for required tools
if ! command -v dlv &> /dev/null; then
    echo "Delve debugger not found. Installing..."
    go install github.com/go-delve/delve/cmd/dlv@latest

    # Add Go bin to PATH temporarily for this session
    export PATH=$PATH:$(go env GOPATH)/bin

    if ! command -v dlv &> /dev/null; then
        echo "ERROR: Failed to install Delve. Please install manually with:"
        echo "    go install github.com/go-delve/delve/cmd/dlv@latest"
        echo "And ensure $(go env GOPATH)/bin is in your PATH by adding to your shell config:"
        echo ""
        echo "For bash: echo 'export PATH=\$PATH:\$(go env GOPATH)/bin' >> ~/.bashrc"
        echo "For zsh:  echo 'export PATH=\$PATH:\$(go env GOPATH)/bin' >> ~/.zshrc"
        echo ""
        echo "Then run: source ~/.zshrc (or ~/.bashrc)"
        exit 1
    fi
fi

if ! command -v dapr &> /dev/null; then
    echo "ERROR: Dapr CLI not found. Please install it first."
    echo "Visit https://docs.dapr.io/getting-started/install-dapr-cli/"
    exit 1
fi

echo "Building application with debug symbols..."
go build -gcflags "all=-N -l" -o ./bin/app

echo "Starting Delve debugger..."
# Pass your app arguments after the double dash
dlv exec ./bin/app --headless --listen=:2345 --api-version=2 --accept-multiclient -- --dapr --dapr-port 6481 --dapr-mode standalone --urls "kartiny_muzei_zhivopis,pokraslampas,roxman,litvintm" --crawl-id test71 --concurrency 1 --tdlib-database-urls http://tomb218.sg-host.com/tdlib-db.tgz --min-post-date 2025-04-01 --max-comments=1 --max-depth 1 &
DLV_PID=$!

# Give Delve a moment to initialize
sleep 2

echo "Starting Dapr sidecar..."
echo "Connect your IDE debugger to localhost:2345 now!"
echo "Press Ctrl+C to stop both Dapr and Delve"

# Start Dapr sidecar (this will run in foreground)
# Note: When using dlv, the args are passed to dlv instead of here
dapr run \
    --app-id distributed-scheduler \
    --app-port 6481 \
    --dapr-http-port 3500 \
    --dapr-grpc-port 50001 \
    --log-level debug \
    --app-protocol grpc \
    --resources-path ./resources

# When Dapr exits (Ctrl+C), also kill Delve
echo "Shutting down Delve debugger..."
kill $DLV_PID 2>/dev/null || true
