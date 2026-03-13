#TODO: REVERT CHANGES BEFORE MERGE

FROM acrnetcus.azurecr.io/tdlib:latest AS builder

RUN apk add --no-cache g++ make cmake git linux-headers binutils graphviz valgrind curl

# Install Go 1.25 explicitly to match go.mod requirement
RUN ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/') && \
    curl -fsSL https://go.dev/dl/go1.25.0.linux-${ARCH}.tar.gz | tar -C /usr/local -xz
ENV PATH="/usr/local/go/bin:${PATH}"

RUN ln -s /usr/include/asm-generic /usr/include/asm

# Set the working directory
WORKDIR /app

# Copy Go modules manifests
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy application source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 CGO_LDFLAGS="-Wl,--start-group -ltde2e -ltdutils -Wl,--end-group" go build -o main .

# # Stage 2: Minimal runtime image
# FROM acrnetcus.azurecr.io/tdlib:latest

# # Set the working directory
# WORKDIR /app

# # Copy the binary from the builder stage
# COPY --from=builder /app/main .

# Fix issue creating subdirectories on readonlyrootfs
#RUN umask 002

# Set the entrypoint.
ENTRYPOINT ["./main"]
CMD ["--dapr", "--dapr-mode", "job"]
