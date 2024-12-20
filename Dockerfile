FROM acrnetcus.azurecr.io/tdlib:latest AS builder

RUN apk add --no-cache g++ make cmake git linux-headers binutils go

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
RUN CGO_ENABLED=1 go build -o main .

# Stage 2: Minimal runtime image
FROM acrnetcus.azurecr.io/tdlib:latest

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Set the entrypoint
ENTRYPOINT ["./main"]
CMD []
