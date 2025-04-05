# Telegram Scraper Development Guidelines

## Build Commands
- Build: `go build -o telegram-scraper`
- Debug build: `go build -gcflags "all=-N -l" -o ./bin/app`
- Docker build: `docker build -t telegram-scraper .`

## Test Commands
- Run all tests: `go test ./...`
- Run specific test: `go test ./path/to/package -run TestName`
- Test with verbose output: `go test -v ./...`

## Code Style Guidelines
- **Imports**: Group standard library first, then project imports, then third-party
- **Error handling**: Check errors with `if err != nil`, use `fmt.Errorf("context: %w", err)` for wrapping
- **Logging**: Use zerolog with contextual fields, e.g., `log.Error().Err(err).Msg("message")`
- **Naming**: PascalCase for exported, camelCase for unexported, acronyms capitalized (e.g., `TDLibClient`)
- **Comments**: Document exported functions with `// FunctionName does...` style
- **Receivers**: Use pointer receivers for methods that modify state
- **Concurrency**: Use context for cancellation, proper goroutine management with WaitGroups

## Dependencies (macOS)
```
export CGO_CFLAGS=-I/opt/homebrew/include
export CGO_LDFLAGS=-L/opt/homebrew/lib -lssl -lcrypto
```