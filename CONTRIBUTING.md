# Contributing to go-kiro-gateway

Thanks for your interest in contributing! Here's how to get started.

## Ways to Contribute

- Report bugs via [GitHub Issues](https://github.com/chasedputnam/go-kiro-gateway/issues)
- Suggest features or improvements
- Submit pull requests for bug fixes or new features
- Improve documentation

## Development Setup

### Prerequisites

- Go 1.25+
- Docker (for container builds)
- A valid Kiro/AWS session for integration testing

### Getting Started

```bash
git clone https://github.com/chasedputnam/go-kiro-gateway.git
cd go-kiro-gateway
cp .env.example .env
# Edit .env with your credentials
cd gateway
go mod download
go test ./...
```

## Making Changes

1. Fork the repository and create a branch from `main`:
   ```bash
   git checkout -b feat/your-feature-name
   ```

2. Make your changes, following the conventions below.

3. Add or update tests for any changed behavior.

4. Run the test suite:
   ```bash
   cd gateway
   go test ./... -timeout 120s
   ```

5. Ensure the Docker build passes:
   ```bash
   docker build -t go-kiro-gateway .
   ```

6. Commit using a clear, descriptive message.

7. Open a pull request against `main`.

## Code Conventions

- Follow standard Go formatting (`gofmt`)
- Keep functions focused and single-purpose
- Handle errors explicitly — no silent failures
- Avoid introducing new dependencies without discussion
- Match the existing logging pattern (structured, leveled)

## Security Issues

Do **not** open a public issue for security vulnerabilities. See [SECURITY.md](SECURITY.md) for the private reporting process.

## License

By contributing, you agree that your contributions will be licensed under the [AGPL-3.0 License](LICENSE).
