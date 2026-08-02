# Repository Guidelines

## Project Structure & Module Organization

This repository is the independent native Go SDK for `rabbit-jetstream`; server, management UI, and deployment assets live in the sibling server repository. Public client APIs stay in the root `rabbitjetstream` package. Keep protocol names and headers in `contract.go`, publishing in `publisher.go`, pull-consumer behavior in `consumer.go`, topology helpers in `admin.go`, and the pure scheduling state machine in `scheduler.go`. Unit tests use adjacent `*_test.go` files. Docker-backed tests use `integration_test.go` with the `integration` build tag. Reusable test entry points belong in `scripts/`.

## Build, Test, and Development Commands

- `go test ./...` runs fast platform-independent tests.
- `go test -race ./...` runs the race detector on Linux, the primary production platform.
- `go vet ./...` performs static checks.
- `./scripts/test-integration.sh` starts an isolated NATS container and runs real JetStream tests.
- `pwsh ./scripts/test-integration.ps1` provides the same Docker Desktop workflow on Windows without WSL.

Run `gofmt -w` on changed Go files and `go mod tidy` after dependency changes.

## Coding Style & Naming Conventions

Follow standard Go formatting and effective Go conventions. Exported identifiers need concise GoDoc. Return wrapped errors with operational context (`fmt.Errorf("publish %s: %w", id, err)`). Keep queue naming and priority validation centralized; never duplicate wire-format literals. Avoid hidden topology mutation in publish or consume paths.

## Testing Guidelines

Every protocol, ordering, acknowledgement, reconnect, or backpressure change requires tests. Use `TestFeatureCondition` names and table-driven cases for validation boundaries. Production-sensitive concurrency changes must pass Linux `-race`. Integration tests must use unique queue names and clean up streams and containers. Do not weaken coverage gates without an explained design decision.

## Commit & Pull Request Guidelines

Use imperative Conventional Commit subjects such as `feat: add priority publisher` or `test: cover consumer redelivery`. Keep commits focused. Pull requests must describe contract impact, failure behavior, tests run, and any server-version dependency. Link issues and include benchmark results for hot-path changes; UI screenshots do not apply to this SDK.

## Security & Compatibility

Never commit credentials. Accept authentication and TLS through standard `nats.Option` values. Treat `v1alpha1` headers and resource names as a compatibility boundary; coordinate incompatible changes with the server repository.
