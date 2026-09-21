# Contributing

Use Go 1.24 or newer. Discuss public API changes in an issue before opening a
large pull request. Keep domain behavior independent of storage and cloud SDKs.

Run the core checks from the repository root:

```sh
gofmt -w *.go
go vet ./...
go test -race -covermode=atomic -coverprofile=coverage.out ./...
./scripts/check-coverage.sh coverage.out
```

The separate `spanner/` module needs the Cloud Spanner emulator for integration
tests and full coverage. Follow its README, then run the same vet/test commands
there and `../scripts/check-coverage.sh coverage.out`.

Both modules require exact 100% statement coverage. Include observable behavior
assertions, failure paths, and regression cases. Do not exclude source files,
rewrite coverage profiles, or add build tags to improve the percentage.

Preserve at-least-once delivery, atomic batch writes, tenant metadata, stable
partition ownership, and optimistic concurrency across both backends. Keep
examples executable through `go test`. Run the race detector for concurrent code.

By contributing, you agree to license your contribution under Apache-2.0.
