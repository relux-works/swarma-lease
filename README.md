# Swarma Lease

Swarma Lease is a local service for registering shared resources and leasing one resource at a time. Each resource has a stable ID and a tag that places it in a pool. Requests have a bounded duration; queued requests are served in per-tag FIFO order. See the [MVP contract](docs/contract.md) for the state model, refusal behavior, defaults, and implementation slices.

## Status

The v1 design is documented in the [MVP contract](docs/contract.md). The Go module, CLI, and lease service are not implemented yet. The first implementation slice covers local manual-pool leasing; command execution follows in a later slice.

## Tools and commands

These are the intended contributor entry points; they become runnable when the implementation is added.

| Tool or entry point | Use | Command | Output |
| --- | --- | --- | --- |
| Go toolchain | Build, inspect, and test the module | `go vet ./...`<br>`go test -race -count=1 ./...`<br>`mkdir -p .temp/bin`<br>`go build -o .temp/bin/swarma-lease ./cmd/swarma-lease` | Validation results go to the terminal; the executable is `.temp/bin/swarma-lease`. The module is not present yet. |
| Swarma Lease CLI | Start the local service and manage resources/leases | `.temp/bin/swarma-lease serve`<br>`.temp/bin/swarma-lease resource add --tag qa.pool --id qa-01 --mode manual --json`<br>`.temp/bin/swarma-lease lease request --tag qa.pool --duration 20m --wait --wait-timeout 30m --json`<br>`.temp/bin/swarma-lease lease release --id <lease-id> --json` | These entry points are specified in the contract and become runnable when the implementation is added. Service state is stored under `~/.swarma-lease/`. |
| SQLite storage | Persist single-host lease state | Managed by the Go service; no standalone command is required. | `~/.swarma-lease/leases.sqlite`, its WAL files, `service.lock`, and `service.sock`. SQLite is the only source of truth. |

Build artifacts are kept under the ignored `.temp/` directory; runtime state stays under the user's home directory.

## License

Apache License 2.0. See [LICENSE](LICENSE).
