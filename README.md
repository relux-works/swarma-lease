# Swarma Lease

Swarma Lease is a local service for registering shared resources and leasing one resource at a time. Each resource has a stable ID and a tag that places it in a pool. Requests have a bounded duration; queued requests are served in per-tag FIFO order. See the [MVP contract](docs/contract.md) for the state model, refusal behavior, defaults, and implementation slices.

## Status

The MVP contract is specified. The Go module, CLI, and lease service are not implemented yet. The first implementation slice covers local leasing; the Mac test command adapter follows in a later slice.

## Tools and commands

These are the intended contributor entry points; they become runnable when the implementation is added.

| Tool or entry point | Use | Command | Output |
| --- | --- | --- | --- |
| Go toolchain | Build and test the module | `go test ./...`<br>`mkdir -p .temp/bin`<br>`go build -o .temp/bin/swarma-lease ./cmd/swarma-lease` | Test results go to the terminal; the executable is `.temp/bin/swarma-lease`. |
| Swarma Lease CLI | Start the local service and manage resources/leases | `.temp/bin/swarma-lease serve` | Local service state is stored in `~/.swarma-lease/` (`config.json`, `leases.sqlite`, and `service.sock`). |
| SQLite storage | Persist single-host lease state | Managed by the Go service; no standalone command is required. | `~/.swarma-lease/leases.sqlite` and its local WAL files. |

Build artifacts are kept under the ignored `.temp/` directory; runtime state stays under the user's home directory.

## License

Apache License 2.0. See [LICENSE](LICENSE).
