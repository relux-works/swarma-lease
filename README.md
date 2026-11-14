# Swarma Lease

Swarma Lease is a local service for registering shared resources and leasing one resource at a time. Each resource has a stable ID and a tag that places it in a pool. Requests have a bounded duration; queued requests are served in per-tag FIFO order. See the [MVP contract](docs/contract.md) for the state model, refusal behavior, defaults, and implementation slices.

## Status

The v1 design is documented in the [MVP contract](docs/contract.md). The Go module, CLI, and local service are under implementation; the current candidate still needs its required Mac build and test run before it can be treated as verified. The first implementation slice covers manual-pool leasing; command execution follows in a later slice.

## Tools and commands

Contributor entry points:

| Tool or entry point | Use | Command | Output |
| --- | --- | --- | --- |
| Go toolchain and system SQLite library | Format, vet, race-test, and build the CGO-backed service | `gofmt -w cmd/swarma-lease/*.go internal/cli/*.go internal/lease/*.go internal/sqlite/*.go`<br>`go vet ./...`<br>`go test -race -count=1 ./...`<br>`mkdir -p .temp/bin`<br>`go build -o .temp/bin/swarma-lease ./cmd/swarma-lease` | Validation output is written to the terminal; the executable is `.temp/bin/swarma-lease`. The SQLite library and headers must be available from the platform toolchain. |
| GitHub Actions | Check pull requests and pushes to `main` on Ubuntu and macOS | Runs from `.github/workflows/ci.yml`; the hosted job checks formatting, runs `go vet ./...`, and runs `go test -race -count=1 ./...`. | Hosted job results stay in the GitHub Actions run; the workflow uses GitHub-hosted runners. |
| Swarma Lease CLI | Start the service and manage resources and leases | `.temp/bin/swarma-lease serve`<br>`.temp/bin/swarma-lease resource add --tag qa.pool --id qa-01 --mode manual --json`<br>`.temp/bin/swarma-lease lease request --tag qa.pool --duration 20m --no-wait --json`<br>`.temp/bin/swarma-lease lease request --tag qa.pool --duration 20m --wait --wait-timeout 30m --request-id req-01 --json`<br>`.temp/bin/swarma-lease lease wait --request-id req-01 --json`<br>`.temp/bin/swarma-lease lease cancel --request-id req-01 --json`<br>`.temp/bin/swarma-lease lease release --id <lease-id> --json` | Management commands return one JSON object on stdout. Blocking commands print only the final result. Service state is stored under `~/.swarma-lease/`. |
| SQLite database | Persist single-host lease state | Managed by the Go service; no standalone command is required. | `~/.swarma-lease/leases.sqlite`, its WAL files, `service.lock`, and `service.sock`. SQLite is the only source of truth. |

Build artifacts are kept under the ignored `.temp/` directory; runtime state stays under the effective user's system-account home. The service checks every state-directory and custom socket-parent component without following symlinks. On macOS, custom paths rooted under system symlinks such as `/tmp` or `/var` are refused; the default account-home path is resolved from the system account record. Socket paths that exceed the platform Unix-domain address capacity are refused before state creation with `socket_path_too_long`. Blocking FIFO requests, wait, and cancellation are implemented; hard-end/restart recovery and the full first-slice acceptance run remain pending.

## License

Apache License 2.0. See [LICENSE](LICENSE).
