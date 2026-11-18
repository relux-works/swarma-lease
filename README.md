# Swarma Lease

Swarma Lease is a local service for registering shared resources and granting one resource per lease. Each resource has a stable ID and a tag that places it in a pool. Requests have a bounded duration; unclassified pools use per-tag FIFO, while Go tests and JVM builds use class-wide FIFO with build-before-Go priority. See the [MVP contract](docs/contract.md) for the state model, refusal behavior, defaults, and implementation slices.

## Status

The service includes manual-pool leasing, FIFO waits up to four hours, class-wide Go and JVM build limits, build-before-Go queue priority, lease hard ends, same-boot and reboot recovery, the local-socket protocol, tag and class queue status, service status, and history. Lease requests and resource mutations require an opaque caller ID. Audit rows identify direct caller actions separately from scheduler transitions; scheduler rows inherit caller and request IDs when a v3 request caused them, and pre-v3-derived rows stay explicitly unattributed. Existing databases migrate transactionally from schema versions 1 and 2 to version 3 at startup. Command execution follows in a later slice.

## Tools and commands

Contributor entry points:

| Tool or entry point | Use | Command | Output |
| --- | --- | --- | --- |
| Go toolchain and system SQLite library | Format, vet, race-test, and build the CGO-backed service | `gofmt -w cmd/swarma-lease/*.go internal/cli/*.go internal/lease/*.go internal/sqlite/*.go`<br>`go vet ./...`<br>`go test -race -count=1 ./...`<br>`mkdir -p .temp/bin`<br>`go build -o .temp/bin/swarma-lease ./cmd/swarma-lease` | Validation output is written to the terminal; the executable is `.temp/bin/swarma-lease`. The SQLite library and headers must be available from the platform toolchain. |
| GitHub Actions | Check pull requests and pushes to `main` on Ubuntu and macOS | Runs from `.github/workflows/ci.yml`; the hosted job checks formatting, runs `go vet ./...`, and runs `go test -race -count=1 ./...`. | Hosted job results stay in the GitHub Actions run; the workflow uses GitHub-hosted runners. |
| Swarma Lease CLI | Start the service, generate its per-user LaunchAgent plist, and manage resources and leases | `.temp/bin/swarma-lease serve`<br>`.temp/bin/swarma-lease launch-agent`<br>`.temp/bin/swarma-lease resource add --tag qa.pool --id qa-01 --mode manual --requester-id example-caller --json`<br>`.temp/bin/swarma-lease resource add --tag mac.jvm-build --id jvm-build-01 --mode manual --class jvm-build --requester-id example-caller --json`<br>`.temp/bin/swarma-lease resource disable --id qa-01 --requester-id example-caller --json`<br>`.temp/bin/swarma-lease resource enable --id qa-01 --requester-id example-caller --json`<br>`.temp/bin/swarma-lease resource recover --id qa-01 --confirm-stopped --requester-id example-caller --json`<br>`.temp/bin/swarma-lease lease request --tag qa.pool --duration 20m --no-wait --requester-id example-caller --json`<br>`.temp/bin/swarma-lease lease request --tag qa.pool --duration 20m --wait --wait-timeout 4h --request-id req-01 --requester-id example-caller --json`<br>`.temp/bin/swarma-lease lease wait --request-id req-01 --json`<br>`.temp/bin/swarma-lease lease cancel --request-id req-01 --json`<br>`.temp/bin/swarma-lease lease release --id <lease-id> --json`<br>`.temp/bin/swarma-lease queue status --tag qa.pool --json`<br>`.temp/bin/swarma-lease queue status --class go-test --json` | Management commands return one JSON object on stdout. `launch-agent` emits a per-user XML plist to stdout. Tag status reports only the tag, queue count, and oldest qualifying queue timestamp. Class status adds the active lease and queued requester rows without internal IDs or payloads. Blocking commands print only the final result. Service state remains under `~/.swarma-lease/`. |
| macOS shell tools (`install`, `plutil`, and `launchctl`) | Install the stable binary, validate the generated plist, and manage the per-user LaunchAgent | `mkdir -p "$HOME/Library/Application Support/swarma-lease/bin" "$HOME/Library/LaunchAgents"`<br>`install -m 0755 .temp/bin/swarma-lease "$HOME/Library/Application Support/swarma-lease/bin/swarma-lease"`<br>`.temp/bin/swarma-lease launch-agent > "$HOME/Library/LaunchAgents/works.relux.swarma-lease.plist"`<br>`plutil -lint "$HOME/Library/LaunchAgents/works.relux.swarma-lease.plist"`<br>`launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/works.relux.swarma-lease.plist"`<br>`launchctl print "gui/$(id -u)/works.relux.swarma-lease"`<br>`launchctl bootout "gui/$(id -u)/works.relux.swarma-lease"` | The binary is installed under the account's Application Support directory, the rendered plist is stored under `~/Library/LaunchAgents/`, and `plutil` reports syntax validity. `launchctl` acts on the exact per-user service. |
| SQLite database | Persist single-host lease state | Managed by the Go service; no standalone command is required. | `~/.swarma-lease/leases.sqlite`, its WAL files, `service.lock`, and `service.sock`. SQLite is the only source of truth. |

Build artifacts are kept under the ignored `.temp/` directory; runtime state stays under the effective user's system-account home. The service checks every state-directory and custom socket-parent component without following symlinks. On macOS, custom paths rooted under system symlinks such as `/tmp` or `/var` are refused; the default account-home path is resolved from the system account record. Socket paths that exceed the platform Unix-domain address capacity are refused before state creation with `socket_path_too_long`. The command adapter remains out of scope for this slice.

## macOS LaunchAgent

The generator writes a plist for the current effective user's system-account home, with an absolute executable path under `~/Library/Application Support/swarma-lease/bin/`. Install the built binary there, then render and validate the plist:

```sh
mkdir -p "$HOME/Library/Application Support/swarma-lease/bin" "$HOME/Library/LaunchAgents"
install -m 0755 .temp/bin/swarma-lease "$HOME/Library/Application Support/swarma-lease/bin/swarma-lease"
.temp/bin/swarma-lease launch-agent > "$HOME/Library/LaunchAgents/works.relux.swarma-lease.plist"
plutil -lint "$HOME/Library/LaunchAgents/works.relux.swarma-lease.plist"
```

To include the isolated startup/crash-restart check in the race suite on macOS, run:

```sh
SWARMA_LEASE_LAUNCHD_TEST=1 \
SWARMA_LEASE_LAUNCHD_TEST_DOMAIN="user/$(id -u)" \
go test -race -count=1 ./...
```

The fixture uses a unique label and temporary state in `user/<current UID>`; it refuses the GUI domain and foreign-user domains. The test loads the service, crashes it, checks restart, and unloads that exact fixture label.

The agent starts `serve` at load and keeps it alive. It uses the service's existing `~/.swarma-lease/` state directory; it does not create a second database. Do not load it while another service process is using that directory. Keep the current service and its operating procedure in place until cutover authorization is explicitly announced, then use the existing procedure to stop the old process before loading the agent.

Schema v3 is not readable by a pre-v3 binary. To return to a pre-v3 release after migration, stop the service and restore the complete state-directory backup captured before the migration, then use the matching older binary and service procedure. To roll back between v3-capable releases, use a binary that supports schema v3 and preserve a backup of the current state directory.

## License

Apache License 2.0. See [LICENSE](LICENSE).
