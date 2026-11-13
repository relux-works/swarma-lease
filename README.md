# Swarma Lease

Swarma Lease is being designed as a Go module for registering shared resources and leasing them to one requester at a time. The planned model gives each resource a stable ID and a tag that places it in a pool. A request asks for one resource from a tag pool for a bounded duration; queued requests are served fairly, and a lease ends when it is released or reaches its hard end.

## Status

Research and MVP definition are in progress. The module, CLI, and lease service are not implemented yet.

## Development tools

- Go is the planned implementation and test tool. There is no project build, test, or run command until the first implementation is added.
- No executable outputs are produced in the current research stage.

## License

Apache License 2.0. See [LICENSE](LICENSE).
