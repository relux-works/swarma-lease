# Resource Leasing Research

Date: 2026-11-13

## Decision to unblock

Choose a first implementation path for a local resource registry and lease queue that can replace the current manual request/grant/done exchange for Mac build and test slots, while leaving a clean route to multiple hosts later.

## Recommendation

Build a small Go module with a single local service, a durable SQLite store, and a CLI that waits over a local Unix-domain socket. Keep one exact tag per resource, one stable resource ID, one resource per request, a durable sequence number per tag queue, and a fixed hard end. Do not add priorities, multi-resource requests, capability queries, heartbeats, or distributed consensus in the first slice.

For the first Mac integration, use the manual-pool service slice in the [MVP contract](contract.md); it supports blocking FIFO requests and release but does not execute build/test commands. The default Go and Swift test pools remain command-mode and refuse raw requests with `adapter_required`. A later command adapter must implement the hard-end stop and cleanup contract before those pools open, so the existing hand-granted slot gate stays in force until that adapter and the configured pool are accepted.

The first slice should use SQLite transactions for state changes and a local socket for request/notification. SQLite provides serializable transactions with one writer at a time. Its WAL mode can let readers and a writer proceed concurrently, but WAL requires all database users to be on one host, so it is suitable for a local file and unsuitable as a multi-host shared database. Keep the store behind a small backend interface; choose a central linearizable service only when multi-host leasing is actually scheduled.

## Prior-art comparison

| System or primitive | What it provides | Fit and gap for this module |
| --- | --- | --- |
| etcd leases and locks | A lease-bound lock, release, automatic lock release when the attached lease expires, and notification of the next waiting lock caller. | Strong model for future distributed coordination. It requires a running etcd cluster and exposes locks/keys rather than the required general resource catalog, tags, exact ID selection, and CLI contract. Keep as a later backend candidate, not an MVP dependency. |
| Consul sessions and KV locks | Sessions bind a node, health checks, TTL, and lock-delay to KV lock operations. Session invalidation can release or delete held keys. | Useful failure and recovery lessons. Health checks may release a lock after a false positive; TTL expiration is a lower bound and may happen later; locks are advisory. That does not give the first Mac user a strict hard end without a separate resource reclaimer. Do not install Consul just to replace a local file handshake. |
| Chubby | A coarse-grained lock service with reliable small-file storage, event notification, sessions, and sequencers for protecting external resources. | The paper highlights why a lease alone cannot stop an old holder from continuing to act: external operations need fencing/sequencing. Chubby is a distributed service design reference, not an available dependency for this small module. |
| macOS `flock` and lock files | A kernel advisory shared/exclusive lock on a file; callers can block or request nonblocking failure. | Useful for a cooperating single-daemon guard. It does not define a durable resource inventory, tag pools, queue tickets, lease records, or hard-end cleanup. Build those above it rather than treating one lock file as the product. |
| POSIX semaphores and Go condition variables | A process/thread can block until a count or condition changes. | These are wake-up/count primitives, not a persistent queue of identified resources. The Go `Cond.Signal` contract does not give a waiting goroutine scheduling priority. Store explicit ticket order and grant in that order; use notifications only to wake the already-selected waiter. |
| SQLite | Atomic transactions, serialized writers, and optional WAL for local reader/writer concurrency. | Best match for one Mac host: persistent queue and lease state without introducing a service cluster. Keep the database on local storage. WAL is not a cross-host coordination mechanism. |
| Jenkins Lockable Resources | Named resources and labels, quantities, waiting builds, timeouts, resource selection, FIFO by default, and configurable priority or reverse precedence. | The closest CI analogue. It can fit if a Jenkins controller is already the product boundary. This module must also serve non-Jenkins requests, device inventories, and local commands, so adopting a Jenkins plugin would make the module platform-dependent. |
| GitLab resource groups | A job-level mutex with `unordered`, `oldest_first`, `newest_first`, and `newest_ready_first` process modes. | Useful for serializing jobs in that CI platform. It does not return a stable resource ID or coordinate general local callers; the modes also demonstrate that “fair” must be specified rather than assumed. |
| GitHub Actions concurrency and runner groups | Concurrency groups limit overlapping workflow/job runs; runner groups route jobs to selected runner sets and manage access/capacity. | Suitable for GitHub-hosted workflow policy. The default concurrency queue replaces an older pending run; `queue: max` permits at most 100 pending entries, and documented FIFO is based on actual waiting start time with no guaranteed run order. Runner groups route CI jobs, not arbitrary local resource requests. |
| Buildkite agent queues | Route jobs to agent groups; self-hosted jobs use FIFO by default, while configured priorities affect dispatch order. | Useful for assigning jobs to a CI worker pool. It does not provide a cross-tool device/resource lease API or a standalone local registry. |
| DeviceFarmer/STF | A device inventory and REST operations to use/reserve and release devices, with booking/partitioning features for device labs. | Relevant if the product later manages a large Android-focused device lab. It is a full web/device-farm system with its own deployment, identity, and lifecycle model, not a lightweight general registry for Go/Swift slots, CI runners, and any other resource. |
| Xcode Device Hub and Xcode Cloud | Device Hub manages physical/simulated Xcode run destinations; Xcode Cloud runs tests across selected device configurations in Apple's hosted CI. | Useful for Apple-platform testing and device configuration. It is not a general local queue for multiple agents sharing named resources across tools. |
| AWS Device Farm | Static or rule-based device pools and scheduled test runs on managed devices; job timeouts bound a test job. | A managed cloud option when remote device testing is the goal. It is not local-first, has service/billing boundaries, and cannot allocate arbitrary Mac slots or runners on the first host. |
| Nomad device plugins | Discover/fingerprint device groups and reserve devices for scheduled tasks. | Relevant if the product later becomes part of a Nomad cluster. The device plugin API does not monitor the task after provisioning and does not itself provide this module's interactive local FIFO lease contract. |
| Kubernetes device plugins | Advertise integer device resources to the scheduler and allocate exclusive devices to Pods. | Strong container-cluster integration, but requires a kubelet plugin and privileged host integration. It is disproportionate for one Mac's local build slots and does not supply the desired CLI waiter semantics. |

## Implications

1. Keep fairness explicit: assign each accepted waiter a durable sequence number and allocate only from the head of that tag's FIFO queue. No priority or newest-first mode in the MVP.
2. A lease expiry changes the holder's entitlement; it does not magically stop a command or reset a physical device. The later command adapter must supervise the command through expiry. A device adapter that cannot prove cleanup must quarantine the device rather than hand it to the next waiter.
3. Use one local service as the queue authority. Clients block on a socket response; they do not poll the database or sleep-loop looking for a free slot.
4. Preserve the queue sequence and active hard-end timestamps in durable storage. On restart, restore the queue, expire overdue leases, and never issue a lease when storage state is uncertain.
5. Multi-host allocation needs a new decision: a central strongly consistent backend and monotonically increasing fencing tokens. A SQLite WAL file on a network share is not an acceptable shortcut.

## First production slice

Implement the first slice in the [MVP contract](contract.md): a single-host Go service and CLI with SQLite, a local socket, manual pools, FIFO wait, release, hard ends, restart recovery, and audit/status. It does not execute commands. The default Mac test pools remain command-mode and refuse raw requests with `adapter_required`. The later `lease run` slice may supervise the command pools only after a separate reviewed process identity, descendant-discovery, and cleanup contract is accepted. This research phase creates no production code and runs no Go or Swift build.

## Open questions for the product owner

First-host decisions are settled in the [MVP contract](contract.md): one `go-test-01` resource under `mac.go-test`, one `swift-test-01` resource under `mac.swift-test`, one configured macOS user, a 20-minute maximum lease, a 30-minute maximum queue wait, and no resource reuse until command/descendant stop and cleanup are confirmed.

- Which exact process-identity and descendant-stopping method should the later `lease run` contract use, including reparented descendants and cleanup failure recovery?
- When multi-host support is scheduled, which central linearizable backend should own global allocation state and fencing tokens?

## Sources

- etcd, [concurrency API reference](https://etcd.io/docs/v3.7/dev-guide/api_concurrency_reference_v3/) and [lease API](https://etcd.io/docs/v3.7/learning/api/).
- HashiCorp Consul, [sessions and distributed locks](https://developer.hashicorp.com/consul/docs/automate/session) and [`consul lock`](https://developer.hashicorp.com/consul/commands/lock).
- Mike Burrows, [The Chubby Lock Service for Loosely-Coupled Distributed Systems](https://www.usenix.org/conference/osdi-06/chubby-lock-service-loosely-coupled-distributed-systems), USENIX OSDI 2006.
- Apple, [macOS `flock(2)`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/flock.2.html) and [`sem_wait(2)`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/sem_wait.2.html); Go, [`sync.Cond`](https://pkg.go.dev/sync#Cond).
- SQLite, [isolation](https://www.sqlite.org/isolation.html) and [write-ahead logging](https://sqlite.org/wal.html).
- Jenkins, [Lockable Resources Pipeline step](https://www.jenkins.io/doc/pipeline/steps/lockable-resources/); GitLab, [resource groups](https://docs.gitlab.com/ci/resource_groups/); GitHub, [workflow concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency) and [runner groups](https://docs.github.com/en/actions/concepts/runners/runner-groups); Buildkite, [agent queues](https://buildkite.com/docs/agent/queues) and [job priority](https://buildkite.com/docs/pipelines/configure/workflows/job-priority).
- DeviceFarmer, [STF API](https://github.com/DeviceFarmer/stf/blob/master/doc/API.md) and [project README](https://github.com/DeviceFarmer/stf); Apple, [Device Hub](https://developer.apple.com/documentation/xcode/device-hub) and [Xcode Cloud](https://developer.apple.com/xcode-cloud/); AWS, [device pools](https://docs.aws.amazon.com/devicefarm/latest/developerguide/how-to-create-device-pool.html).
- HashiCorp Nomad, [device plugins](https://developer.hashicorp.com/nomad/plugins/author/device); Kubernetes, [device plugins](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/).
