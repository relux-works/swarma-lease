# Resource Lease Contract and MVP

Date: 2026-11-13

## Product rule

A resource has one stable ID and one tag. Resources with the same tag form a pool. A request names one tag and a duration, joins that tag's FIFO queue if no resource is free, receives exactly one available resource ID, and releases the lease explicitly or at a fixed hard end. The first use is one Mac host's Go and Swift test slots.

## First-host defaults

The owner fixed these defaults on 2026-11-13:

| Setting | Default |
| --- | --- |
| `mac.go-test` pool | One resource: `go-test-01`. |
| `mac.swift-test` pool | One resource: `swift-test-01`. |
| OS-user boundary | One configured macOS user owns the service, database, socket, and wrapped commands. Other OS users cannot access them. Processes under the same OS user are equivalent; this is not per-agent isolation. |
| Maximum lease duration | 20 minutes. This preserves the current grant limit. |
| Maximum queue wait | 30 minutes. The value is required per waiting request and cannot exceed this cap. |
| Resource reuse after command | Reuse only after the wrapper confirms the command and its descendants have stopped and cleanup succeeds. Otherwise the resource stays unavailable or quarantined. |

These are initial configuration defaults. Adding resources or changing limits later requires an explicit configuration change; service startup never silently increases a pool or extends a request.

## MVP boundary

The first implementation slice includes one Go service and CLI on one host, one local SQLite database, one local Unix-domain socket, resource registration/disable/enable/recovery, exact-tag requests, strict per-tag FIFO, blocking wait, explicit release, fixed hard end, status, audit history, and restart recovery.

The first slice does not execute commands. The two default Mac test pools are registered as `command` pools, and raw `lease request` calls for them refuse with `adapter_required` before creating a waiter or lease. This keeps those pools closed until the later command-adapter slice implements the stop-and-cleanup contract below. Core scheduling can be used with explicitly registered `manual` pools; these pools do not launch or supervise commands.

The next implementation slice adds `lease run` for the Go and Swift test pools. It starts the supplied argument vector without shell interpretation, enforces the lease hard end, and returns a command pool to service only after stop and cleanup are confirmed.

Multi-host allocation, network-filesystem state, priorities, preemption, multiple-resource requests, arbitrary capability expressions, recurring bookings, application credentials, remote TCP access, automatic hardware reset, and lease heartbeat/renewal are excluded. Processes running as the same configured macOS user are one authority boundary; the service does not isolate agents from each other.

## Names and admission

Accept tag and resource ID values only when they match `^[a-z0-9]+([._-][a-z0-9]+)*$`. This allows lowercase ASCII letters and digits separated by one `.`, `_`, or `-`; first and last characters are alphanumeric. Tags are case-sensitive and compared byte-for-byte. IDs are globally unique. No paths, URLs, whitespace, shell syntax, Unicode lookalikes, or empty values are accepted.

| Form | Expected result | Reason |
| --- | --- | --- |
| `mac.go-test` | Accept | Dotted tag for the first Mac Go test pool. |
| `mac.swift-test` | Accept | Dotted tag for the first Mac Swift test pool. |
| `qa.device` | Accept | Dotted tag for a manual device pool. |
| `qa.pool` | Accept | Explicit manual-pool example. |
| `go-test-01` and `swift-test-01` | Accept | Hyphenated resource IDs for the defaults. |
| `qa-01` | Accept | Hyphenated resource ID for a manual pool. |
| `ci_runner-01` | Accept | Underscore/hyphen form for a resource ID. |
| `Mac Go Test` | Reject | Uppercase and whitespace are outside the canonical form. |
| `../private` | Reject | Path traversal syntax is not a tag or ID. |
| `https://host/path` | Reject | URLs are not tag or ID values. |
| `x;command` | Reject | Shell metacharacters are not accepted. |
| Empty string or repeated separators | Reject | Empty components are invalid. |

The first slice must keep this boundary aligned with every legitimate tag and ID emitted by registration and configuration. Invalid syntax fails before state changes. A valid manual tag with no currently available resource may wait; an unknown tag fails immediately.

`resource add --tag TAG --id ID --mode manual|command` requires all three values; there is no implicit pool mode. A new tag is registered with the specified mode. Adding another resource to an existing tag requires the same mode; a mode mismatch or duplicate resource ID refuses before writing. A mode change requires an explicit configuration change while the pool is idle.

## Service and local state

The service runs in the foreground with `swarma-lease serve`. It listens only on a local Unix-domain socket; clients do not poll the database or use TCP. Ctrl+C or SIGTERM stops the service from accepting new work and preserves durable state for restart recovery.

The default state directory is `~/.swarma-lease`. It contains `config.json`, `leases.sqlite`, and `service.sock`. On a fresh install, the config defaults are:

```json
{
  "version": 1,
  "max_lease_duration": "20m",
  "max_wait_timeout": "30m",
  "pools": [
    {"tag": "mac.go-test", "mode": "command", "resources": ["go-test-01"]},
    {"tag": "mac.swift-test", "mode": "command", "resources": ["swift-test-01"]}
  ]
}
```

The directory is created with mode `0700`; the configuration, database, WAL files, and socket use mode `0600`. Startup refuses an existing path with broader permissions instead of silently changing its mode. State remains on local storage. A second live service instance refuses to start; startup never replaces a live or unverified socket path. A fresh state directory seeds the listed pools once. An existing configuration/database pair is not overwritten or silently expanded; a missing half of that pair is an inconsistent state and startup refuses it.

Only the configured macOS user may connect. A socket or database access attempt from another OS user is refused by the filesystem boundary. The service does not create a second user, group, application credential, or per-agent identity. All processes under the configured user share lease authority.

## State model

| Object | States and transitions |
| --- | --- |
| Resource | Manual pool: `available → leased → available` on release or hard end. Any pool: `available → disabled → available` through `resource disable` and `resource enable`. Command pool: `available → leased → cleanup_pending`; `cleanup_pending → available` only after stop and cleanup are confirmed; `cleanup_pending → quarantined` when cleanup fails or is uncertain. A quarantined command resource returns to `available` only through explicit operator recovery after confirming the command stopped and the resource is safe. |
| Request | `queued → leased`; `queued → cancelled`; `queued → timed_out`. Enqueue IDs and sequence numbers never change. |
| Lease | `active → released`; `active → expired`. Both terminal states remain in history. Releasing an already-ended lease from any process under the configured OS user is idempotent. |

The store enforces one active lease per resource and one result per idempotency key. Each resource has one primary tag, and each tag has one immutable mode (`manual` or `command`) while it has resources. All resources registered to a tag must use that mode. This prevents overlapping queues from racing to allocate the same resource and makes FIFO semantics unambiguous.

## Queue and fairness

1. A request is validated before state changes. A waiting request is persisted before it is acknowledged as queued. The caller must supply exactly one of `--wait` or `--no-wait`; neither is implicit, and supplying both or neither is a syntax refusal with no state change.
2. The service assigns a strictly increasing sequence number within the tag queue in the same transaction that creates the waiter.
3. Allocation runs after enqueue, registration, release, cancellation, timeout, expiry, and recovery. If the queue is nonempty, a new request cannot bypass its head to take a free resource.
4. The service grants the oldest waiter the first available matching resource, ordered by stable resource ID for deterministic selection. It grants one resource only.
5. There are no priority or reverse-order controls. Separate tags have independent queues. Multi-tag requests are out of scope because they add head-of-line and deadlock policy questions.
6. `--wait-timeout` is required with `--wait`, must be positive, and cannot exceed 30 minutes. It is refused with `--no-wait`; there is no default wait duration. Expiry marks the waiter timed out and grants the next waiter. The client blocks for an event response over the local socket; it does not poll resource state.
7. Raw requests to a `command` pool refuse as `adapter_required`, whether waiting or immediate; they create neither a waiter nor a lease. A `manual` pool is required for raw requests in the first implementation slice.

## Lease, hard end, and cleanup

- `duration` is required and positive. The service rejects a duration over the configured 20-minute maximum; it never silently clamps or extends the request.
- `hard_end_at = granted_at + duration`. The grant response includes the hard-end time and a persistent per-resource fencing token, incremented on every grant. Adapters that can reject stale work check this token before acting.
- The MVP has no heartbeat or renewal. A requester that needs more time must release and request again, taking a new FIFO position.
- For a `manual` pool, release or hard end ends the lease and makes the resource available. The service does not claim to start, stop, or supervise a process for a manual lease.
- For a `command` pool, a lease is acquired only by the later `lease run` adapter. At hard end, lease entitlement ends and the resource becomes `cleanup_pending`, not `available`, until the adapter confirms that the command and descendants stopped and cleanup succeeded. The adapter runs an argument vector without shell interpretation. Normal command exit releases the lease after stop is confirmed. At hard end, the adapter requests command termination and waits for proof that the full command process tree has stopped. If stop proof or cleanup is unavailable, it returns `command_unconfirmed` and leaves the resource quarantined; the next waiter is not granted that resource.
- Raw `lease request` always refuses for a `command` pool, including the default Mac test slots. This prevents callers from bypassing the adapter.
- After service restart, an active unexpired lease remains leased until its recorded hard end. The service restores queued sequence numbers before new grants. An overdue manual lease expires before allocation resumes. A command resource with uncertain stop state stays unavailable or quarantined until the adapter or explicit operator recovery confirms it safe. A backward wall-clock change must never make a lease end before its persisted hard-end time.
- `resource recover --id ID --confirm-stopped` is an explicit same-user operator action. It refuses unless the resource is quarantined and there is no active lease. The operator must first confirm the command and cleanup state outside the service; an unconfirmed resource remains quarantined.

## Storage and restart

SQLite is the single-host source of truth. Use short write transactions for enqueue, grant, release, expiry, and recovery. A grant changes the waiter, lease, and resource in one transaction; storage errors fail closed with no partial grant. WAL may be enabled for local readers, but the database must stay on local storage and never be shared over a network filesystem.

On restart, active lease hard ends and queued request sequence numbers remain durable. The service expires overdue manual leases and restores the queue before accepting new grants. A command resource is not freed unless stop and cleanup are confirmed. If a waiting client's socket closes while the service is up, its waiter is cancelled. If the service becomes unavailable while the client waits, the client receives `daemon_unavailable` with its request ID and may reconnect with `lease wait --request-id ID --wait-timeout ...`. Queue wait deadlines prevent an abandoned request from blocking the head forever.

SQLite/WAL does not coordinate multiple hosts. When multi-host support is scheduled, replace the local store behind the backend boundary with a central linearizable store/service. Preserve global resource IDs and per-resource fencing tokens; never infer cross-host safety from a shared file or an eventually consistent cache.

## CLI and JSON

The service is started with `swarma-lease serve`. The first-run configuration seeds one Go test resource and one Swift test resource with the defaults above; it does not overwrite an existing configuration.

```text
swarma-lease serve
swarma-lease resource add --tag qa.pool --id qa-01 --mode manual --json
swarma-lease resource add --tag qa.pool --id qa-02 --mode manual --json
swarma-lease lease request --tag qa.pool --duration 20m --wait --wait-timeout 30m --request-id req-example --json
swarma-lease lease request --tag qa.pool --duration 20m --no-wait --json
swarma-lease lease release --id <lease-id> --json
swarma-lease lease status --id <lease-id> --json
swarma-lease resource disable --id qa-01
swarma-lease resource enable --id qa-01
swarma-lease resource recover --id go-test-01 --confirm-stopped
```

`resource add` requires `--mode manual|command`; resources under the same tag must use one mode. In the first implementation slice, the Mac defaults have mode `command` but cannot be leased until the command-adapter slice is available. For manual pools, a request returns one resource ID after the grant is committed. If the daemon becomes unavailable during a blocking request, use the same request ID with `lease wait --request-id req-example --wait-timeout 30m --json` to reconnect while the original queue deadline remains active.

The later command-adapter slice adds this entry point:

```text
swarma-lease lease run --tag mac.go-test --duration 20m --wait --wait-timeout 30m --request-id req-example -- go test ./...
```

`lease run` passes the argument vector after `--` directly to the command, with no shell interpretation. It forwards the command's stdout and stderr and returns the command's exit code only after confirmed stop and cleanup. A nonzero command exit still releases the slot when the command stopped and cleanup succeeded. A retry with the same request ID and identical command input returns the recorded run result without starting a second command.

Successful acquisition returns one JSON object:

```json
{
  "version": 1,
  "state": "leased",
  "request_id": "req-example",
  "lease": {
    "id": "lease-example",
    "tag": "qa.pool",
    "resource_id": "qa-01",
    "granted_at": "2026-11-13T17:00:00Z",
    "hard_end_at": "2026-11-13T17:20:00Z",
    "fencing_token": 1
  }
}
```

JSON errors have a stable code, message, and request ID when one exists, for example `{"version":1,"state":"error","code":"unknown_tag","message":"tag is not registered"}`. Timestamps are RFC 3339 UTC. The client must not print a lease as granted until the transaction is committed and the service has sent the response.

`--request-id` is an optional caller-supplied idempotency key; when omitted, the CLI generates one before submitting. Retrying the same request ID with identical inputs returns the original queued request or lease. Reusing it with different inputs refuses as `request_id_conflict`. The wait command reconnects to an existing queued request; it does not create a second waiter. Lease authority is the configured OS user: any process under that user may release a known lease, and the service does not provide per-agent ownership checks.

| Exit code | Meaning |
| ---: | --- |
| `0` | Lease operation succeeded. For `lease run`, a successfully stopped command returns its own exit status; the codes below describe lease-service outcomes. |
| `2` | Invalid syntax, malformed ID, or nonpositive duration/wait timeout. |
| `3` | Unknown tag, resource ID, or lease ID. |
| `4` | Queue wait timed out or was cancelled; no active lease remains for that request. |
| `5` | Policy refusal: duration or wait over maximum, pool disabled, adapter required, request ID conflict, unconfirmed command stop, or invalid transition. |
| `69` | Service unavailable or storage could not be read/written; no new lease is issued. |
| `75` | Immediate `--no-wait` request found no free resource; no waiter was queued. |

## Refusals and invariant checks for implementation

This table freezes the full contract. The first implementation slice tests the manual-pool and core-service behavior, including raw-request refusal for command pools. Cases that require stopping or cleaning up a child command are tested with the later command-adapter slice.

| Case | Required result and state/effect |
| --- | --- |
| Valid known tag with one free resource | Grant exactly one resource and persist the active lease before returning success. |
| Valid known tag with all resources leased | Persist one FIFO waiter; later resource release wakes the head without polling. |
| Unknown tag/resource/lease ID or malformed tag/ID | Refuse before writing; valid neighboring forms still work. |
| Duplicate resource ID or adding a resource with a mode different from its tag | Refuse before changing the pool; a unique ID with the matching mode remains accepted. |
| Invalid/nonpositive duration, duration above 20 minutes, or wait timeout that is nonpositive or above 30 minutes | Refuse before writing a waiter or lease; valid boundary values remain accepted. |
| Missing or conflicting `--wait`/`--no-wait`; wait timeout missing with `--wait` or present with `--no-wait` | Refuse as invalid syntax; no waiter or lease is written. An explicit valid mode remains accepted. |
| `--no-wait` on a busy manual pool | Return `75`; do not queue. A waiting request with a valid timeout remains accepted. |
| Request on a disabled pool | Refuse without a grant or waiter; an enabled pool with capacity remains available. |
| Duplicate request ID with identical input | Return the existing request/lease; do not allocate twice. |
| Duplicate request ID with different input | Refuse as `request_id_conflict`; do not alter the existing request. |
| Raw request for any command-mode tag, including either Mac default | Refuse as `adapter_required`; no waiter or lease is created. A manual-mode tag remains available through the same request API. |
| Release another lease from the configured OS user's process | Permit the release because all same-user processes share authority; a caller under another OS user cannot open the socket and changes no state. |
| Release an unknown lease ID | Refuse as `unknown_lease`; resource state is unchanged. |
| Repeat release by any process under the configured OS user | Return success with the terminal state; do not free a different active lease. |
| Enable a resource that is not disabled, or recover a resource that is not quarantined | Refuse without changing resource or lease state; valid adjacent transitions remain available. |
| Disable a leased resource | Refuse without changing its lease; a free resource may be disabled and then re-enabled. |
| Service unavailable or SQLite transaction failure | Return an error without a new grant, partial queue entry, or partial resource update; valid requests work after service/storage recovery. |
| Existing state directory has invalid permissions, an invalid config, or only one of config/database | Refuse startup without silently changing permissions, overwriting config, or reseeding resources; a valid fresh directory starts with the two defaults. |
| Cancellation racing with grant | Serialize the outcome: either cancellation wins with no lease, or grant wins and the service releases an unclaimed lease before acknowledging cancellation. Never leave a phantom allocation. |
| Database write failure during grant | Return service error; no partial lease or resource transition. |
| Hard end with successful cleanup | Expire the lease, prove the command stopped, then make the resource available and grant the oldest waiter. |
| Hard end with failed/unknown cleanup | Expire the lease and quarantine the resource; do not grant it to another requester. |
| Service restart with active/unexpired lease | Preserve the lease until its recorded hard end. |
| Service restart with overdue lease | Recover expiry before new grants; cleanup/quarantine before reuse. |
| Recovery without `--confirm-stopped` or while a lease is active | Refuse without changing the resource. |
| State directory, database, or socket permissions are broader than required | Refuse without changing permissions or creating a service endpoint. |

These are implementation acceptance cases, not test results. The first slice must add executable positive and negative tests before it is considered ready; each test comment must say the exact invariant it proves.

## First production slice

The first implementation slice is the Go CLI and service for a single Mac host, SQLite-backed resource and lease state, blocking local-socket wait, strict per-tag FIFO, hard-end handling for manual pools, restart recovery, and the one-user boundary. Its acceptance target is a valid manual-pool acquisition/release path plus the core-service refusal and recovery cases above. Both default Mac test pools are registered in command mode and reject raw requests as `adapter_required`; the command wrapper is a later slice. The Swift slot uses the same service and later command-adapter path under its own tag.
