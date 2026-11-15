# macOS Command Adapter Process Contract

Contract version: `mac-command-process/v1`.

This contract defines process identity, stop and cleanup requirements for the later macOS command adapter described in the [MVP contract](contract.md). It specifies behavior; it does not claim that the adapter exists or that command-mode leases are enabled.

## Supported boundary

V1 applies only to the configured `mac.go-test` and `mac.swift-test` command profiles, under the one configured macOS user. It adds no CLI, wire-protocol, or configuration fields. The adapter receives the already accepted command as an argument vector, passes it directly to the fixed helper without a shell, and does not persist or audit argv or inherited environment. Command text, executable names, and paths are never used to find or target a running process.

A supported host must provide the domain/service-target form of `launchctl bootout --wait`. If the installed command does not accept that exact form, command-mode acquisition is refused. The blocking wait runs outside the request loop so status and unrelated resource tags remain serviceable; a blocked or incomplete cleanup never releases the resource.

The process-group mechanism below is not a sandbox. It cannot contain a descendant that leaves the job's process group, and it cannot prevent another process in the same session from joining that group. V1 is therefore limited to trusted test profiles whose execution stays in the launchd job's process group. It does not support hostile workloads, arbitrary commands, daemonizing descendants, or workloads that require `setpgid`/`setsid`. If any such workload must be supported, command mode remains disabled until a stronger macOS containment mechanism is selected and separately reviewed.

## Attempt identity

Each command attempt has a random, never-reused attempt ID and an exact launchd service target made from the configured per-user bootstrap domain and a unique per-attempt service label. The label is the administrative identity and the only stop target. A label collision, missing domain, or inability to confirm the exact target refuses acquisition; an existing label is never taken over.

The launchd job invokes only the fixed adapter helper through `ProgramArguments`. The command argument vector travels through a launchd-managed local Unix socket with an explicit user-only mode (`0600`, plist integer `384`); the helper binds each request to the attempt ID. Launchd passes the socket descriptor to the helper, which passes argv directly to the command runner. This is a one-user boundary, not isolation among processes of that user. The job configuration must set `KeepAlive=false`, `AbandonProcessGroup=false`, and `ExitTimeOut=20` seconds explicitly. It must not place command arguments or inherited environment in the plist, lease database, audit record, or logs. The helper must verify before command start that its process group does not match the coordinator or another active attempt and contains no other process; otherwise it refuses the attempt. This startup check does not prevent a same-session process from joining later, which is why hostile same-user workloads are outside V1.

Before command start, record the helper's current process observation tuple: PID, process start seconds and microseconds, UID, and process-group ID. XNU's `proc_bsdinfo` exposes those fields. A PID without its start-time tuple is not an identity. The tuple rejects stale observations after PID reuse, but it is a snapshot, not an atomic process handle; it never authorizes a PID-directed signal.

## Durable attempt state

The current service schema v1 remains manual-only and is not modified in place. Before command-mode implementation, a separate, explicitly versioned v1-to-v2 SQLite migration must add a `command_attempts` table. Startup never silently upgrades an existing database; command acquisition remains refused until the migration is installed and its exact schema is verified.

Each `command_attempts` row contains: `attempt_id` (128 random bits, lowercase hex, primary key, never reused), `lease_id` (unique reference to the lease), `launchd_domain`, `service_label` (unique with the domain), `helper_pid`, `helper_start_sec`, `helper_start_usec` (0 through 999999), `helper_uid`, `process_group_id`, `state` (`starting`, `running`, `cleanup_pending`, `quarantined`, or `stopped`), `stop_receipt_version` (null until proof succeeds, then 1), and nullable `group_empty` and `cleanup_complete` booleans. The receipt fields remain null before stop proof and are set together to version 1 and true only after every stop and cleanup check succeeds. A `stopped` attempt requires that complete receipt. The row is written as `starting` before the helper receives argv and updated to `running` only after the helper tuple is recorded. It never contains argv, inherited environment, or child output. Process IDs are internal recovery metadata and never appear in audit or user-facing output.

The successful stop transaction atomically changes the attempt to `stopped` with receipt version 1, records `group_empty=true` and `cleanup_complete=true`, changes the lease to its terminal result, and makes the resource `available`. A transaction failure leaves the prior durable state fenced. A crash or restart that finds `starting`, `running`, or `cleanup_pending` without this receipt sets the resource to `quarantined`; absence of a receipt is never repaired by inference. A valid receipt is bound to exactly one attempt ID and launchd service target.

## Descendant discovery and stop

Parent-chain walking is insufficient: macOS changes a child's parent ID after its parent exits, while process-group membership is a separate field. Enumerate current members of the recorded process group with `proc_listpids(PROC_PGRP_ONLY, ...)` and read each current tuple with `proc_pidinfo`. This finds a reparented descendant that remains in the attempt group. If a process-info read races with exit or PID reuse, retry from a fresh enumeration; if a complete consistent view cannot be obtained, stop proof is unavailable.

On normal command exit, explicit release, or hard end:

1. Durably move the attempt and its resource to `cleanup_pending` before requesting any stop. The lease remains fenced from new grants until the successful stop transaction records its terminal result.
2. From the lease coordinator, which runs outside the attempt group, request `launchctl bootout --wait` for the exact `<domain>/<label>` service target. Do not address a PID, process-group number, executable name, command line, or path. The installed `launchctl help bootout` warns that `--wait` can block indefinitely; while it waits, the resource stays unavailable.
3. Require successful bootout completion, complete process-group enumeration, and no remaining member in the attempt group. An empty or missing result caused by a failed, truncated, or inconsistent query is not proof. An unexpected member is not targeted separately; it keeps the resource unavailable.
4. Remove the attempt's temporary IPC and launchd configuration. If any cleanup step fails, keep the resource unavailable.
5. In one durable transaction, persist the stop receipt bound to the attempt ID and exact service label, record the lease's terminal result, mark the attempt `stopped`, and make the resource `available`. A transaction failure leaves the prior durable state fenced.

The `AbandonProcessGroup=false` launchd rule terminates remaining members of the job's process group when the job dies. `bootout --wait` completion and a complete empty-group observation are both required; neither the command's exit nor the bootout request alone proves that the process tree stopped. The receipt is internal recovery state, not an audit payload; it contains no argv, environment, command output, boot-session UUID, or monotonic clock value.

## Refusal, crash, and restart behavior

Any failed or uncertain step—bootout refusal, a surviving group member, incomplete process enumeration, PID/start-time mismatch, helper death, IPC or configuration cleanup failure, or inability to persist the stop receipt—leaves the resource in `cleanup_pending` or quarantined. It never wakes a waiter or creates a second grant.

If the adapter or lease service dies before the receipt is durable, restart preserves the resource as unavailable. A missing launchd job, an empty best-effort process list, or an absent helper is not a substitute for the receipt. This contract defines no automatic recovery from a missing receipt; a separate, reviewed recovery decision is required.

## Required acceptance cases

These are implementation requirements, not claims that tests have run. Run them only in isolated disposable fixtures. Each negative case must assert the refusal and unchanged resource/waiter state, with a valid positive control.

| Case | Setup | Required result |
| --- | --- | --- |
| Positive full-tree stop | A command creates a child and grandchild; the parent exits first, leaving the reparented grandchild in the attempt process group. | Exact service-target bootout completes; complete group enumeration reaches empty; cleanup and durable receipt succeed before the resource becomes available. |
| Reparented survivor | Keep the grandchild alive after its parent exits. | Group enumeration still finds it despite the changed parent ID. The resource remains unavailable until it exits and all proof succeeds. |
| Reused PID | Present a prior PID with a different start-time tuple, UID, or process group. | Treat the old observation as stale; never target that numeric PID. The attempt's unique service label remains the only stop target. |
| Same-name unrelated process | Run a separate process with the same executable basename outside the attempt group. | Exact service-target bootout leaves it untouched; process names and command lines are not queried for targeting. |
| Missing or partial proof | Make a process query fail, truncate, or return inconsistent identity data. | Refuse completion; preserve the fence and do not wake a waiter. |
| Cleanup failure | Make removal of the attempt's IPC or launchd configuration fail after stop proof. | Keep the resource unavailable; do not write a successful stop receipt or wake a waiter. |
| Adapter death | End the helper before it commits the stop receipt. | Keep the attempt unavailable; helper absence is not proof. |
| Restart without proof | Restart with an attempt in `cleanup_pending` and no valid durable receipt. | Keep the resource unavailable or quarantined; do not infer success from the missing job or an empty best-effort observation. |
| Schema migration | Migrate a valid v1 database and verify all pre-existing lease state is preserved in v2. | A missing or malformed v2 attempt table refuses command acquisition and leaves the database and manual-pool state unchanged. |

The reparented-descendant control proves coverage only while the descendant remains in the job group. No acceptance case may be described as proving containment of a process that escapes that group.

## Platform references

- [Apple launchd plist reference: `ProgramArguments`](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5#L1455-L1474), [`KeepAlive`](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5#L1512-L1534), [`ExitTimeOut`](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5#L1615-L1621), [`AbandonProcessGroup`](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5#L1870-L1877), and [`Sockets`](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5#L1930-L1946).
- [Apple XNU `proc_bsdinfo`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/proc_info.h#L2251-L2307) and [process listing wrappers](https://github.com/apple-oss-distributions/xnu/blob/main/libsyscall/wrappers/libproc/libproc.c#L2284-L2323).
- Apple's [`intro(2)` process overview](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/intro.2.html#L388-L405), [`setsid(2)`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setsid.2.html), and [`setpgid(2)`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setpgid.2.html).
- [Apple `kqueue(2)` process events](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/kqueue.2.html#L225-L250) and [XNU's event definitions](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/event.h). `NOTE_FORK` alone does not provide the child PID and is not a process identity or containment mechanism.
