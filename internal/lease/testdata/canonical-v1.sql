CREATE TABLE settings (
  singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),
  schema_version INTEGER NOT NULL CHECK(schema_version=1),
  owner_uid INTEGER NOT NULL,
  max_lease_ns INTEGER NOT NULL CHECK(max_lease_ns=1200000000000),
  max_wait_ns INTEGER NOT NULL CHECK(max_wait_ns=1800000000000),
  boot_session_id TEXT NOT NULL CHECK(length(boot_session_id)=36),
  created_at TEXT NOT NULL
);
CREATE TABLE pools (
  tag TEXT PRIMARY KEY COLLATE BINARY,
  mode TEXT NOT NULL CHECK(mode IN ('manual','command')),
  next_queue_sequence INTEGER NOT NULL CHECK(next_queue_sequence>=1),
  created_at TEXT NOT NULL
);
CREATE TABLE resources (
  id TEXT PRIMARY KEY COLLATE BINARY,
  tag TEXT NOT NULL REFERENCES pools(tag),
  state TEXT NOT NULL CHECK(state IN ('available','leased','disabled','cleanup_pending','quarantined')),
  fencing_token INTEGER NOT NULL CHECK(fencing_token>=0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE requests (
  request_id TEXT PRIMARY KEY COLLATE BINARY,
  tag TEXT NOT NULL,
  duration_ns INTEGER NOT NULL CHECK(duration_ns>0),
  wait_mode TEXT NOT NULL CHECK(wait_mode IN ('wait','no_wait')),
  wait_timeout_ns INTEGER CHECK(wait_timeout_ns>0),
  queue_sequence INTEGER CHECK(queue_sequence>=1),
  queued_at TEXT,
  wait_deadline_at TEXT,
  queue_boot_session_id TEXT CHECK(queue_boot_session_id IS NULL OR length(queue_boot_session_id)=36),
  queue_started_monotonic_ns INTEGER CHECK(queue_started_monotonic_ns>=0),
  wait_deadline_monotonic_ns INTEGER CHECK(wait_deadline_monotonic_ns>0),
  state TEXT NOT NULL CHECK(state IN ('queued','leased','released','expired','cancelled','timed_out','refused')),
  outcome_code TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK((wait_mode='no_wait' AND wait_timeout_ns IS NULL) OR (wait_mode='wait' AND wait_timeout_ns IS NOT NULL)),
  CHECK((queue_sequence IS NULL AND queued_at IS NULL AND wait_deadline_at IS NULL AND queue_boot_session_id IS NULL AND queue_started_monotonic_ns IS NULL AND wait_deadline_monotonic_ns IS NULL) OR (queue_sequence IS NOT NULL AND queued_at IS NOT NULL AND wait_deadline_at IS NOT NULL AND queue_boot_session_id IS NOT NULL AND queue_started_monotonic_ns IS NOT NULL AND wait_deadline_monotonic_ns IS NOT NULL)),
  CHECK((state='refused' AND outcome_code IS NOT NULL) OR (state!='refused'))
);
CREATE UNIQUE INDEX requests_tag_queue_sequence ON requests(tag, queue_sequence);
CREATE TABLE leases (
  id TEXT PRIMARY KEY COLLATE BINARY,
  request_id TEXT NOT NULL UNIQUE REFERENCES requests(request_id),
  resource_id TEXT NOT NULL REFERENCES resources(id),
  state TEXT NOT NULL CHECK(state IN ('active','released','expired')),
  duration_ns INTEGER NOT NULL CHECK(duration_ns>0),
  granted_at TEXT NOT NULL,
  hard_end_at TEXT NOT NULL,
  boot_session_id TEXT NOT NULL CHECK(length(boot_session_id)=36),
  granted_monotonic_ns INTEGER NOT NULL CHECK(granted_monotonic_ns>=0),
  hard_end_monotonic_ns INTEGER NOT NULL CHECK(hard_end_monotonic_ns>0),
  fencing_token INTEGER NOT NULL CHECK(fencing_token>=1),
  ended_at TEXT,
  ended_resource_state TEXT CHECK(ended_resource_state IN ('available','disabled','cleanup_pending','quarantined')),
  ended_fencing_token INTEGER CHECK(ended_fencing_token>=1),
  CHECK((state='active' AND ended_at IS NULL AND ended_resource_state IS NULL AND ended_fencing_token IS NULL) OR (state IN ('released','expired') AND ended_at IS NOT NULL AND ended_resource_state IS NOT NULL AND ended_fencing_token IS NOT NULL))
);
CREATE UNIQUE INDEX leases_one_active_resource ON leases(resource_id) WHERE state='active';
CREATE TABLE audit (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  operation TEXT NOT NULL,
  request_id TEXT,
  target_id TEXT,
  result_code TEXT NOT NULL,
  from_state TEXT,
  to_state TEXT
);
PRAGMA user_version=1;
