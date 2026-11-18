package lease

const settingsV2SQL = `CREATE TABLE settings (
  singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),
  schema_version INTEGER NOT NULL CHECK(schema_version=2),
  owner_uid INTEGER NOT NULL,
  max_lease_ns INTEGER NOT NULL CHECK(max_lease_ns=1200000000000),
  max_wait_ns INTEGER NOT NULL CHECK(max_wait_ns=14400000000000),
  boot_session_id TEXT NOT NULL CHECK(length(boot_session_id)=36),
  created_at TEXT NOT NULL
);`

const settingsV1SQL = `CREATE TABLE settings (
  singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),
  schema_version INTEGER NOT NULL CHECK(schema_version=1),
  owner_uid INTEGER NOT NULL,
  max_lease_ns INTEGER NOT NULL CHECK(max_lease_ns=1200000000000),
  max_wait_ns INTEGER NOT NULL CHECK(max_wait_ns=1800000000000),
  boot_session_id TEXT NOT NULL CHECK(length(boot_session_id)=36),
  created_at TEXT NOT NULL
);`

const schemaV2SQL = `
CREATE TABLE settings (
  singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),
  schema_version INTEGER NOT NULL CHECK(schema_version=2),
  owner_uid INTEGER NOT NULL,
  max_lease_ns INTEGER NOT NULL CHECK(max_lease_ns=1200000000000),
  max_wait_ns INTEGER NOT NULL CHECK(max_wait_ns=14400000000000),
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
PRAGMA user_version=2;
`

const schemaSQL = `
CREATE TABLE settings (
  singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),
  schema_version INTEGER NOT NULL CHECK(schema_version=3),
  owner_uid INTEGER NOT NULL,
  max_lease_ns INTEGER NOT NULL CHECK(max_lease_ns=1200000000000),
  max_wait_ns INTEGER NOT NULL CHECK(max_wait_ns=14400000000000),
  boot_session_id TEXT NOT NULL CHECK(length(boot_session_id)=36),
  created_at TEXT NOT NULL
);
CREATE TABLE lease_classes (
  class TEXT PRIMARY KEY COLLATE BINARY,
  priority INTEGER NOT NULL CHECK(priority>=0),
  max_active INTEGER NOT NULL CHECK(max_active>=1),
  max_lease_ns INTEGER NOT NULL CHECK(max_lease_ns>0),
  next_queue_sequence INTEGER NOT NULL CHECK(next_queue_sequence>=1),
  created_at TEXT NOT NULL
);
CREATE TABLE pools (
  tag TEXT PRIMARY KEY COLLATE BINARY,
  class TEXT REFERENCES lease_classes(class),
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
  requester_id TEXT,
  legacy_unattributed INTEGER NOT NULL CHECK(legacy_unattributed IN (0,1)),
  tag TEXT NOT NULL,
  class TEXT REFERENCES lease_classes(class),
  duration_ns INTEGER NOT NULL CHECK(duration_ns>0),
  wait_mode TEXT NOT NULL CHECK(wait_mode IN ('wait','no_wait')),
  wait_timeout_ns INTEGER CHECK(wait_timeout_ns>0),
  queue_sequence INTEGER CHECK(queue_sequence>=1),
  class_queue_sequence INTEGER CHECK(class_queue_sequence>=1),
  queued_at TEXT,
  wait_deadline_at TEXT,
  queue_boot_session_id TEXT CHECK(queue_boot_session_id IS NULL OR length(queue_boot_session_id)=36),
  queue_started_monotonic_ns INTEGER CHECK(queue_started_monotonic_ns>=0),
  wait_deadline_monotonic_ns INTEGER CHECK(wait_deadline_monotonic_ns>0),
  state TEXT NOT NULL CHECK(state IN ('queued','leased','released','expired','cancelled','timed_out','refused')),
  outcome_code TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK((legacy_unattributed=1 AND requester_id IS NULL) OR (legacy_unattributed=0 AND requester_id IS NOT NULL AND length(CAST(requester_id AS BLOB)) BETWEEN 1 AND 128)),
  CHECK((wait_mode='no_wait' AND wait_timeout_ns IS NULL) OR (wait_mode='wait' AND wait_timeout_ns IS NOT NULL)),
  CHECK((queue_sequence IS NULL AND class_queue_sequence IS NULL AND queued_at IS NULL AND wait_deadline_at IS NULL AND queue_boot_session_id IS NULL AND queue_started_monotonic_ns IS NULL AND wait_deadline_monotonic_ns IS NULL) OR (queue_sequence IS NOT NULL AND queued_at IS NOT NULL AND wait_deadline_at IS NOT NULL AND queue_boot_session_id IS NOT NULL AND queue_started_monotonic_ns IS NOT NULL AND wait_deadline_monotonic_ns IS NOT NULL)),
  CHECK(class_queue_sequence IS NULL OR class IS NOT NULL),
  CHECK((state='refused' AND outcome_code IS NOT NULL) OR (state!='refused'))
);
CREATE UNIQUE INDEX requests_tag_queue_sequence ON requests(tag, queue_sequence);
CREATE UNIQUE INDEX requests_class_queue_sequence ON requests(class, class_queue_sequence) WHERE class_queue_sequence IS NOT NULL;
CREATE TABLE leases (
  id TEXT PRIMARY KEY COLLATE BINARY,
  request_id TEXT NOT NULL UNIQUE REFERENCES requests(request_id),
  holder_id TEXT,
  legacy_unattributed INTEGER NOT NULL CHECK(legacy_unattributed IN (0,1)),
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
  CHECK((legacy_unattributed=1 AND holder_id IS NULL) OR (legacy_unattributed=0 AND holder_id IS NOT NULL AND length(CAST(holder_id AS BLOB)) BETWEEN 1 AND 128)),
  CHECK((state='active' AND ended_at IS NULL AND ended_resource_state IS NULL AND ended_fencing_token IS NULL) OR (state IN ('released','expired') AND ended_at IS NOT NULL AND ended_resource_state IS NOT NULL AND ended_fencing_token IS NOT NULL))
);
CREATE UNIQUE INDEX leases_one_active_resource ON leases(resource_id) WHERE state='active';
CREATE TABLE audit (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  operation TEXT NOT NULL,
  actor_kind TEXT NOT NULL CHECK(actor_kind IN ('caller','system','legacy')),
  actor_component TEXT CHECK(actor_component IS NULL OR actor_component='scheduler'),
  cause TEXT CHECK(cause IS NULL OR cause IN ('grant','promotion','expiry','timeout','exhaustion','boot_change')),
  cause_ref_kind TEXT CHECK(cause_ref_kind IS NULL OR cause_ref_kind IN ('request','lease','resource')),
  cause_ref_id TEXT,
  origin TEXT CHECK(origin IS NULL OR origin='pre_v3'),
  request_id TEXT,
  requester_id TEXT,
  holder_id TEXT,
  legacy_unattributed INTEGER NOT NULL CHECK(legacy_unattributed IN (0,1)),
  target_id TEXT,
  result_code TEXT NOT NULL,
  from_state TEXT,
  to_state TEXT,
  CHECK((cause_ref_kind IS NULL AND cause_ref_id IS NULL) OR (cause_ref_kind IS NOT NULL AND cause_ref_id IS NOT NULL AND length(CAST(cause_ref_id AS BLOB)) BETWEEN 1 AND 128)),
  CHECK(COALESCE(
    (actor_kind='caller' AND actor_component IS NULL AND cause IS NULL AND cause_ref_kind IS NULL AND cause_ref_id IS NULL AND ((legacy_unattributed=0 AND origin IS NULL AND requester_id IS NOT NULL AND length(CAST(requester_id AS BLOB)) BETWEEN 1 AND 128 AND (holder_id IS NULL OR length(CAST(holder_id AS BLOB)) BETWEEN 1 AND 128)) OR (legacy_unattributed=1 AND origin='pre_v3' AND requester_id IS NULL AND holder_id IS NULL)))
    OR (actor_kind='system' AND actor_component='scheduler' AND cause IS NOT NULL AND ((legacy_unattributed=0 AND origin IS NULL AND ((request_id IS NULL AND requester_id IS NULL AND holder_id IS NULL) OR (request_id IS NOT NULL AND requester_id IS NOT NULL AND length(CAST(requester_id AS BLOB)) BETWEEN 1 AND 128 AND (holder_id IS NULL OR length(CAST(holder_id AS BLOB)) BETWEEN 1 AND 128)))) OR (legacy_unattributed=1 AND origin='pre_v3' AND requester_id IS NULL AND holder_id IS NULL)))
    OR (actor_kind='legacy' AND actor_component IS NULL AND cause IS NULL AND cause_ref_kind IS NULL AND cause_ref_id IS NULL AND legacy_unattributed=1 AND origin='pre_v3' AND requester_id IS NULL AND holder_id IS NULL),
    0
  ))
);
PRAGMA user_version=3;
`
