CREATE SCHEMA IF NOT EXISTS read;

CREATE TABLE IF NOT EXISTS eventdb_chain (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace_id, chain_id)
);

CREATE TABLE IF NOT EXISTS eventdb_event (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  sequence BIGINT NOT NULL,
  prev_hash TEXT NOT NULL,
  account_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  signature TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace_id, chain_id, event_id),
  UNIQUE (namespace_id, chain_id, sequence),
  FOREIGN KEY (namespace_id, chain_id)
    REFERENCES eventdb_chain (namespace_id, chain_id)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS eventdb_seal (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  window_start_sequence BIGINT NOT NULL,
  window_end_sequence BIGINT NOT NULL,
  prev_seal_hash TEXT NOT NULL,
  window_commitment_hash TEXT NOT NULL,
  seal_hash TEXT NOT NULL,
  account_id TEXT NOT NULL,
  seal_time TIMESTAMPTZ NOT NULL,
  signature TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace_id, chain_id, window_id),
  UNIQUE (namespace_id, chain_id, window_end_sequence),
  FOREIGN KEY (namespace_id, chain_id)
    REFERENCES eventdb_chain (namespace_id, chain_id)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS eventdb_snapshot (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  basis_sequence BIGINT NOT NULL,
  basis_seal_hash TEXT,
  snapshot_time TIMESTAMPTZ NOT NULL,
  snapshot_hash TEXT NOT NULL,
  snapshot_data JSONB NOT NULL,
  signature TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace_id, chain_id, snapshot_id),
  FOREIGN KEY (namespace_id, chain_id)
    REFERENCES eventdb_chain (namespace_id, chain_id)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS projection_registry (
  projection_name TEXT NOT NULL,
  projection_ver INTEGER NOT NULL,
  logic_checksum TEXT NOT NULL,
  status TEXT NOT NULL,
  rebuild_strategy TEXT,
  migration_policy TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (projection_name, projection_ver)
);

CREATE TABLE IF NOT EXISTS projection_checkpoint (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  projection_name TEXT NOT NULL,
  projection_ver INTEGER NOT NULL,
  last_sequence BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace_id, chain_id, projection_name, projection_ver)
);

CREATE TABLE IF NOT EXISTS sql_write_idempotency (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  statement_hash TEXT NOT NULL,
  event_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace_id, chain_id, idempotency_key),
  UNIQUE (namespace_id, chain_id, event_id)
);

CREATE TABLE IF NOT EXISTS read.orders_v1 (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT NOT NULL,
  source_sequence BIGINT NOT NULL,
  projection_ver INTEGER NOT NULL,
  PRIMARY KEY (namespace_id, chain_id, order_id)
);

CREATE INDEX IF NOT EXISTS idx_eventdb_event_chain_sequence
  ON eventdb_event (namespace_id, chain_id, sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_event_type
  ON eventdb_event (namespace_id, chain_id, event_type, sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_seal_chain_end
  ON eventdb_seal (namespace_id, chain_id, window_end_sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_snapshot_chain_time
  ON eventdb_snapshot (namespace_id, chain_id, snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_projection_checkpoint_lookup
  ON projection_checkpoint (namespace_id, chain_id, projection_name, projection_ver);

CREATE INDEX IF NOT EXISTS idx_orders_v1_status
  ON read.orders_v1 (namespace_id, chain_id, status, updated_at DESC);
