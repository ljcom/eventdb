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
  domain TEXT NOT NULL DEFAULT 'legacy',
  entity_type TEXT,
  entity_id TEXT,
  tenant_id TEXT,
  store_id TEXT,
  outlet_id TEXT,
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

ALTER TABLE eventdb_event ADD COLUMN IF NOT EXISTS domain TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE eventdb_event ADD COLUMN IF NOT EXISTS entity_type TEXT;
ALTER TABLE eventdb_event ADD COLUMN IF NOT EXISTS entity_id TEXT;
ALTER TABLE eventdb_event ADD COLUMN IF NOT EXISTS tenant_id TEXT;
ALTER TABLE eventdb_event ADD COLUMN IF NOT EXISTS store_id TEXT;
ALTER TABLE eventdb_event ADD COLUMN IF NOT EXISTS outlet_id TEXT;

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

CREATE TABLE IF NOT EXISTS read.market_orders_v1 (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  status TEXT NOT NULL,
  customer_name TEXT,
  order_date TEXT,
  subtotal NUMERIC(14,2),
  shipping_fee NUMERIC(14,2),
  total NUMERIC(14,2),
  delivery TEXT,
  payment TEXT,
  payment_method TEXT,
  delivery_method TEXT,
  notes TEXT,
  items JSONB NOT NULL DEFAULT '[]'::jsonb,
  order_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT NOT NULL,
  source_sequence BIGINT NOT NULL,
  projection_ver INTEGER NOT NULL,
  tenant_id TEXT,
  store_id TEXT,
  PRIMARY KEY (namespace_id, chain_id, order_id)
);

CREATE TABLE IF NOT EXISTS read.pos_sales_v1 (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  sale_id TEXT NOT NULL,
  status TEXT NOT NULL,
  event_name TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT NOT NULL,
  source_sequence BIGINT NOT NULL,
  projection_ver INTEGER NOT NULL,
  tenant_id TEXT,
  outlet_id TEXT,
  PRIMARY KEY (namespace_id, chain_id, sale_id)
);

CREATE TABLE IF NOT EXISTS read.crm_customers_v1 (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  status TEXT NOT NULL,
  event_name TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT NOT NULL,
  source_sequence BIGINT NOT NULL,
  projection_ver INTEGER NOT NULL,
  tenant_id TEXT,
  PRIMARY KEY (namespace_id, chain_id, customer_id)
);

CREATE TABLE IF NOT EXISTS read.products_v1 (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  name TEXT NOT NULL,
  sku TEXT,
  unit TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT NOT NULL,
  source_sequence BIGINT NOT NULL,
  projection_ver INTEGER NOT NULL,
  tenant_id TEXT,
  PRIMARY KEY (namespace_id, chain_id, product_id)
);

CREATE TABLE IF NOT EXISTS read.stock_balances_v1 (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  location_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT NOT NULL,
  source_sequence BIGINT NOT NULL,
  projection_ver INTEGER NOT NULL,
  tenant_id TEXT,
  PRIMARY KEY (namespace_id, chain_id, location_id, product_id)
);

CREATE TABLE IF NOT EXISTS read.sales_report_v1 (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  report_key TEXT NOT NULL,
  order_count INTEGER NOT NULL DEFAULT 0,
  invoice_count INTEGER NOT NULL DEFAULT 0,
  amount_total NUMERIC(18,2) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT NOT NULL,
  source_sequence BIGINT NOT NULL,
  projection_ver INTEGER NOT NULL,
  tenant_id TEXT,
  PRIMARY KEY (namespace_id, chain_id, report_key)
);

CREATE TABLE IF NOT EXISTS read.gl_balances_v1 (
  namespace_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  period TEXT NOT NULL,
  debit_total NUMERIC(18,2) NOT NULL DEFAULT 0,
  credit_total NUMERIC(18,2) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT NOT NULL,
  source_sequence BIGINT NOT NULL,
  projection_ver INTEGER NOT NULL,
  tenant_id TEXT,
  PRIMARY KEY (namespace_id, chain_id, account_id, period)
);

CREATE INDEX IF NOT EXISTS idx_eventdb_event_chain_sequence
  ON eventdb_event (namespace_id, chain_id, sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_event_type
  ON eventdb_event (namespace_id, chain_id, event_type, sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_event_domain
  ON eventdb_event (namespace_id, domain, chain_id, sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_event_entity
  ON eventdb_event (namespace_id, domain, entity_type, entity_id, sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_event_tenant
  ON eventdb_event (namespace_id, domain, tenant_id, store_id, outlet_id, sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_seal_chain_end
  ON eventdb_seal (namespace_id, chain_id, window_end_sequence);

CREATE INDEX IF NOT EXISTS idx_eventdb_snapshot_chain_time
  ON eventdb_snapshot (namespace_id, chain_id, snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_projection_checkpoint_lookup
  ON projection_checkpoint (namespace_id, chain_id, projection_name, projection_ver);

CREATE INDEX IF NOT EXISTS idx_orders_v1_status
  ON read.orders_v1 (namespace_id, chain_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_orders_v1_status
  ON read.market_orders_v1 (namespace_id, chain_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_pos_sales_v1_status
  ON read.pos_sales_v1 (namespace_id, chain_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_crm_customers_v1_status
  ON read.crm_customers_v1 (namespace_id, chain_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_products_v1_name
  ON read.products_v1 (namespace_id, chain_id, name);

CREATE INDEX IF NOT EXISTS idx_stock_balances_v1_product
  ON read.stock_balances_v1 (namespace_id, chain_id, product_id);

CREATE INDEX IF NOT EXISTS idx_sales_report_v1_updated
  ON read.sales_report_v1 (namespace_id, chain_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_gl_balances_v1_period
  ON read.gl_balances_v1 (namespace_id, chain_id, period, account_id);
