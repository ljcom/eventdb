import dotenv from 'dotenv';

dotenv.config();

function parseBoolean(value, defaultValue = false) {
  if (value === undefined) return defaultValue;
  return String(value).toLowerCase() === 'true';
}

function parseAccountSecrets(value) {
  if (!value) return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function parseOptionalString(value) {
  if (value === undefined || value === null) return '';
  return String(value).trim();
}

function parseNumber(value, defaultValue) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : defaultValue;
}

function parseJsonArray(value, defaultValue = []) {
  if (!value) return defaultValue;
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : defaultValue;
  } catch {
    return defaultValue;
  }
}

function parseCsv(value, defaultValue = []) {
  if (!value) return defaultValue;
  return String(value)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

const nodeEnv = process.env.NODE_ENV || 'development';

export const config = {
  nodeEnv,
  port: Number(process.env.PORT || 3000),
  databaseUrl: process.env.DATABASE_URL || '',
  dbSsl: parseBoolean(process.env.DB_SSL, false),
  defaultNamespaceId: process.env.DEFAULT_NAMESPACE_ID || 'default',
  eventGenesisPrevHash: process.env.EVENT_GENESIS_PREV_HASH || 'GENESIS',
  sealGenesisPrevHash: process.env.SEAL_GENESIS_PREV_HASH || 'SEAL_GENESIS',
  signatureMode: process.env.SIGNATURE_MODE || 'none',
  accountSecrets: parseAccountSecrets(process.env.ACCOUNT_SECRETS_JSON),
  apiAuthEnabled: parseBoolean(process.env.API_AUTH_ENABLED, nodeEnv === 'production'),
  apiKeys: {
    ingest: parseOptionalString(process.env.API_KEY_INGEST),
    ops: parseOptionalString(process.env.API_KEY_OPS),
    verify: parseOptionalString(process.env.API_KEY_VERIFY),
    admin: parseOptionalString(process.env.API_KEY_ADMIN)
  },
  rateLimitEnabled: parseBoolean(process.env.RATE_LIMIT_ENABLED, nodeEnv === 'production'),
  rateLimitWindowMs: parseNumber(process.env.RATE_LIMIT_WINDOW_MS, 60_000),
  rateLimitMax: {
    ingest: parseNumber(process.env.RATE_LIMIT_MAX_INGEST, 300),
    ops: parseNumber(process.env.RATE_LIMIT_MAX_OPS, 60),
    verify: parseNumber(process.env.RATE_LIMIT_MAX_VERIFY, 120)
  },
  auditLogEnabled: parseBoolean(process.env.AUDIT_LOG_ENABLED, nodeEnv === 'production'),
  anchor: {
    enabled: parseBoolean(process.env.ANCHOR_ENABLED, false),
    provider: parseOptionalString(process.env.ANCHOR_PROVIDER || 'evm_read'),
    evmRpcUrl: parseOptionalString(process.env.ANCHOR_EVM_RPC_URL),
    evmContractAddress: parseOptionalString(process.env.ANCHOR_EVM_CONTRACT_ADDRESS),
    evmContractAbi: parseJsonArray(process.env.ANCHOR_EVM_CONTRACT_ABI_JSON, [
      'function getCommitment(string chainId, string checkpointId) view returns (bytes32)',
      'function getPublishedAt(string chainId, string checkpointId) view returns (uint256)'
    ]),
    evmMethodCommitment: parseOptionalString(process.env.ANCHOR_EVM_METHOD_COMMITMENT || 'getCommitment'),
    evmMethodTimestamp: parseOptionalString(process.env.ANCHOR_EVM_METHOD_TIMESTAMP || 'getPublishedAt')
  },
  sqlWriteAdapter: {
    enabled: parseBoolean(process.env.SQL_WRITE_ADAPTER_ENABLED, false),
    allowGenericPatch: parseBoolean(process.env.SQL_WRITE_ALLOW_GENERIC_PATCH, false),
    allowedEntities: parseCsv(process.env.SQL_WRITE_ALLOWED_ENTITIES, ['orders']),
    requireIdempotencyKey: parseBoolean(process.env.SQL_WRITE_REQUIRE_IDEMPOTENCY_KEY, true)
  }
};

if (!config.databaseUrl) {
  throw new Error('DATABASE_URL is required');
}
