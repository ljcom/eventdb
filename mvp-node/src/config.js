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

export const config = {
  nodeEnv: process.env.NODE_ENV || 'development',
  port: Number(process.env.PORT || 3000),
  databaseUrl: process.env.DATABASE_URL || '',
  dbSsl: parseBoolean(process.env.DB_SSL, false),
  defaultNamespaceId: process.env.DEFAULT_NAMESPACE_ID || 'default',
  eventGenesisPrevHash: process.env.EVENT_GENESIS_PREV_HASH || 'GENESIS',
  sealGenesisPrevHash: process.env.SEAL_GENESIS_PREV_HASH || 'SEAL_GENESIS',
  signatureMode: process.env.SIGNATURE_MODE || 'none',
  accountSecrets: parseAccountSecrets(process.env.ACCOUNT_SECRETS_JSON)
};

if (!config.databaseUrl) {
  throw new Error('DATABASE_URL is required');
}
