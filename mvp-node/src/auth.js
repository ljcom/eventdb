import crypto from 'crypto';
import { config } from './config.js';

function safeEquals(left, right) {
  if (left.length !== right.length) {
    return false;
  }
  return crypto.timingSafeEqual(Buffer.from(left, 'utf8'), Buffer.from(right, 'utf8'));
}

function keyFingerprint(apiKey) {
  return crypto.createHash('sha256').update(apiKey).digest('hex').slice(0, 12);
}

function hasAnyApiKeyConfigured() {
  return Boolean(config.apiKeys.ingest || config.apiKeys.ops || config.apiKeys.verify || config.apiKeys.admin);
}

function isAllowedForRole(role, apiKey) {
  if (!apiKey) {
    return false;
  }

  if (config.apiKeys.admin && safeEquals(config.apiKeys.admin, apiKey)) {
    return true;
  }

  const expected = config.apiKeys[role];
  if (!expected) {
    return false;
  }

  return safeEquals(expected, apiKey);
}

export function requireApiRole(role) {
  return (req, res, next) => {
    req.authRole = role;

    if (!config.apiAuthEnabled) {
      req.authMode = 'disabled';
      return next();
    }

    if (!hasAnyApiKeyConfigured()) {
      return res.status(503).json({
        status: 'FAIL',
        error_code: 'AUTH_NOT_CONFIGURED',
        message: 'API auth is enabled but no API keys are configured'
      });
    }

    const apiKey = req.header('x-api-key') || req.header('authorization')?.replace(/^Bearer\s+/i, '') || '';

    if (!isAllowedForRole(role, apiKey)) {
      req.authMode = 'enabled';
      return res.status(401).json({
        status: 'FAIL',
        error_code: 'AUTH_INVALID_API_KEY',
        message: `Invalid API key for role=${role}`
      });
    }

    req.authMode = 'enabled';
    req.authKeyFingerprint = keyFingerprint(apiKey);
    return next();
  };
}
