import crypto from 'crypto';
import { config } from './config.js';

function getClientIp(req) {
  const forwarded = req.header('x-forwarded-for');
  if (forwarded) {
    return forwarded.split(',')[0].trim();
  }
  return req.ip || req.socket?.remoteAddress || 'unknown';
}

function apiKeyFingerprint(rawApiKey) {
  if (!rawApiKey) {
    return '';
  }
  return crypto.createHash('sha256').update(rawApiKey).digest('hex').slice(0, 12);
}

function resolvePresentedApiKey(req) {
  return req.header('x-api-key') || req.header('authorization')?.replace(/^Bearer\s+/i, '') || '';
}

const buckets = new Map();

function cleanupOldBuckets(nowMs, windowMs) {
  for (const [key, value] of buckets.entries()) {
    if (nowMs - value.windowStart >= windowMs * 2) {
      buckets.delete(key);
    }
  }
}

export function rateLimitByRole(role) {
  return (req, res, next) => {
    if (!config.rateLimitEnabled) {
      return next();
    }

    const maxRequests = config.rateLimitMax[role] || config.rateLimitMax.verify;
    const windowMs = config.rateLimitWindowMs;
    const now = Date.now();
    const ip = getClientIp(req);
    const rawApiKey = resolvePresentedApiKey(req);
    const keyId = rawApiKey ? apiKeyFingerprint(rawApiKey) : `ip:${ip}`;
    const bucketKey = `${role}:${keyId}`;

    const existing = buckets.get(bucketKey);
    if (!existing || now - existing.windowStart >= windowMs) {
      buckets.set(bucketKey, { windowStart: now, count: 1 });
      cleanupOldBuckets(now, windowMs);
      return next();
    }

    existing.count += 1;
    if (existing.count > maxRequests) {
      const retryAfterSeconds = Math.ceil((windowMs - (now - existing.windowStart)) / 1000);
      res.setHeader('retry-after', String(Math.max(retryAfterSeconds, 1)));
      return res.status(429).json({
        status: 'FAIL',
        error_code: 'RATE_LIMIT_EXCEEDED',
        message: `Rate limit exceeded for role=${role}`
      });
    }

    return next();
  };
}

export function auditLogMiddleware(req, res, next) {
  if (!config.auditLogEnabled || req.path === '/health') {
    return next();
  }

  const startedAt = Date.now();
  const ip = getClientIp(req);

  res.on('finish', () => {
    const logEntry = {
      ts: new Date().toISOString(),
      method: req.method,
      path: req.originalUrl,
      status_code: res.statusCode,
      duration_ms: Date.now() - startedAt,
      ip,
      auth_mode: req.authMode || (config.apiAuthEnabled ? 'enabled' : 'disabled'),
      auth_role: req.authRole || null,
      api_key_fingerprint: req.authKeyFingerprint || null,
      request_id: req.header('x-request-id') || null
    };

    // eslint-disable-next-line no-console
    console.log(JSON.stringify({ event: 'audit_request', ...logEntry }));
  });

  return next();
}
