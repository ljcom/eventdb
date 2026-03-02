import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import crypto from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import test from 'node:test';
import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

function canonicalize(value) {
  if (value === null) return 'null';
  const valueType = typeof value;
  if (valueType === 'number') {
    if (!Number.isFinite(value)) throw new Error('Non-finite number');
    return JSON.stringify(value);
  }
  if (valueType === 'boolean' || valueType === 'string') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => canonicalize(item)).join(',')}]`;
  if (valueType === 'object') {
    const keys = Object.keys(value).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${canonicalize(value[key])}`).join(',')}}`;
  }
  throw new Error(`Unsupported type: ${valueType}`);
}

function hashCanonicalObject(value) {
  return crypto.createHash('sha256').update(Buffer.from(canonicalize(value), 'utf8')).digest('hex');
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const payload = await response.json();
  return { status: response.status, payload };
}

test('integration: event -> seal -> snapshot -> verify -> tamper', async () => {
  assert.ok(process.env.DATABASE_URL, 'DATABASE_URL is required for integration test');

  const port = 3400 + Math.floor(Math.random() * 200);
  const baseUrl = `http://127.0.0.1:${port}`;
  const namespaceId = process.env.DEFAULT_NAMESPACE_ID || 'default';
  const chainId = `it-${Date.now()}`;

  const server = spawn('node', ['src/app.js'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port)
    },
    stdio: 'pipe'
  });

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DB_SSL || 'false').toLowerCase() === 'true' ? { rejectUnauthorized: false } : false
  });

  const cleanup = async () => {
    await pool.query('delete from eventdb_snapshot where namespace_id = $1 and chain_id = $2', [namespaceId, chainId]);
    await pool.query('delete from eventdb_seal where namespace_id = $1 and chain_id = $2', [namespaceId, chainId]);
    await pool.query('delete from eventdb_event where namespace_id = $1 and chain_id = $2', [namespaceId, chainId]);
    await pool.query('delete from eventdb_chain where namespace_id = $1 and chain_id = $2', [namespaceId, chainId]);
  };

  try {
    let healthy = false;
    for (let i = 0; i < 30; i += 1) {
      try {
        const { status, payload } = await requestJson(`${baseUrl}/health`);
        if (status === 200 && payload.status === 'ok') {
          healthy = true;
          break;
        }
      } catch {
        // Retry until server is ready.
      }
      await sleep(200);
    }
    assert.equal(healthy, true, 'server did not become healthy in time');

    await cleanup();

    const eventSpecs = [
      { event_id: 'evt-1', event_type: 'record_created', event_time: '2026-01-01T00:00:00Z', payload: { ref: 'X-1', status: 'created' }, account_id: 'acct-ops-01' },
      { event_id: 'evt-2', event_type: 'record_updated', event_time: '2026-01-01T00:01:00Z', payload: { ref: 'X-1', status: 'validated' }, account_id: 'acct-ops-01' },
      { event_id: 'evt-3', event_type: 'record_finalized', event_time: '2026-01-01T00:02:00Z', payload: { ref: 'X-1', status: 'final' }, account_id: 'acct-ops-02' }
    ];

    let prevHash = process.env.EVENT_GENESIS_PREV_HASH || 'GENESIS';

    for (let i = 0; i < eventSpecs.length; i += 1) {
      const sequence = i + 1;
      const spec = eventSpecs[i];
      const reqBody = {
        namespace_id: namespaceId,
        event_id: spec.event_id,
        sequence,
        prev_hash: prevHash,
        account_id: spec.account_id,
        event_type: spec.event_type,
        event_time: spec.event_time,
        payload: spec.payload,
        signature: `sig_${spec.event_id}`
      };

      const { status, payload } = await requestJson(`${baseUrl}/v1/chains/${chainId}/events`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(reqBody)
      });

      assert.equal(status, 201);
      assert.equal(payload.status, 'PASS');

      prevHash = hashCanonicalObject({
        namespace_id: namespaceId,
        chain_id: chainId,
        event_id: spec.event_id,
        sequence,
        prev_hash: reqBody.prev_hash,
        account_id: spec.account_id,
        event_type: spec.event_type,
        event_time: spec.event_time,
        payload: spec.payload
      });
    }

    const chainVerify = await requestJson(`${baseUrl}/v1/chains/${chainId}/verify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId })
    });
    assert.equal(chainVerify.status, 200);
    assert.equal(chainVerify.payload.status, 'PASS');

    const sealBuild = await requestJson(`${baseUrl}/v1/seals/${chainId}/build`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId, account_id: 'acct-seal-01' })
    });
    assert.equal(sealBuild.status, 201);
    assert.equal(sealBuild.payload.status, 'PASS');

    const sealVerify = await requestJson(`${baseUrl}/v1/seals/${chainId}/verify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId })
    });
    assert.equal(sealVerify.status, 200);
    assert.equal(sealVerify.payload.status, 'PASS');

    const snapshotBuild = await requestJson(`${baseUrl}/v1/snapshots/${chainId}/build`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId })
    });
    assert.equal(snapshotBuild.status, 201);
    assert.equal(snapshotBuild.payload.status, 'PASS');

    const snapshotVerify = await requestJson(`${baseUrl}/v1/snapshots/${chainId}/verify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId })
    });
    assert.equal(snapshotVerify.status, 200);
    assert.equal(snapshotVerify.payload.status, 'PASS');

    await pool.query(
      `update eventdb_event
       set payload = jsonb_set(payload, '{status}', '"tampered"'::jsonb)
       where namespace_id = $1 and chain_id = $2 and sequence = 2`,
      [namespaceId, chainId]
    );

    const chainVerifyAfterTamper = await requestJson(`${baseUrl}/v1/chains/${chainId}/verify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId })
    });

    assert.equal(chainVerifyAfterTamper.status, 422);
    assert.equal(chainVerifyAfterTamper.payload.status, 'FAIL');
    assert.equal(chainVerifyAfterTamper.payload.error_code, 'CHAIN_PREV_HASH_INVALID');
  } finally {
    await cleanup().catch(() => {});
    await pool.end();
    server.kill('SIGTERM');
    await sleep(200);
  }
});
