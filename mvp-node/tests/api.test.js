import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import test from 'node:test';
import dotenv from 'dotenv';

dotenv.config();

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const payload = await response.json();
  return { status: response.status, payload };
}

async function waitForHealthy(baseUrl) {
  for (let i = 0; i < 30; i += 1) {
    try {
      const { status, payload } = await requestJson(`${baseUrl}/health`);
      if (status === 200 && payload.status === 'ok') {
        return true;
      }
    } catch {
      // Retry until server is ready.
    }
    await sleep(200);
  }
  return false;
}

test('api: read model flow define -> insert -> update -> select -> delete', async () => {
  const port = 4200 + Math.floor(Math.random() * 200);
  const baseUrl = `http://127.0.0.1:${port}`;
  const namespaceId = process.env.DEFAULT_NAMESPACE_ID || 'default';
  const chainId = `api-rm-${Date.now()}`;

  const server = spawn('node', ['src/app.js'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port),
      SQL_WRITE_ADAPTER_ENABLED: 'true',
      SQL_WRITE_REQUIRE_IDEMPOTENCY_KEY: 'true'
    },
    stdio: 'pipe'
  });

  try {
    const healthy = await waitForHealthy(baseUrl);
    assert.equal(healthy, true, 'server did not become healthy in time');

    const defineReadModel = await requestJson(`${baseUrl}/v1/projections/${chainId}/orders/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId })
    });
    assert.equal(defineReadModel.status, 200);
    assert.equal(defineReadModel.payload.status, 'PASS');

    const insertResp = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-1`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "INSERT INTO orders (order_id, status) VALUES ('API-1001', 'CREATED')"
      })
    });
    assert.equal(insertResp.status, 201);
    assert.equal(insertResp.payload.status, 'PASS');

    const readAfterInsert = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&status=CREATED&limit=10`,
      { method: 'GET' }
    );
    assert.equal(readAfterInsert.status, 200);
    assert.equal(readAfterInsert.payload.status, 'PASS');
    assert.equal(readAfterInsert.payload.artifact.rows.length, 1);
    assert.equal(readAfterInsert.payload.artifact.rows[0].order_id, 'API-1001');

    const updateResp = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-2`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "UPDATE orders SET status='PAID' WHERE order_id='API-1001'"
      })
    });
    assert.equal(updateResp.status, 201);
    assert.equal(updateResp.payload.status, 'PASS');

    const readAfterUpdate = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&status=PAID&limit=10`,
      { method: 'GET' }
    );
    assert.equal(readAfterUpdate.status, 200);
    assert.equal(readAfterUpdate.payload.status, 'PASS');
    assert.equal(readAfterUpdate.payload.artifact.rows.length, 1);
    assert.equal(readAfterUpdate.payload.artifact.rows[0].status, 'PAID');

    const deleteResp = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-3`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "DELETE FROM orders WHERE order_id='API-1001'"
      })
    });
    assert.equal(deleteResp.status, 201);
    assert.equal(deleteResp.payload.status, 'PASS');

    const readAfterDelete = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&limit=10`,
      { method: 'GET' }
    );
    assert.equal(readAfterDelete.status, 200);
    assert.equal(readAfterDelete.payload.status, 'PASS');
    assert.equal(readAfterDelete.payload.artifact.rows.length, 0);
  } finally {
    server.kill('SIGTERM');
    await sleep(200);
  }
});

test('api: negative cases for projection and sql write', async () => {
  const port = 4400 + Math.floor(Math.random() * 200);
  const baseUrl = `http://127.0.0.1:${port}`;
  const namespaceId = process.env.DEFAULT_NAMESPACE_ID || 'default';
  const chainId = `api-rm-neg-${Date.now()}`;

  const server = spawn('node', ['src/app.js'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port),
      SQL_WRITE_ADAPTER_ENABLED: 'true',
      SQL_WRITE_REQUIRE_IDEMPOTENCY_KEY: 'true',
      SQL_WRITE_ALLOWED_ENTITIES: 'invoices'
    },
    stdio: 'pipe'
  });

  try {
    const healthy = await waitForHealthy(baseUrl);
    assert.equal(healthy, true, 'server did not become healthy in time');

    const invalidProjection = await requestJson(`${baseUrl}/v1/projections/${chainId}/orders/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId, upto_sequence: 0 })
    });
    assert.equal(invalidProjection.status, 422);
    assert.equal(invalidProjection.payload.status, 'FAIL');
    assert.equal(invalidProjection.payload.error_code, 'PROJECTION_SEQUENCE_INVALID');

    const missingIdempotency = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "INSERT INTO orders (order_id, status) VALUES ('API-NEG-1', 'CREATED')"
      })
    });
    assert.equal(missingIdempotency.status, 422);
    assert.equal(missingIdempotency.payload.status, 'FAIL');
    assert.equal(missingIdempotency.payload.error_code, 'SQL_WRITE_IDEMPOTENCY_REQUIRED');

    const invalidSql = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-neg-1`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: 'SELECT * FROM orders'
      })
    });
    assert.equal(invalidSql.status, 422);
    assert.equal(invalidSql.payload.status, 'FAIL');
    assert.equal(invalidSql.payload.error_code, 'SQL_WRITE_INVALID_STATEMENT');

    const forbiddenEntity = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-neg-2`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "DELETE FROM orders WHERE order_id='API-NEG-1'"
      })
    });
    assert.equal(forbiddenEntity.status, 422);
    assert.equal(forbiddenEntity.payload.status, 'FAIL');
    assert.equal(forbiddenEntity.payload.error_code, 'SQL_WRITE_ENTITY_FORBIDDEN');

    const readInvalidLimit = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&limit=-10`,
      { method: 'GET' }
    );
    assert.equal(readInvalidLimit.status, 200);
    assert.equal(readInvalidLimit.payload.status, 'PASS');
    assert.equal(readInvalidLimit.payload.artifact.limit, 100);
  } finally {
    server.kill('SIGTERM');
    await sleep(200);
  }
});
