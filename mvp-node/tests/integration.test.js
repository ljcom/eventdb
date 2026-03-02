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
  const sqlChainId = `${chainId}-sql`;

  const server = spawn('node', ['src/app.js'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port),
      SQL_WRITE_ADAPTER_ENABLED: 'true'
    },
    stdio: 'pipe'
  });

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DB_SSL || 'false').toLowerCase() === 'true' ? { rejectUnauthorized: false } : false
  });

  const cleanup = async () => {
    await pool.query('delete from read.orders_v1 where namespace_id = $1 and chain_id in ($2, $3)', [
      namespaceId,
      chainId,
      sqlChainId
    ]);
    await pool.query(
      `delete from projection_checkpoint
       where namespace_id = $1 and chain_id in ($2, $3) and projection_name = 'orders' and projection_ver = 1`,
      [namespaceId, chainId, sqlChainId]
    );
    await pool.query('delete from sql_write_idempotency where namespace_id = $1 and chain_id = $2', [namespaceId, sqlChainId]);
    await pool.query('delete from eventdb_snapshot where namespace_id = $1 and chain_id in ($2, $3)', [
      namespaceId,
      chainId,
      sqlChainId
    ]);
    await pool.query('delete from eventdb_seal where namespace_id = $1 and chain_id in ($2, $3)', [
      namespaceId,
      chainId,
      sqlChainId
    ]);
    await pool.query('delete from eventdb_event where namespace_id = $1 and chain_id in ($2, $3)', [
      namespaceId,
      chainId,
      sqlChainId
    ]);
    await pool.query('delete from eventdb_chain where namespace_id = $1 and chain_id in ($2, $3)', [
      namespaceId,
      chainId,
      sqlChainId
    ]);
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

    const projectionRun = await requestJson(`${baseUrl}/v1/projections/${chainId}/orders/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId })
    });
    assert.equal(projectionRun.status, 200);
    assert.equal(projectionRun.payload.status, 'PASS');
    assert.equal(projectionRun.payload.artifact.rows_applied, 3);

    const readOrders = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&status=final&limit=10`,
      {
        method: 'GET'
      }
    );
    assert.equal(readOrders.status, 200);
    assert.equal(readOrders.payload.status, 'PASS');
    assert.equal(readOrders.payload.artifact.rows.length, 1);
    assert.equal(readOrders.payload.artifact.rows[0].order_id, 'X-1');
    assert.equal(readOrders.payload.artifact.rows[0].status, 'final');

    const sqlInsert = await requestJson(`${baseUrl}/v1/sql/${sqlChainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${sqlChainId}-1`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "INSERT INTO orders (order_id, status) VALUES ('O-1', 'CREATED')"
      })
    });
    assert.equal(sqlInsert.status, 201);
    assert.equal(sqlInsert.payload.status, 'PASS');
    assert.equal(sqlInsert.payload.artifact.idempotent_replay, false);

    const sqlInsertReplay = await requestJson(`${baseUrl}/v1/sql/${sqlChainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${sqlChainId}-1`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "INSERT INTO orders (order_id, status) VALUES ('O-1', 'CREATED')"
      })
    });
    assert.equal(sqlInsertReplay.status, 201);
    assert.equal(sqlInsertReplay.payload.status, 'PASS');
    assert.equal(sqlInsertReplay.payload.artifact.idempotent_replay, true);

    const sqlUpdate = await requestJson(`${baseUrl}/v1/sql/${sqlChainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${sqlChainId}-2`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "UPDATE orders SET status='PAID' WHERE order_id='O-1'"
      })
    });
    assert.equal(sqlUpdate.status, 201);
    assert.equal(sqlUpdate.payload.status, 'PASS');

    const sqlReadPaid = await requestJson(
      `${baseUrl}/v1/read/orders/${sqlChainId}?namespace_id=${namespaceId}&status=PAID&limit=10`,
      { method: 'GET' }
    );
    assert.equal(sqlReadPaid.status, 200);
    assert.equal(sqlReadPaid.payload.status, 'PASS');
    assert.equal(sqlReadPaid.payload.artifact.rows.length, 1);
    assert.equal(sqlReadPaid.payload.artifact.rows[0].order_id, 'O-1');
    assert.equal(sqlReadPaid.payload.artifact.rows[0].status, 'PAID');

    const sqlDelete = await requestJson(`${baseUrl}/v1/sql/${sqlChainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${sqlChainId}-3`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "DELETE FROM orders WHERE order_id='O-1'"
      })
    });
    assert.equal(sqlDelete.status, 201);
    assert.equal(sqlDelete.payload.status, 'PASS');

    const sqlReadAfterDelete = await requestJson(
      `${baseUrl}/v1/read/orders/${sqlChainId}?namespace_id=${namespaceId}&limit=10`,
      { method: 'GET' }
    );
    assert.equal(sqlReadAfterDelete.status, 200);
    assert.equal(sqlReadAfterDelete.payload.status, 'PASS');
    assert.equal(sqlReadAfterDelete.payload.artifact.rows.length, 0);

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

test('integration: read model define -> insert -> update -> select -> delete', async () => {
  assert.ok(process.env.DATABASE_URL, 'DATABASE_URL is required for integration test');

  const port = 3700 + Math.floor(Math.random() * 200);
  const baseUrl = `http://127.0.0.1:${port}`;
  const namespaceId = process.env.DEFAULT_NAMESPACE_ID || 'default';
  const chainId = `it-rm-${Date.now()}`;

  const server = spawn('node', ['src/app.js'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port),
      SQL_WRITE_ADAPTER_ENABLED: 'true'
    },
    stdio: 'pipe'
  });

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DB_SSL || 'false').toLowerCase() === 'true' ? { rejectUnauthorized: false } : false
  });

  const cleanup = async () => {
    await pool.query('delete from read.orders_v1 where namespace_id = $1 and chain_id = $2', [namespaceId, chainId]);
    await pool.query(
      `delete from projection_checkpoint
       where namespace_id = $1 and chain_id = $2 and projection_name = 'orders' and projection_ver = 1`,
      [namespaceId, chainId]
    );
    await pool.query('delete from sql_write_idempotency where namespace_id = $1 and chain_id = $2', [namespaceId, chainId]);
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

    const defineReadModel = await requestJson(`${baseUrl}/v1/projections/${chainId}/orders/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace_id: namespaceId })
    });
    assert.equal(defineReadModel.status, 200);
    assert.equal(defineReadModel.payload.status, 'PASS');
    assert.equal(defineReadModel.payload.artifact.events_scanned, 0);
    assert.equal(defineReadModel.payload.artifact.rows_applied, 0);

    const { rows: registryRows } = await pool.query(
      `select projection_name, projection_ver, status
       from projection_registry
       where projection_name = 'orders' and projection_ver = 1`
    );
    assert.equal(registryRows.length, 1);
    assert.equal(registryRows[0].status, 'active');

    const { rows: checkpointRows } = await pool.query(
      `select last_sequence
       from projection_checkpoint
       where namespace_id = $1 and chain_id = $2 and projection_name = 'orders' and projection_ver = 1`,
      [namespaceId, chainId]
    );
    assert.equal(checkpointRows.length, 1);
    assert.equal(Number(checkpointRows[0].last_sequence), 0);

    const sqlInsert = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-1`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "INSERT INTO orders (order_id, status) VALUES ('RM-1001', 'CREATED')"
      })
    });
    assert.equal(sqlInsert.status, 201);
    assert.equal(sqlInsert.payload.status, 'PASS');

    const readAfterInsert = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&status=CREATED&limit=10`,
      { method: 'GET' }
    );
    assert.equal(readAfterInsert.status, 200);
    assert.equal(readAfterInsert.payload.status, 'PASS');
    assert.equal(readAfterInsert.payload.artifact.rows.length, 1);
    assert.equal(readAfterInsert.payload.artifact.rows[0].order_id, 'RM-1001');
    assert.equal(readAfterInsert.payload.artifact.rows[0].status, 'CREATED');

    const sqlUpdate = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-2`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "UPDATE orders SET status='PAID' WHERE order_id='RM-1001'"
      })
    });
    assert.equal(sqlUpdate.status, 201);
    assert.equal(sqlUpdate.payload.status, 'PASS');

    const readAfterUpdate = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&status=PAID&limit=10`,
      { method: 'GET' }
    );
    assert.equal(readAfterUpdate.status, 200);
    assert.equal(readAfterUpdate.payload.status, 'PASS');
    assert.equal(readAfterUpdate.payload.artifact.rows.length, 1);
    assert.equal(readAfterUpdate.payload.artifact.rows[0].order_id, 'RM-1001');
    assert.equal(readAfterUpdate.payload.artifact.rows[0].status, 'PAID');

    const sqlDelete = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-3`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "DELETE FROM orders WHERE order_id='RM-1001'"
      })
    });
    assert.equal(sqlDelete.status, 201);
    assert.equal(sqlDelete.payload.status, 'PASS');

    const readAfterDelete = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&limit=10`,
      { method: 'GET' }
    );
    assert.equal(readAfterDelete.status, 200);
    assert.equal(readAfterDelete.payload.status, 'PASS');
    assert.equal(readAfterDelete.payload.artifact.rows.length, 0);
    assert.ok(readAfterDelete.payload.artifact.checkpoint);
    assert.equal(Number(readAfterDelete.payload.artifact.checkpoint.last_sequence), 3);
  } finally {
    await cleanup().catch(() => {});
    await pool.end();
    server.kill('SIGTERM');
    await sleep(200);
  }
});

test('integration: read model negative cases', async () => {
  assert.ok(process.env.DATABASE_URL, 'DATABASE_URL is required for integration test');

  const port = 3900 + Math.floor(Math.random() * 200);
  const baseUrl = `http://127.0.0.1:${port}`;
  const namespaceId = process.env.DEFAULT_NAMESPACE_ID || 'default';
  const chainId = `it-rm-neg-${Date.now()}`;

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

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DB_SSL || 'false').toLowerCase() === 'true' ? { rejectUnauthorized: false } : false
  });

  const cleanup = async () => {
    await pool.query('delete from read.orders_v1 where namespace_id = $1 and chain_id = $2', [namespaceId, chainId]);
    await pool.query(
      `delete from projection_checkpoint
       where namespace_id = $1 and chain_id = $2 and projection_name = 'orders' and projection_ver = 1`,
      [namespaceId, chainId]
    );
    await pool.query('delete from sql_write_idempotency where namespace_id = $1 and chain_id = $2', [namespaceId, chainId]);
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
        sql: "INSERT INTO orders (order_id, status) VALUES ('RM-NEG-1', 'CREATED')"
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
        sql: "DELETE FROM orders WHERE order_id='RM-NEG-1'"
      })
    });
    assert.equal(forbiddenEntity.status, 422);
    assert.equal(forbiddenEntity.payload.status, 'FAIL');
    assert.equal(forbiddenEntity.payload.error_code, 'SQL_WRITE_ENTITY_FORBIDDEN');

    const invalidWhereClause = await requestJson(`${baseUrl}/v1/sql/${chainId}/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'idempotency-key': `idem-${chainId}-neg-3`
      },
      body: JSON.stringify({
        namespace_id: namespaceId,
        actor_id: 'acct-ops-01',
        sql: "UPDATE orders SET status='PAID' WHERE customer_id='C-1'"
      })
    });
    assert.equal(invalidWhereClause.status, 422);
    assert.equal(invalidWhereClause.payload.status, 'FAIL');
    assert.equal(invalidWhereClause.payload.error_code, 'SQL_WRITE_INVALID_STATEMENT');

    const readInvalidLimit = await requestJson(
      `${baseUrl}/v1/read/orders/${chainId}?namespace_id=${namespaceId}&limit=-10`,
      { method: 'GET' }
    );
    assert.equal(readInvalidLimit.status, 200);
    assert.equal(readInvalidLimit.payload.status, 'PASS');
    assert.equal(readInvalidLimit.payload.artifact.limit, 100);
    assert.equal(readInvalidLimit.payload.artifact.rows.length, 0);
  } finally {
    await cleanup().catch(() => {});
    await pool.end();
    server.kill('SIGTERM');
    await sleep(200);
  }
});
