import { config } from './config.js';
import { hashCanonicalObject, sha256Hex, signCanonicalObject } from './crypto.js';
import { withTx } from './db.js';
import { buildEventSigningObject, normalizeUtcTimestamp } from './verification.js';
import { runOrdersProjection } from './projections.js';

function fail(errorCode, message, checkedScope = {}) {
  return {
    status: 'FAIL',
    checked_scope: checkedScope,
    error_code: errorCode,
    message
  };
}

function pass(message, checkedScope = {}, artifact = {}) {
  return {
    status: 'PASS',
    checked_scope: checkedScope,
    error_code: null,
    message,
    artifact
  };
}

function normalizeSqlStatement(sql) {
  return String(sql || '').trim().replace(/\s+/g, ' ');
}

function parseSqlValue(raw) {
  const value = String(raw || '').trim();
  if (/^'.*'$/.test(value)) {
    return value.slice(1, -1).replace(/''/g, "'");
  }
  if (/^-?\d+$/.test(value)) {
    return Number(value);
  }
  throw new Error(`Unsupported SQL literal: ${value}`);
}

function parseAssignments(clause) {
  const segments = clause.split(',').map((part) => part.trim()).filter(Boolean);
  const out = {};
  for (const segment of segments) {
    const match = segment.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(.+)$/);
    if (!match) {
      throw new Error('Invalid assignment clause');
    }
    out[match[1].toLowerCase()] = parseSqlValue(match[2]);
  }
  return out;
}

function parseWherePrimaryKey(whereClause) {
  const match = whereClause.match(/^order_id\s*=\s*(.+)$/i);
  if (!match) {
    throw new Error('WHERE clause must be primary key only: order_id=<value>');
  }
  const orderId = parseSqlValue(match[1]);
  if (typeof orderId !== 'string' || !orderId) {
    throw new Error('order_id must be a non-empty string');
  }
  return { order_id: orderId };
}

function parseWriteSql(sqlRaw) {
  const sql = normalizeSqlStatement(sqlRaw);

  const insertMatch = sql.match(/^INSERT\s+INTO\s+orders\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*;?$/i);
  if (insertMatch) {
    const columns = insertMatch[1].split(',').map((item) => item.trim().toLowerCase());
    const values = insertMatch[2].split(',').map((item) => item.trim());
    if (columns.length !== values.length) {
      throw new Error('INSERT columns and values length mismatch');
    }

    const payload = {};
    for (let i = 0; i < columns.length; i += 1) {
      payload[columns[i]] = parseSqlValue(values[i]);
    }

    if (typeof payload.order_id !== 'string' || !payload.order_id) {
      throw new Error('INSERT requires order_id');
    }

    return {
      op: 'insert',
      entity: 'orders',
      key: payload.order_id,
      fields: payload
    };
  }

  const updateMatch = sql.match(/^UPDATE\s+orders\s+SET\s+(.+)\s+WHERE\s+(.+)\s*;?$/i);
  if (updateMatch) {
    const assignments = parseAssignments(updateMatch[1]);
    const where = parseWherePrimaryKey(updateMatch[2]);

    if (Object.keys(assignments).length === 0) {
      throw new Error('UPDATE requires at least one assignment');
    }

    return {
      op: 'update',
      entity: 'orders',
      key: where.order_id,
      fields: assignments
    };
  }

  const deleteMatch = sql.match(/^DELETE\s+FROM\s+orders\s+WHERE\s+(.+)\s*;?$/i);
  if (deleteMatch) {
    const where = parseWherePrimaryKey(deleteMatch[1]);
    return {
      op: 'delete',
      entity: 'orders',
      key: where.order_id,
      fields: {}
    };
  }

  throw new Error('Only INSERT INTO orders, UPDATE orders, or DELETE FROM orders are supported');
}

function mapIntentToEvent(intent) {
  if (intent.op === 'insert') {
    const status = typeof intent.fields.status === 'string' ? intent.fields.status : 'CREATED';
    return {
      eventType: 'OrderCreated',
      payload: { ref: intent.key, status }
    };
  }

  if (intent.op === 'delete') {
    return {
      eventType: 'OrderDeleted',
      payload: { ref: intent.key, status: 'DELETED' }
    };
  }

  if (intent.op === 'update') {
    const status = typeof intent.fields.status === 'string' ? intent.fields.status : '';
    if (status.toUpperCase() === 'PAID') {
      return { eventType: 'PaymentConfirmed', payload: { ref: intent.key, status: 'PAID' } };
    }
    if (status.toUpperCase() === 'CANCELLED') {
      return { eventType: 'OrderCancelled', payload: { ref: intent.key, status: 'CANCELLED' } };
    }
    if (!status && !config.sqlWriteAdapter.allowGenericPatch) {
      throw new Error('Unrecognized UPDATE assignment. Enable SQL_WRITE_ALLOW_GENERIC_PATCH to allow generic patch events');
    }

    const normalizedStatus = status || 'UPDATED';
    return {
      eventType: 'OrderUpdated',
      payload: { ref: intent.key, status: normalizedStatus, patch: intent.fields }
    };
  }

  throw new Error(`Unsupported intent op=${intent.op}`);
}

function buildEventId({ namespaceId, chainId, idempotencyKey, statementHash }) {
  if (idempotencyKey) {
    return `sqlw-${sha256Hex(`${namespaceId}:${chainId}:${idempotencyKey}`).slice(0, 20)}`;
  }
  return `sqlw-${statementHash.slice(0, 20)}`;
}

async function appendDerivedEventTx({
  client,
  namespaceId,
  chainId,
  accountId,
  eventType,
  payload,
  idempotencyKey,
  statementHash,
  statement
}) {
  await client.query(
    `insert into eventdb_chain (namespace_id, chain_id)
     values ($1, $2)
     on conflict (namespace_id, chain_id) do nothing`,
    [namespaceId, chainId]
  );

  if (idempotencyKey) {
    const { rows: idemRows } = await client.query(
      `select event_id
       from sql_write_idempotency
       where namespace_id = $1 and chain_id = $2 and idempotency_key = $3
       limit 1`,
      [namespaceId, chainId, idempotencyKey]
    );

    if (idemRows.length > 0) {
      const eventId = idemRows[0].event_id;
      const { rows: replayRows } = await client.query(
        `select sequence
         from eventdb_event
         where namespace_id = $1 and chain_id = $2 and event_id = $3
         limit 1`,
        [namespaceId, chainId, eventId]
      );

      return {
        replay: true,
        event_id: eventId,
        sequence: replayRows[0]?.sequence ? Number(replayRows[0].sequence) : null
      };
    }
  }

  const { rows: lastRows } = await client.query(
    `select namespace_id, chain_id, event_id, sequence, prev_hash, account_id, event_type, event_time, payload
     from eventdb_event
     where namespace_id = $1 and chain_id = $2
     order by sequence desc
     limit 1
     for update`,
    [namespaceId, chainId]
  );

  const last = lastRows[0] || null;
  const sequence = last ? Number(last.sequence) + 1 : 1;
  const prevHash = last ? hashCanonicalObject(buildEventSigningObject(last)) : config.eventGenesisPrevHash;
  const eventTime = normalizeUtcTimestamp(new Date());
  const eventId = buildEventId({ namespaceId, chainId, idempotencyKey, statementHash });

  const signingObject = {
    namespace_id: namespaceId,
    chain_id: chainId,
    event_id: eventId,
    sequence,
    prev_hash: prevHash,
    account_id: accountId,
    event_type: eventType,
    event_time: eventTime,
    payload: {
      ...payload,
      sql_statement: statement,
      sql_statement_hash: statementHash
    }
  };

  const signature = signCanonicalObject({
    accountId,
    signingObject,
    fallbackSignature: `sig_sql_${eventId}`
  });

  await client.query(
    `insert into eventdb_event (
      namespace_id, chain_id, event_id, sequence, prev_hash,
      account_id, event_type, event_time, payload, signature
    ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [
      namespaceId,
      chainId,
      eventId,
      sequence,
      prevHash,
      accountId,
      eventType,
      eventTime,
      signingObject.payload,
      signature
    ]
  );

  if (idempotencyKey) {
    await client.query(
      `insert into sql_write_idempotency (
        namespace_id, chain_id, idempotency_key, statement_hash, event_id
      ) values ($1,$2,$3,$4,$5)`,
      [namespaceId, chainId, idempotencyKey, statementHash, eventId]
    );
  }

  return {
    replay: false,
    event_id: eventId,
    sequence,
    prev_hash: prevHash,
    event_hash: hashCanonicalObject(signingObject),
    event_type: eventType,
    payload: signingObject.payload
  };
}

export async function executeSqlWrite({ namespaceId, chainId, sql, actorId, idempotencyKey }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const normalizedSql = normalizeSqlStatement(sql);

  if (!config.sqlWriteAdapter.enabled) {
    return fail('SQL_WRITE_ADAPTER_DISABLED', 'SQL write adapter is disabled', {
      namespace_id: ns,
      chain_id: chainId
    });
  }

  if (!normalizedSql) {
    return fail('SQL_WRITE_INVALID_STATEMENT', 'sql statement is required', {
      namespace_id: ns,
      chain_id: chainId
    });
  }

  if (config.sqlWriteAdapter.requireIdempotencyKey && !idempotencyKey) {
    return fail('SQL_WRITE_IDEMPOTENCY_REQUIRED', 'Idempotency key is required', {
      namespace_id: ns,
      chain_id: chainId
    });
  }

  let intent;
  try {
    intent = parseWriteSql(normalizedSql);
  } catch (error) {
    return fail('SQL_WRITE_INVALID_STATEMENT', error.message, {
      namespace_id: ns,
      chain_id: chainId
    });
  }

  if (!config.sqlWriteAdapter.allowedEntities.includes(intent.entity)) {
    return fail('SQL_WRITE_ENTITY_FORBIDDEN', `Entity is not allowed: ${intent.entity}`, {
      namespace_id: ns,
      chain_id: chainId,
      entity: intent.entity
    });
  }

  const accountId = actorId || 'acct-sql-adapter-01';
  const statementHash = sha256Hex(Buffer.from(normalizedSql, 'utf8'));

  let mapped;
  try {
    mapped = mapIntentToEvent(intent);
  } catch (error) {
    return fail('SQL_WRITE_TRANSLATION_FAILED', error.message, {
      namespace_id: ns,
      chain_id: chainId,
      entity: intent.entity,
      op: intent.op
    });
  }

  const appended = await withTx((client) =>
    appendDerivedEventTx({
      client,
      namespaceId: ns,
      chainId,
      accountId,
      eventType: mapped.eventType,
      payload: {
        ...mapped.payload,
        actor_id: accountId,
        write_intent: intent.op,
        entity: intent.entity
      },
      idempotencyKey,
      statementHash,
      statement: normalizedSql
    })
  );

  const projectionResult = await runOrdersProjection({
    namespaceId: ns,
    chainId
  });

  if (projectionResult.status !== 'PASS') {
    return fail('PROJECTION_RUN_FAILED', projectionResult.message, {
      namespace_id: ns,
      chain_id: chainId,
      projection_name: 'orders',
      projection_ver: 1
    });
  }

  return pass(
    appended.replay ? 'Idempotent replay: event already appended' : 'SQL translated to Event and projected',
    {
      namespace_id: ns,
      chain_id: chainId,
      entity: intent.entity,
      op: intent.op
    },
    {
      idempotent_replay: appended.replay,
      statement_hash: statementHash,
      event: appended,
      projection: projectionResult.artifact
    }
  );
}
