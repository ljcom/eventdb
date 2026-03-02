import { config } from './config.js';
import { withTx } from './db.js';

const ORDERS_PROJECTION = {
  projectionName: 'orders',
  projectionVer: 1,
  logicChecksum: 'orders-v1-ref-status',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: new Set([
    'record_created',
    'record_updated',
    'record_finalized',
    'record_canceled',
    'OrderCreated',
    'OrderUpdated',
    'PaymentConfirmed',
    'OrderCancelled',
    'OrderDeleted'
  ])
};

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

function toPositiveIntOrNull(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) {
    return null;
  }
  return num;
}

function extractOrderState(event) {
  if (!ORDERS_PROJECTION.trackedEventTypes.has(event.event_type)) {
    return null;
  }

  const payload = event.payload;
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const orderId = typeof payload.ref === 'string' ? payload.ref : '';
  const status = typeof payload.status === 'string' ? payload.status : '';
  if (!orderId || !status) {
    return null;
  }

  return {
    op: event.event_type === 'OrderDeleted' ? 'delete' : 'upsert',
    orderId,
    status,
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time
  };
}

export async function runOrdersProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const targetSequence = toPositiveIntOrNull(uptoSequence);
  if (uptoSequence !== undefined && uptoSequence !== null && targetSequence === null) {
    return fail('PROJECTION_SEQUENCE_INVALID', 'upto_sequence must be a positive integer', {
      namespace_id: ns,
      chain_id: chainId,
      projection_name: ORDERS_PROJECTION.projectionName,
      projection_ver: ORDERS_PROJECTION.projectionVer
    });
  }

  return withTx(async (client) => {
    await client.query(
      `insert into projection_registry (projection_name, projection_ver, logic_checksum, status)
       values ($1, $2, $3, 'active')
       on conflict (projection_name, projection_ver) do update set
         logic_checksum = excluded.logic_checksum,
         status = excluded.status`,
      [ORDERS_PROJECTION.projectionName, ORDERS_PROJECTION.projectionVer, ORDERS_PROJECTION.logicChecksum]
    );
    await client.query(
      `update projection_registry
       set rebuild_strategy = $3,
           migration_policy = $4
       where projection_name = $1 and projection_ver = $2`,
      [
        ORDERS_PROJECTION.projectionName,
        ORDERS_PROJECTION.projectionVer,
        ORDERS_PROJECTION.rebuildStrategy,
        ORDERS_PROJECTION.migrationPolicy
      ]
    );

    await client.query(
      `insert into eventdb_chain (namespace_id, chain_id)
       values ($1, $2)
       on conflict (namespace_id, chain_id) do nothing`,
      [ns, chainId]
    );

    await client.query(
      `insert into projection_checkpoint (
         namespace_id, chain_id, projection_name, projection_ver, last_sequence
       ) values ($1, $2, $3, $4, 0)
       on conflict (namespace_id, chain_id, projection_name, projection_ver) do nothing`,
      [ns, chainId, ORDERS_PROJECTION.projectionName, ORDERS_PROJECTION.projectionVer]
    );

    const { rows: checkpointRows } = await client.query(
      `select last_sequence
       from projection_checkpoint
       where namespace_id = $1 and chain_id = $2 and projection_name = $3 and projection_ver = $4
       for update`,
      [ns, chainId, ORDERS_PROJECTION.projectionName, ORDERS_PROJECTION.projectionVer]
    );

    const lastSequence = Number(checkpointRows[0]?.last_sequence || 0);

    const { rows: maxRows } = await client.query(
      `select max(sequence)::bigint as max_sequence
       from eventdb_event
       where namespace_id = $1 and chain_id = $2`,
      [ns, chainId]
    );

    const maxSequence = maxRows[0]?.max_sequence ? Number(maxRows[0].max_sequence) : 0;
    const resolvedTarget = targetSequence ? Math.min(targetSequence, maxSequence) : maxSequence;

    if (resolvedTarget <= lastSequence) {
      return pass(
        'Projection already up to date',
        {
          namespace_id: ns,
          chain_id: chainId,
          projection_name: ORDERS_PROJECTION.projectionName,
          projection_ver: ORDERS_PROJECTION.projectionVer
        },
        {
          from_sequence: lastSequence + 1,
          to_sequence: resolvedTarget,
          events_scanned: 0,
          rows_applied: 0,
          last_sequence: lastSequence
        }
      );
    }

    const { rows: events } = await client.query(
      `select namespace_id, chain_id, event_id, sequence, event_type, event_time, payload
       from eventdb_event
       where namespace_id = $1 and chain_id = $2 and sequence > $3 and sequence <= $4
       order by sequence asc`,
      [ns, chainId, lastSequence, resolvedTarget]
    );

    let rowsApplied = 0;
    for (const event of events) {
      const state = extractOrderState(event);
      if (!state) {
        continue;
      }

      if (state.op === 'delete') {
        await client.query(
          `delete from read.orders_v1
           where namespace_id = $1 and chain_id = $2 and order_id = $3`,
          [ns, chainId, state.orderId]
        );
      } else {
        await client.query(
          `insert into read.orders_v1 (
             namespace_id, chain_id, order_id, status,
             updated_at, source_event_id, source_sequence, projection_ver
           ) values ($1,$2,$3,$4,$5,$6,$7,$8)
           on conflict (namespace_id, chain_id, order_id) do update set
             status = excluded.status,
             updated_at = excluded.updated_at,
             source_event_id = excluded.source_event_id,
             source_sequence = excluded.source_sequence,
             projection_ver = excluded.projection_ver`,
          [
            ns,
            chainId,
            state.orderId,
            state.status,
            state.eventTime,
            state.eventId,
            state.sequence,
            ORDERS_PROJECTION.projectionVer
          ]
        );
      }
      rowsApplied += 1;
    }

    const processedTo = events.length > 0 ? Number(events[events.length - 1].sequence) : lastSequence;

    await client.query(
      `update projection_checkpoint
       set last_sequence = $5,
           updated_at = now()
       where namespace_id = $1 and chain_id = $2 and projection_name = $3 and projection_ver = $4`,
      [ns, chainId, ORDERS_PROJECTION.projectionName, ORDERS_PROJECTION.projectionVer, processedTo]
    );

    return pass(
      'Orders projection run completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: ORDERS_PROJECTION.projectionName,
        projection_ver: ORDERS_PROJECTION.projectionVer
      },
      {
        from_sequence: lastSequence + 1,
        to_sequence: resolvedTarget,
        events_scanned: events.length,
        rows_applied: rowsApplied,
        last_sequence: processedTo
      }
    );
  });
}

export async function queryOrdersReadModel({ namespaceId, chainId, status, limit }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const maxLimit = 1000;
  const safeLimit = Number.isInteger(Number(limit)) && Number(limit) > 0 ? Math.min(Number(limit), maxLimit) : 100;

  const values = [ns, chainId];
  let sql = `select namespace_id, chain_id, order_id, status, updated_at, source_event_id, source_sequence, projection_ver
             from read.orders_v1
             where namespace_id = $1 and chain_id = $2`;

  if (status) {
    values.push(status);
    sql += ` and status = $${values.length}`;
  }

  values.push(safeLimit);
  sql += ` order by updated_at desc, order_id asc limit $${values.length}`;

  return withTx(async (client) => {
    const { rows } = await client.query(sql, values);

    const { rows: checkpointRows } = await client.query(
      `select last_sequence, updated_at
       from projection_checkpoint
       where namespace_id = $1 and chain_id = $2 and projection_name = $3 and projection_ver = $4
       limit 1`,
      [ns, chainId, ORDERS_PROJECTION.projectionName, ORDERS_PROJECTION.projectionVer]
    );

    return pass(
      'Orders read model query completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: ORDERS_PROJECTION.projectionName,
        projection_ver: ORDERS_PROJECTION.projectionVer
      },
      {
        rows,
        limit: safeLimit,
        checkpoint: checkpointRows[0]
          ? {
              last_sequence: Number(checkpointRows[0].last_sequence),
              updated_at: checkpointRows[0].updated_at
            }
          : null
      }
    );
  });
}
