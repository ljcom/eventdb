import { config } from './config.js';
import { withTx } from './db.js';
import { getEventTypesForProjection } from './event-types.js';

const ORDERS_PROJECTION = {
  projectionName: 'orders',
  projectionVer: 1,
  logicChecksum: 'orders-v1-registry',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: getEventTypesForProjection('orders')
};

const MARKET_ORDERS_PROJECTION = {
  projectionName: 'market_orders',
  projectionVer: 1,
  logicChecksum: 'market-orders-v1-registry',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: getEventTypesForProjection('market_orders')
};

const POS_SALES_PROJECTION = {
  projectionName: 'pos_sales',
  projectionVer: 1,
  logicChecksum: 'pos-sales-v1-registry',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: getEventTypesForProjection('pos_sales')
};

const CRM_CUSTOMERS_PROJECTION = {
  projectionName: 'crm_customers',
  projectionVer: 1,
  logicChecksum: 'crm-customers-v1-registry',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: getEventTypesForProjection('crm_customers')
};

const PRODUCT_PROJECTION = {
  projectionName: 'product',
  projectionVer: 1,
  logicChecksum: 'product-v1-registry',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: getEventTypesForProjection('product')
};

const STOCK_BALANCE_PROJECTION = {
  projectionName: 'stock_balance',
  projectionVer: 1,
  logicChecksum: 'stock-balance-v1-registry',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: getEventTypesForProjection('stock_balance')
};

const SALES_REPORT_PROJECTION = {
  projectionName: 'sales_report',
  projectionVer: 1,
  logicChecksum: 'sales-report-v1-registry',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: getEventTypesForProjection('sales_report')
};

const GL_BALANCE_PROJECTION = {
  projectionName: 'gl_balance',
  projectionVer: 1,
  logicChecksum: 'gl-balance-v1-registry',
  rebuildStrategy: 'full_rebuild',
  migrationPolicy: 'replay_only',
  trackedEventTypes: getEventTypesForProjection('gl_balance')
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

function normalizeOrderStatus(value) {
  if (!value || typeof value !== 'string') return '';
  return value.trim().toLowerCase();
}

async function ensureProjectionCheckpoint(client, { namespaceId, chainId, projection }) {
  await client.query(
    `insert into projection_registry (projection_name, projection_ver, logic_checksum, status)
     values ($1, $2, $3, 'active')
     on conflict (projection_name, projection_ver) do update set
       logic_checksum = excluded.logic_checksum,
       status = excluded.status`,
    [projection.projectionName, projection.projectionVer, projection.logicChecksum]
  );

  await client.query(
    `update projection_registry
     set rebuild_strategy = $3,
         migration_policy = $4
     where projection_name = $1 and projection_ver = $2`,
    [
      projection.projectionName,
      projection.projectionVer,
      projection.rebuildStrategy,
      projection.migrationPolicy
    ]
  );

  await client.query(
    `insert into eventdb_chain (namespace_id, chain_id)
     values ($1, $2)
     on conflict (namespace_id, chain_id) do nothing`,
    [namespaceId, chainId]
  );

  await client.query(
    `insert into projection_checkpoint (
       namespace_id, chain_id, projection_name, projection_ver, last_sequence
     ) values ($1, $2, $3, $4, 0)
     on conflict (namespace_id, chain_id, projection_name, projection_ver) do nothing`,
    [namespaceId, chainId, projection.projectionName, projection.projectionVer]
  );

  const { rows } = await client.query(
    `select last_sequence
     from projection_checkpoint
     where namespace_id = $1 and chain_id = $2 and projection_name = $3 and projection_ver = $4
     for update`,
    [namespaceId, chainId, projection.projectionName, projection.projectionVer]
  );

  return Number(rows[0]?.last_sequence || 0);
}

async function resolveProjectionTarget(client, { namespaceId, chainId, lastSequence, uptoSequence }) {
  const targetSequence = toPositiveIntOrNull(uptoSequence);
  if (uptoSequence !== undefined && uptoSequence !== null && targetSequence === null) {
    return {
      error: fail('PROJECTION_SEQUENCE_INVALID', 'upto_sequence must be a positive integer', {
        namespace_id: namespaceId,
        chain_id: chainId
      })
    };
  }

  const { rows: maxRows } = await client.query(
    `select max(sequence)::bigint as max_sequence
     from eventdb_event
     where namespace_id = $1 and chain_id = $2`,
    [namespaceId, chainId]
  );

  const maxSequence = maxRows[0]?.max_sequence ? Number(maxRows[0].max_sequence) : 0;
  const resolvedTarget = targetSequence ? Math.min(targetSequence, maxSequence) : maxSequence;

  return { resolvedTarget };
}

async function loadEvents(client, { namespaceId, chainId, fromSequence, toSequence }) {
  const { rows } = await client.query(
    `select namespace_id, chain_id, event_id, sequence, event_type, event_time, payload, domain,
            entity_type, entity_id, tenant_id, store_id, outlet_id
     from eventdb_event
     where namespace_id = $1 and chain_id = $2 and sequence > $3 and sequence <= $4
     order by sequence asc`,
    [namespaceId, chainId, fromSequence, toSequence]
  );
  return rows;
}

async function updateCheckpoint(client, { namespaceId, chainId, projection, processedTo }) {
  await client.query(
    `update projection_checkpoint
     set last_sequence = $5,
         updated_at = now()
     where namespace_id = $1 and chain_id = $2 and projection_name = $3 and projection_ver = $4`,
    [namespaceId, chainId, projection.projectionName, projection.projectionVer, processedTo]
  );
}

async function readCheckpoint(client, { namespaceId, chainId, projection }) {
  const { rows } = await client.query(
    `select last_sequence, updated_at
     from projection_checkpoint
     where namespace_id = $1 and chain_id = $2 and projection_name = $3 and projection_ver = $4
     limit 1`,
    [namespaceId, chainId, projection.projectionName, projection.projectionVer]
  );

  if (!rows[0]) {
    return null;
  }

  return {
    last_sequence: Number(rows[0].last_sequence),
    updated_at: rows[0].updated_at
  };
}

function extractOrderState(event) {
  if (!ORDERS_PROJECTION.trackedEventTypes.has(event.event_type)) {
    return null;
  }

  const payload = event.payload;
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const orderId =
    (typeof payload.orderId === 'string' && payload.orderId) ||
    (typeof payload.ref === 'string' && payload.ref) ||
    '';
  const status =
    (typeof payload.status === 'string' && payload.status) ||
    (event.event_type === 'sales.order.created' ? 'created' : '') ||
    '';
  if (!orderId || (!status && event.event_type !== 'sales.order.deleted')) {
    return null;
  }

  return {
    op: event.event_type === 'sales.order.deleted' ? 'delete' : 'upsert',
    orderId,
    status,
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time
  };
}

function extractMarketOrderState(event) {
  if (!MARKET_ORDERS_PROJECTION.trackedEventTypes.has(event.event_type)) {
    return null;
  }

  const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
  const payloadEntity = payload.entity && typeof payload.entity === 'object' ? payload.entity : {};
  const payloadData = payload.data && typeof payload.data === 'object' ? payload.data : {};
  const orderSnapshot = payloadData.order && typeof payloadData.order === 'object' ? payloadData.order : {};

  const orderId =
    (typeof payloadEntity.id === 'string' && payloadEntity.id) ||
    (typeof orderSnapshot.id === 'string' && orderSnapshot.id) ||
    (typeof payloadData.orderId === 'string' && payloadData.orderId) ||
    (typeof payload.ref === 'string' && payload.ref) ||
    (typeof event.entity_id === 'string' && event.entity_id) ||
    '';

  if (!orderId) {
    return null;
  }

  const eventStatusMap = {
    'market.order.created': 'new_coming',
    'market.order.paid': 'confirmed',
    'market.order.packed': 'packed',
    'market.order.shipped': 'packed',
    'market.order.delivered': 'delivered',
    'market.order.cancelled': 'cancelled'
  };

  const status =
    normalizeOrderStatus(orderSnapshot.status) ||
    normalizeOrderStatus(payloadData.status) ||
    eventStatusMap[event.event_type] ||
    'new_coming';

  return {
    orderId,
    status,
    orderSnapshot,
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time,
    tenantId: event.tenant_id || (payload.tenant && payload.tenant.tenantId) || null,
    storeId: event.store_id || (payload.tenant && payload.tenant.storeId) || null
  };
}

function extractPosState(event) {
  if (!POS_SALES_PROJECTION.trackedEventTypes.has(event.event_type)) {
    return null;
  }

  const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
  const payloadEntity = payload.entity && typeof payload.entity === 'object' ? payload.entity : {};
  const payloadData = payload.data && typeof payload.data === 'object' ? payload.data : {};

  const entityId =
    (typeof payloadEntity.id === 'string' && payloadEntity.id) ||
    (typeof payloadData.saleId === 'string' && payloadData.saleId) ||
    (typeof event.entity_id === 'string' && event.entity_id) ||
    '';

  if (!entityId) {
    return null;
  }

  const statusMap = {
    'pos.sale.created': 'created',
    'pos.sale.completed': 'completed',
    'pos.sale.voided': 'voided',
    'pos.receipt.issued': 'receipt_issued',
    'pos.shift.opened': 'shift_opened',
    'pos.shift.closed': 'shift_closed'
  };

  return {
    saleId: entityId,
    status: statusMap[event.event_type] || 'updated',
    eventName: event.event_type,
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time,
    tenantId: event.tenant_id || (payload.tenant && payload.tenant.tenantId) || null,
    outletId: event.outlet_id || (payload.tenant && payload.tenant.outletId) || null,
    payload
  };
}

function extractCrmState(event) {
  if (!CRM_CUSTOMERS_PROJECTION.trackedEventTypes.has(event.event_type)) {
    return null;
  }

  const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
  const payloadEntity = payload.entity && typeof payload.entity === 'object' ? payload.entity : {};
  const payloadData = payload.data && typeof payload.data === 'object' ? payload.data : {};

  const customerId =
    (typeof payloadEntity.id === 'string' && payloadEntity.id) ||
    (typeof payloadData.customerId === 'string' && payloadData.customerId) ||
    (typeof payloadData.leadId === 'string' && payloadData.leadId) ||
    (typeof event.entity_id === 'string' && event.entity_id) ||
    '';

  if (!customerId) {
    return null;
  }

  const statusMap = {
    'crm.lead.created': 'lead_created',
    'crm.customer.created': 'customer_created',
    'crm.customer.updated': 'customer_updated',
    'crm.activity.logged': 'activity_logged',
    'crm.followup.scheduled': 'followup_scheduled',
    'crm.deal.closed': 'deal_closed'
  };

  return {
    customerId,
    status: statusMap[event.event_type] || 'updated',
    eventName: event.event_type,
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time,
    tenantId: event.tenant_id || (payload.tenant && payload.tenant.tenantId) || null,
    payload
  };
}

function extractProductState(event) {
  if (!PRODUCT_PROJECTION.trackedEventTypes.has(event.event_type)) return null;
  const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
  const productId = typeof payload.productId === 'string' ? payload.productId : '';
  if (!productId) return null;
  return {
    productId,
    name: typeof payload.name === 'string' ? payload.name : '',
    sku: typeof payload.sku === 'string' ? payload.sku : null,
    unit: typeof payload.unit === 'string' ? payload.unit : '',
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time,
    tenantId: event.tenant_id || null
  };
}

function extractStockBalanceState(event) {
  if (!STOCK_BALANCE_PROJECTION.trackedEventTypes.has(event.event_type)) return null;
  const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
  const productId = typeof payload.productId === 'string' ? payload.productId : '';
  const locationId = typeof payload.locationId === 'string' ? payload.locationId : '';
  const quantity = Number(payload.quantity);
  if (!productId || !locationId || !Number.isFinite(quantity)) return null;
  return {
    productId,
    locationId,
    quantityDelta: String(payload.direction).toLowerCase() === 'out' ? -quantity : quantity,
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time,
    tenantId: event.tenant_id || null
  };
}

function extractSalesReportState(event) {
  if (!SALES_REPORT_PROJECTION.trackedEventTypes.has(event.event_type)) return null;
  const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
  const reportKey = String(event.event_time || '').slice(0, 10) || 'undated';
  return {
    reportKey,
    orderDelta: event.event_type === 'sales.order.created' ? 1 : 0,
    invoiceDelta: event.event_type === 'sales.invoice.created' ? 1 : 0,
    amountDelta: Number.isFinite(Number(payload.amount)) ? Number(payload.amount) : 0,
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time,
    tenantId: event.tenant_id || null
  };
}

function extractGlBalanceStates(event) {
  if (!GL_BALANCE_PROJECTION.trackedEventTypes.has(event.event_type)) return [];
  const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
  const period = typeof payload.period === 'string' ? payload.period : String(event.event_time || '').slice(0, 7) || 'undated';

  if (Array.isArray(payload.lines)) {
    return payload.lines
      .map((line) => {
        const accountId = line && typeof line.accountId === 'string' ? line.accountId : '';
        if (!accountId) return null;
        return {
          accountId,
          period: typeof line.period === 'string' ? line.period : period,
          debitDelta: Number.isFinite(Number(line.debit)) ? Number(line.debit) : 0,
          creditDelta: Number.isFinite(Number(line.credit)) ? Number(line.credit) : 0,
          eventId: event.event_id,
          sequence: Number(event.sequence),
          eventTime: event.event_time,
          tenantId: event.tenant_id || null
        };
      })
      .filter(Boolean);
  }

  const accountId = typeof payload.accountId === 'string' ? payload.accountId : '';
  if (!accountId) return [];
  return [{
    accountId,
    period,
    debitDelta: Number.isFinite(Number(payload.amount)) ? Number(payload.amount) : 0,
    creditDelta: 0,
    eventId: event.event_id,
    sequence: Number(event.sequence),
    eventTime: event.event_time,
    tenantId: event.tenant_id || null
  }];
}

export async function runOrdersProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;

  return withTx(async (client) => {
    const lastSequence = await ensureProjectionCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: ORDERS_PROJECTION
    });

    const target = await resolveProjectionTarget(client, {
      namespaceId: ns,
      chainId,
      lastSequence,
      uptoSequence
    });

    if (target.error) {
      return target.error;
    }

    const resolvedTarget = target.resolvedTarget;

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

    const events = await loadEvents(client, {
      namespaceId: ns,
      chainId,
      fromSequence: lastSequence,
      toSequence: resolvedTarget
    });

    let rowsApplied = 0;
    for (const event of events) {
      const state = extractOrderState(event);
      if (!state) continue;

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

    await updateCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: ORDERS_PROJECTION,
      processedTo
    });

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

export async function runMarketOrdersProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;

  return withTx(async (client) => {
    const lastSequence = await ensureProjectionCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: MARKET_ORDERS_PROJECTION
    });

    const target = await resolveProjectionTarget(client, {
      namespaceId: ns,
      chainId,
      lastSequence,
      uptoSequence
    });

    if (target.error) {
      return target.error;
    }

    const resolvedTarget = target.resolvedTarget;

    if (resolvedTarget <= lastSequence) {
      return pass(
        'Projection already up to date',
        {
          namespace_id: ns,
          chain_id: chainId,
          projection_name: MARKET_ORDERS_PROJECTION.projectionName,
          projection_ver: MARKET_ORDERS_PROJECTION.projectionVer
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

    const events = await loadEvents(client, {
      namespaceId: ns,
      chainId,
      fromSequence: lastSequence,
      toSequence: resolvedTarget
    });

    let rowsApplied = 0;
    for (const event of events) {
      const state = extractMarketOrderState(event);
      if (!state) continue;

      const snapshot = state.orderSnapshot || {};
      const customer = snapshot.customer || {};

      await client.query(
        `insert into read.market_orders_v1 (
           namespace_id, chain_id, order_id, status, customer_name, order_date,
           subtotal, shipping_fee, total, delivery, payment, payment_method,
           delivery_method, notes, items, order_snapshot,
           updated_at, source_event_id, source_sequence, projection_ver,
           tenant_id, store_id
         ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16::jsonb,$17,$18,$19,$20,$21,$22)
         on conflict (namespace_id, chain_id, order_id) do update set
           status = excluded.status,
           customer_name = excluded.customer_name,
           order_date = excluded.order_date,
           subtotal = excluded.subtotal,
           shipping_fee = excluded.shipping_fee,
           total = excluded.total,
           delivery = excluded.delivery,
           payment = excluded.payment,
           payment_method = excluded.payment_method,
           delivery_method = excluded.delivery_method,
           notes = excluded.notes,
           items = excluded.items,
           order_snapshot = excluded.order_snapshot,
           updated_at = excluded.updated_at,
           source_event_id = excluded.source_event_id,
           source_sequence = excluded.source_sequence,
           projection_ver = excluded.projection_ver,
           tenant_id = excluded.tenant_id,
           store_id = excluded.store_id`,
        [
          ns,
          chainId,
          state.orderId,
          state.status,
          snapshot.customerName || customer.name || null,
          snapshot.date || null,
          Number.isFinite(Number(snapshot.subtotal)) ? Number(snapshot.subtotal) : null,
          Number.isFinite(Number(snapshot.shippingFee)) ? Number(snapshot.shippingFee) : null,
          Number.isFinite(Number(snapshot.total)) ? Number(snapshot.total) : null,
          snapshot.delivery || null,
          snapshot.payment || null,
          snapshot.paymentMethod || null,
          snapshot.deliveryMethod || null,
          snapshot.notes || null,
          JSON.stringify(Array.isArray(snapshot.items) ? snapshot.items : []),
          JSON.stringify(snapshot),
          state.eventTime,
          state.eventId,
          state.sequence,
          MARKET_ORDERS_PROJECTION.projectionVer,
          state.tenantId,
          state.storeId
        ]
      );

      rowsApplied += 1;
    }

    const processedTo = events.length > 0 ? Number(events[events.length - 1].sequence) : lastSequence;

    await updateCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: MARKET_ORDERS_PROJECTION,
      processedTo
    });

    return pass(
      'Market orders projection run completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: MARKET_ORDERS_PROJECTION.projectionName,
        projection_ver: MARKET_ORDERS_PROJECTION.projectionVer
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

export async function runPosSalesProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;

  return withTx(async (client) => {
    const lastSequence = await ensureProjectionCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: POS_SALES_PROJECTION
    });

    const target = await resolveProjectionTarget(client, {
      namespaceId: ns,
      chainId,
      lastSequence,
      uptoSequence
    });

    if (target.error) {
      return target.error;
    }

    const resolvedTarget = target.resolvedTarget;

    if (resolvedTarget <= lastSequence) {
      return pass(
        'Projection already up to date',
        {
          namespace_id: ns,
          chain_id: chainId,
          projection_name: POS_SALES_PROJECTION.projectionName,
          projection_ver: POS_SALES_PROJECTION.projectionVer
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

    const events = await loadEvents(client, {
      namespaceId: ns,
      chainId,
      fromSequence: lastSequence,
      toSequence: resolvedTarget
    });

    let rowsApplied = 0;
    for (const event of events) {
      const state = extractPosState(event);
      if (!state) continue;

      await client.query(
        `insert into read.pos_sales_v1 (
           namespace_id, chain_id, sale_id, status, event_name, payload,
           updated_at, source_event_id, source_sequence, projection_ver,
           tenant_id, outlet_id
         ) values ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12)
         on conflict (namespace_id, chain_id, sale_id) do update set
           status = excluded.status,
           event_name = excluded.event_name,
           payload = excluded.payload,
           updated_at = excluded.updated_at,
           source_event_id = excluded.source_event_id,
           source_sequence = excluded.source_sequence,
           projection_ver = excluded.projection_ver,
           tenant_id = excluded.tenant_id,
           outlet_id = excluded.outlet_id`,
        [
          ns,
          chainId,
          state.saleId,
          state.status,
          state.eventName,
          JSON.stringify(state.payload),
          state.eventTime,
          state.eventId,
          state.sequence,
          POS_SALES_PROJECTION.projectionVer,
          state.tenantId,
          state.outletId
        ]
      );

      rowsApplied += 1;
    }

    const processedTo = events.length > 0 ? Number(events[events.length - 1].sequence) : lastSequence;

    await updateCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: POS_SALES_PROJECTION,
      processedTo
    });

    return pass(
      'POS sales projection run completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: POS_SALES_PROJECTION.projectionName,
        projection_ver: POS_SALES_PROJECTION.projectionVer
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

export async function runCrmCustomersProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;

  return withTx(async (client) => {
    const lastSequence = await ensureProjectionCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: CRM_CUSTOMERS_PROJECTION
    });

    const target = await resolveProjectionTarget(client, {
      namespaceId: ns,
      chainId,
      lastSequence,
      uptoSequence
    });

    if (target.error) {
      return target.error;
    }

    const resolvedTarget = target.resolvedTarget;

    if (resolvedTarget <= lastSequence) {
      return pass(
        'Projection already up to date',
        {
          namespace_id: ns,
          chain_id: chainId,
          projection_name: CRM_CUSTOMERS_PROJECTION.projectionName,
          projection_ver: CRM_CUSTOMERS_PROJECTION.projectionVer
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

    const events = await loadEvents(client, {
      namespaceId: ns,
      chainId,
      fromSequence: lastSequence,
      toSequence: resolvedTarget
    });

    let rowsApplied = 0;
    for (const event of events) {
      const state = extractCrmState(event);
      if (!state) continue;

      await client.query(
        `insert into read.crm_customers_v1 (
           namespace_id, chain_id, customer_id, status, event_name, payload,
           updated_at, source_event_id, source_sequence, projection_ver,
           tenant_id
         ) values ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11)
         on conflict (namespace_id, chain_id, customer_id) do update set
           status = excluded.status,
           event_name = excluded.event_name,
           payload = excluded.payload,
           updated_at = excluded.updated_at,
           source_event_id = excluded.source_event_id,
           source_sequence = excluded.source_sequence,
           projection_ver = excluded.projection_ver,
           tenant_id = excluded.tenant_id`,
        [
          ns,
          chainId,
          state.customerId,
          state.status,
          state.eventName,
          JSON.stringify(state.payload),
          state.eventTime,
          state.eventId,
          state.sequence,
          CRM_CUSTOMERS_PROJECTION.projectionVer,
          state.tenantId
        ]
      );

      rowsApplied += 1;
    }

    const processedTo = events.length > 0 ? Number(events[events.length - 1].sequence) : lastSequence;

    await updateCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: CRM_CUSTOMERS_PROJECTION,
      processedTo
    });

    return pass(
      'CRM customers projection run completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: CRM_CUSTOMERS_PROJECTION.projectionName,
        projection_ver: CRM_CUSTOMERS_PROJECTION.projectionVer
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

export async function runProductProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;

  return withTx(async (client) => {
    const lastSequence = await ensureProjectionCheckpoint(client, { namespaceId: ns, chainId, projection: PRODUCT_PROJECTION });
    const target = await resolveProjectionTarget(client, { namespaceId: ns, chainId, lastSequence, uptoSequence });
    if (target.error) return target.error;

    const events = await loadEvents(client, { namespaceId: ns, chainId, fromSequence: lastSequence, toSequence: target.resolvedTarget });
    let rowsApplied = 0;
    for (const event of events) {
      const state = extractProductState(event);
      if (!state) continue;
      await client.query(
        `insert into read.products_v1 (
           namespace_id, chain_id, product_id, name, sku, unit, updated_at,
           source_event_id, source_sequence, projection_ver, tenant_id
         ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
         on conflict (namespace_id, chain_id, product_id) do update set
           name = coalesce(nullif(excluded.name, ''), read.products_v1.name),
           sku = coalesce(excluded.sku, read.products_v1.sku),
           unit = coalesce(nullif(excluded.unit, ''), read.products_v1.unit),
           updated_at = excluded.updated_at,
           source_event_id = excluded.source_event_id,
           source_sequence = excluded.source_sequence,
           projection_ver = excluded.projection_ver,
           tenant_id = excluded.tenant_id`,
        [
          ns,
          chainId,
          state.productId,
          state.name,
          state.sku,
          state.unit,
          state.eventTime,
          state.eventId,
          state.sequence,
          PRODUCT_PROJECTION.projectionVer,
          state.tenantId
        ]
      );
      rowsApplied += 1;
    }

    const processedTo = events.length > 0 ? Number(events[events.length - 1].sequence) : lastSequence;
    await updateCheckpoint(client, { namespaceId: ns, chainId, projection: PRODUCT_PROJECTION, processedTo });
    return pass('Product projection run completed', {
      namespace_id: ns,
      chain_id: chainId,
      projection_name: PRODUCT_PROJECTION.projectionName,
      projection_ver: PRODUCT_PROJECTION.projectionVer
    }, {
      from_sequence: lastSequence + 1,
      to_sequence: target.resolvedTarget,
      events_scanned: events.length,
      rows_applied: rowsApplied,
      last_sequence: processedTo
    });
  });
}

export async function runStockBalanceProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;

  return withTx(async (client) => {
    const lastSequence = await ensureProjectionCheckpoint(client, { namespaceId: ns, chainId, projection: STOCK_BALANCE_PROJECTION });
    const target = await resolveProjectionTarget(client, { namespaceId: ns, chainId, lastSequence, uptoSequence });
    if (target.error) return target.error;

    const events = await loadEvents(client, { namespaceId: ns, chainId, fromSequence: lastSequence, toSequence: target.resolvedTarget });
    let rowsApplied = 0;
    for (const event of events) {
      const state = extractStockBalanceState(event);
      if (!state) continue;
      await client.query(
        `insert into read.stock_balances_v1 (
           namespace_id, chain_id, location_id, product_id, quantity, updated_at,
           source_event_id, source_sequence, projection_ver, tenant_id
         ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         on conflict (namespace_id, chain_id, location_id, product_id) do update set
           quantity = read.stock_balances_v1.quantity + excluded.quantity,
           updated_at = excluded.updated_at,
           source_event_id = excluded.source_event_id,
           source_sequence = excluded.source_sequence,
           projection_ver = excluded.projection_ver,
           tenant_id = excluded.tenant_id`,
        [ns, chainId, state.locationId, state.productId, state.quantityDelta, state.eventTime, state.eventId, state.sequence, STOCK_BALANCE_PROJECTION.projectionVer, state.tenantId]
      );
      rowsApplied += 1;
    }

    const processedTo = events.length > 0 ? Number(events[events.length - 1].sequence) : lastSequence;
    await updateCheckpoint(client, { namespaceId: ns, chainId, projection: STOCK_BALANCE_PROJECTION, processedTo });
    return pass('Stock balance projection run completed', {
      namespace_id: ns,
      chain_id: chainId,
      projection_name: STOCK_BALANCE_PROJECTION.projectionName,
      projection_ver: STOCK_BALANCE_PROJECTION.projectionVer
    }, {
      from_sequence: lastSequence + 1,
      to_sequence: target.resolvedTarget,
      events_scanned: events.length,
      rows_applied: rowsApplied,
      last_sequence: processedTo
    });
  });
}

export async function runSalesReportProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;

  return withTx(async (client) => {
    const lastSequence = await ensureProjectionCheckpoint(client, { namespaceId: ns, chainId, projection: SALES_REPORT_PROJECTION });
    const target = await resolveProjectionTarget(client, { namespaceId: ns, chainId, lastSequence, uptoSequence });
    if (target.error) return target.error;

    const events = await loadEvents(client, { namespaceId: ns, chainId, fromSequence: lastSequence, toSequence: target.resolvedTarget });
    let rowsApplied = 0;
    for (const event of events) {
      const state = extractSalesReportState(event);
      if (!state) continue;
      await client.query(
        `insert into read.sales_report_v1 (
           namespace_id, chain_id, report_key, order_count, invoice_count, amount_total, updated_at,
           source_event_id, source_sequence, projection_ver, tenant_id
         ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
         on conflict (namespace_id, chain_id, report_key) do update set
           order_count = read.sales_report_v1.order_count + excluded.order_count,
           invoice_count = read.sales_report_v1.invoice_count + excluded.invoice_count,
           amount_total = read.sales_report_v1.amount_total + excluded.amount_total,
           updated_at = excluded.updated_at,
           source_event_id = excluded.source_event_id,
           source_sequence = excluded.source_sequence,
           projection_ver = excluded.projection_ver,
           tenant_id = excluded.tenant_id`,
        [ns, chainId, state.reportKey, state.orderDelta, state.invoiceDelta, state.amountDelta, state.eventTime, state.eventId, state.sequence, SALES_REPORT_PROJECTION.projectionVer, state.tenantId]
      );
      rowsApplied += 1;
    }

    const processedTo = events.length > 0 ? Number(events[events.length - 1].sequence) : lastSequence;
    await updateCheckpoint(client, { namespaceId: ns, chainId, projection: SALES_REPORT_PROJECTION, processedTo });
    return pass('Sales report projection run completed', {
      namespace_id: ns,
      chain_id: chainId,
      projection_name: SALES_REPORT_PROJECTION.projectionName,
      projection_ver: SALES_REPORT_PROJECTION.projectionVer
    }, {
      from_sequence: lastSequence + 1,
      to_sequence: target.resolvedTarget,
      events_scanned: events.length,
      rows_applied: rowsApplied,
      last_sequence: processedTo
    });
  });
}

export async function runGlBalanceProjection({ namespaceId, chainId, uptoSequence }) {
  const ns = namespaceId || config.defaultNamespaceId;

  return withTx(async (client) => {
    const lastSequence = await ensureProjectionCheckpoint(client, { namespaceId: ns, chainId, projection: GL_BALANCE_PROJECTION });
    const target = await resolveProjectionTarget(client, { namespaceId: ns, chainId, lastSequence, uptoSequence });
    if (target.error) return target.error;

    const events = await loadEvents(client, { namespaceId: ns, chainId, fromSequence: lastSequence, toSequence: target.resolvedTarget });
    let rowsApplied = 0;
    for (const event of events) {
      const states = extractGlBalanceStates(event);
      for (const state of states) {
        await client.query(
          `insert into read.gl_balances_v1 (
             namespace_id, chain_id, account_id, period, debit_total, credit_total, updated_at,
             source_event_id, source_sequence, projection_ver, tenant_id
           ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           on conflict (namespace_id, chain_id, account_id, period) do update set
             debit_total = read.gl_balances_v1.debit_total + excluded.debit_total,
             credit_total = read.gl_balances_v1.credit_total + excluded.credit_total,
             updated_at = excluded.updated_at,
             source_event_id = excluded.source_event_id,
             source_sequence = excluded.source_sequence,
             projection_ver = excluded.projection_ver,
             tenant_id = excluded.tenant_id`,
          [ns, chainId, state.accountId, state.period, state.debitDelta, state.creditDelta, state.eventTime, state.eventId, state.sequence, GL_BALANCE_PROJECTION.projectionVer, state.tenantId]
        );
        rowsApplied += 1;
      }
    }

    const processedTo = events.length > 0 ? Number(events[events.length - 1].sequence) : lastSequence;
    await updateCheckpoint(client, { namespaceId: ns, chainId, projection: GL_BALANCE_PROJECTION, processedTo });
    return pass('GL balance projection run completed', {
      namespace_id: ns,
      chain_id: chainId,
      projection_name: GL_BALANCE_PROJECTION.projectionName,
      projection_ver: GL_BALANCE_PROJECTION.projectionVer
    }, {
      from_sequence: lastSequence + 1,
      to_sequence: target.resolvedTarget,
      events_scanned: events.length,
      rows_applied: rowsApplied,
      last_sequence: processedTo
    });
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

    const checkpoint = await readCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: ORDERS_PROJECTION
    });

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
        checkpoint
      }
    );
  });
}

export async function queryMarketOrdersReadModel({ namespaceId, chainId, status, orderId, limit }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const maxLimit = 1000;
  const safeLimit = Number.isInteger(Number(limit)) && Number(limit) > 0 ? Math.min(Number(limit), maxLimit) : 100;

  const values = [ns, chainId];
  let sql = `select namespace_id, chain_id, order_id, status, customer_name, order_date,
                    subtotal, shipping_fee, total, delivery, payment, payment_method,
                    delivery_method, notes, items, order_snapshot, updated_at,
                    source_event_id, source_sequence, projection_ver, tenant_id, store_id
             from read.market_orders_v1
             where namespace_id = $1 and chain_id = $2`;

  if (status) {
    values.push(status);
    sql += ` and status = $${values.length}`;
  }

  if (orderId) {
    values.push(orderId);
    sql += ` and order_id = $${values.length}`;
  }

  values.push(safeLimit);
  sql += ` order by updated_at desc, order_id asc limit $${values.length}`;

  return withTx(async (client) => {
    const { rows } = await client.query(sql, values);

    const checkpoint = await readCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: MARKET_ORDERS_PROJECTION
    });

    return pass(
      'Market orders read model query completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: MARKET_ORDERS_PROJECTION.projectionName,
        projection_ver: MARKET_ORDERS_PROJECTION.projectionVer
      },
      {
        rows,
        limit: safeLimit,
        checkpoint
      }
    );
  });
}

export async function queryProductsReadModel({ namespaceId, chainId, productId, limit }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const maxLimit = 1000;
  const safeLimit = Number.isInteger(Number(limit)) && Number(limit) > 0 ? Math.min(Number(limit), maxLimit) : 100;

  const values = [ns, chainId];
  let sql = `select namespace_id, chain_id, product_id, name, sku, unit, updated_at,
                    source_event_id, source_sequence, projection_ver, tenant_id
             from read.products_v1
             where namespace_id = $1 and chain_id = $2`;

  if (productId) {
    values.push(productId);
    sql += ` and product_id = $${values.length}`;
  }

  values.push(safeLimit);
  sql += ` order by updated_at desc, product_id asc limit $${values.length}`;

  return withTx(async (client) => {
    const { rows } = await client.query(sql, values);
    const checkpoint = await readCheckpoint(client, { namespaceId: ns, chainId, projection: PRODUCT_PROJECTION });

    return pass(
      'Products read model query completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: PRODUCT_PROJECTION.projectionName,
        projection_ver: PRODUCT_PROJECTION.projectionVer
      },
      { rows, limit: safeLimit, checkpoint }
    );
  });
}

export async function queryStockBalancesReadModel({ namespaceId, chainId, productId, locationId, limit }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const maxLimit = 1000;
  const safeLimit = Number.isInteger(Number(limit)) && Number(limit) > 0 ? Math.min(Number(limit), maxLimit) : 100;

  const values = [ns, chainId];
  let sql = `select namespace_id, chain_id, location_id, product_id, quantity, updated_at,
                    source_event_id, source_sequence, projection_ver, tenant_id
             from read.stock_balances_v1
             where namespace_id = $1 and chain_id = $2`;

  if (productId) {
    values.push(productId);
    sql += ` and product_id = $${values.length}`;
  }

  if (locationId) {
    values.push(locationId);
    sql += ` and location_id = $${values.length}`;
  }

  values.push(safeLimit);
  sql += ` order by updated_at desc, location_id asc, product_id asc limit $${values.length}`;

  return withTx(async (client) => {
    const { rows } = await client.query(sql, values);
    const checkpoint = await readCheckpoint(client, { namespaceId: ns, chainId, projection: STOCK_BALANCE_PROJECTION });

    return pass(
      'Stock balances read model query completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: STOCK_BALANCE_PROJECTION.projectionName,
        projection_ver: STOCK_BALANCE_PROJECTION.projectionVer
      },
      { rows, limit: safeLimit, checkpoint }
    );
  });
}

export async function querySalesReportReadModel({ namespaceId, chainId, reportKey, limit }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const maxLimit = 1000;
  const safeLimit = Number.isInteger(Number(limit)) && Number(limit) > 0 ? Math.min(Number(limit), maxLimit) : 100;

  const values = [ns, chainId];
  let sql = `select namespace_id, chain_id, report_key, order_count, invoice_count, amount_total,
                    updated_at, source_event_id, source_sequence, projection_ver, tenant_id
             from read.sales_report_v1
             where namespace_id = $1 and chain_id = $2`;

  if (reportKey) {
    values.push(reportKey);
    sql += ` and report_key = $${values.length}`;
  }

  values.push(safeLimit);
  sql += ` order by report_key desc limit $${values.length}`;

  return withTx(async (client) => {
    const { rows } = await client.query(sql, values);
    const checkpoint = await readCheckpoint(client, { namespaceId: ns, chainId, projection: SALES_REPORT_PROJECTION });

    return pass(
      'Sales report read model query completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: SALES_REPORT_PROJECTION.projectionName,
        projection_ver: SALES_REPORT_PROJECTION.projectionVer
      },
      { rows, limit: safeLimit, checkpoint }
    );
  });
}

export async function queryGlBalancesReadModel({ namespaceId, chainId, accountId, period, limit }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const maxLimit = 1000;
  const safeLimit = Number.isInteger(Number(limit)) && Number(limit) > 0 ? Math.min(Number(limit), maxLimit) : 100;

  const values = [ns, chainId];
  let sql = `select namespace_id, chain_id, account_id, period, debit_total, credit_total,
                    updated_at, source_event_id, source_sequence, projection_ver, tenant_id
             from read.gl_balances_v1
             where namespace_id = $1 and chain_id = $2`;

  if (accountId) {
    values.push(accountId);
    sql += ` and account_id = $${values.length}`;
  }

  if (period) {
    values.push(period);
    sql += ` and period = $${values.length}`;
  }

  values.push(safeLimit);
  sql += ` order by period desc, account_id asc limit $${values.length}`;

  return withTx(async (client) => {
    const { rows } = await client.query(sql, values);
    const checkpoint = await readCheckpoint(client, { namespaceId: ns, chainId, projection: GL_BALANCE_PROJECTION });

    return pass(
      'GL balances read model query completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: GL_BALANCE_PROJECTION.projectionName,
        projection_ver: GL_BALANCE_PROJECTION.projectionVer
      },
      { rows, limit: safeLimit, checkpoint }
    );
  });
}

export async function queryPosSalesReadModel({ namespaceId, chainId, limit }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const maxLimit = 1000;
  const safeLimit = Number.isInteger(Number(limit)) && Number(limit) > 0 ? Math.min(Number(limit), maxLimit) : 100;

  return withTx(async (client) => {
    const { rows } = await client.query(
      `select namespace_id, chain_id, sale_id, status, event_name, payload, updated_at,
              source_event_id, source_sequence, projection_ver, tenant_id, outlet_id
       from read.pos_sales_v1
       where namespace_id = $1 and chain_id = $2
       order by updated_at desc, sale_id asc
       limit $3`,
      [ns, chainId, safeLimit]
    );

    const checkpoint = await readCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: POS_SALES_PROJECTION
    });

    return pass(
      'POS sales read model query completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: POS_SALES_PROJECTION.projectionName,
        projection_ver: POS_SALES_PROJECTION.projectionVer
      },
      {
        rows,
        limit: safeLimit,
        checkpoint
      }
    );
  });
}

export async function queryCrmCustomersReadModel({ namespaceId, chainId, limit }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const maxLimit = 1000;
  const safeLimit = Number.isInteger(Number(limit)) && Number(limit) > 0 ? Math.min(Number(limit), maxLimit) : 100;

  return withTx(async (client) => {
    const { rows } = await client.query(
      `select namespace_id, chain_id, customer_id, status, event_name, payload, updated_at,
              source_event_id, source_sequence, projection_ver, tenant_id
       from read.crm_customers_v1
       where namespace_id = $1 and chain_id = $2
       order by updated_at desc, customer_id asc
       limit $3`,
      [ns, chainId, safeLimit]
    );

    const checkpoint = await readCheckpoint(client, {
      namespaceId: ns,
      chainId,
      projection: CRM_CUSTOMERS_PROJECTION
    });

    return pass(
      'CRM customers read model query completed',
      {
        namespace_id: ns,
        chain_id: chainId,
        projection_name: CRM_CUSTOMERS_PROJECTION.projectionName,
        projection_ver: CRM_CUSTOMERS_PROJECTION.projectionVer
      },
      {
        rows,
        limit: safeLimit,
        checkpoint
      }
    );
  });
}
