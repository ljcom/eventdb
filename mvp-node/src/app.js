import express from 'express';
import { query } from './db.js';
import { config } from './config.js';
import { hashCanonicalObject } from './crypto.js';
import { requireApiRole } from './auth.js';
import { buildSeal, buildSnapshot } from './builders.js';
import { getEventTypeSpec, validatePayloadForEventType } from './event-types.js';
import {
  queryCrmCustomersReadModel,
  queryGlBalancesReadModel,
  queryMarketOrdersReadModel,
  queryOrdersReadModel,
  queryPosSalesReadModel,
  queryProductsReadModel,
  querySalesReportReadModel,
  queryStockBalancesReadModel,
  runCrmCustomersProjection,
  runGlBalanceProjection,
  runMarketOrdersProjection,
  runOrdersProjection,
  runProductProjection,
  runPosSalesProjection,
  runSalesReportProjection,
  runStockBalanceProjection
} from './projections.js';
import { auditLogMiddleware, rateLimitByRole } from './security.js';
import { executeSqlWrite } from './sql-write-adapter.js';
import { buildEventSigningObject, verifyAnchor, verifyChain, verifySeals, verifySnapshot } from './verification.js';

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(auditLogMiddleware);

app.get('/health', async (_req, res) => {
  try {
    await query('select 1');
    res.json({ status: 'ok' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/v1/chains/:chainId/verify', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await verifyChain({ namespaceId, chainId: req.params.chainId });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/seals/:chainId/verify', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await verifySeals({ namespaceId, chainId: req.params.chainId });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/snapshots/:chainId/verify', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await verifySnapshot({
    namespaceId,
    chainId: req.params.chainId,
    snapshotId: req.body?.snapshot_id
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/anchors/:chainId/verify', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await verifyAnchor({
    namespaceId,
    chainId: req.params.chainId,
    checkpointId: req.body?.checkpoint_id
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/seals/:chainId/build', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await buildSeal({
    namespaceId,
    chainId: req.params.chainId,
    windowEndSequence: req.body?.window_end_sequence,
    accountId: req.body?.account_id,
    sealTime: req.body?.seal_time,
    signature: req.body?.signature
  });
  res.status(result.status === 'PASS' ? 201 : 422).json(result);
});

app.post('/v1/snapshots/:chainId/build', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await buildSnapshot({
    namespaceId,
    chainId: req.params.chainId,
    basisSequence: req.body?.basis_sequence,
    snapshotId: req.body?.snapshot_id,
    snapshotTime: req.body?.snapshot_time,
    signature: req.body?.signature
  });
  res.status(result.status === 'PASS' ? 201 : 422).json(result);
});

app.post('/v1/chains/:chainId/events', rateLimitByRole('ingest'), requireApiRole('ingest'), async (req, res) => {
  const incomingNamespace = req.body?.namespace && typeof req.body.namespace === 'object' ? req.body.namespace : {};
  const namespaceId = req.body?.namespace_id || incomingNamespace.tenantid || config.defaultNamespaceId;
  const chainId = req.params.chainId;

  const payload = req.body?.payload && typeof req.body.payload === 'object' ? req.body.payload : {};
  const payloadEntity = payload.entity && typeof payload.entity === 'object' ? payload.entity : {};
  const payloadTenant = payload.tenant && typeof payload.tenant === 'object' ? payload.tenant : {};

  const incomingEventType =
    (typeof req.body?.event_type === 'string' && req.body.event_type.trim()) ||
    (typeof req.body?.type === 'string' && req.body.type.trim()) ||
    '';
  const eventTypeSpec = getEventTypeSpec(incomingEventType);
  if (!eventTypeSpec) {
    return res.status(422).json({
      status: 'FAIL',
      error_code: 'EVENT_TYPE_NOT_REGISTERED',
      message: 'event_type is not registered',
      checked_scope: { event_type: incomingEventType }
    });
  }

  const incomingVersion = req.body?.version === undefined ? eventTypeSpec.version : Number(req.body.version);
  if (!Number.isInteger(incomingVersion) || incomingVersion !== eventTypeSpec.version) {
    return res.status(422).json({
      status: 'FAIL',
      error_code: 'EVENT_TYPE_VERSION_INVALID',
      message: 'event version does not match registry',
      checked_scope: {
        event_type: incomingEventType,
        expected_version: eventTypeSpec.version,
        received_version: req.body?.version
      }
    });
  }

  const payloadValidation = validatePayloadForEventType(incomingEventType, payload);
  if (!payloadValidation.ok) {
    return res.status(422).json({
      status: 'FAIL',
      error_code: payloadValidation.error_code,
      message: payloadValidation.message,
      checked_scope: payloadValidation.detail
    });
  }

  const incomingDomain = typeof req.body?.domain === 'string' ? req.body.domain.trim() : eventTypeSpec.domain;
  const derivedDomain = incomingDomain || (incomingEventType.includes('.') ? incomingEventType.split('.')[0] : '') || 'legacy';

  const requestedSequence = Number(req.body?.sequence);
  const hasRequestedSequence = Number.isInteger(requestedSequence) && requestedSequence > 0;

  let sequence = hasRequestedSequence ? requestedSequence : 0;
  let prevHash = typeof req.body?.prev_hash === 'string' && req.body.prev_hash ? req.body.prev_hash : '';

  if (!hasRequestedSequence || !prevHash) {
    const { rows } = await query(
      `select namespace_id, chain_id, event_id, sequence, prev_hash, account_id, event_type, event_time, payload
       from eventdb_event
       where namespace_id = $1 and chain_id = $2
       order by sequence desc
       limit 1`,
      [namespaceId, chainId]
    );

    const lastEvent = rows[0] || null;

    if (lastEvent) {
      if (!hasRequestedSequence) {
        sequence = Number(lastEvent.sequence) + 1;
      }
      if (!prevHash) {
        prevHash = hashCanonicalObject(buildEventSigningObject(lastEvent));
      }
    } else {
      if (!hasRequestedSequence) {
        sequence = 1;
      }
      if (!prevHash) {
        prevHash = config.eventGenesisPrevHash;
      }
    }
  }

  const event = {
    namespace_id: namespaceId,
    chain_id: chainId,
    event_id: req.body?.event_id || req.body?.eventid,
    sequence,
    prev_hash: prevHash,
    account_id: req.body?.account_id || incomingNamespace.accountid || req.body?.actorid,
    event_type: incomingEventType,
    domain: derivedDomain,
    entity_type:
      (typeof req.body?.entity_type === 'string' && req.body.entity_type) ||
      (typeof payloadEntity.type === 'string' && payloadEntity.type) ||
      eventTypeSpec.entity,
    entity_id:
      (typeof req.body?.entity_id === 'string' && req.body.entity_id) ||
      (typeof payloadEntity.id === 'string' && payloadEntity.id) ||
      (typeof payload[`${eventTypeSpec.entity}Id`] === 'string' && payload[`${eventTypeSpec.entity}Id`]) ||
      null,
    tenant_id:
      (typeof req.body?.tenant_id === 'string' && req.body.tenant_id) ||
      (typeof incomingNamespace.tenantid === 'string' && incomingNamespace.tenantid) ||
      (typeof payloadTenant.tenantId === 'string' && payloadTenant.tenantId) ||
      null,
    store_id:
      (typeof req.body?.store_id === 'string' && req.body.store_id) ||
      (typeof incomingNamespace.branchid === 'string' && incomingNamespace.branchid) ||
      (typeof payloadTenant.storeId === 'string' && payloadTenant.storeId) ||
      null,
    outlet_id:
      (typeof req.body?.outlet_id === 'string' && req.body.outlet_id) ||
      (typeof payloadTenant.outletId === 'string' && payloadTenant.outletId) ||
      null,
    event_time: req.body?.event_time || req.body?.occurred_at,
    payload,
    signature: req.body?.signature
  };

  const eventHash = hashCanonicalObject(buildEventSigningObject(event));
  if (typeof req.body?.hash === 'string' && req.body.hash && req.body.hash !== eventHash) {
    return res.status(422).json({
      status: 'FAIL',
      error_code: 'EVENT_HASH_INVALID',
      message: 'hash does not match canonical event hash',
      checked_scope: { event_type: incomingEventType, event_id: event.event_id }
    });
  }

  try {
    await query(
      `insert into eventdb_chain (namespace_id, chain_id)
       values ($1, $2)
       on conflict (namespace_id, chain_id) do nothing`,
      [namespaceId, chainId]
    );

    await query(
      `insert into eventdb_event (
        namespace_id, chain_id, event_id, sequence, prev_hash,
        account_id, event_type, domain, entity_type, entity_id,
        tenant_id, store_id, outlet_id, event_time, payload, signature
      ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [
        event.namespace_id,
        event.chain_id,
        event.event_id,
        event.sequence,
        event.prev_hash,
        event.account_id,
        event.event_type,
        event.domain,
        event.entity_type,
        event.entity_id,
        event.tenant_id,
        event.store_id,
        event.outlet_id,
        event.event_time,
        event.payload,
        event.signature
      ]
    );
  } catch (error) {
    return res.status(400).json({ status: 'FAIL', error_code: 'EVT_FIELD_FORMAT_INVALID', message: error.message });
  }

  return res.status(201).json({
    status: 'PASS',
    chain_id: chainId,
    event_id: event.event_id,
    sequence: event.sequence,
    event_hash: eventHash
  });
});

app.post('/v1/projections/:chainId/orders/run', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await runOrdersProjection({
    namespaceId,
    chainId: req.params.chainId,
    uptoSequence: req.body?.upto_sequence
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/projections/:chainId/market-orders/run', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await runMarketOrdersProjection({
    namespaceId,
    chainId: req.params.chainId,
    uptoSequence: req.body?.upto_sequence
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/projections/:chainId/pos-sales/run', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await runPosSalesProjection({
    namespaceId,
    chainId: req.params.chainId,
    uptoSequence: req.body?.upto_sequence
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/projections/:chainId/crm-customers/run', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await runCrmCustomersProjection({
    namespaceId,
    chainId: req.params.chainId,
    uptoSequence: req.body?.upto_sequence
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/projections/:chainId/products/run', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await runProductProjection({
    namespaceId,
    chainId: req.params.chainId,
    uptoSequence: req.body?.upto_sequence
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/projections/:chainId/stock-balances/run', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await runStockBalanceProjection({
    namespaceId,
    chainId: req.params.chainId,
    uptoSequence: req.body?.upto_sequence
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/projections/:chainId/sales-report/run', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await runSalesReportProjection({
    namespaceId,
    chainId: req.params.chainId,
    uptoSequence: req.body?.upto_sequence
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/projections/:chainId/gl-balances/run', rateLimitByRole('ops'), requireApiRole('ops'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await runGlBalanceProjection({
    namespaceId,
    chainId: req.params.chainId,
    uptoSequence: req.body?.upto_sequence
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/sql/:chainId/write', rateLimitByRole('ingest'), requireApiRole('ingest'), async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const idempotencyKey = req.header('idempotency-key') || req.body?.idempotency_key || '';
  const actorId = req.body?.actor_id || '';
  const result = await executeSqlWrite({
    namespaceId,
    chainId: req.params.chainId,
    sql: req.body?.sql,
    actorId,
    idempotencyKey
  });
  res.status(result.status === 'PASS' ? 201 : 422).json(result);
});

app.get('/v1/read/orders/:chainId', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.query?.namespace_id || config.defaultNamespaceId;
  const result = await queryOrdersReadModel({
    namespaceId,
    chainId: req.params.chainId,
    status: req.query?.status || '',
    limit: req.query?.limit
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.get('/v1/read/market-orders/:chainId', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.query?.namespace_id || config.defaultNamespaceId;
  const result = await queryMarketOrdersReadModel({
    namespaceId,
    chainId: req.params.chainId,
    status: req.query?.status || '',
    orderId: req.query?.order_id || '',
    limit: req.query?.limit
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.get('/v1/read/pos-sales/:chainId', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.query?.namespace_id || config.defaultNamespaceId;
  const result = await queryPosSalesReadModel({
    namespaceId,
    chainId: req.params.chainId,
    limit: req.query?.limit
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.get('/v1/read/crm-customers/:chainId', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.query?.namespace_id || config.defaultNamespaceId;
  const result = await queryCrmCustomersReadModel({
    namespaceId,
    chainId: req.params.chainId,
    limit: req.query?.limit
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.get('/v1/read/products/:chainId', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.query?.namespace_id || config.defaultNamespaceId;
  const result = await queryProductsReadModel({
    namespaceId,
    chainId: req.params.chainId,
    productId: req.query?.product_id || '',
    limit: req.query?.limit
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.get('/v1/read/stock-balances/:chainId', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.query?.namespace_id || config.defaultNamespaceId;
  const result = await queryStockBalancesReadModel({
    namespaceId,
    chainId: req.params.chainId,
    productId: req.query?.product_id || '',
    locationId: req.query?.location_id || '',
    limit: req.query?.limit
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.get('/v1/read/sales-report/:chainId', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.query?.namespace_id || config.defaultNamespaceId;
  const result = await querySalesReportReadModel({
    namespaceId,
    chainId: req.params.chainId,
    reportKey: req.query?.report_key || '',
    limit: req.query?.limit
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.get('/v1/read/gl-balances/:chainId', rateLimitByRole('verify'), requireApiRole('verify'), async (req, res) => {
  const namespaceId = req.query?.namespace_id || config.defaultNamespaceId;
  const result = await queryGlBalancesReadModel({
    namespaceId,
    chainId: req.params.chainId,
    accountId: req.query?.account_id || '',
    period: req.query?.period || '',
    limit: req.query?.limit
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.use((error, _req, res, _next) => {
  res.status(500).json({ status: 'FAIL', error_code: 'INTERNAL', message: error.message });
});

app.listen(config.port, () => {
  // eslint-disable-next-line no-console
  console.log(`eventdb-mvp listening on :${config.port}`);
});
