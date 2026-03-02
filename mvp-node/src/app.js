import express from 'express';
import { query } from './db.js';
import { config } from './config.js';
import { hashCanonicalObject } from './crypto.js';
import { buildSeal, buildSnapshot } from './builders.js';
import { verifyAnchor, verifyChain, verifySeals, verifySnapshot } from './verification.js';

const app = express();
app.use(express.json({ limit: '1mb' }));

app.get('/health', async (_req, res) => {
  try {
    await query('select 1');
    res.json({ status: 'ok' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/v1/chains/:chainId/verify', async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await verifyChain({ namespaceId, chainId: req.params.chainId });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/seals/:chainId/verify', async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await verifySeals({ namespaceId, chainId: req.params.chainId });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/snapshots/:chainId/verify', async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await verifySnapshot({
    namespaceId,
    chainId: req.params.chainId,
    snapshotId: req.body?.snapshot_id
  });
  res.status(result.status === 'PASS' ? 200 : 422).json(result);
});

app.post('/v1/anchors/:chainId/verify', async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const result = await verifyAnchor({ namespaceId, chainId: req.params.chainId });
  res.status(422).json(result);
});

app.post('/v1/seals/:chainId/build', async (req, res) => {
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

app.post('/v1/snapshots/:chainId/build', async (req, res) => {
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

app.post('/v1/chains/:chainId/events', async (req, res) => {
  const namespaceId = req.body?.namespace_id || config.defaultNamespaceId;
  const chainId = req.params.chainId;

  const event = {
    namespace_id: namespaceId,
    chain_id: chainId,
    event_id: req.body?.event_id,
    sequence: Number(req.body?.sequence),
    prev_hash: req.body?.prev_hash,
    account_id: req.body?.account_id,
    event_type: req.body?.event_type,
    event_time: req.body?.event_time,
    payload: req.body?.payload,
    signature: req.body?.signature
  };

  const eventHash = hashCanonicalObject({
    namespace_id: event.namespace_id,
    chain_id: event.chain_id,
    event_id: event.event_id,
    sequence: event.sequence,
    prev_hash: event.prev_hash,
    account_id: event.account_id,
    event_type: event.event_type,
    event_time: event.event_time,
    payload: event.payload
  });

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
        account_id, event_type, event_time, payload, signature
      ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        event.namespace_id,
        event.chain_id,
        event.event_id,
        event.sequence,
        event.prev_hash,
        event.account_id,
        event.event_type,
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

app.use((error, _req, res, _next) => {
  res.status(500).json({ status: 'FAIL', error_code: 'INTERNAL', message: error.message });
});

app.listen(config.port, () => {
  // eslint-disable-next-line no-console
  console.log(`eventdb-mvp listening on :${config.port}`);
});
