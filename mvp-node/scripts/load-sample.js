import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { config } from '../src/config.js';
import { pool, withTx } from '../src/db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function parseJsonl(content) {
  return content
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function main() {
  const sampleDir = path.resolve(__dirname, '../../../05-mvp/sample');
  const eventsPath = path.join(sampleDir, 'sample-events.jsonl');
  const sealPath = path.join(sampleDir, 'sample-seal.json');
  const snapshotPath = path.join(sampleDir, 'sample-snapshot.json');

  const [eventsJsonl, sealText, snapshotText] = await Promise.all([
    fs.readFile(eventsPath, 'utf8'),
    fs.readFile(sealPath, 'utf8'),
    fs.readFile(snapshotPath, 'utf8')
  ]);

  const events = parseJsonl(eventsJsonl);
  const seal = JSON.parse(sealText);
  const snapshot = JSON.parse(snapshotText);

  const namespaceId =
    events[0]?.namespace_id || seal.namespace_id || snapshot.namespace_id || config.defaultNamespaceId;
  const chainId = events[0]?.chain_id;

  if (!chainId) {
    throw new Error('sample-events.jsonl has no chain_id');
  }

  await withTx(async (client) => {
    await client.query(
      `insert into eventdb_chain(namespace_id, chain_id)
       values ($1, $2)
       on conflict (namespace_id, chain_id) do nothing`,
      [namespaceId, chainId]
    );

    for (const event of events) {
      await client.query(
        `insert into eventdb_event (
          namespace_id, chain_id, event_id, sequence, prev_hash,
          account_id, event_type, event_time, payload, signature
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        on conflict (namespace_id, chain_id, sequence) do update set
          event_id = excluded.event_id,
          prev_hash = excluded.prev_hash,
          account_id = excluded.account_id,
          event_type = excluded.event_type,
          event_time = excluded.event_time,
          payload = excluded.payload,
          signature = excluded.signature`,
        [
          event.namespace_id || namespaceId,
          event.chain_id,
          event.event_id,
          Number(event.sequence),
          event.prev_hash,
          event.account_id,
          event.event_type,
          event.event_time,
          event.payload,
          event.signature
        ]
      );
    }

    await client.query(
      `insert into eventdb_seal (
        namespace_id, chain_id, window_id, window_start_sequence, window_end_sequence,
        prev_seal_hash, window_commitment_hash, seal_hash, account_id, seal_time, signature
      ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      on conflict (namespace_id, chain_id, window_id) do update set
        window_start_sequence = excluded.window_start_sequence,
        window_end_sequence = excluded.window_end_sequence,
        prev_seal_hash = excluded.prev_seal_hash,
        window_commitment_hash = excluded.window_commitment_hash,
        seal_hash = excluded.seal_hash,
        account_id = excluded.account_id,
        seal_time = excluded.seal_time,
        signature = excluded.signature`,
      [
        seal.namespace_id || namespaceId,
        seal.chain_id,
        seal.window_id,
        Number(seal.window_start_sequence),
        Number(seal.window_end_sequence),
        seal.prev_seal_hash,
        seal.window_commitment_hash,
        seal.seal_hash,
        seal.account_id,
        seal.seal_time,
        seal.signature
      ]
    );

    await client.query(
      `insert into eventdb_snapshot (
        namespace_id, chain_id, snapshot_id, basis_sequence, basis_seal_hash,
        snapshot_time, snapshot_hash, snapshot_data, signature
      ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      on conflict (namespace_id, chain_id, snapshot_id) do update set
        basis_sequence = excluded.basis_sequence,
        basis_seal_hash = excluded.basis_seal_hash,
        snapshot_time = excluded.snapshot_time,
        snapshot_hash = excluded.snapshot_hash,
        snapshot_data = excluded.snapshot_data,
        signature = excluded.signature`,
      [
        snapshot.namespace_id || namespaceId,
        snapshot.chain_id,
        snapshot.snapshot_id,
        Number(snapshot.basis_sequence),
        snapshot.basis_seal_hash,
        snapshot.snapshot_time,
        snapshot.snapshot_hash,
        snapshot.snapshot_data,
        snapshot.signature
      ]
    );
  });

  // eslint-disable-next-line no-console
  console.log(`Sample loaded for namespace=${namespaceId}, chain=${chainId}`);
}

main()
  .catch((error) => {
    // eslint-disable-next-line no-console
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
