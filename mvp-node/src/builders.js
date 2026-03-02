import { config } from './config.js';
import { hashCanonicalObject, signCanonicalObject } from './crypto.js';
import { query } from './db.js';
import {
  buildEventSigningObject,
  buildSealSigningObject,
  buildSnapshotSigningObject,
  buildWindowCommitment,
  deriveSnapshotData,
  getEventHashesBySequenceRange,
  normalizeUtcTimestamp
} from './verification.js';

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

function toPositiveInt(value) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) {
    return null;
  }
  return num;
}

function currentUtcTimestamp() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function buildWindowId({ namespaceId, chainId, start, end }) {
  const tupleHash = hashCanonicalObject({
    namespace_id: namespaceId,
    chain_id: chainId,
    window_start_sequence: start,
    window_end_sequence: end
  });
  return `win-${start}-${end}-${tupleHash.slice(0, 12)}`;
}

export async function buildSeal({ namespaceId, chainId, windowEndSequence, accountId, sealTime, signature }) {
  const ns = namespaceId || config.defaultNamespaceId;

  const { rows: lastSeals } = await query(
    `select window_end_sequence, seal_hash
     from eventdb_seal
     where namespace_id = $1 and chain_id = $2
     order by window_end_sequence desc
     limit 1`,
    [ns, chainId]
  );

  const lastSeal = lastSeals[0] || null;
  const windowStart = lastSeal ? Number(lastSeal.window_end_sequence) + 1 : 1;

  const { rows: maxRows } = await query(
    `select max(sequence)::bigint as max_sequence
     from eventdb_event where namespace_id = $1 and chain_id = $2`,
    [ns, chainId]
  );

  const maxSequence = maxRows[0]?.max_sequence ? Number(maxRows[0].max_sequence) : 0;
  const resolvedEnd = windowEndSequence ? toPositiveInt(windowEndSequence) : maxSequence;

  if (!resolvedEnd) {
    return fail('SEAL_WINDOW_INVALID', 'window_end_sequence is invalid', {
      namespace_id: ns,
      chain_id: chainId
    });
  }

  if (resolvedEnd < windowStart) {
    return fail('SEAL_WINDOW_INVALID', 'No new window range to seal', {
      namespace_id: ns,
      chain_id: chainId,
      window_start_sequence: windowStart,
      window_end_sequence: resolvedEnd
    });
  }

  let rangeRows;
  let rangeHashes;
  try {
    const range = await getEventHashesBySequenceRange({
      namespaceId: ns,
      chainId,
      start: windowStart,
      end: resolvedEnd
    });
    rangeRows = range.rows;
    rangeHashes = range.hashes;
  } catch (error) {
    return fail('SEAL_WINDOW_INVALID', error.message, {
      namespace_id: ns,
      chain_id: chainId,
      window_start_sequence: windowStart,
      window_end_sequence: resolvedEnd
    });
  }

  if (rangeRows.length !== resolvedEnd - windowStart + 1) {
    return fail('SEAL_WINDOW_INVALID', 'Window events are incomplete', {
      namespace_id: ns,
      chain_id: chainId,
      window_start_sequence: windowStart,
      window_end_sequence: resolvedEnd
    });
  }

  const windowCommitmentHash = buildWindowCommitment({
    namespaceId: ns,
    chainId,
    start: windowStart,
    end: resolvedEnd,
    eventHashes: rangeHashes
  });

  const resolvedAccountId = accountId || 'acct-seal-01';
  const resolvedSealTime = normalizeUtcTimestamp(sealTime || currentUtcTimestamp());
  const prevSealHash = lastSeal ? lastSeal.seal_hash : config.sealGenesisPrevHash;
  const windowId = buildWindowId({
    namespaceId: ns,
    chainId,
    start: windowStart,
    end: resolvedEnd
  });

  const signingObject = buildSealSigningObject({
    namespace_id: ns,
    chain_id: chainId,
    window_id: windowId,
    window_start_sequence: windowStart,
    window_end_sequence: resolvedEnd,
    prev_seal_hash: prevSealHash,
    window_commitment_hash: windowCommitmentHash,
    account_id: resolvedAccountId,
    seal_time: resolvedSealTime
  });

  const sealHash = hashCanonicalObject(signingObject);
  const resolvedSignature = signCanonicalObject({
    accountId: resolvedAccountId,
    signingObject,
    fallbackSignature: signature || `sig_auto_${windowId}`
  });

  await query(
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
      ns,
      chainId,
      windowId,
      windowStart,
      resolvedEnd,
      prevSealHash,
      windowCommitmentHash,
      sealHash,
      resolvedAccountId,
      resolvedSealTime,
      resolvedSignature
    ]
  );

  return pass(
    'Seal built successfully',
    {
      namespace_id: ns,
      chain_id: chainId,
      window_start_sequence: windowStart,
      window_end_sequence: resolvedEnd
    },
    {
      namespace_id: ns,
      chain_id: chainId,
      window_id: windowId,
      window_start_sequence: windowStart,
      window_end_sequence: resolvedEnd,
      prev_seal_hash: prevSealHash,
      window_commitment_hash: windowCommitmentHash,
      seal_hash: sealHash,
      account_id: resolvedAccountId,
      seal_time: resolvedSealTime,
      signature: resolvedSignature
    }
  );
}

export async function buildSnapshot({ namespaceId, chainId, basisSequence, snapshotId, snapshotTime, signature }) {
  const ns = namespaceId || config.defaultNamespaceId;

  const { rows: maxRows } = await query(
    `select max(sequence)::bigint as max_sequence
     from eventdb_event where namespace_id = $1 and chain_id = $2`,
    [ns, chainId]
  );
  const maxSequence = maxRows[0]?.max_sequence ? Number(maxRows[0].max_sequence) : 0;

  const resolvedBasis = basisSequence ? toPositiveInt(basisSequence) : maxSequence;
  if (!resolvedBasis) {
    return fail('SNAPSHOT_REFERENCE_INVALID', 'basis_sequence is invalid', {
      namespace_id: ns,
      chain_id: chainId
    });
  }

  const { rows: basisEvents } = await query(
    `select namespace_id, chain_id, event_id, sequence, prev_hash, account_id, event_type, event_time, payload, signature
     from eventdb_event
     where namespace_id = $1 and chain_id = $2 and sequence <= $3
     order by sequence asc`,
    [ns, chainId, resolvedBasis]
  );

  if (basisEvents.length !== resolvedBasis) {
    return fail('SNAPSHOT_REFERENCE_INVALID', 'Basis sequence is missing chain evidence', {
      namespace_id: ns,
      chain_id: chainId,
      basis_sequence: resolvedBasis
    });
  }

  // Force validation of event shape through canonical signing object construction.
  for (const event of basisEvents) {
    buildEventSigningObject(event);
  }

  const { rows: sealRows } = await query(
    `select seal_hash
     from eventdb_seal
     where namespace_id = $1 and chain_id = $2 and window_end_sequence <= $3
     order by window_end_sequence desc
     limit 1`,
    [ns, chainId, resolvedBasis]
  );

  const basisSealHash = sealRows[0]?.seal_hash || null;
  const snapshotData = deriveSnapshotData(basisEvents);
  const resolvedSnapshotId = snapshotId || `snap-${resolvedBasis}`;
  const resolvedSnapshotTime = normalizeUtcTimestamp(snapshotTime || currentUtcTimestamp());

  const signingObject = buildSnapshotSigningObject({
    namespace_id: ns,
    chain_id: chainId,
    snapshot_id: resolvedSnapshotId,
    basis_sequence: resolvedBasis,
    basis_seal_hash: basisSealHash,
    snapshot_time: resolvedSnapshotTime,
    snapshot_data: snapshotData
  });

  const snapshotHash = hashCanonicalObject(signingObject);
  const snapshotSignerId = `snapshot:${chainId}`;
  const resolvedSignature = signCanonicalObject({
    accountId: snapshotSignerId,
    signingObject,
    fallbackSignature: signature || `sig_auto_${resolvedSnapshotId}`
  });

  await query(
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
      ns,
      chainId,
      resolvedSnapshotId,
      resolvedBasis,
      basisSealHash,
      resolvedSnapshotTime,
      snapshotHash,
      snapshotData,
      resolvedSignature
    ]
  );

  return pass(
    'Snapshot built successfully',
    {
      namespace_id: ns,
      chain_id: chainId,
      snapshot_id: resolvedSnapshotId,
      basis_sequence: resolvedBasis
    },
    {
      namespace_id: ns,
      chain_id: chainId,
      snapshot_id: resolvedSnapshotId,
      basis_sequence: resolvedBasis,
      basis_seal_hash: basisSealHash,
      snapshot_time: resolvedSnapshotTime,
      snapshot_hash: snapshotHash,
      snapshot_data: snapshotData,
      signature: resolvedSignature
    }
  );
}
