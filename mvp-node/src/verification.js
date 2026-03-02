import { query } from './db.js';
import { config } from './config.js';
import { hashCanonicalObject, verifySignature } from './crypto.js';
import { canonicalJsonString } from './canonical-json.js';

function fail(errorCode, message, checkedScope = {}) {
  return {
    status: 'FAIL',
    checked_scope: checkedScope,
    error_code: errorCode,
    message
  };
}

function pass(message, checkedScope = {}) {
  return {
    status: 'PASS',
    checked_scope: checkedScope,
    error_code: null,
    message
  };
}

function isRfc3339Utc(value) {
  if (typeof value !== 'string') {
    return false;
  }
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(value)) {
    return false;
  }
  const date = new Date(value);
  return !Number.isNaN(date.getTime());
}

export function normalizeUtcTimestamp(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error('Invalid timestamp');
  }
  return date.toISOString().replace('.000Z', 'Z');
}

export function buildEventSigningObject(event) {
  const signing = {
    chain_id: event.chain_id,
    event_id: event.event_id,
    sequence: Number(event.sequence),
    prev_hash: event.prev_hash,
    account_id: event.account_id,
    event_type: event.event_type,
    event_time: normalizeUtcTimestamp(event.event_time),
    payload: event.payload
  };

  if (event.namespace_id) {
    signing.namespace_id = event.namespace_id;
  }

  return signing;
}

export function buildSealSigningObject(seal) {
  return {
    namespace_id: seal.namespace_id,
    chain_id: seal.chain_id,
    window_id: seal.window_id,
    window_start_sequence: Number(seal.window_start_sequence),
    window_end_sequence: Number(seal.window_end_sequence),
    prev_seal_hash: seal.prev_seal_hash,
    window_commitment_hash: seal.window_commitment_hash,
    account_id: seal.account_id,
    seal_time: normalizeUtcTimestamp(seal.seal_time)
  };
}

export function buildSnapshotSigningObject(snapshot) {
  return {
    namespace_id: snapshot.namespace_id,
    chain_id: snapshot.chain_id,
    snapshot_id: snapshot.snapshot_id,
    basis_sequence: Number(snapshot.basis_sequence),
    basis_seal_hash: snapshot.basis_seal_hash || null,
    snapshot_time: normalizeUtcTimestamp(snapshot.snapshot_time),
    snapshot_data: snapshot.snapshot_data
  };
}

function validateEventShape(event) {
  const required = [
    'chain_id',
    'event_id',
    'sequence',
    'prev_hash',
    'account_id',
    'event_type',
    'event_time',
    'payload',
    'signature'
  ];

  for (const key of required) {
    if (event[key] === undefined || event[key] === null) {
      return `Missing field ${key}`;
    }
  }

  if (typeof event.chain_id !== 'string' || !event.chain_id) return 'Invalid chain_id';
  if (typeof event.event_id !== 'string' || !event.event_id) return 'Invalid event_id';
  if (!Number.isInteger(Number(event.sequence)) || Number(event.sequence) <= 0) return 'Invalid sequence';
  if (typeof event.prev_hash !== 'string' || !event.prev_hash) return 'Invalid prev_hash';
  if (typeof event.account_id !== 'string' || !event.account_id) return 'Invalid account_id';
  if (typeof event.event_type !== 'string' || !event.event_type) return 'Invalid event_type';
  try {
    if (!isRfc3339Utc(normalizeUtcTimestamp(event.event_time))) return 'Invalid event_time';
  } catch {
    return 'Invalid event_time';
  }
  if (typeof event.payload !== 'object' || event.payload === null) return 'Invalid payload';
  if (typeof event.signature !== 'string' || !event.signature) return 'Invalid signature';

  return null;
}

export async function verifyChain({ namespaceId, chainId }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const { rows: events } = await query(
    `select namespace_id, chain_id, event_id, sequence, prev_hash, account_id, event_type, event_time, payload, signature
     from eventdb_event
     where namespace_id = $1 and chain_id = $2
     order by sequence asc`,
    [ns, chainId]
  );

  if (events.length === 0) {
    return fail('CHAIN_SEQUENCE_INVALID', 'No events found for chain', { namespace_id: ns, chain_id: chainId });
  }

  let expectedSequence = Number(events[0].sequence);
  let prevEventHash = null;

  for (let i = 0; i < events.length; i += 1) {
    const event = events[i];
    const shapeError = validateEventShape(event);
    if (shapeError) {
      return fail('EVT_FIELD_FORMAT_INVALID', shapeError, {
        namespace_id: ns,
        chain_id: chainId,
        sequence: Number(event.sequence)
      });
    }

    const seq = Number(event.sequence);
    if (seq !== expectedSequence) {
      return fail('CHAIN_SEQUENCE_INVALID', `Expected sequence ${expectedSequence}, got ${seq}`, {
        namespace_id: ns,
        chain_id: chainId,
        sequence: seq
      });
    }

    if (i === 0) {
      if (event.prev_hash !== config.eventGenesisPrevHash) {
        return fail('CHAIN_PREV_HASH_INVALID', 'Genesis prev_hash is invalid', {
          namespace_id: ns,
          chain_id: chainId,
          sequence: seq
        });
      }
    } else if (event.prev_hash !== prevEventHash) {
      return fail('CHAIN_PREV_HASH_INVALID', 'prev_hash does not match previous event hash', {
        namespace_id: ns,
        chain_id: chainId,
        sequence: seq
      });
    }

    const signingObject = buildEventSigningObject(event);
    const eventHash = hashCanonicalObject(signingObject);

    const signatureValid = verifySignature({
      accountId: event.account_id,
      signature: event.signature,
      signingObject
    });

    if (!signatureValid) {
      return fail('EVT_SIGNATURE_INVALID', 'Event signature validation failed', {
        namespace_id: ns,
        chain_id: chainId,
        sequence: seq
      });
    }

    prevEventHash = eventHash;
    expectedSequence += 1;
  }

  return pass('Event chain verification passed', {
    namespace_id: ns,
    chain_id: chainId,
    events_checked: events.length
  });
}

export function buildWindowCommitment({ namespaceId, chainId, start, end, eventHashes }) {
  return hashCanonicalObject({
    namespace_id: namespaceId,
    chain_id: chainId,
    window_start_sequence: start,
    window_end_sequence: end,
    event_hashes: eventHashes
  });
}

export async function getEventHashesBySequenceRange({ namespaceId, chainId, start, end }) {
  const { rows } = await query(
    `select namespace_id, chain_id, event_id, sequence, prev_hash, account_id, event_type, event_time, payload, signature
     from eventdb_event
     where namespace_id = $1 and chain_id = $2 and sequence between $3 and $4
     order by sequence asc`,
    [namespaceId, chainId, start, end]
  );

  const hashes = [];
  for (const row of rows) {
    const shapeError = validateEventShape(row);
    if (shapeError) {
      throw new Error(`Invalid event in window: ${shapeError}`);
    }
    hashes.push(hashCanonicalObject(buildEventSigningObject(row)));
  }
  return { rows, hashes };
}

export async function verifySeals({ namespaceId, chainId }) {
  const ns = namespaceId || config.defaultNamespaceId;
  const { rows: seals } = await query(
    `select namespace_id, chain_id, window_id, window_start_sequence, window_end_sequence,
            prev_seal_hash, window_commitment_hash, seal_hash, account_id, seal_time, signature
     from eventdb_seal
     where namespace_id = $1 and chain_id = $2
     order by window_end_sequence asc`,
    [ns, chainId]
  );

  if (seals.length === 0) {
    return fail('SEAL_REQUIRED_FIELD_MISSING', 'No seals found for chain', { namespace_id: ns, chain_id: chainId });
  }

  let prevSealHash = config.sealGenesisPrevHash;
  let lastWindowEnd = 0;

  for (const seal of seals) {
    const start = Number(seal.window_start_sequence);
    const end = Number(seal.window_end_sequence);

    if (!seal.window_id || start <= 0 || end < start || start !== lastWindowEnd + 1) {
      return fail('SEAL_WINDOW_INVALID', 'Window boundary is invalid or non-contiguous', {
        namespace_id: ns,
        chain_id: chainId,
        window_id: seal.window_id
      });
    }

    if (seal.prev_seal_hash !== prevSealHash) {
      return fail('SEAL_PREV_HASH_INVALID', 'prev_seal_hash linkage mismatch', {
        namespace_id: ns,
        chain_id: chainId,
        window_id: seal.window_id
      });
    }

    let windowEvents;
    let hashes;
    try {
      const result = await getEventHashesBySequenceRange({
        namespaceId: ns,
        chainId,
        start,
        end
      });
      windowEvents = result.rows;
      hashes = result.hashes;
    } catch (error) {
      return fail('SEAL_WINDOW_INVALID', error.message, {
        namespace_id: ns,
        chain_id: chainId,
        window_id: seal.window_id
      });
    }

    if (windowEvents.length !== end - start + 1) {
      return fail('SEAL_WINDOW_INVALID', 'Window events are incomplete', {
        namespace_id: ns,
        chain_id: chainId,
        window_id: seal.window_id
      });
    }

    const recomputedCommitment = buildWindowCommitment({
      namespaceId: ns,
      chainId,
      start,
      end,
      eventHashes: hashes
    });

    if (recomputedCommitment !== seal.window_commitment_hash) {
      return fail('SEAL_COMMITMENT_MISMATCH', 'window_commitment_hash mismatch', {
        namespace_id: ns,
        chain_id: chainId,
        window_id: seal.window_id
      });
    }

    const signingObject = buildSealSigningObject(seal);
    const recomputedSealHash = hashCanonicalObject(signingObject);
    if (recomputedSealHash !== seal.seal_hash) {
      return fail('EVT_HASH_MISMATCH', 'seal_hash mismatch', {
        namespace_id: ns,
        chain_id: chainId,
        window_id: seal.window_id
      });
    }

    const signatureValid = verifySignature({
      accountId: seal.account_id,
      signature: seal.signature,
      signingObject
    });

    if (!signatureValid) {
      return fail('SEAL_SIGNATURE_INVALID', 'Seal signature validation failed', {
        namespace_id: ns,
        chain_id: chainId,
        window_id: seal.window_id
      });
    }

    prevSealHash = seal.seal_hash;
    lastWindowEnd = end;
  }

  return pass('Seal verification passed', {
    namespace_id: ns,
    chain_id: chainId,
    seals_checked: seals.length
  });
}

export function deriveSnapshotData(events) {
  const state = {};
  let lastSequence = 0;

  for (const event of events) {
    lastSequence = Number(event.sequence);
    if (event.payload && typeof event.payload === 'object' && event.payload.ref && event.payload.status) {
      state[event.payload.ref] = event.payload.status;
    }
  }

  return {
    last_sequence: lastSequence,
    state
  };
}

export async function verifySnapshot({ namespaceId, chainId, snapshotId }) {
  const ns = namespaceId || config.defaultNamespaceId;

  const snapshotQuery = snapshotId
    ? `select namespace_id, chain_id, snapshot_id, basis_sequence, basis_seal_hash, snapshot_time, snapshot_hash, snapshot_data, signature
       from eventdb_snapshot where namespace_id = $1 and chain_id = $2 and snapshot_id = $3 limit 1`
    : `select namespace_id, chain_id, snapshot_id, basis_sequence, basis_seal_hash, snapshot_time, snapshot_hash, snapshot_data, signature
       from eventdb_snapshot where namespace_id = $1 and chain_id = $2 order by snapshot_time desc limit 1`;

  const params = snapshotId ? [ns, chainId, snapshotId] : [ns, chainId];
  const { rows: snapshots } = await query(snapshotQuery, params);

  if (snapshots.length === 0) {
    return fail('SNAPSHOT_REFERENCE_INVALID', 'Snapshot not found', {
      namespace_id: ns,
      chain_id: chainId,
      snapshot_id: snapshotId || null
    });
  }

  const snapshot = snapshots[0];
  const basisSequence = Number(snapshot.basis_sequence);

  const { rows: basisEvents } = await query(
    `select namespace_id, chain_id, event_id, sequence, prev_hash, account_id, event_type, event_time, payload, signature
     from eventdb_event
     where namespace_id = $1 and chain_id = $2 and sequence <= $3
     order by sequence asc`,
    [ns, chainId, basisSequence]
  );

  if (basisEvents.length !== basisSequence) {
    return fail('SNAPSHOT_REFERENCE_INVALID', 'Basis sequence is missing chain evidence', {
      namespace_id: ns,
      chain_id: chainId,
      snapshot_id: snapshot.snapshot_id
    });
  }

  const derivedData = deriveSnapshotData(basisEvents);
  const derivedHash = hashCanonicalObject(buildSnapshotSigningObject(snapshot));

  if (canonicalJsonString(derivedData) !== canonicalJsonString(snapshot.snapshot_data)) {
    return fail('SNAPSHOT_DERIVATION_MISMATCH', 'Snapshot data derivation mismatch', {
      namespace_id: ns,
      chain_id: chainId,
      snapshot_id: snapshot.snapshot_id
    });
  }

  if (derivedHash !== snapshot.snapshot_hash) {
    return fail('EVT_HASH_MISMATCH', 'snapshot_hash mismatch', {
      namespace_id: ns,
      chain_id: chainId,
      snapshot_id: snapshot.snapshot_id
    });
  }

  const signingObject = buildSnapshotSigningObject(snapshot);
  const signatureValid = verifySignature({
    accountId: 'snapshot:' + snapshot.chain_id,
    signature: snapshot.signature,
    signingObject
  });

  if (config.signatureMode !== 'none' && !signatureValid) {
    return fail('SNAPSHOT_SIGNATURE_INVALID', 'Snapshot signature validation failed', {
      namespace_id: ns,
      chain_id: chainId,
      snapshot_id: snapshot.snapshot_id
    });
  }

  return pass('Snapshot verification passed', {
    namespace_id: ns,
    chain_id: chainId,
    snapshot_id: snapshot.snapshot_id
  });
}

export async function verifyAnchor({ namespaceId, chainId }) {
  const ns = namespaceId || config.defaultNamespaceId;
  return fail('ANCHOR_REFERENCE_NOT_FOUND', 'Anchor adapter is not implemented in this MVP', {
    namespace_id: ns,
    chain_id: chainId
  });
}
