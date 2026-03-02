import { Contract, JsonRpcProvider, isHexString } from 'ethers';
import { config } from './config.js';

function normalizeHex32(value) {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (isHexString(trimmed, 32)) {
    return trimmed.toLowerCase();
  }

  if (/^[0-9a-fA-F]{64}$/.test(trimmed)) {
    return `0x${trimmed.toLowerCase()}`;
  }

  return null;
}

function toIsoFromUnix(value) {
  const asNumber = Number(value);
  if (!Number.isFinite(asNumber) || asNumber <= 0) {
    return null;
  }
  return new Date(asNumber * 1000).toISOString();
}

function ensureAnchorConfig() {
  if (!config.anchor.evmRpcUrl) {
    throw new Error('ANCHOR_EVM_RPC_URL is required when anchor is enabled');
  }
  if (!config.anchor.evmContractAddress) {
    throw new Error('ANCHOR_EVM_CONTRACT_ADDRESS is required when anchor is enabled');
  }
  if (!config.anchor.evmMethodCommitment) {
    throw new Error('ANCHOR_EVM_METHOD_COMMITMENT is required when anchor is enabled');
  }
}

export async function verifyAnchorOnEvm({ chainId, checkpointId, expectedCommitmentHash }) {
  ensureAnchorConfig();

  const provider = new JsonRpcProvider(config.anchor.evmRpcUrl);
  const contract = new Contract(config.anchor.evmContractAddress, config.anchor.evmContractAbi, provider);

  const commitmentMethod = contract[config.anchor.evmMethodCommitment];
  if (typeof commitmentMethod !== 'function') {
    throw new Error(`Contract method not found: ${config.anchor.evmMethodCommitment}`);
  }

  const externalRawCommitment = await commitmentMethod(chainId, checkpointId);
  const normalizedExternal = normalizeHex32(String(externalRawCommitment));
  const normalizedExpected = normalizeHex32(expectedCommitmentHash);

  if (!normalizedExternal) {
    return {
      status: 'FAIL',
      error_code: 'ANCHOR_REFERENCE_NOT_FOUND',
      message: 'External commitment is missing or invalid format',
      external: {
        commitment_raw: String(externalRawCommitment)
      }
    };
  }

  if (!normalizedExpected) {
    return {
      status: 'FAIL',
      error_code: 'ANCHOR_CONTEXT_INVALID',
      message: 'Local expected commitment is invalid format',
      external: {
        commitment_raw: String(externalRawCommitment)
      }
    };
  }

  let publishedAt = null;
  if (config.anchor.evmMethodTimestamp) {
    const timestampMethod = contract[config.anchor.evmMethodTimestamp];
    if (typeof timestampMethod === 'function') {
      try {
        const publishedAtRaw = await timestampMethod(chainId, checkpointId);
        publishedAt = toIsoFromUnix(publishedAtRaw);
      } catch {
        publishedAt = null;
      }
    }
  }

  if (normalizedExternal !== normalizedExpected) {
    return {
      status: 'FAIL',
      error_code: 'ANCHOR_COMMITMENT_MISMATCH',
      message: 'External and local commitment differ',
      external: {
        commitment_hash: normalizedExternal,
        published_at: publishedAt
      }
    };
  }

  return {
    status: 'PASS',
    error_code: null,
    message: 'Anchor verification passed against EVM contract',
    external: {
      commitment_hash: normalizedExternal,
      published_at: publishedAt,
      provider: config.anchor.provider,
      contract_address: config.anchor.evmContractAddress
    }
  };
}
