function canonicalize(value) {
  if (value === null) return 'null';

  const valueType = typeof value;
  if (valueType === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error('Non-finite number is not canonicalizable');
    }
    return JSON.stringify(value);
  }

  if (valueType === 'boolean' || valueType === 'string') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalize(item)).join(',')}]`;
  }

  if (valueType === 'object') {
    const keys = Object.keys(value).sort();
    const pairs = keys.map((key) => `${JSON.stringify(key)}:${canonicalize(value[key])}`);
    return `{${pairs.join(',')}}`;
  }

  throw new Error(`Unsupported type for canonicalization: ${valueType}`);
}

export function canonicalJsonString(value) {
  return canonicalize(value);
}

export function canonicalJsonBuffer(value) {
  return Buffer.from(canonicalize(value), 'utf8');
}
