import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_REGISTRY_PATH = path.resolve(__dirname, '../../../../config/event-types.json');

function fail(errorCode, message, detail = {}) {
  return {
    ok: false,
    error_code: errorCode,
    message,
    detail
  };
}

function pass(artifact = {}) {
  return {
    ok: true,
    artifact
  };
}

function parseSchemaRule(rule) {
  if (typeof rule !== 'string' || !rule.trim()) {
    return null;
  }
  const trimmed = rule.trim();
  const optional = trimmed.endsWith('?');
  const type = optional ? trimmed.slice(0, -1) : trimmed;
  return { type, optional };
}

function matchesSchemaType(value, type) {
  if (type === 'array') return Array.isArray(value);
  if (type === 'object') return value !== null && typeof value === 'object' && !Array.isArray(value);
  if (type === 'number') return typeof value === 'number' && Number.isFinite(value);
  return typeof value === type;
}

function validateRegistryShape(registry) {
  if (!registry || typeof registry !== 'object' || Array.isArray(registry)) {
    return fail('EVENT_TYPE_REGISTRY_INVALID', 'Event type registry must be an object');
  }

  for (const [eventType, spec] of Object.entries(registry)) {
    if (!spec || typeof spec !== 'object' || Array.isArray(spec)) {
      return fail('EVENT_TYPE_SPEC_INVALID', 'Event type spec must be an object', { event_type: eventType });
    }

    const expectedType = [spec.domain, spec.entity, spec.action].join('.');
    if (eventType !== expectedType) {
      return fail('EVENT_TYPE_KEY_MISMATCH', 'Event type key must match domain.entity.action', {
        event_type: eventType,
        expected_type: expectedType
      });
    }

    if (!Number.isInteger(spec.version) || spec.version <= 0) {
      return fail('EVENT_TYPE_VERSION_INVALID', 'Event type version must be a positive integer', {
        event_type: eventType
      });
    }

    if (!spec.schema || typeof spec.schema !== 'object' || Array.isArray(spec.schema)) {
      return fail('EVENT_TYPE_SCHEMA_INVALID', 'Event type schema must be an object', { event_type: eventType });
    }

    for (const [fieldName, rule] of Object.entries(spec.schema)) {
      if (!parseSchemaRule(rule)) {
        return fail('EVENT_TYPE_SCHEMA_RULE_INVALID', 'Event type schema rule must be a non-empty string', {
          event_type: eventType,
          field: fieldName
        });
      }
    }
  }

  return pass({ event_type_count: Object.keys(registry).length });
}

export function loadEventTypeRegistry(registryPath = process.env.EVENT_TYPES_CONFIG_PATH || DEFAULT_REGISTRY_PATH) {
  const raw = fs.readFileSync(registryPath, 'utf8');
  const registry = JSON.parse(raw);
  const validation = validateRegistryShape(registry);
  if (!validation.ok) {
    const error = new Error(validation.message);
    error.code = validation.error_code;
    error.detail = validation.detail;
    throw error;
  }
  return registry;
}

export const eventTypeRegistry = loadEventTypeRegistry();

export function validateEventTypeRegistry(registry = eventTypeRegistry) {
  return validateRegistryShape(registry);
}

export function getEventTypeSpec(eventType, registry = eventTypeRegistry) {
  return registry[eventType] || null;
}

export function getEventTypesForProjection(projectionName, registry = eventTypeRegistry) {
  return new Set(
    Object.entries(registry)
      .filter(([, spec]) => Array.isArray(spec.projections) && spec.projections.includes(projectionName))
      .map(([eventType]) => eventType)
  );
}

export function validatePayloadForEventType(eventType, payload, registry = eventTypeRegistry) {
  const spec = getEventTypeSpec(eventType, registry);
  if (!spec) {
    return fail('EVENT_TYPE_NOT_REGISTERED', 'event_type is not registered', { event_type: eventType });
  }

  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return fail('EVENT_PAYLOAD_INVALID', 'payload must be an object', { event_type: eventType });
  }

  for (const [fieldName, rule] of Object.entries(spec.schema)) {
    const parsedRule = parseSchemaRule(rule);
    const hasField = Object.prototype.hasOwnProperty.call(payload, fieldName);
    if (!hasField) {
      if (parsedRule.optional) continue;
      return fail('EVENT_PAYLOAD_FIELD_REQUIRED', 'payload field is required', {
        event_type: eventType,
        field: fieldName
      });
    }

    const value = payload[fieldName];
    if (value === null || value === undefined) {
      if (parsedRule.optional) continue;
      return fail('EVENT_PAYLOAD_FIELD_REQUIRED', 'payload field is required', {
        event_type: eventType,
        field: fieldName
      });
    }

    if (!matchesSchemaType(value, parsedRule.type)) {
      return fail('EVENT_PAYLOAD_FIELD_TYPE_INVALID', 'payload field type is invalid', {
        event_type: eventType,
        field: fieldName,
        expected_type: parsedRule.type
      });
    }
  }

  return pass({ event_type: eventType, version: spec.version });
}
