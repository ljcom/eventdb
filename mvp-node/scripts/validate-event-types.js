import { validateEventTypeRegistry } from '../src/event-types.js';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const result = validateEventTypeRegistry();
if (!result.ok) {
  console.error(`${result.error_code}: ${result.message}`);
  process.exit(1);
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const registryPath = path.resolve(__dirname, '../../../../config/event-types.json');
const docsPath = path.resolve(__dirname, '../../../../config/EVENTDB_SCHEMA_REGISTRY.md');
const registry = JSON.parse(fs.readFileSync(registryPath, 'utf8'));
const docs = fs.readFileSync(docsPath, 'utf8');

const documentedEventTypes = new Set(
  docs
    .split('\n')
    .map((line) => line.match(/^\| `([^`]+)` \|/))
    .filter(Boolean)
    .map((match) => match[1])
    .filter((eventType) => eventType.includes('.'))
);

for (const eventType of Object.keys(registry)) {
  if (!documentedEventTypes.has(eventType)) {
    console.error(`EVENT_TYPE_DOC_MISSING: ${eventType}`);
    process.exit(1);
  }
}

for (const eventType of documentedEventTypes) {
  if (!registry[eventType]) {
    console.error(`EVENT_TYPE_REGISTRY_MISSING: ${eventType}`);
    process.exit(1);
  }
}

console.log(`${result.artifact.event_type_count} event types valid and documented`);
