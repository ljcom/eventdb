# endpoints.md
EventDB Core - API Endpoints
Version: 0.1
Status: Draft

## Endpoint Set

- `POST /v1/chains/{chain_id}/verify`: run Event Chain verification.
- `POST /v1/seals/{chain_id}/verify`: run Seal verification.
- `POST /v1/snapshots/{chain_id}/verify`: run Snapshot verification.
- `POST /v1/anchors/{chain_id}/verify`: run Anchor verification.
- `POST /v1/chains/{chain_id}/events`: append one Event into Chain storage.
- `POST /v1/seals/{chain_id}/build`: build and persist next Seal window from stored Events.
- `POST /v1/snapshots/{chain_id}/build`: build and persist Snapshot from verified basis sequence.

## Response Contract

Each endpoint SHOULD return deterministic verification output:
- `status`: `PASS` or `FAIL`
- `checked_scope`: evaluated range or object reference
- `error_code`: from `03-spec/12-error-codes.md` when failed
- `message`: concise operator-readable note

For current MVP implementation:

- verification endpoints accept optional body field `namespace_id` (default: `default`);
- anchor verification endpoint is a stub and returns `ANCHOR_REFERENCE_NOT_FOUND`.
