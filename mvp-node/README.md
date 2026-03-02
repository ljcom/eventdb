# EventDB Core MVP (Node.js + PostgreSQL)

MVP service untuk verifikasi `chain`, `seal`, dan `snapshot` sesuai draft spec EventDB Core.

## Lokasi

- App: `eventdb/mvp-node`
- Schema DB: `paper/05-mvp/schema/postgres.sql`

## Setup

```bash
cd eventdb/mvp-node
npm install
cp .env.example .env
```

Isi `.env` sesuai PostgreSQL Anda (minimal `DATABASE_URL`).

## Menjalankan

1. Apply schema:

```bash
npm run db:schema
```

Jika database pada `DATABASE_URL` belum ada, script akan mencoba membuatnya otomatis lewat koneksi admin ke database `postgres`.

2. (Opsional) Load sample data dari folder `paper/05-mvp/sample`:

```bash
npm run sample:load
```

`sample:load` melakukan upsert, jadi aman dijalankan ulang untuk sinkronisasi sample terbaru.

3. Start API:

```bash
npm run dev
```

## Docker

```bash
cd eventdb/mvp-node
docker compose up --build
```

Service akan tersedia di `http://localhost:3000` dengan Postgres internal di `localhost:5432`.

## Endpoint

- `GET /health`
- `POST /v1/chains/:chainId/verify`
- `POST /v1/seals/:chainId/verify`
- `POST /v1/snapshots/:chainId/verify`
- `POST /v1/anchors/:chainId/verify` (EVM read adapter, configurable via env)
- `POST /v1/chains/:chainId/events` (append event sederhana)
- `POST /v1/seals/:chainId/build` (build Seal dari Event di DB)
- `POST /v1/snapshots/:chainId/build` (build Snapshot dari basis Event/Seal di DB)
- `POST /v1/projections/:chainId/orders/run` (run SQL read model projection `orders` v1)
- `POST /v1/sql/:chainId/write` (optional SQL write adapter -> append event -> run projection)
- `GET /v1/read/orders/:chainId` (query read model `orders` v1)

## API Key security

- Semua endpoint non-`/health` bisa diproteksi API key.
- Header yang diterima: `x-api-key` atau `Authorization: Bearer <key>`.
- Role:
  - `API_KEY_INGEST` untuk `/events` dan `/sql/.../write`
  - `API_KEY_OPS` untuk `/build`
  - `API_KEY_VERIFY` untuk `/verify`
  - `API_KEY_ADMIN` untuk semua role
- Aktifkan dengan `API_AUTH_ENABLED=true`.
- Default behavior: otomatis aktif jika `NODE_ENV=production`.

Contoh body verifikasi:

```json
{
  "namespace_id": "default"
}
```

Contoh body anchor verify:

```json
{
  "namespace_id": "default",
  "checkpoint_id": "win-1-3-abc123def456"
}
```

Contoh request verify dengan API key:

```bash
curl -X POST http://localhost:3000/v1/chains/inst-a-chain-01/verify \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: verify-local-change-me' \
  -d '{"namespace_id":"default"}'
```

## Rate limiting

- Mekanisme: fixed-window in-memory per role (`ingest`, `ops`, `verify`).
- Konfigurasi env:
  - `RATE_LIMIT_ENABLED`
  - `RATE_LIMIT_WINDOW_MS`
  - `RATE_LIMIT_MAX_INGEST`
  - `RATE_LIMIT_MAX_OPS`
  - `RATE_LIMIT_MAX_VERIFY`
- Saat limit terlewati, API mengembalikan:
  - HTTP `429`
  - `error_code=RATE_LIMIT_EXCEEDED`
  - header `retry-after`

## Audit log

- Audit request non-`/health` ditulis sebagai JSON ke stdout.
- Aktif/nonaktif lewat `AUDIT_LOG_ENABLED`.
- Field utama: `ts`, `method`, `path`, `status_code`, `duration_ms`, `ip`, `auth_role`, `api_key_fingerprint`.

## Smart contract anchoring

- Endpoint anchor verify membandingkan `seal_hash` lokal (checkpoint window) dengan commitment pada smart contract.
- Konfigurasi minimal:
  - `ANCHOR_ENABLED=true`
  - `ANCHOR_EVM_RPC_URL`
  - `ANCHOR_EVM_CONTRACT_ADDRESS`
  - method default:
    - `getCommitment(string chainId, string checkpointId) returns (bytes32)`
    - `getPublishedAt(string chainId, string checkpointId) returns (uint256)` (opsional)
- Jika ABI/method berbeda, set `ANCHOR_EVM_CONTRACT_ABI_JSON` dan nama method via env.

Contoh body append event:

```json
{
  "namespace_id": "default",
  "event_id": "evt-0004",
  "sequence": 4,
  "prev_hash": "<hash-event-sebelumnya>",
  "account_id": "acct-ops-01",
  "event_type": "record_updated",
  "event_time": "2026-01-01T00:15:00Z",
  "payload": { "ref": "A-1001", "status": "archived" },
  "signature": "sig_base64_evt_0004"
}
```

Contoh run projection:

```bash
curl -X POST http://localhost:3000/v1/projections/inst-a-chain-01/orders/run \
  -H 'Content-Type: application/json' \
  -d '{"namespace_id":"default"}'
```

Contoh query read model:

```bash
curl "http://localhost:3000/v1/read/orders/inst-a-chain-01?namespace_id=default&status=final&limit=20"
```

Contoh SQL write adapter:

```bash
curl -X POST http://localhost:3000/v1/sql/inst-a-chain-01/write \
  -H 'Content-Type: application/json' \
  -H 'idempotency-key: sqlw-001' \
  -d '{"namespace_id":"default","actor_id":"acct-ops-01","sql":"UPDATE orders SET status='\''PAID'\'' WHERE order_id='\''A-1001'\''"}'
```

## Projection CLI

Jalankan projector tanpa API:

```bash
npm run projection:orders -- --chain-id inst-a-chain-01 --namespace-id default
```

## SQL Write Adapter (Optional)

- Aktifkan dengan `SQL_WRITE_ADAPTER_ENABLED=true`.
- Endpoint hanya menerima subset:
  - `INSERT INTO orders (...) VALUES (...)`
  - `UPDATE orders SET ... WHERE order_id='...'`
  - `DELETE FROM orders WHERE order_id='...'`
- Adapter tidak menulis langsung ke read table.
- Alur: SQL -> event append (`eventdb_event`) -> projection `orders` v1 -> `read.orders_v1`.
- Idempotency key bisa dikirim via header `idempotency-key` (direkomendasikan/mandatory by config).

## Catatan signature mode

- `SIGNATURE_MODE=none` (default): hanya cek field signature non-empty.
- `SIGNATURE_MODE=hmac_sha256`: verifikasi dengan `ACCOUNT_SECRETS_JSON`.

Format `ACCOUNT_SECRETS_JSON`:

```json
{"acct-ops-01":"secret-ops","acct-seal-01":"secret-seal","snapshot:inst-a-chain-01":"secret-snapshot"}
```

## Integration test

```bash
npm run test:integration
```

Test melakukan alur:
1. insert Event baru;
2. build Seal;
3. build Snapshot;
4. verify `PASS`;
5. tamper 1 Event langsung di DB;
6. verify berubah menjadi `FAIL`.
