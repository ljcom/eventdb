# EventDB Core MVP (Node.js + PostgreSQL)

MVP service untuk verifikasi `chain`, `seal`, dan `snapshot` sesuai draft spec EventDB Core.

## Lokasi

- App: `paper/06-implementation/mvp-node`
- Schema DB: `paper/05-mvp/schema/postgres.sql`

## Setup

```bash
cd paper/06-implementation/mvp-node
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

## Endpoint

- `GET /health`
- `POST /v1/chains/:chainId/verify`
- `POST /v1/seals/:chainId/verify`
- `POST /v1/snapshots/:chainId/verify`
- `POST /v1/anchors/:chainId/verify` (stub, belum implement adapter)
- `POST /v1/chains/:chainId/events` (append event sederhana)
- `POST /v1/seals/:chainId/build` (build Seal dari Event di DB)
- `POST /v1/snapshots/:chainId/build` (build Snapshot dari basis Event/Seal di DB)

Contoh body verifikasi:

```json
{
  "namespace_id": "default"
}
```

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
