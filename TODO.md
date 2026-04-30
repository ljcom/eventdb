# TODO - EventDB

## Event Type Registry Sync

- [x] Runtime EventDB membaca registry event type dari `config/event-types.json`.
- [x] Ingest API menolak event type yang belum terdaftar.
- [x] Ingest API memvalidasi payload berdasarkan schema registry.
- [x] Ingest API menerima canonical envelope dan field legacy untuk backward compatibility.
- [x] SQL write adapter menghasilkan canonical sales order events.
- [x] Projection orders memakai daftar event type dari registry.
- [x] Projection product, stock balance, sales report, dan GL balance tersedia.
- [x] Test valid/invalid ingest dan replay/projection canonical tersedia.
- [x] Refactor projection market orders, POS sales, dan CRM customers agar daftar event type berasal dari config.
- [x] Tambahkan read endpoint untuk projection product, stock balance, sales report, dan GL balance jika dibutuhkan aplikasi.
- [ ] Tambahkan validasi schema nested untuk item array, journal lines, dan payload kompleks.
- [ ] Sinkronkan seluruh path OpenAPI dengan route runtime `mvp-node/src/app.js`.
