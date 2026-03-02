# postgres.md
EventDB Core - Storage Adapter Notes (PostgreSQL)
Version: 0.1
Status: Draft

Storage mapping MUST preserve canonical Event, Seal, and Snapshot semantics.
Query and index optimization MAY vary, but verification output MUST remain equivalent.
Adapter SHOULD support append-oriented write path and deterministic ordered reads by `chain_id` and `sequence`.
Retention tier migration MUST preserve required re-verification artifacts.
