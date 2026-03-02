# mssql.md
EventDB Core - Storage Adapter Notes (MSSQL)
Version: 0.1
Status: Draft

Storage mapping MUST preserve canonical Event, Seal, and Snapshot semantics.
Implementation MAY use engine-native features for performance, but MUST NOT change logical verification behavior.
Adapter SHOULD support deterministic sequence retrieval and checkpoint queries.
Backup and recovery procedures MUST include integrity re-verification steps.
