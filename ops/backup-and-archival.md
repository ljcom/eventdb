# backup-and-archival.md
EventDB Core - Backup and Archival Operations
Version: 0.1
Status: Draft

## Backup Scope

Backups MUST include Event, Seal, Snapshot, and Anchor-related verification artifacts required by policy.

## Archival Model

- Hot: active verification domain.
- Warm: periodic review domain.
- Cold: long-term evidence domain.

Tier movement MUST preserve deterministic re-verification capability.

## Recovery Rule

Recovery completion MUST include integrity verification checks before trust restoration.
