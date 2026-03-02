# ethereum.md
EventDB Core - Anchoring Adapter Notes (Ethereum)
Version: 0.1
Status: Draft

This file defines adapter-level mapping guidance for optional Anchor publication.
The adapter MUST publish commitment hash plus checkpoint reference metadata.
Adapter behavior MUST remain idempotent for repeated publish requests with same input.
The adapter MUST return retrievable external reference and timestamp context.
