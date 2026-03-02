# polygon.md
EventDB Core - Anchoring Adapter Notes (Polygon)
Version: 0.1
Status: Draft

This file defines adapter-level mapping guidance for optional Anchor publication.
The adapter MUST preserve canonical commitment bytes from local verifier.
The adapter SHOULD support deterministic retry policy for transient submission failure.
The adapter MUST expose external reference needed for later Anchor verification.
