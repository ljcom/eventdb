# mock.md
EventDB Core - Anchoring Adapter Notes (Mock)
Version: 0.1
Status: Draft

Mock adapter provides local test-only Anchor emulation.
The mock store MUST preserve published commitment and publication timestamp.
Mock verification MUST execute the same local-to-external commitment comparison logic.
Mock adapter MUST NOT be used as legal or external publication evidence source.
