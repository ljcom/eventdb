# key-management.md
EventDB Core - Key Management Operations
Version: 0.1
Status: Draft

## Requirements

- Key ownership MUST map to accountable Account identity.
- Key issuance and rotation MUST be policy-approved.
- Revocation MUST be immediate on confirmed compromise.

## Operational Controls

- Separate key custody from routine operator duties.
- Log key lifecycle events with accountable identity.
- Periodically test compromise-response and re-verification workflow.

## Verification Impact

Any key status change SHOULD trigger scoped re-verification and incident review.
