# monitoring.md
EventDB Core - Monitoring Operations
Version: 0.1
Status: Draft

## Monitoring Objectives

- Detect verification failures early.
- Detect Seal cadence deviation.
- Detect Anchor publish/verify mismatch.

## Required Signals

- PASS/FAIL counts by verification scope.
- Error-code distribution over time.
- Seal generation latency and omission alerts.
- Snapshot mismatch events.

## Escalation

Critical integrity mismatch MUST raise incident workflow with accountable operator assignment.
