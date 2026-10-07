# Governance: data quality gates

Quality is enforced at every layer transition with a `check(name, status, detail, hard_fail)` framework. The status is PASS, WARN, FAIL or HARD_FAIL. There is no separate quality step.

- Acquisition: licence and attribution capture; content check on real rows.
- Bronze: schema snapshot and drift detection; non-zero row count.
- Bronze to Silver: contract enforcement, which blocks on a violation; a field-class check on every column (an unclassified column warns and is reconciled later, it does not block the write); quarantine of unknown codes, failed parses, out-of-bounds coordinates and conflicting keys. Quarantined values are kept with a reason and are never dropped.
- Silver to Gold: grain, point-in-time and referential-integrity checks.
- Gold to Analytics: mart grain, and no look-ahead.
- Analytics to ML: leakage audit and dataset freeze gate.
- Analytics to Serving: type-map test and row-count reconciliation.

The shared `quality` schema holds `quality_audit_log`, `pipeline_watermarks`, `quarantine` and `field_class_registry` for every layer and ecosystem. Failure detection is after the fact: the operator reads these tables. There is no live alerting.
