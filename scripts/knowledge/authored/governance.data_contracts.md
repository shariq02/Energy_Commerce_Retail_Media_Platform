# Governance: data contracts and schema evolution

Every ingested source has an explicit data contract. A contract defines the expected schema and types, the required and optional fields, the null and range constraints, and the expected update frequency.

Every contract records the derived field `ecosystem`. The Silver layer assigns it from a governed map of source system to ecosystem. The allowed values are `energy` and `commerce`. A change to the map is a recorded decision.

A contract violation blocks the matching Silver build.

Most sources are static, one-time acquisitions. SMARD and DWD are scripted and can be re-run. GA4 can be queried again through BigQuery. If a source changes shape, profiling detects it and the contract version is raised. Each Silver, Gold and Analytics table has one contract-defined shape. A change is a new contract version and a regenerated table. It is never a parallel `_v2` table.
