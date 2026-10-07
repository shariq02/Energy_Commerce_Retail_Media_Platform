# Governance: lineage

Data flows from Source to Bronze, Silver, Gold and Analytical Processing, then to a metric or mart. A local export loads PostgreSQL serving, which feeds Power BI and Grafana. The semantic layer and the governed model outputs feed the AI capability.

The `source_system` value traces every downstream row, serving row, mart and metric back to its origin. From Silver onward every lineage node also carries the `ecosystem` attribute. Bronze nodes carry no `ecosystem`. Lineage, ownership, cost and access reporting can be sliced per ecosystem.

GenAI-assisted engineering is not a data stage, so it is not part of the lineage.
