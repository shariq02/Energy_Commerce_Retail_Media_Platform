# Pipeline: processing layers in Databricks

One chain runs in Databricks: Bronze, Silver, Gold, Analytical Processing. There is no second input path.

- `bronze/`: one notebook per source, flat table names.
- `silver/`: source-scoped tables, cleaned, typed and localised. There is one primary Silver table per Bronze source table. Folders are grouped by ecosystem and domain.
- `gold/`: the governed domain model, in `{ecosystem}_gold` and `shared_conformed`.
- `analytics/`: derived facts, domain marts, metric definitions and cross-ecosystem contextual analytics.
- `serving/`: local export of selected marts for the PostgreSQL load.
- `quality/`: layer-transition gates and the shared `quality` schema objects.
- `setup/`: creation of schemas, the catalog and Volumes.
