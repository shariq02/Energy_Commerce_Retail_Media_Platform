# Pipeline: execution model

The project uses no orchestration or scheduling platform. The operator runs the Databricks notebooks and the local scripts directly, in the required order.

Rerun safety and the layer-transition gates belong to the notebooks and to the `quality` schema (`pipeline_watermarks` and `quality_audit_log`). They do not belong to an external scheduler.
