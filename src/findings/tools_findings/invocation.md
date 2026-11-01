# Tool servers: invocation check

Run: 2026-11-01 00:02 UTC

Every call goes through `invoke_tool`. `expected` is what the call must give; the wrong calls are an unknown argument and an agent without a grant.

| server | tool | expected | status | ms | retries | trust | untrusted fields | error code or note |
|---|---|---|---|---|---|---|---|---|
| energy_tools | energy_analyse_market_conditions | ok | ok | 13906 | 0 | pass | - |  |
| energy_tools | energy_analyse_weather_conditions | ok | ok | 1406 | 0 | pass | - |  |
| energy_tools | energy_compare_periods | ok | ok | 375 | 0 | pass | - |  |
| energy_tools | energy_get_metric | ok | ok | 375 | 0 | pass | - |  |
| energy_tools | energy_analyse_market_conditions | invalid_arguments | invalid_arguments | 0 | 0 | - | - | invalid_argument |
| energy_tools | energy_analyse_market_conditions | denied | denied | 0 | 0 | - | - | denied |
| commerce_tools | commerce_compare_periods | ok | ok | 8625 | 0 | pass | - |  |
| commerce_tools | commerce_get_metric | ok | ok | 375 | 0 | pass | - |  |
| commerce_tools | commerce_compare_periods | invalid_arguments | invalid_arguments | 0 | 0 | - | - | invalid_argument |
| commerce_tools | commerce_compare_periods | denied | denied | 0 | 0 | - | - | denied |
| ml_tools | ml_read_model_card | ok | ok | 4921 | 0 | - | text |  |
| ml_tools | ml_read_monitoring | ok | ok | 2984 | 0 | - | detail,subject |  |
| ml_tools | ml_read_predictions | ok | ok | 453 | 0 | - | row_key |  |
| ml_tools | ml_read_model_card | invalid_arguments | invalid_arguments | 14 | 0 | - | - | invalid_argument |
| ml_tools | ml_read_model_card | denied | denied | 0 | 0 | - | - | denied |
| dq_tools | dq_read_field_class | ok | ok | 8421 | 0 | - | derivation_rule,source_reference |  |
| dq_tools | dq_read_quality_log | ok | ok | 421 | 0 | - | error_detail |  |
| dq_tools | dq_read_quarantine | ok | ok | 437 | 0 | - | offending_value,reason |  |
| dq_tools | dq_read_watermarks | ok | ok | 406 | 0 | - | - |  |
| dq_tools | dq_read_field_class | invalid_arguments | invalid_arguments | 14 | 0 | - | - | invalid_argument |
| dq_tools | dq_read_field_class | denied | denied | 0 | 0 | - | - | denied |
| knowledge_tools | knowledge_retrieve | ok | invalid_arguments | 5406 | 0 | - | - | invalid_argument |
| knowledge_tools | knowledge_retrieve | ok | invalid_arguments | 15 | 0 | - | - | invalid_argument |
| knowledge_tools | knowledge_retrieve | invalid_arguments | invalid_arguments | 0 | 0 | - | - | invalid_argument |
| knowledge_tools | knowledge_retrieve | denied | denied | 0 | 0 | - | - | denied |

23 of 25 calls gave the expected status.

Log lines of this run: `logs\tool_calls`.
