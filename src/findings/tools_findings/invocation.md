# Tool servers: invocation check

Run: 2026-10-31 20:03 UTC

Every call goes through `invoke_tool`. `expected` is what the call must give; the wrong calls are an unknown argument and an agent without a grant.

| server | tool | expected | status | ms | retries | trust | untrusted fields | error code or note |
|---|---|---|---|---|---|---|---|---|
| energy_tools | energy_analyse_market_conditions | ok | ok | 16643 | 0 | pass | - |  |
| energy_tools | energy_analyse_weather_conditions | ok | ok | 2823 | 0 | pass | - |  |
| energy_tools | energy_compare_periods | ok | ok | 654 | 0 | pass | - |  |
| energy_tools | energy_get_metric | ok | ok | 553 | 0 | pass | - |  |
| energy_tools | energy_analyse_market_conditions | invalid_arguments | invalid_arguments | 4 | 0 | - | - | invalid_argument |
| energy_tools | energy_analyse_market_conditions | denied | denied | 0 | 0 | - | - | denied |
| commerce_tools | commerce_compare_periods | ok | ok | 4903 | 0 | pass | - |  |
| commerce_tools | commerce_get_metric | ok | ok | 432 | 0 | pass | - |  |
| commerce_tools | commerce_compare_periods | invalid_arguments | invalid_arguments | 5 | 0 | - | - | invalid_argument |
| commerce_tools | commerce_compare_periods | denied | denied | 0 | 0 | - | - | denied |
| ml_tools | ml_read_model_card | ok | ok | 1216 | 0 | - | text |  |
| ml_tools | ml_read_monitoring | ok | ok | 2205 | 0 | - | detail,subject |  |
| ml_tools | ml_read_predictions | ok | ok | 646 | 0 | - | row_key |  |
| ml_tools | ml_read_model_card | invalid_arguments | invalid_arguments | 6 | 0 | - | - | invalid_argument |
| ml_tools | ml_read_model_card | denied | denied | 0 | 0 | - | - | denied |
| dq_tools | dq_read_field_class | ok | ok | 3471 | 0 | - | derivation_rule,source_reference |  |
| dq_tools | dq_read_quality_log | ok | ok | 697 | 0 | - | error_detail |  |
| dq_tools | dq_read_quarantine | ok | ok | 627 | 0 | - | offending_value,reason |  |
| dq_tools | dq_read_watermarks | ok | ok | 1211 | 0 | - | - |  |
| dq_tools | dq_read_field_class | invalid_arguments | invalid_arguments | 5 | 0 | - | - | invalid_argument |
| dq_tools | dq_read_field_class | denied | denied | 0 | 0 | - | - | denied |
| knowledge_tools | knowledge_retrieve | ok | ok | 51734 | 0 | - | - |  |
| knowledge_tools | knowledge_retrieve | ok | ok | 263 | 0 | - | - | repeat call |
| knowledge_tools | knowledge_retrieve | invalid_arguments | invalid_arguments | 5 | 0 | - | - | invalid_argument |
| knowledge_tools | knowledge_retrieve | denied | denied | 0 | 0 | - | - | denied |

25 of 25 calls gave the expected status.

Log lines of this run: `logs/tool_calls`.
