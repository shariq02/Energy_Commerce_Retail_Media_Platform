# Tool servers: smoke check

Run: 2026-10-28 16:27 UTC

| server | tool | status | rows | seconds | note |
|---|---|---|---|---|---|
| energy_tools | (tools/list) | ok | 4 | 1.1 | start and list |
| energy_tools | energy_analyse_market_conditions | ok | 12 | 19.6 | as_of 2026-09-05; 2 warning(s) |
| energy_tools | energy_analyse_weather_conditions | ok | 4 | 3.3 | as_of 2023-12-31; 2 warning(s) |
| energy_tools | energy_compare_periods | ok | 2 | 1.1 | as_of 2026-09-05; 1 warning(s) |
| energy_tools | energy_get_metric | ok | 10 | 0.7 | as_of 2026-09-05; 2 warning(s) |
| commerce_tools | (tools/list) | ok | 2 | 1.1 | start and list |
| commerce_tools | commerce_compare_periods | ok | 2 | 3.5 | as_of 2021-01-31; 1 warning(s) |
| commerce_tools | commerce_get_metric | ok | 10 | 0.6 | as_of 2021-01-31; 2 warning(s) |
| ml_tools | (tools/list) | ok | 3 | 1.1 | start and list |
| ml_tools | ml_read_model_card | ok | 1 | 0.0 | as_of None |
| ml_tools | ml_read_monitoring | ok | 5 | 2.3 | as_of 2026-10-27T14:02:57.106664+00:00; 1 warning(s) |
| ml_tools | ml_read_predictions | ok | 5 | 1.5 | as_of 2026-10-26T21:51:59.745289+00:00; 1 warning(s) |
| dq_tools | (tools/list) | ok | 4 | 1.2 | start and list |
| dq_tools | dq_read_field_class | ok | 5 | 2.4 | as_of None; 1 warning(s) |
| dq_tools | dq_read_quality_log | ok | 5 | 1.1 | as_of 2026-09-30T18:46:51.416870+00:00; 2 warning(s) |
| dq_tools | dq_read_quarantine | ok | 3 | 1.2 | as_of 2026-09-26T13:04:38.169406+00:00; 2 warning(s) |
| dq_tools | dq_read_watermarks | ok | 5 | 1.0 | as_of 2026-09-30T18:47:04.835634+00:00; 1 warning(s) |
| cross_ecosystem_tools | (tools/list) | ok | 0 | 1.0 | start and list |
| knowledge_tools | (tools/list) | ok | 1 | 1.0 | start and list |
| knowledge_tools | knowledge_retrieve | ok | 6 | 46.8 | as_of 2026-10-06 |

## metrics.yml against metric_definition

- none: keys and marts match
