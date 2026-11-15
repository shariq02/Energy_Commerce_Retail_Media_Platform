# Layer retirement plan

_Generated: 2026-11-15T19:32Z_

Switches: tables and views True, Volumes False, schemas False

## Schemas to drop

| layer | schema | tables | views | volumes | decision |
| --- | --- | --- | --- | --- | --- |
| gold (fresh rebuild) | energy_gold | 18 | 12 | 0 | drop |
| gold (fresh rebuild) | commerce_gold | 14 | 3 | 0 | drop |
| gold (fresh rebuild) | genomics_gold | 0 | 0 | 0 | drop |
| gold (fresh rebuild) | shared_conformed | 13 | 1 | 0 | drop |

## Successor check

| layer | successor schema | tables | problem | passes |
| --- | --- | --- | --- | --- |
| gold (fresh rebuild) | energy_silver | 15 |  | 1 |
| gold (fresh rebuild) | energy_silver_reference | 12 |  | 1 |
| gold (fresh rebuild) | commerce_silver | 5 |  | 1 |
| gold (fresh rebuild) | genomics_silver | 27 |  | 1 |
| gold (fresh rebuild) | genomics_silver_reference | 11 |  | 1 |
