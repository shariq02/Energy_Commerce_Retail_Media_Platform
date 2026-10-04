# Model card: redispatch.event_energy

_model `gbt_sklearn`, dataset `redispatch`, frozen version 2, primary metric `mae` (lower is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_log_event_energy_mwh`
- baseline fallback: `zone_mean`

## Intended use

- estimates the logarithm of the energy of a redispatch event in MWh
- target `target_log_event_energy_mwh`, dataset `redispatch`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1012 rows, columns 32, rows by partition {'test': 1012}
- held-out read: partition test, frozen_delta_version 2, rows 1012, time_min 2020-01-01 00:00:00, time_max 2020-12-31 00:00:00, groups 362
- frame: 970 rows, columns 32, rows by partition {'validation': 970}

## Training

- model family: gbt_sklearn; MLflow run `958132873ea4435fa0b4a1841c8508fa`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | zone_mean held-out |
|---|---|---|---|
| mae | 1.077 | 1.033 | 1.334 |
| mean_error | -0.1221 | -0.07086 | -0.4554 |
| mean_error_relative | -0.01426 | -0.008421 | -0.05318 |
| primary_change_relative | 0.04293 |  | 0.1289 |
| rmse | 1.392 | 1.334 | 1.612 |
| skill_mae | 0.1926 | 0.126 |  |
| skill_primary | 0.1926 |  |  |
| skill_primary_ci_high | 0.2388 |  |  |
| skill_primary_ci_low | 0.1544 |  |  |
| skill_rmse | 0.1366 | 0.08881 |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `zone_mean`: validation 1.182, held-out 1.334 on `mae`
