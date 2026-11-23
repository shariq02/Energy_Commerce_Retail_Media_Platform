# Model card: load.load

_model `gbt_sklearn`, dataset `load`, frozen version 14, primary metric `mae` (lower is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_value_mwh`
- baseline fallback: `seasonal_mean`

## Intended use

- estimates the electricity load in MWh
- target `target_value_mwh`, dataset `load`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 4073 rows, columns 104, rows by partition {'test': 4073}
- held-out read: partition test, frozen_delta_version 14, rows 4073, time_min 2025-01-01 00:00:00, time_max 2026-11-10 00:00:00, groups 679
- frame: 2184 rows, columns 104, rows by partition {'validation': 2184}

## Training

- model family: gbt_sklearn; MLflow run `81b073bbb971472b9d07ff79e19ee069`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | seasonal_mean held-out |
|---|---|---|---|
| mae | 2.647e+04 | 2.451e+04 | 6.491e+04 |
| mean_error | 3887 | 539.4 | 4.606e+04 |
| mean_error_relative | 0.007178 | 0.0009835 | 0.08504 |
| primary_change_relative | 0.07965 |  | 0.1386 |
| rmse | 4.327e+04 | 4.16e+04 | 1.273e+05 |
| skill_mae | 0.5922 | 0.57 |  |
| skill_primary | 0.5922 |  |  |
| skill_primary_ci_high | 0.6379 |  |  |
| skill_primary_ci_low | 0.5412 |  |  |
| skill_rmse | 0.6602 | 0.6224 |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `seasonal_mean`: validation 5.7e+04, held-out 6.491e+04 on `mae`
