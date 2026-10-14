# Model card: zone_generation.generation

_model `gbt_lightgbm`, dataset `zone_generation`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_generation_mwh`
- baseline fallback: `capacity_factor`

## Intended use

- estimates the generation in MWh of a control zone
- target `target_generation_mwh`, dataset `zone_generation`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 6110 rows, columns 23, rows by partition {'test': 6110}
- held-out read: partition test, frozen_delta_version 0, rows 6110, time_min 2025-01-01 00:00:00, time_max 2026-09-03 00:00:00, groups 611
- frame: 3640 rows, columns 23, rows by partition {'validation': 3640}

## Training

- model family: gbt_lightgbm; MLflow run `5cc0cef088374428907bef305222d7ae`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | capacity_factor held-out |
|---|---|---|---|
| mae | 1.617e+04 | 1.093e+04 | 3.144e+04 |
| mean_error | -7477 | -959.3 | -2594 |
| mean_error_relative | -0.1258 | -0.01745 | -0.04364 |
| primary_change_relative | 0.4796 |  | 0.2229 |
| rmse | 2.412e+04 | 1.587e+04 | 4.644e+04 |
| skill_mae | 0.4857 | 0.5749 |  |
| skill_primary | 0.4857 |  |  |
| skill_primary_ci_high | 0.5632 |  |  |
| skill_primary_ci_low | 0.3815 |  |  |
| skill_rmse | 0.4807 | 0.6171 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +48.0% worse than on validation

## Conditions and known limits

- primary metric +48.0% worse than on validation; revalidate on newer data before relying on it

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `capacity_factor`: validation 2.571e+04, held-out 3.144e+04 on `mae`
