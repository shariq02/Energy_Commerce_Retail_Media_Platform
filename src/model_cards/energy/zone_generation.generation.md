# Model card: zone_generation.generation

_model `gbt_lightgbm`, dataset `zone_generation`, frozen version 34, primary metric `mae` (lower is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04, R10

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

- frame: 6788 rows, columns 23, rows by partition {'test': 6788}
- held-out read: partition test, frozen_delta_version 34, rows 6788, time_min 2025-01-01 00:00:00, time_max 2026-11-10 00:00:00, groups 679
- frame: 3640 rows, columns 23, rows by partition {'validation': 3640}

## Training

- model family: gbt_lightgbm; MLflow run `43aaeefeda404a56a0aa333bb6a176c6`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | capacity_factor held-out |
|---|---|---|---|
| mae | 1.981e+04 | 1.102e+04 | 3.187e+04 |
| mean_error | -1.088e+04 | -37.66 | -2943 |
| mean_error_relative | -0.1828 | -0.0006851 | -0.04943 |
| primary_change_relative | 0.7978 |  | 0.2396 |
| rmse | 3.429e+04 | 1.597e+04 | 4.721e+04 |
| skill_mae | 0.3783 | 0.5714 |  |
| skill_primary | 0.3783 |  |  |
| skill_primary_ci_high | 0.5081 |  |  |
| skill_primary_ci_low | 0.1989 |  |  |
| skill_rmse | 0.2737 | 0.6148 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +79.8% worse than on validation

## Conditions and known limits

- primary metric +79.8% worse than on validation; revalidate on newer data before relying on it

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `capacity_factor`: validation 2.571e+04, held-out 3.187e+04 on `mae`
