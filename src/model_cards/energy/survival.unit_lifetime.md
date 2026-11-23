# Model card: survival.unit_lifetime

_model `boosted_cox_xgboost`, dataset `survival`, frozen version 34, primary metric `concordance` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R10, R12

## Purpose and task

- paradigm: survival; task type: survival
- target: `target_duration_years`
- baseline fallback: `kaplan_meier`

## Intended use

- estimates the lifetime in years of a generation unit and its probability of still being in operation after 1, 3 and 5 years
- target `target_duration_years`, dataset `survival`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 5244 rows, columns 21, rows by partition {'test': 5244}
- held-out read: partition test, frozen_delta_version 34, rows 5244, groups 5244
- frame: 5243 rows, columns 21, rows by partition {'validation': 5243}
- events in the frozen data, train: 1895 of 24469 rows
- events in the frozen data, validation: 406 of 5243 rows
- events in the held-out partition: not recorded

## Training

- model family: boosted_cox_xgboost; MLflow run `97f69ea4bc35421fb7300a2c55034ec3`
- parameters: {'entry': 'ignored', 'row_fraction': 1.0}
- library versions: not recorded
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | boosted_cox_xgboost held-out | boosted_cox_xgboost validation | kaplan_meier held-out |
|---|---|---|---|
| brier_1y | 0.0007142 | 0.0008154 | 0.001169 |
| brier_3y | 0.0009005 | 0.001609 | 0.001779 |
| brier_5y | 0.0007621 | 0.001511 | 0.001779 |
| calibration_gap_1y | 0.000213 | 0.0008841 | 3.065e-05 |
| calibration_gap_3y | 2.369e-06 | 0.0004565 | 0.0003227 |
| calibration_gap_5y | 0.0009044 | 0.0001305 | 4.052e-05 |
| concordance | 0.9669 | 0.9687 | 0.5031 |
| primary_change_relative | 0.001793 |  | 0.001769 |
| skill_primary | 0.4638 |  |  |
| skill_primary_ci_high | 0.4695 |  |  |
| skill_primary_ci_low | 0.4576 |  |  |

## Flags

- none

## Conditions and known limits

- 5-year calibration gap 0.000904 against 4.05e-05 for the baseline; predicted risk is less well calibrated than the baseline
- the number of events in the held-out partition was not recorded; the concordance and Brier scores rest on an unknown number of events
- revalidate on a new frozen dataset version, with the held-out event count recorded, before relying on the risk values

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `kaplan_meier`: validation 0.504, held-out 0.5031 on `concordance`
