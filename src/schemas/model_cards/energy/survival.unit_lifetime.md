# Model card: survival.unit_lifetime

_model `survival_forest`, dataset `survival`, frozen version 0, primary metric `concordance` (higher is better). Generated: 2026-10-04T17:19Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

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
- held-out read: partition test, frozen_delta_version 0, rows 5244, groups 5244
- frame: 5243 rows, columns 21, rows by partition {'validation': 5243}
- events in the frozen data, train: 1895 of 24469 rows
- events in the frozen data, validation: 406 of 5243 rows
- events in the held-out partition: not recorded

## Training

- model family: survival_forest; MLflow run `04ffcdafa746415b93443fa14253e54b`
- parameters: {'entry': 'ignored', 'row_fraction': 1.0}
- library versions: sksurv 0.25.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | survival_forest held-out | survival_forest validation | kaplan_meier held-out |
|---|---|---|---|
| brier_1y | 0.0008775 | 0.0007608 | 0.001169 |
| brier_3y | 0.001194 | 0.001825 | 0.001779 |
| brier_5y | 0.0009867 | 0.001729 | 0.001779 |
| calibration_gap_1y | 0.000237 | 0.0007636 | 3.065e-05 |
| calibration_gap_3y | 0.002679 | 0.002123 | 0.0003227 |
| calibration_gap_5y | 0.00359 | 0.002643 | 4.052e-05 |
| concordance | 0.9626 | 0.9642 | 0.5031 |
| primary_change_relative | 0.001632 |  | 0.001769 |
| skill_primary | 0.4595 |  |  |
| skill_primary_ci_high | 0.4673 |  |  |
| skill_primary_ci_low | 0.4516 |  |  |

## Flags

- none

## Conditions and known limits

- 5-year calibration gap 0.00359 against 4.05e-05 for the baseline; predicted risk is less well calibrated than the baseline
- the number of events in the held-out partition was not recorded; the concordance and Brier scores rest on an unknown number of events
- revalidate on a new frozen dataset version, with the held-out event count recorded, before relying on the risk values

## Disclosures

- the held-out partition was read in at least 7 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `kaplan_meier`: validation 0.504, held-out 0.5031 on `concordance`
