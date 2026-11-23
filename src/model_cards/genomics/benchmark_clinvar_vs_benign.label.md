# Model card: benchmark_clinvar_vs_benign.label

_model `logistic`, dataset `benchmark_clinvar_vs_benign`, frozen version 56, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_label_integer`
- baseline fallback: `base_rate`

## Intended use

- separates ClinVar variants from benign variants on a benchmark set
- target `target_label_integer`, dataset `benchmark_clinvar_vs_benign`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 8007 rows, columns 28, rows by partition {'test': 8007}
- held-out read: partition test, frozen_delta_version 56, rows 8007, groups 1180
- frame: 7120 rows, columns 28, rows by partition {'validation': 7120}

## Training

- model family: logistic; MLflow run `22a3b93fc19a4607940ac5c8bc814297`
- parameters: {'row_fraction': 1.0, 'C': 1.0}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | logistic held-out | logistic validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.4339 | 0.4667 | 0.4339 |
| calibration_error | 0.01267 | 0.02502 | 0.007292 |
| log_loss | 0.3365 | 0.3474 | 0.6845 |
| pr_auc | 0.9085 | 0.9182 | 0.4339 |
| pr_auc_lift | 2.094 | 1.967 | 1 |
| primary_change_relative | 0.01048 |  | 0.07037 |
| skill_primary | 0.4747 |  |  |
| skill_primary_ci_high | 0.5097 |  |  |
| skill_primary_ci_low | 0.4334 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.4667, held-out 0.4339 on `pr_auc`
