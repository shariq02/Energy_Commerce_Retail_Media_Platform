# Model card: gene_pharmacogene.pharmacogene

_model `logistic`, dataset `gene_pharmacogene`, frozen version 12, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_is_pharmacogene`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a gene is a pharmacogene
- target `target_is_pharmacogene`, dataset `gene_pharmacogene`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 34195 rows, columns 608, rows by partition {'test': 34195}
- held-out read: partition test, frozen_delta_version 12, rows 34195, groups 34195
- frame: 34657 rows, columns 608, rows by partition {'validation': 34657}

## Training

- model family: logistic; MLflow run `cf3e7c265da94119ada7972c39e7032a`
- parameters: {'row_fraction': 1.0, 'C': 10.0}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | logistic held-out | logistic validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.1096 | 0.1086 | 0.1096 |
| calibration_error | 7.712e-05 | 6.325e-05 | 0.0004594 |
| log_loss | 8.18e-05 | 6.359e-05 | 0.3457 |
| pr_auc | 1 | 1 | 0.1096 |
| pr_auc_lift | 9.124 | 9.21 | 1 |
| primary_change_relative | 0 |  | -0.009471 |
| skill_primary | 0.8904 |  |  |
| skill_primary_ci_high | 0.8937 |  |  |
| skill_primary_ci_low | 0.8869 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.1086, held-out 0.1096 on `pr_auc`
