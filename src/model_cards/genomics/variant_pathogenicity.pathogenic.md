# Model card: variant_pathogenicity.pathogenic

_model `gbt_lightgbm`, dataset `variant_pathogenicity`, frozen version 51, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_is_pathogenic`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a variant is pathogenic from its annotation
- target `target_is_pathogenic`, dataset `variant_pathogenicity`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 314548 rows, columns 392, rows by partition {'test': 314548}
- held-out read: partition test, frozen_delta_version 51, rows 314548, groups 2651
- frame: 141509 rows, columns 392, rows by partition {'validation': 141509}

## Training

- model family: gbt_lightgbm; MLflow run `267b4f47ac144634bc3ac4cd1a2fe1e5`
- parameters: {'row_fraction': 0.55, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.2315 | 0.2085 | 0.2315 |
| calibration_error | 0.01417 | 5.425e-05 | 0.02842 |
| log_loss | 0.1308 | 0.0002005 | 0.5435 |
| pr_auc | 0.9991 | 1 | 0.2315 |
| pr_auc_lift | 4.315 | 4.796 | 1 |
| primary_change_relative | 0.0009097 |  | -0.1104 |
| skill_primary | 0.7676 |  |  |
| skill_primary_ci_high | 0.8019 |  |  |
| skill_primary_ci_low | 0.6465 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.2085, held-out 0.2315 on `pr_auc`
