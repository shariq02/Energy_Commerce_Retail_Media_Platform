# Model card: gene_cancer_role.cancer_role

_model `gbt_lightgbm`, dataset `gene_cancer_role`, frozen version 12, primary metric `balanced_accuracy` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: multiclass
- target: `target_gene_cancer_role`
- baseline fallback: `majority_class`

## Intended use

- predicts the cancer role class of a gene
- target `target_gene_cancer_role`, dataset `gene_cancer_role`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 3132 rows, columns 611, rows by partition {'test': 3132}
- frame: 3136 rows, columns 611, rows by partition {'validation': 3136}
- held-out read: partition test, frozen_delta_version 12, rows 3132, groups 3132

## Training

- model family: gbt_lightgbm; MLflow run `485f84a6bb9f49709df6382f2c8c05ed`
- parameters: {'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | majority_class held-out |
|---|---|---|---|
| accuracy | 0.9936 | 0.9927 | 0.9029 |
| balanced_accuracy | 0.8668 | 0.8406 | 0.3333 |
| estimable_classes | 3 | 3 | 3 |
| primary_change_relative | -0.03117 |  | 0 |
| skill_primary | 0.5334 |  |  |
| skill_primary_ci_high | 0.5781 |  |  |
| skill_primary_ci_low | 0.4905 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `majority_class`: validation 0.3333, held-out 0.3333 on `balanced_accuracy`
