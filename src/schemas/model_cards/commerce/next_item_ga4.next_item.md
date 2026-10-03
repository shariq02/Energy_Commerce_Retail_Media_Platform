# Model card: next_item_ga4.next_item

_model `sequence_transformer`, dataset `next_item_ga4`, frozen version 0, primary metric `ndcg_at_10` (higher is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: ranking; task type: ranking
- target: `target_next_product_id`
- baseline fallback: `popularity`

## Intended use

- estimates `target_next_product_id` for the entities and period of dataset `next_item_ga4`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 578604 rows, columns 6, rows by partition {'test': 578604}
- held-out read: partition test, frozen_delta_version 0, rows 299434, groups 4082
- frame: 587893 rows, columns 6, rows by partition {'validation': 587893}

## Training

- model family: sequence_transformer; MLflow run `b126e634fdd243789ae9ccac74b31ef9`
- parameters: {'vocab': 1356, 'dim': 64, 'epochs': 2, 'history': 10, 'train_rows': 1000000, 'eval_rows': 297217, 'row_fraction': 1.0}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | sequence_transformer held-out | sequence_transformer validation | popularity held-out |
|---|---|---|---|
| bootstrap_blocks | 1367 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 9.91e+04 |  |  |
| mrr | 0.373 | 0.3748 | 0.07059 |
| ndcg_at_10 | 0.4665 | 0.4684 | 0.0749 |
| ndcg_at_20 | 0.4925 | 0.4942 | 0.1036 |
| ndcg_at_5 | 0.3987 | 0.4004 | 0.05385 |
| primary_change_relative | 0.004057 |  | 0.01144 |
| recall_at_10 | 0.7995 | 0.8014 | 0.154 |
| recall_at_20 | 0.9004 | 0.9014 | 0.2687 |
| recall_at_5 | 0.5905 | 0.5919 | 0.08795 |
| reproduced_validation_value | 0.4684 |  | 0.07577 |
| reproduction_gap_relative | 0 |  | 0 |
| skill_primary | 0.3916 |  |  |
| skill_primary_ci_high | 0.4034 |  |  |
| skill_primary_ci_low | 0.3897 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- The only baseline is item popularity; there is no baseline that uses the items already seen in the session.

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `popularity`: validation 0.07577, held-out 0.0749 on `ndcg_at_10`
