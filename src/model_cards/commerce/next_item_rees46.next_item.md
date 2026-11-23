# Model card: next_item_rees46.next_item

_model `sequence_transformer`, dataset `next_item_rees46`, frozen version 28, primary metric `ndcg_at_10` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: ranking; task type: ranking
- target: `target_next_product_id`
- baseline fallback: `popularity`

## Intended use

- ranks products by the probability of being the next product in a REES46 session
- target `target_next_product_id`, dataset `next_item_rees46`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 695418 rows, columns 6, rows by partition {'test': 695418}
- held-out read: partition test, frozen_delta_version 28, rows 300524, groups 21853
- frame: 688462 rows, columns 6, rows by partition {'validation': 688462}

## Training

- model family: sequence_transformer; MLflow run `db94d8614568422a836f7b26d7ae87cb`
- parameters: {'vocab': 20000, 'dim': 64, 'epochs': 2, 'history': 10, 'train_rows': 1000000, 'eval_rows': 302614, 'row_fraction': 1.0}
- library versions: torch 2.14.1+cpu
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | sequence_transformer held-out | sequence_transformer validation | popularity held-out |
|---|---|---|---|
| mrr | 0.08371 | 0.08156 | 0.01557 |
| ndcg_at_10 | 0.09919 | 0.0963 | 0.01693 |
| ndcg_at_20 | 0.1141 | 0.1112 | 0.02188 |
| ndcg_at_5 | 0.08156 | 0.07952 | 0.01251 |
| primary_change_relative | -0.03003 |  | -0.04211 |
| recall_at_10 | 0.1764 | 0.1709 | 0.03409 |
| recall_at_20 | 0.2355 | 0.2298 | 0.05386 |
| recall_at_5 | 0.1217 | 0.1189 | 0.02032 |
| skill_primary | 0.08227 |  |  |
| skill_primary_ci_high | 0.08695 |  |  |
| skill_primary_ci_low | 0.07947 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `popularity`: validation 0.01624, held-out 0.01693 on `ndcg_at_10`
