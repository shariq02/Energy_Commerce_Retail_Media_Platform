# Model card: next_item_ga4.next_item

_model `sequence_transformer`, dataset `next_item_ga4`, frozen version 28, primary metric `ndcg_at_10` (higher is better). Generated: 2026-11-23T13:07Z_

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

- ranks products by the probability of being the next product in a GA4 session
- target `target_next_product_id`, dataset `next_item_ga4`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 578611 rows, columns 6, rows by partition {'test': 578611}
- held-out read: partition test, frozen_delta_version 28, rows 299438, groups 4082
- frame: 587889 rows, columns 6, rows by partition {'validation': 587889}

## Training

- model family: sequence_transformer; MLflow run `49a4b7e7675f4a0fa4c3924ef2798745`
- parameters: {'vocab': 1356, 'dim': 64, 'epochs': 2, 'history': 10, 'train_rows': 1000000, 'eval_rows': 297216, 'row_fraction': 1.0}
- library versions: torch 2.14.1+cpu
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | sequence_transformer held-out | sequence_transformer validation | popularity held-out |
|---|---|---|---|
| mrr | 0.374 | 0.3749 | 0.07062 |
| ndcg_at_10 | 0.4674 | 0.4689 | 0.0749 |
| ndcg_at_20 | 0.4933 | 0.4943 | 0.1036 |
| ndcg_at_5 | 0.3999 | 0.4008 | 0.05386 |
| primary_change_relative | 0.003066 |  | 0.01128 |
| recall_at_10 | 0.8002 | 0.803 | 0.1539 |
| recall_at_20 | 0.9006 | 0.9017 | 0.2687 |
| recall_at_5 | 0.592 | 0.5932 | 0.08793 |
| skill_primary | 0.3925 |  |  |
| skill_primary_ci_high | 0.4032 |  |  |
| skill_primary_ci_low | 0.3899 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- The only baseline is item popularity; there is no baseline that uses the items already seen in the session.
- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `popularity`: validation 0.07576, held-out 0.0749 on `ndcg_at_10`
