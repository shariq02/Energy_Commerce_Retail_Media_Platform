# Model card: next_item_ga4.next_item

_model `sequence_transformer`, dataset `next_item_ga4`, frozen version 8, primary metric `ndcg_at_10` (higher is better). Generated: 2026-10-26T17:52Z_

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

- frame: 578604 rows, columns 6, rows by partition {'test': 578604}
- held-out read: partition test, frozen_delta_version 8, rows 299434, groups 4082
- frame: 587893 rows, columns 6, rows by partition {'validation': 587893}

## Training

- model family: sequence_transformer; MLflow run `a46ef3f4856548739b5ca8890c5c3f4a`
- parameters: {'vocab': 1356, 'dim': 64, 'epochs': 2, 'history': 10, 'train_rows': 1000000, 'eval_rows': 297217, 'row_fraction': 1.0}
- library versions: torch 2.9.0+cu129
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | sequence_transformer held-out | sequence_transformer validation | popularity held-out |
|---|---|---|---|
| mrr | 0.3732 | 0.3749 | 0.07059 |
| ndcg_at_10 | 0.4667 | 0.4684 | 0.0749 |
| ndcg_at_20 | 0.4926 | 0.4943 | 0.1036 |
| ndcg_at_5 | 0.3989 | 0.4005 | 0.05385 |
| primary_change_relative | 0.003791 |  | 0.01144 |
| recall_at_10 | 0.7997 | 0.8014 | 0.154 |
| recall_at_20 | 0.9003 | 0.9015 | 0.2687 |
| recall_at_5 | 0.5909 | 0.5921 | 0.08795 |
| skill_primary | 0.3918 |  |  |
| skill_primary_ci_high | 0.4038 |  |  |
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

- `popularity`: validation 0.07577, held-out 0.0749 on `ndcg_at_10`
