---
unit_id: model_card.rl_session_sequences.policy
kind: model_card
ecosystem: commerce
source:
- path: src/model_cards/commerce/rl_session_sequences.policy.md
  sha256: ece734a1b19ac290e289bfffc9840d3b0e6bdb76084697923b2b60cf63314b3b
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 4755ac87d09960301b3a8f755972c5b549ed457c323614c239ffe76ef3163686
---

# Model card: rl_session_sequences.policy

_model `sequence_gru`, primary metric `action_agreement` (higher is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R02, R09, R10

## Purpose and task
- paradigm: offline_rl; task type: policy
- target: `target_action_event_type`
- baseline fallback: `most_frequent_event`

## Intended use
- proposes the next event type of a session; evaluated on logged data only
- target `target_action_event_type`, dataset `rl_session_sequences`
- restricted to offline only (see the conditions below)

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- action_agreement: sequence_gru held-out 0.9439; sequence_gru validation 0.9445; most_frequent_event held-out 0.9433
- skill_primary: sequence_gru held-out 0.0006054
- skill_primary_ci_high: sequence_gru held-out 0.003587
- skill_primary_ci_low: sequence_gru held-out -0.0005137

## Flags
- skill interval includes zero: interval -0.0005137 to 0.003587

## Conditions and known limits
- skill interval -0.0005137 to 0.003587 includes zero
- offline evidence only; operational feasibility is not shown

## Disclosures
- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `most_frequent_event`: validation 0.9442, held-out 0.9433 on `action_agreement`
