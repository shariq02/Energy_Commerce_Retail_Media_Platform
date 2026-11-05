# COMMERCE NOTEBOOK FINDINGS

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-10-27T19:49Z_

<!-- section: commerce__approve__00_approval_setup -->
## Approval Setup

### approval_spec

16 row(s)

| rule_id | rule_class | parameter | parameter_value |
|---|---|---|---|
| R01 | selection |  |  |
| R02 | conditional |  |  |
| R03 | blocking |  |  |
| R04 | conditional | degradation_relative | 0.2 |
| R05 | blocking | min_rows | 30 |
| R05 | blocking | min_blocks | 5 |
| R06 | restriction |  |  |
| R07 | segment |  |  |
| R08 | conditional | coverage_target | 0.8 |
| R08 | conditional | coverage_tolerance | 0.1 |
| R09 | restriction |  |  |
| R10 | disclosure |  |  |
| R11 | conditional |  |  |
| R12 | conditional |  |  |
| R13 | conditional |  |  |
| R14 | blocking |  |  |

<!-- section: commerce__approve__01_recommend -->
## Approval Recommendation

### model_approval

27 row(s)

| task_id | model_name | role | decision | restriction | overridden | primary_metric | validation_value | evaluated_value |
|---|---|---|---|---|---|---|---|---|
| session_purchase_ga4.purchase | gbt_sklearn | selected | approved |  | False | pr_auc | 0.3957 | 0.4584 |
| session_purchase_ga4.purchase | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.3917 | 0.4579 |
| session_purchase_ga4.purchase | tabpfn_25_in_context | runner_up | not_approved |  | False | pr_auc |  |  |
| session_purchase_ga4.purchase | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.2376 | 0.2551 |
| session_purchase_rees46.purchase | gbt_sklearn | selected | approved |  | False | pr_auc | 0.4378 | 0.4398 |
| session_purchase_rees46.purchase | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.4376 | 0.4393 |
| session_purchase_rees46.purchase | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.0813 | 0.0817 |
| lapse_ga4.return | gbt_sklearn | selected | approved |  | False | pr_auc | 0.2147 | 0.1933 |
| lapse_ga4.return | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.2095 | 0.1871 |
| lapse_ga4.return | tabpfn_v2_in_context | runner_up | not_approved |  | False | pr_auc |  |  |
| lapse_ga4.return | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.0786 | 0.0781 |
| lapse_rees46.return | gbt_lightgbm | selected | approved |  | False | pr_auc | 0.7318 | 0.7321 |
| lapse_rees46.return | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 0.7317 | 0.732 |
| lapse_rees46.return | tabpfn_25_in_context | runner_up | not_approved |  | False | pr_auc |  |  |
| lapse_rees46.return | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.4634 | 0.463 |
| next_item_ga4.next_item | sequence_transformer | selected | approved |  | False | ndcg_at_10 | 0.4684 | 0.4667 |
| next_item_ga4.next_item | sequence_gru | runner_up | not_approved |  | False | ndcg_at_10 | 0.4482 | 0.4476 |
| next_item_ga4.next_item | sasrec_sequence | runner_up | not_approved |  | False | ndcg_at_10 | 0.2381 | 0.2371 |
| next_item_ga4.next_item | popularity | baseline_fallback | approved |  | False | ndcg_at_10 | 0.0758 | 0.0749 |
| next_item_rees46.next_item | sequence_transformer | selected | approved |  | False | ndcg_at_10 | 0.0965 | 0.0988 |
| next_item_rees46.next_item | cooccurrence | runner_up | not_approved |  | False | ndcg_at_10 | 0.0877 | 0.0902 |
| next_item_rees46.next_item | sasrec_sequence | runner_up | not_approved |  | False | ndcg_at_10 | 0.0167 | 0.0178 |
| next_item_rees46.next_item | popularity | baseline_fallback | approved |  | False | ndcg_at_10 | 0.0162 | 0.0169 |
| rl_session_sequences.policy | sequence_gru | selected | approved_with_conditions | offline_only | False | action_agreement | 0.9445 | 0.9439 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | runner_up | not_approved |  | False | action_agreement | 0.9443 | 0.9437 |
| rl_session_sequences.policy | decision_transformer | runner_up | not_approved |  | False | action_agreement | 0.9442 | 0.9433 |
| rl_session_sequences.policy | most_frequent_event | baseline_fallback | approved |  | False | action_agreement | 0.9442 | 0.9433 |

<!-- section: commerce__approve__02_record_decisions -->
## Approval Decisions

### model_approval

27 row(s)

| task_id | model_name | role | decision | restriction | overridden | primary_metric | validation_value | evaluated_value |
|---|---|---|---|---|---|---|---|---|
| session_purchase_ga4.purchase | gbt_sklearn | selected | approved |  | False | pr_auc | 0.3957 | 0.4584 |
| session_purchase_ga4.purchase | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.3917 | 0.4579 |
| session_purchase_ga4.purchase | tabpfn_25_in_context | runner_up | not_approved |  | False | pr_auc |  |  |
| session_purchase_ga4.purchase | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.2376 | 0.2551 |
| session_purchase_rees46.purchase | gbt_sklearn | selected | approved |  | False | pr_auc | 0.4378 | 0.4398 |
| session_purchase_rees46.purchase | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.4376 | 0.4393 |
| session_purchase_rees46.purchase | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.0813 | 0.0817 |
| lapse_ga4.return | gbt_sklearn | selected | approved |  | False | pr_auc | 0.2147 | 0.1933 |
| lapse_ga4.return | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.2095 | 0.1871 |
| lapse_ga4.return | tabpfn_v2_in_context | runner_up | not_approved |  | False | pr_auc |  |  |
| lapse_ga4.return | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.0786 | 0.0781 |
| lapse_rees46.return | gbt_lightgbm | selected | approved |  | False | pr_auc | 0.7318 | 0.7321 |
| lapse_rees46.return | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 0.7317 | 0.732 |
| lapse_rees46.return | tabpfn_25_in_context | runner_up | not_approved |  | False | pr_auc |  |  |
| lapse_rees46.return | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.4634 | 0.463 |
| next_item_ga4.next_item | sequence_transformer | selected | approved |  | False | ndcg_at_10 | 0.4684 | 0.4667 |
| next_item_ga4.next_item | sequence_gru | runner_up | not_approved |  | False | ndcg_at_10 | 0.4482 | 0.4476 |
| next_item_ga4.next_item | sasrec_sequence | runner_up | not_approved |  | False | ndcg_at_10 | 0.2381 | 0.2371 |
| next_item_ga4.next_item | popularity | baseline_fallback | approved |  | False | ndcg_at_10 | 0.0758 | 0.0749 |
| next_item_rees46.next_item | sequence_transformer | selected | approved |  | False | ndcg_at_10 | 0.0965 | 0.0988 |
| next_item_rees46.next_item | cooccurrence | runner_up | not_approved |  | False | ndcg_at_10 | 0.0877 | 0.0902 |
| next_item_rees46.next_item | sasrec_sequence | runner_up | not_approved |  | False | ndcg_at_10 | 0.0167 | 0.0178 |
| next_item_rees46.next_item | popularity | baseline_fallback | approved |  | False | ndcg_at_10 | 0.0162 | 0.0169 |
| rl_session_sequences.policy | sequence_gru | selected | approved_with_conditions | offline_only | False | action_agreement | 0.9445 | 0.9439 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | runner_up | not_approved |  | False | action_agreement | 0.9443 | 0.9437 |
| rl_session_sequences.policy | decision_transformer | runner_up | not_approved |  | False | action_agreement | 0.9442 | 0.9433 |
| rl_session_sequences.policy | most_frequent_event | baseline_fallback | approved |  | False | action_agreement | 0.9442 | 0.9433 |

<!-- section: commerce__approve__03_model_cards -->
## Model Cards

7 card(s) written to src/model_cards/commerce

<!-- section: commerce__commerce__01_session_purchase_ga4 -->
## Model Session Purchase Ga4

### candidate_results

34 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | pr_auc | 0.2376 | 2609 | ok | seconds=51.6 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | log_loss | 0.5485 | 2609 | ok | seconds=51.6 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | calibration_error | 0.0063 | 2609 | ok | seconds=51.6 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | base_rate | 0.2376 | 2609 | ok | seconds=51.6 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | pred_mean | 0.2439 | 2609 | ok | seconds=51.6 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | pred_positive_share | 0.0 | 2609 | ok | seconds=51.6 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 2609 | ok | seconds=51.6 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | pr_auc | 0.3672 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | log_loss | 0.5174 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | calibration_error | 0.0231 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | base_rate | 0.2376 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | pred_mean | 0.2496 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | pred_positive_share | 0.0448 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | pr_auc_lift | 1.5454 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | train_pr_auc | 0.4034 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | train_log_loss | 0.5144 | 2609 | ok | seconds=1.9 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.3917 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.5054 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0302 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.2376 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.2527 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.0755 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 1.6483 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.6246 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.4372 | 2609 | ok | seconds=293.5 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.3957 | 2609 | ok | seconds=415.9 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | log_loss | 0.5002 | 2609 | ok | seconds=415.9 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0185 | 2609 | ok | seconds=415.9 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | base_rate | 0.2376 | 2609 | ok | seconds=415.9 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.2517 | 2609 | ok | seconds=415.9 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| session_purchase_ga4.purchase | data_ready | False | 0 |  | 2026-10-23 18:31:54.559535 |
| session_purchase_ga4.purchase | finished | False | 0 | baseline_ok=True | 2026-10-23 18:45:21.510238 |
| session_purchase_ga4.purchase | started | False | 0 |  | 2026-10-23 18:31:32.015183 |

<!-- section: commerce__commerce__02_session_purchase_rees46 -->
## Model Session Purchase Rees46

### candidate_results

34 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.4376 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.2123 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0139 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.0813 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.0952 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.0423 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 5.3813 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.4547 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.2174 | 785035 | ok | seconds=228.4 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.4378 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | log_loss | 0.2125 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0144 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | base_rate | 0.0813 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.0957 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | pred_positive_share | 0.0424 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | pr_auc_lift | 5.384 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | train_pr_auc | 0.4545 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | train_log_loss | 0.2174 | 785035 | ok | seconds=226.5 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | pr_auc | 0.3672 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | log_loss | 0.2389 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | calibration_error | 0.0169 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | base_rate | 0.0813 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | pred_mean | 0.0963 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | pred_positive_share | 0.0422 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | pr_auc_lift | 4.5154 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | train_pr_auc | 0.3929 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | train_log_loss | 0.2345 | 785035 | ok | seconds=76.2 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | pr_auc | 0.0813 | 785035 | ok | seconds=143.0 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | log_loss | 0.282 | 785035 | ok | seconds=143.0 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | calibration_error | 0.0018 | 785035 | ok | seconds=143.0 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| session_purchase_rees46.purchase | data_ready | False | 1 |  | 2026-10-23 18:33:33.542626 |
| session_purchase_rees46.purchase | finished | False | 1 | baseline_ok=True | 2026-10-23 18:45:36.726518 |
| session_purchase_rees46.purchase | started | False | 1 |  | 2026-10-23 18:31:32.012158 |

<!-- section: commerce__commerce__03_lapse_ga4 -->
## Model Lapse Ga4

### candidate_results

34 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| lapse_ga4.return | base_rate | baseline | baseline | validation | pr_auc | 0.0786 | 6524 | ok | seconds=42.8 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | log_loss | 0.2779 | 6524 | ok | seconds=42.8 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | calibration_error | 0.0202 | 6524 | ok | seconds=42.8 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | base_rate | 0.0786 | 6524 | ok | seconds=42.8 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | pred_mean | 0.0989 | 6524 | ok | seconds=42.8 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | pred_positive_share | 0.0 | 6524 | ok | seconds=42.8 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 6524 | ok | seconds=42.8 |
| lapse_ga4.return | logistic | tabular | candidate | validation | pr_auc | 0.2059 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | logistic | tabular | candidate | validation | log_loss | 0.2557 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | logistic | tabular | candidate | validation | calibration_error | 0.0102 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | logistic | tabular | candidate | validation | base_rate | 0.0786 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | logistic | tabular | candidate | validation | pred_mean | 0.0799 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | logistic | tabular | candidate | validation | pred_positive_share | 0.006 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | logistic | tabular | candidate | validation | pr_auc_lift | 2.618 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | logistic | tabular | candidate | validation | train_pr_auc | 0.2177 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | logistic | tabular | candidate | validation | train_log_loss | 0.3032 | 6524 | ok | seconds=2.3 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.2095 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.2543 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0079 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.0786 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.0814 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.0063 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 2.6637 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.3519 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.275 | 6524 | ok | seconds=293.0 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.2147 | 6524 | ok | seconds=158.2 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | log_loss | 0.2508 | 6524 | ok | seconds=158.2 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0052 | 6524 | ok | seconds=158.2 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | base_rate | 0.0786 | 6524 | ok | seconds=158.2 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.0807 | 6524 | ok | seconds=158.2 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| lapse_ga4.return | data_ready | False | 0 |  | 2026-10-23 18:31:58.006115 |
| lapse_ga4.return | finished | False | 0 | baseline_ok=True | 2026-10-23 18:40:53.439124 |
| lapse_ga4.return | started | False | 0 |  | 2026-10-23 18:31:33.231656 |

<!-- section: commerce__commerce__04_lapse_rees46 -->
## Model Lapse Rees46

### candidate_results

34 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| lapse_rees46.return | base_rate | baseline | baseline | validation | pr_auc | 0.4634 | 453829 | ok | seconds=40.8 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | log_loss | 0.6905 | 453829 | ok | seconds=40.8 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | calibration_error | 0.0002 | 453829 | ok | seconds=40.8 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | base_rate | 0.4634 | 453829 | ok | seconds=40.8 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | pred_mean | 0.4631 | 453829 | ok | seconds=40.8 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | pred_positive_share | 0.0 | 453829 | ok | seconds=40.8 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 453829 | ok | seconds=40.8 |
| lapse_rees46.return | logistic | tabular | candidate | validation | pr_auc | 0.7088 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | logistic | tabular | candidate | validation | log_loss | 0.6091 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | logistic | tabular | candidate | validation | calibration_error | 0.0198 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | logistic | tabular | candidate | validation | base_rate | 0.4634 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | logistic | tabular | candidate | validation | pred_mean | 0.463 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | logistic | tabular | candidate | validation | pred_positive_share | 0.3076 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | logistic | tabular | candidate | validation | pr_auc_lift | 1.5297 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | logistic | tabular | candidate | validation | train_pr_auc | 0.7076 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | logistic | tabular | candidate | validation | train_log_loss | 0.6091 | 453829 | ok | seconds=13.0 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.7318 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.5906 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0022 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.4634 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.4629 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.3653 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 1.5795 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.7324 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.5895 | 453829 | ok | seconds=130.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.7317 | 453829 | ok | seconds=283.6 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | log_loss | 0.5907 | 453829 | ok | seconds=283.6 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0023 | 453829 | ok | seconds=283.6 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | base_rate | 0.4634 | 453829 | ok | seconds=283.6 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.4628 | 453829 | ok | seconds=283.6 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| lapse_rees46.return | data_ready | False | 0 |  | 2026-10-23 18:32:14.100777 |
| lapse_rees46.return | finished | False | 0 | baseline_ok=True | 2026-10-23 18:40:36.933915 |
| lapse_rees46.return | started | False | 0 |  | 2026-10-23 18:31:33.242855 |

<!-- section: commerce__commerce__05_next_item_ga4 -->
## Model Next Item Ga4

### candidate_results

40 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| next_item_ga4.next_item | popularity | baseline | baseline | validation | recall_at_5 | 0.0895 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | ndcg_at_5 | 0.0548 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | recall_at_10 | 0.1553 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | ndcg_at_10 | 0.0758 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | recall_at_20 | 0.2679 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | ndcg_at_20 | 0.1039 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | mrr | 0.0711 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | train_recall_at_10 | 0.1466 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | train_ndcg_at_10 | 0.0712 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | train_mrr | 0.0675 | 297217 | ok | seconds=36.2 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | recall_at_5 | 0.3541 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_5 | 0.2166 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | recall_at_10 | 0.6274 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_10 | 0.3043 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | recall_at_20 | 0.8101 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_20 | 0.3507 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | mrr | 0.2252 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | train_recall_at_10 | 0.6486 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | train_ndcg_at_10 | 0.315 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | train_mrr | 0.2325 | 297217 | ok | seconds=1.5 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | recall_at_5 | 0.5921 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_5 | 0.4005 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | recall_at_10 | 0.8014 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_10 | 0.4684 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | recall_at_20 | 0.9015 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_20 | 0.4943 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | mrr | 0.3749 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | train_recall_at_10 | 0.8102 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | train_ndcg_at_10 | 0.4754 | 297217 | ok | seconds=616.0 |
| next_item_ga4.next_item | sequence_transformer | sequence | candidate | validation | train_mrr | 0.381 | 297217 | ok | seconds=616.0 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| next_item_ga4.next_item | data_ready | False | 8 |  | 2026-10-23 18:32:09.613689 |
| next_item_ga4.next_item | finished | False | 8 | baseline_ok=True | 2026-10-23 19:02:02.909524 |
| next_item_ga4.next_item | started | False | 8 |  | 2026-10-23 18:31:32.015099 |

<!-- section: commerce__commerce__06_next_item_rees46 -->
## Model Next Item Rees46

### candidate_results

40 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| next_item_rees46.next_item | popularity | baseline | baseline | validation | recall_at_5 | 0.0191 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | ndcg_at_5 | 0.0118 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | recall_at_10 | 0.0329 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | ndcg_at_10 | 0.0162 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | recall_at_20 | 0.0521 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | ndcg_at_20 | 0.0211 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | mrr | 0.0149 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | train_recall_at_10 | 0.0357 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | train_ndcg_at_10 | 0.0176 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | train_mrr | 0.0166 | 302614 | ok | seconds=42.6 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | recall_at_5 | 0.1098 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_5 | 0.0736 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | recall_at_10 | 0.1535 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_10 | 0.0877 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | recall_at_20 | 0.1995 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_20 | 0.0994 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | mrr | 0.0732 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | train_recall_at_10 | 0.2266 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | train_ndcg_at_10 | 0.1297 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | cooccurrence | ranking | candidate | validation | train_mrr | 0.1084 | 302614 | ok | seconds=1.4 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | recall_at_5 | 0.1185 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_5 | 0.0792 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | recall_at_10 | 0.1722 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_10 | 0.0965 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | recall_at_20 | 0.2328 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_20 | 0.1118 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | mrr | 0.0815 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | train_recall_at_10 | 0.2381 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | train_ndcg_at_10 | 0.1367 | 302614 | ok | seconds=1849.6 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | train_mrr | 0.1156 | 302614 | ok | seconds=1849.6 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| next_item_rees46.next_item | data_ready | False | 14 |  | 2026-10-23 19:17:36.511887 |
| next_item_rees46.next_item | finished | False | 14 | baseline_ok=True | 2026-10-23 20:16:09.392550 |
| next_item_rees46.next_item | started | False | 14 |  | 2026-10-23 19:17:12.998261 |

<!-- section: commerce__commerce__07_rl_session_sequences -->
## Model Session Policy

### candidate_results

40 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | action_agreement | 0.9319 | 1014461 | ok | seconds=557.3 |
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | reward_when_agree | 3.4505 | 1014461 | ok | seconds=557.3 |
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | reward_when_disagree | 26.0952 | 1014461 | ok | seconds=557.3 |
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | reward_weighted_agreement | 0.6441 | 1014461 | ok | seconds=557.3 |
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | train_action_agreement | 0.9551 | 1014461 | ok | seconds=557.3 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | action_agreement | 0.9445 | 1014461 | ok | seconds=876.7 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | reward_when_agree | 1.7635 | 1014461 | ok | seconds=876.7 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | reward_when_disagree | 59.9657 | 1014461 | ok | seconds=876.7 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | reward_weighted_agreement | 0.3337 | 1014461 | ok | seconds=876.7 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | train_action_agreement | 0.9627 | 1014461 | ok | seconds=876.7 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | action_agreement | 0.9443 | 1014461 | ok | seconds=279.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_when_agree | 1.5519 | 1014461 | ok | seconds=279.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_when_disagree | 63.3278 | 1014461 | ok | seconds=279.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_weighted_agreement | 0.2936 | 1014461 | ok | seconds=279.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | train_action_agreement | 0.9627 | 1014461 | ok | seconds=279.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | action_agreement | 0.9443 | 1014461 | ok | seconds=296.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_when_agree | 1.5031 | 1014461 | ok | seconds=296.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_when_disagree | 64.1373 | 1014461 | ok | seconds=296.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_weighted_agreement | 0.2843 | 1014461 | ok | seconds=296.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | train_action_agreement | 0.9628 | 1014461 | ok | seconds=296.0 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | action_agreement | 0.932 | 1014461 | ok | seconds=263.6 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_when_agree | 3.4867 | 1014461 | ok | seconds=263.6 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_when_disagree | 25.6416 | 1014461 | ok | seconds=263.6 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_weighted_agreement | 0.6509 | 1014461 | ok | seconds=263.6 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | train_action_agreement | 0.956 | 1014461 | ok | seconds=263.6 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | action_agreement | 0.9323 | 1014461 | ok | seconds=224.1 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_when_agree | 3.4515 | 1014461 | ok | seconds=224.1 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_when_disagree | 26.2085 | 1014461 | ok | seconds=224.1 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_weighted_agreement | 0.6446 | 1014461 | ok | seconds=224.1 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | train_action_agreement | 0.9568 | 1014461 | ok | seconds=224.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| rl_session_sequences.policy | data_ready | False | 9 |  | 2026-10-23 19:28:50.490130 |
| rl_session_sequences.policy | finished | False | 9 | baseline_ok=True | 2026-10-23 20:16:29.623236 |
| rl_session_sequences.policy | started | False | 9 |  | 2026-10-23 19:28:00.139475 |

<!-- section: commerce__evaluate__00_evaluation_setup -->
## Evaluation Setup

### evaluation_spec

_no rows for this run_

<!-- section: commerce__evaluate__08_tabular_commerce -->
## Evaluate Tabular Models Commerce

### evaluation_results

195 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| session_purchase_ga4.purchase | base_rate | baseline | test | pr_auc | 0.2551 | 0.2376 | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | log_loss | 0.5682 | 0.5485 | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | calibration_error | 0.0112 | 0.0063 | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | base_rate | 0.2551 | 0.2376 | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | pred_mean | 0.2439 | 0.2439 | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | pred_positive_share | 0.0 | 0.0 | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | pr_auc_lift | 1.0 | 1.0 | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | primary_change_relative | -0.0736 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | pred_finite_share | 1.0 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | pred_std | 0.0 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | reproduced_validation_value | 0.2376 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | base_rate | baseline | test | reproduction_gap_relative | 0.0 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | pr_auc | 0.4584 | 0.3957 | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | log_loss | 0.5054 | 0.5002 | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | calibration_error | 0.0189 | 0.0185 | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | base_rate | 0.2551 | 0.2376 | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | pred_mean | 0.2474 | 0.2517 | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | pred_positive_share | 0.0569 | 0.0579 | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | pr_auc_lift | 1.7968 | 1.6652 | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | primary_change_relative | -0.1585 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | skill_primary | 0.2033 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | skill_primary_ci_low | 0.1731 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | skill_primary_ci_high | 0.2341 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | bootstrap_resamples_valid | 200.0 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | bootstrap_blocks | 2180.0 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | bootstrap_rows_used | 2724.0 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | pred_finite_share | 1.0 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | pred_std | 0.1363 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | reproduced_validation_value | 0.3957 |  | 2724 | ok | on test |
| session_purchase_ga4.purchase | gbt_sklearn | candidate | test | reproduction_gap_relative | 0.0 |  | 2724 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

12 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| session_purchase_rees46.purchase | data_ready | False | 1 |  | 2026-10-25 21:44:57.948936 |
| session_purchase_ga4.purchase | data_ready | False | 0 |  | 2026-10-25 21:43:49.888454 |
| lapse_rees46.return | data_ready | False | 0 |  | 2026-10-25 21:46:45.798437 |
| lapse_ga4.return | data_ready | False | 0 |  | 2026-10-25 21:46:07.223471 |
| session_purchase_rees46.purchase | finished | False | 1 | baseline_ok=True | 2026-10-25 21:45:43.820438 |
| session_purchase_ga4.purchase | finished | False | 0 | baseline_ok=True | 2026-10-25 21:44:16.646951 |
| session_purchase_rees46.purchase | started | False | 1 |  | 2026-10-25 21:44:46.755103 |
| lapse_rees46.return | finished | False | 0 | baseline_ok=True | 2026-10-25 21:47:14.374469 |
| session_purchase_ga4.purchase | started | False | 0 |  | 2026-10-25 21:42:59.496061 |
| lapse_ga4.return | finished | False | 0 | baseline_ok=True | 2026-10-25 21:46:18.424429 |
| lapse_rees46.return | started | False | 0 |  | 2026-10-25 21:46:35.838431 |
| lapse_ga4.return | started | False | 0 |  | 2026-10-25 21:45:57.336859 |

<!-- section: commerce__evaluate__09_ranking_commerce -->
## Evaluate Ranking Models Commerce

### evaluation_results

116 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| next_item_ga4.next_item | popularity | baseline | test | recall_at_5 | 0.0879 | 0.0895 | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | ndcg_at_5 | 0.0538 | 0.0548 | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | recall_at_10 | 0.154 | 0.1553 | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | ndcg_at_10 | 0.0749 | 0.0758 | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | recall_at_20 | 0.2687 | 0.2679 | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | ndcg_at_20 | 0.1036 | 0.1039 | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | mrr | 0.0706 | 0.0711 | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | primary_change_relative | 0.0114 |  | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | reproduced_validation_value | 0.0758 |  | 299434 | ok | on test |
| next_item_ga4.next_item | popularity | baseline | test | reproduction_gap_relative | 0.0 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | recall_at_5 | 0.5909 | 0.5921 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | ndcg_at_5 | 0.3989 | 0.4005 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | recall_at_10 | 0.7997 | 0.8014 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | ndcg_at_10 | 0.4667 | 0.4684 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | recall_at_20 | 0.9003 | 0.9015 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | ndcg_at_20 | 0.4926 | 0.4943 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | mrr | 0.3732 | 0.3749 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | primary_change_relative | 0.0038 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | skill_primary | 0.3918 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | skill_primary_ci_low | 0.3899 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | skill_primary_ci_high | 0.4038 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | bootstrap_resamples_valid | 200.0 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | bootstrap_blocks | 1367.0 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | bootstrap_rows_used | 99095.0 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | reproduced_validation_value | 0.4684 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_transformer | candidate | test | reproduction_gap_relative | 0.0 |  | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_gru | candidate | test | recall_at_5 | 0.5694 | 0.5688 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_gru | candidate | test | ndcg_at_5 | 0.3783 | 0.3783 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_gru | candidate | test | recall_at_10 | 0.783 | 0.7842 | 299434 | ok | on test |
| next_item_ga4.next_item | sequence_gru | candidate | test | ndcg_at_10 | 0.4476 | 0.4482 | 299434 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

6 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| next_item_ga4.next_item | started | False | 8 |  | 2026-10-25 14:14:40.738804 |
| next_item_ga4.next_item | data_ready | False | 8 |  | 2026-10-25 14:15:37.188729 |
| next_item_ga4.next_item | finished | False | 8 | baseline_ok=True | 2026-10-25 14:22:03.030938 |
| next_item_rees46.next_item | started | False | 14 |  | 2026-10-25 14:22:37.164990 |
| next_item_rees46.next_item | data_ready | False | 14 |  | 2026-10-25 14:22:58.862281 |
| next_item_rees46.next_item | finished | False | 14 | baseline_ok=True | 2026-10-25 14:29:43.568420 |

<!-- section: commerce__evaluate__10_offline_rl_commerce -->
## Evaluate Offline Policies Commerce

### evaluation_results

46 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| rl_session_sequences.policy | most_frequent_event | baseline | test | action_agreement | 0.9433 | 0.9442 | 1053871 | ok | on test |
| rl_session_sequences.policy | most_frequent_event | baseline | test | reward_when_agree | 0.0 | 0.0 | 1053871 | ok | on test |
| rl_session_sequences.policy | most_frequent_event | baseline | test | reward_when_disagree | 95.2243 | 89.451 | 1053871 | ok | on test |
| rl_session_sequences.policy | most_frequent_event | baseline | test | reward_weighted_agreement | 0.0 | 0.0 | 1053871 | ok | on test |
| rl_session_sequences.policy | most_frequent_event | baseline | test | primary_change_relative | 0.001 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | most_frequent_event | baseline | test | reproduced_validation_value | 0.9442 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | most_frequent_event | baseline | test | reproduction_gap_relative | 0.0 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | action_agreement | 0.9439 | 0.9445 | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | reward_when_agree | 2.0187 | 1.7635 | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | reward_when_disagree | 62.2991 | 59.9657 | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | reward_weighted_agreement | 0.3527 | 0.3337 | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | primary_change_relative | 0.0007 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | skill_primary | 0.0006 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | skill_primary_ci_low | -0.0005 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | skill_primary_ci_high | 0.0036 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | bootstrap_resamples_valid | 200.0 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | bootstrap_blocks | 5332.0 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | bootstrap_rows_used | 104886.0 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | reproduced_validation_value | 0.9445 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | sequence_gru | candidate | test | reproduction_gap_relative | 0.0 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | action_agreement | 0.9437 | 0.9443 | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | reward_when_agree | 1.7698 | 1.5519 | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | reward_when_disagree | 66.2789 | 63.3278 | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | reward_weighted_agreement | 0.3092 | 0.2936 | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | primary_change_relative | 0.0006 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | skill_primary | 0.0004 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | skill_primary_ci_low | 0.0001 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | skill_primary_ci_high | 0.002 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | bootstrap_resamples_valid | 200.0 |  | 1053871 | ok | on test |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | candidate | test | bootstrap_blocks | 5332.0 |  | 1053871 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| rl_session_sequences.policy | data_ready | False | 9 |  | 2026-10-25 14:16:33.691200 |
| rl_session_sequences.policy | finished | False | 9 | baseline_ok=True | 2026-10-25 14:29:32.352033 |
| rl_session_sequences.policy | started | False | 9 |  | 2026-10-25 14:15:43.573034 |

<!-- section: commerce__operate__00_operate_setup -->
## Operate Setup

### monitoring_spec

_no rows for this run_

<!-- section: commerce__operate__01_reference_profiles_commerce -->
## Reference Profiles Commerce

### reference_profile

57 row(s), first 30 shown

| task_id | model_name | subject | value_kind | n_rows | null_rate |
|---|---|---|---|---|---|
| session_purchase_ga4.purchase | gbt_sklearn | prefix_event_count | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_page_view_count | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_view_item_count | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_add_to_cart_count | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_search_count | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_distinct_pages | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_session_number | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_geo_country | categorical | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_span_seconds | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_hour_of_day | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | prefix_day_of_week | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | user_hist_session_count | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | user_hist_transaction_count | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | user_hist_revenue | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | user_hist_event_count | numeric | 11885 | 0.0 |
| session_purchase_ga4.purchase | gbt_sklearn | __prediction__ | numeric | 11885 | 0.0 |
| next_item_ga4.next_item | sequence_transformer | seq_product_id | categorical | 100000 | 0.0 |
| next_item_ga4.next_item | sequence_transformer | __prediction__ | categorical | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_event_count | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_view_count | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_cart_count | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_distinct_products | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_first_category | categorical | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_first_brand | categorical | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_first_price | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_mean_price | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_span_seconds | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_hour_of_day | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | prefix_day_of_week | numeric | 100000 | 0.0 |
| session_purchase_rees46.purchase | gbt_sklearn | user_hist_session_count | numeric | 100000 | 0.0 |

<!-- section: commerce__operate__02_monitor_commerce -->
## Monitor Commerce

### monitoring_results

342 row(s), first 30 shown

| task_id | model_name | window_id | check_kind | subject | value | level |
|---|---|---|---|---|---|---|
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_event_count | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_event_count | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | out_of_range | prefix_event_count | 0.0 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_view_count | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_view_count | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | out_of_range | prefix_view_count | 0.0 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_cart_count | 0.0251 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_cart_count | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | out_of_range | prefix_cart_count | 0.0 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_distinct_products | 0.0105 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_distinct_products | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | out_of_range | prefix_distinct_products | 0.0 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_first_category | 0.0045 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_first_category | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | unseen_category | prefix_first_category | 0.0 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_first_brand | 0.0114 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_first_brand | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | unseen_category | prefix_first_brand | 0.3674 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_first_price | 0.0086 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_first_price | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | out_of_range | prefix_first_price | 0.0 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_mean_price | 0.0007 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_mean_price | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | out_of_range | prefix_mean_price | 0.0 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_span_seconds | 0.0028 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_span_seconds | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | out_of_range | prefix_span_seconds | 0.0 | info |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | prefix_hour_of_day | 0.025 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | null_rate | prefix_hour_of_day | 0.0 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | out_of_range | prefix_hour_of_day | 0.0 | info |

### monitoring_flags

13 row(s)

| task_id | model_name | window_id | check_kind | subject | level | value | threshold |
|---|---|---|---|---|---|---|---|
| session_purchase_rees46.purchase | gbt_sklearn | validation | feature_drift | user_hist_session_count | flag | 0.3637 | 0.25 |
| session_purchase_rees46.purchase | gbt_sklearn | validation | feature_drift | user_hist_event_count | flag | 0.3076 | 0.25 |
| session_purchase_rees46.purchase | gbt_sklearn | validation | feature_drift | user_hist_product_views | flag | 0.3075 | 0.25 |
| session_purchase_rees46.purchase | gbt_sklearn | validation | feature_drift | user_hist_days_since_first_session | flag | 1.1201 | 0.25 |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | user_hist_session_count | flag | 0.3731 | 0.25 |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | user_hist_event_count | flag | 0.3156 | 0.25 |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | user_hist_product_views | flag | 0.3156 | 0.25 |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | feature_drift | user_hist_days_since_first_session | flag | 1.1281 | 0.25 |
| lapse_ga4.return | gbt_sklearn | validation | feature_drift | lapse_days_since_last_session | flag | 0.7674 | 0.25 |
| lapse_ga4.return | gbt_sklearn | validation | prediction_drift | __prediction__ | flag | 0.3989 | 0.25 |
| lapse_ga4.return | gbt_sklearn | held_out | feature_drift | lapse_days_since_last_session | flag | 0.7491 | 0.25 |
| lapse_ga4.return | gbt_sklearn | held_out | prediction_drift | __prediction__ | flag | 0.38 | 0.25 |
| next_item_rees46.next_item | sequence_transformer | validation | prediction_drift | __prediction__ | warning | 0.106 | 0.1 |

### model_predictions

1018729 row(s), first 30 shown

| task_id | model_name | window_id | row_key | prediction |
|---|---|---|---|---|
| lapse_ga4.return | gbt_sklearn | validation | 10025069.3848916196/2020-12-01 | 0.0757 |
| lapse_ga4.return | gbt_sklearn | validation | 10165145.4337231556/2020-12-01 | 0.0965 |
| lapse_ga4.return | gbt_sklearn | validation | 1037526.8209256161/2020-12-01 | 0.026 |
| lapse_ga4.return | gbt_sklearn | validation | 1042546.6434588808/2020-12-01 | 0.0599 |
| lapse_ga4.return | gbt_sklearn | validation | 10489887.5646596416/2020-12-01 | 0.1625 |
| lapse_ga4.return | gbt_sklearn | validation | 1080618.1751497567/2020-12-01 | 0.0427 |
| lapse_ga4.return | gbt_sklearn | validation | 11070217.6371774168/2020-12-01 | 0.026 |
| lapse_ga4.return | gbt_sklearn | validation | 11417436.5771744883/2020-12-01 | 0.0374 |
| lapse_ga4.return | gbt_sklearn | validation | 1147207202.5999340710/2020-12-01 | 0.0374 |
| lapse_ga4.return | gbt_sklearn | validation | 12199079.0538038952/2020-12-01 | 0.039 |
| lapse_ga4.return | gbt_sklearn | validation | 12517664.5607605531/2020-12-01 | 0.1978 |
| lapse_ga4.return | gbt_sklearn | validation | 1253937422.4478893229/2020-12-01 | 0.0664 |
| lapse_ga4.return | gbt_sklearn | validation | 12572552.9153069105/2020-12-01 | 0.0664 |
| lapse_ga4.return | gbt_sklearn | validation | 12668704.0857038297/2020-12-01 | 0.0429 |
| lapse_ga4.return | gbt_sklearn | validation | 1275347607.8532359305/2020-12-01 | 0.0374 |
| lapse_ga4.return | gbt_sklearn | validation | 130335109.4276822537/2020-12-01 | 0.049 |
| lapse_ga4.return | gbt_sklearn | validation | 13099105.7061861161/2020-12-01 | 0.0322 |
| lapse_ga4.return | gbt_sklearn | validation | 1339345.9247260376/2020-12-01 | 0.026 |
| lapse_ga4.return | gbt_sklearn | validation | 13450576.3026228207/2020-12-01 | 0.0374 |
| lapse_ga4.return | gbt_sklearn | validation | 1395551.2258891388/2020-12-01 | 0.0413 |
| lapse_ga4.return | gbt_sklearn | validation | 14303493.7721314103/2020-12-01 | 0.026 |
| lapse_ga4.return | gbt_sklearn | validation | 1489043.3420273286/2020-12-01 | 0.0358 |
| lapse_ga4.return | gbt_sklearn | validation | 1497446.9972252618/2020-12-01 | 0.0421 |
| lapse_ga4.return | gbt_sklearn | validation | 15146875.7859580763/2020-12-01 | 0.026 |
| lapse_ga4.return | gbt_sklearn | validation | 15316815.5257701616/2020-12-01 | 0.0512 |
| lapse_ga4.return | gbt_sklearn | validation | 1540308157.4120074434/2020-12-01 | 0.3878 |
| lapse_ga4.return | gbt_sklearn | validation | 15466766.1264639777/2020-12-01 | 0.026 |
| lapse_ga4.return | gbt_sklearn | validation | 1556324.6435835198/2020-12-01 | 0.0663 |
| lapse_ga4.return | gbt_sklearn | validation | 16183198.4336443456/2020-12-01 | 0.1266 |
| lapse_ga4.return | gbt_sklearn | validation | 16617695.8488058760/2020-12-01 | 0.0351 |

<!-- section: commerce__operate__03_performance_and_triggers -->
## Performance And Triggers

### monitoring_results

7 row(s)

| task_id | model_name | window_id | check_kind | subject | value | level |
|---|---|---|---|---|---|---|
| rl_session_sequences.policy | sequence_gru | held_out | performance | action_agreement | 0.0007 | ok |
| next_item_rees46.next_item | sequence_transformer | held_out | performance | ndcg_at_10 | -0.0236 | ok |
| next_item_ga4.next_item | sequence_transformer | held_out | performance | ndcg_at_10 | 0.0038 | ok |
| session_purchase_rees46.purchase | gbt_sklearn | held_out | performance | pr_auc | -0.0045 | ok |
| session_purchase_ga4.purchase | gbt_sklearn | held_out | performance | pr_auc | -0.1585 | ok |
| lapse_rees46.return | gbt_lightgbm | held_out | performance | pr_auc | -0.0004 | ok |
| lapse_ga4.return | gbt_sklearn | held_out | performance | pr_auc | 0.0999 | ok |

### monitoring_flags

_no rows for this run_

### monitoring_triggers

2 row(s)

| task_id | model_name | trigger_kind | source | condition | value | threshold |
|---|---|---|---|---|---|---|
| session_purchase_rees46.purchase | gbt_sklearn | drift_flag | monitoring | a drift or input-quality check reached the flag level | 1.1281 | 0.25 |
| lapse_ga4.return | gbt_sklearn | drift_flag | monitoring | a drift or input-quality check reached the flag level | 0.7674 | 0.25 |

<!-- section: commerce__operate__04_score_request -->
## Score Request

### scoring_requests

394 row(s), first 30 shown

| request_id | task_id | model_name | registry_version | key_filter | window_start | window_end | row_key | prediction | prediction_q10 | prediction_q90 | frozen_delta_version | run_id | scored_at |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 513575173/54c28966-032a-42d8-978e-74ddc303290f | 0.021 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 513717589/1b9a0a45-f46e-4b17-b676-059bf941f08e | 0.0275 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 513730688/62b8510a-45fb-4e42-bab4-7c04a5bf9744 | 0.2775 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 513734390/32370efe-51fb-4bee-ad31-5265fc766bb3 | 0.029 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 513768887/cbcfe7a2-64d0-4dda-b58b-00a1c68f1e57 | 0.0309 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 514012505/3b8f7fa8-6d1c-4e79-b87a-b98f7cdfc3d5 | 0.0336 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 514512365/8bc9ce14-9032-4375-90ff-756c2933ed43 | 0.018 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 514529169/b33baa04-573e-429a-ad16-94fbea4e5299 | 0.0253 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 514693308/f6dc0c87-fabb-495f-a2bc-7417a0d32cb9 | 0.0385 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 514719036/7ce3bd85-0f37-45c8-a14e-70305685fe20 | 0.0165 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 514724831/cfe76306-9903-42a7-b8d1-bfef2b8b2669 | 0.0169 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 514964712/abb63338-4dff-452d-88a3-36686d5f3ae8 | 0.044 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 515516334/623d8b8b-e1f4-4906-9842-7892898cbbe1 | 0.0224 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 515560986/f3c332a4-7a97-4f7a-9450-c158b6ca75c0 | 0.0452 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 515631067/dd8d9fb7-fef0-4b61-95d7-0ce5b9f7a899 | 0.046 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 515706919/c2ea11de-27a9-473f-872f-d6be8f6adef6 | 0.008 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 516119545/e4878566-1a22-4f74-964e-39998cfd6ad2 | 0.0283 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 516352123/d1ae2165-0d6c-4366-bd4d-829680ce9b50 | 0.0291 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 516539581/3ab77bdb-1bcf-4fcc-babf-7f3de2518f84 | 0.0117 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 516646336/41edf5ec-9649-4a3a-8cfe-d5adaf736ebc | 0.0166 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 516915170/4d18b6aa-2037-4cae-90e3-9b6f2d3f0075 | 0.3756 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 517694782/e7ce72e0-a17e-4e2f-ae0c-7f47ee834b24 | 0.0534 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 518049798/92a67594-3c16-41b2-abf6-eb26b52a662d | 0.0257 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 518634220/dfdc955c-ad6b-44f2-9c3b-36f9897118c8 | 0.0392 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 518735924/377be5d7-a4a2-49c3-b93e-8bf5775b2cfe | 0.0944 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 518739667/693821f2-9c39-44da-848f-48034a7302bd | 0.0527 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 518896750/8a2e8570-6711-4711-b9c8-908ceb73cfa2 | 0.2649 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 519925412/c58762bd-00fb-4f04-b00a-893d575a27b0 | 0.0133 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 520061200/9c1d1a50-a5fa-4a1e-a28a-d109053a6a79 | 0.0446 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |
| ef83fbe4b304 | session_purchase_rees46.purchase | gbt_sklearn | 1 | {} |  |  | 521113122/27fb0d62-fcc6-481e-93f6-098228542bba | 0.0462 |  |  | 1 | model-20261027T154716Z | 2026-10-27 16:01:02.430213 |

<!-- section: commerce__register__00_registry_setup -->
## Registry Setup

registry tables created if missing

<!-- section: commerce__register__01_register_models -->
## Register Models

### model_registry

_no rows for this run_

<!-- section: commerce__register__02_reproducibility_commerce -->
## Reproducibility Check Commerce

### registry_check

14 row(s)

| task_id | model_name | role | registry_version | processor_type | reload_status | rescore_status | gap_relative | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| session_purchase_ga4.purchase | gbt_sklearn | selected | 2 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| session_purchase_ga4.purchase | base_rate | baseline_fallback | 2 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| session_purchase_rees46.purchase | gbt_sklearn | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| session_purchase_rees46.purchase | base_rate | baseline_fallback | 2 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| lapse_ga4.return | gbt_sklearn | selected | 2 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| lapse_ga4.return | base_rate | baseline_fallback | 2 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| lapse_rees46.return | gbt_lightgbm | selected | 2 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| lapse_rees46.return | base_rate | baseline_fallback | 2 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| next_item_ga4.next_item | sequence_transformer | selected | 2 | aarch64 | ok | matched | 0.0 | passed | ndcg_at_10 on validation |
| next_item_ga4.next_item | popularity | baseline_fallback | 2 | aarch64 | ok | matched | 0.0 | passed | ndcg_at_10 on validation |
| next_item_rees46.next_item | sequence_transformer | selected | 2 | aarch64 | ok | matched | 0.0 | passed | ndcg_at_10 on validation |
| next_item_rees46.next_item | popularity | baseline_fallback | 2 | aarch64 | ok | matched | 0.0 | passed | ndcg_at_10 on validation |
| rl_session_sequences.policy | sequence_gru | selected | 2 | aarch64 | ok | matched | 0.0 | passed | action_agreement on validation |
| rl_session_sequences.policy | most_frequent_event | baseline_fallback | 2 | aarch64 | ok | matched | 0.0 | passed | action_agreement on validation |

<!-- section: commerce__register__03_foundation_references -->
## Foundation References

### foundation_registry_entry

_no rows for this run_

<!-- section: commerce__split__03_evaluation_spec -->
## Evaluation Specification

### evaluation_spec

6 row(s)

| dataset_id | spec_key | spec_value |
|---|---|---|
| next_item_rees46 | user_sample_percent | 10 |
| next_item_rees46 | sample_seed | ecrmap-model-sample-s1 |
| next_item_rees46 | rank_ks | (5, 10, 20) |
| rl_session_sequences | user_sample_percent | 10 |
| rl_session_sequences | sample_seed | ecrmap-model-sample-s1 |
| next_item_ga4 | rank_ks | (5, 10, 20) |
