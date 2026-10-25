# COMMERCE NOTEBOOK FINDINGS

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-10-23T20:18Z_

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
