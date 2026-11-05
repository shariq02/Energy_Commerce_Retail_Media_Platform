# COMMERCE NOTEBOOK FINDINGS (SMOKE RUN)

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-10-23T18:30Z_

<!-- section: commerce__commerce__01_session_purchase_ga4 -->
## Model Session Purchase Ga4

### candidate_results

34 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.3544 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.5255 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0395 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.2376 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.245 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.0809 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 1.4914 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.7857 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.3774 | 2609 | ok | seconds=194.3 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.3535 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | log_loss | 0.526 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0486 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | base_rate | 0.2376 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.2461 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | pred_positive_share | 0.0805 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | pr_auc_lift | 1.4876 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | train_pr_auc | 0.7734 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | gbt_sklearn | tabular | candidate | validation | train_log_loss | 0.3812 | 2609 | ok | seconds=460.6 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | pr_auc | 0.3655 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | log_loss | 0.5176 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | calibration_error | 0.0173 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | base_rate | 0.2376 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | pred_mean | 0.2451 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | pred_positive_share | 0.028 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | pr_auc_lift | 1.538 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | train_pr_auc | 0.3891 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | logistic | tabular | candidate | validation | train_log_loss | 0.5201 | 2609 | ok | seconds=1.4 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | pr_auc | 0.2376 | 2609 | ok | seconds=34.1 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | log_loss | 0.5484 | 2609 | ok | seconds=34.1 |
| session_purchase_ga4.purchase | base_rate | baseline | baseline | validation | calibration_error | 0.0059 | 2609 | ok | seconds=34.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| session_purchase_ga4.purchase | data_ready | True | 0 |  | 2026-10-23 18:07:01.634075 |
| session_purchase_ga4.purchase | finished | True | 0 | baseline_ok=True | 2026-10-23 18:18:49.709627 |
| session_purchase_ga4.purchase | started | True | 0 |  | 2026-10-23 18:06:51.000871 |

<!-- section: commerce__commerce__02_session_purchase_rees46 -->
## Model Session Purchase Rees46

### candidate_results

34 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | pr_auc | 0.0772 | 4000 | ok | seconds=60.7 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | log_loss | 0.2721 | 4000 | ok | seconds=60.7 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | calibration_error | 0.0042 | 4000 | ok | seconds=60.7 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | base_rate | 0.0772 | 4000 | ok | seconds=60.7 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | pred_mean | 0.0815 | 4000 | ok | seconds=60.7 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | pred_positive_share | 0.0 | 4000 | ok | seconds=60.7 |
| session_purchase_rees46.purchase | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 4000 | ok | seconds=60.7 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | pr_auc | 0.3909 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | log_loss | 0.2328 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | calibration_error | 0.0314 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | base_rate | 0.0772 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | pred_mean | 0.1087 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | pred_positive_share | 0.0757 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | pr_auc_lift | 5.0601 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | train_pr_auc | 0.424 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | logistic | tabular | candidate | validation | train_log_loss | 0.2262 | 4000 | ok | seconds=3.9 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.3951 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.2393 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0326 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.0772 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.0787 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.051 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 5.1142 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.9654 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.0818 | 4000 | ok | seconds=212.4 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.3919 | 4000 | ok | seconds=67.0 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | log_loss | 0.2388 | 4000 | ok | seconds=67.0 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.031 | 4000 | ok | seconds=67.0 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | base_rate | 0.0772 | 4000 | ok | seconds=67.0 |
| session_purchase_rees46.purchase | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.077 | 4000 | ok | seconds=67.0 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| session_purchase_rees46.purchase | data_ready | True | 1 |  | 2026-10-23 18:13:49.054261 |
| session_purchase_rees46.purchase | finished | True | 1 | baseline_ok=True | 2026-10-23 18:19:52.372251 |
| session_purchase_rees46.purchase | started | True | 1 |  | 2026-10-23 18:13:37.182872 |

<!-- section: commerce__commerce__03_lapse_ga4 -->
## Model Lapse Ga4

### candidate_results

34 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| lapse_ga4.return | base_rate | baseline | baseline | validation | pr_auc | 0.0742 | 4000 | ok | seconds=32.1 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | log_loss | 0.2689 | 4000 | ok | seconds=32.1 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | calibration_error | 0.027 | 4000 | ok | seconds=32.1 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | base_rate | 0.0742 | 4000 | ok | seconds=32.1 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | pred_mean | 0.1013 | 4000 | ok | seconds=32.1 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | pred_positive_share | 0.0 | 4000 | ok | seconds=32.1 |
| lapse_ga4.return | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 4000 | ok | seconds=32.1 |
| lapse_ga4.return | logistic | tabular | candidate | validation | pr_auc | 0.204 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | logistic | tabular | candidate | validation | log_loss | 0.245 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | logistic | tabular | candidate | validation | calibration_error | 0.0135 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | logistic | tabular | candidate | validation | base_rate | 0.0742 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | logistic | tabular | candidate | validation | pred_mean | 0.0846 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | logistic | tabular | candidate | validation | pred_positive_share | 0.0083 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | logistic | tabular | candidate | validation | pr_auc_lift | 2.7469 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | logistic | tabular | candidate | validation | train_pr_auc | 0.2354 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | logistic | tabular | candidate | validation | train_log_loss | 0.3075 | 4000 | ok | seconds=1.6 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.2166 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.2467 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0161 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.0742 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.0829 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.0152 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 2.9167 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.4976 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.2457 | 4000 | ok | seconds=201.9 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.2106 | 4000 | ok | seconds=453.3 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | log_loss | 0.2473 | 4000 | ok | seconds=453.3 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.015 | 4000 | ok | seconds=453.3 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | base_rate | 0.0742 | 4000 | ok | seconds=453.3 |
| lapse_ga4.return | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.0823 | 4000 | ok | seconds=453.3 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| lapse_ga4.return | data_ready | True | 0 |  | 2026-10-23 18:07:17.334565 |
| lapse_ga4.return | finished | True | 0 | baseline_ok=True | 2026-10-23 18:19:09.898941 |
| lapse_ga4.return | started | True | 0 |  | 2026-10-23 18:07:08.814377 |

<!-- section: commerce__commerce__04_lapse_rees46 -->
## Model Lapse Rees46

### candidate_results

34 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| lapse_rees46.return | base_rate | baseline | baseline | validation | pr_auc | 0.4657 | 4000 | ok | seconds=47.4 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | log_loss | 0.6909 | 4000 | ok | seconds=47.4 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | calibration_error | 0.0077 | 4000 | ok | seconds=47.4 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | base_rate | 0.4657 | 4000 | ok | seconds=47.4 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | pred_mean | 0.4735 | 4000 | ok | seconds=47.4 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | pred_positive_share | 0.0 | 4000 | ok | seconds=47.4 |
| lapse_rees46.return | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 4000 | ok | seconds=47.4 |
| lapse_rees46.return | logistic | tabular | candidate | validation | pr_auc | 0.7137 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | logistic | tabular | candidate | validation | log_loss | 0.6073 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | logistic | tabular | candidate | validation | calibration_error | 0.0328 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | logistic | tabular | candidate | validation | base_rate | 0.4657 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | logistic | tabular | candidate | validation | pred_mean | 0.4694 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | logistic | tabular | candidate | validation | pred_positive_share | 0.327 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | logistic | tabular | candidate | validation | pr_auc_lift | 1.5324 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | logistic | tabular | candidate | validation | train_pr_auc | 0.7223 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | logistic | tabular | candidate | validation | train_log_loss | 0.6066 | 4000 | ok | seconds=2.2 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.7109 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | log_loss | 0.6134 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0354 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | base_rate | 0.4657 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.4701 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | pred_positive_share | 0.3817 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | pr_auc_lift | 1.5265 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | train_pr_auc | 0.8451 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_sklearn | tabular | candidate | validation | train_log_loss | 0.5053 | 4000 | ok | seconds=44.9 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.7145 | 4000 | ok | seconds=7.4 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.612 | 4000 | ok | seconds=7.4 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0371 | 4000 | ok | seconds=7.4 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.4657 | 4000 | ok | seconds=7.4 |
| lapse_rees46.return | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.47 | 4000 | ok | seconds=7.4 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| lapse_rees46.return | started | True | 0 |  | 2026-10-23 18:18:50.619886 |
| lapse_rees46.return | data_ready | True | 0 |  | 2026-10-23 18:19:03.722735 |
| lapse_rees46.return | finished | True | 0 | baseline_ok=True | 2026-10-23 18:21:01.652773 |

<!-- section: commerce__commerce__05_next_item_ga4 -->
## Model Next Item Ga4

### candidate_results

40 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| next_item_ga4.next_item | popularity | baseline | baseline | validation | recall_at_5 | 0.0862 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | ndcg_at_5 | 0.0539 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | recall_at_10 | 0.1515 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | ndcg_at_10 | 0.0748 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | recall_at_20 | 0.2682 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | ndcg_at_20 | 0.1039 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | mrr | 0.0711 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | train_recall_at_10 | 0.1655 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | train_ndcg_at_10 | 0.0828 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | popularity | baseline | baseline | validation | train_mrr | 0.0773 | 4000 | ok | seconds=36.2 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | recall_at_5 | 0.2612 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_5 | 0.1575 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | recall_at_10 | 0.4305 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_10 | 0.2124 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | recall_at_20 | 0.5258 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | ndcg_at_20 | 0.2369 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | mrr | 0.1578 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | train_recall_at_10 | 0.835 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | train_ndcg_at_10 | 0.4883 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | cooccurrence | ranking | candidate | validation | train_mrr | 0.3925 | 4000 | ok | seconds=1.5 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | recall_at_5 | 0.0222 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | ndcg_at_5 | 0.0161 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | recall_at_10 | 0.0382 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | ndcg_at_10 | 0.0212 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | recall_at_20 | 0.0732 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | ndcg_at_20 | 0.0301 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | mrr | 0.0258 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | train_recall_at_10 | 0.0503 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | train_ndcg_at_10 | 0.0244 | 4000 | ok | seconds=296.0 |
| next_item_ga4.next_item | sequence_gru | sequence | candidate | validation | train_mrr | 0.0277 | 4000 | ok | seconds=296.0 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| next_item_ga4.next_item | data_ready | True | 8 |  | 2026-10-23 18:07:19.756497 |
| next_item_ga4.next_item | finished | True | 8 | baseline_ok=True | 2026-10-23 18:19:14.851708 |
| next_item_ga4.next_item | started | True | 8 |  | 2026-10-23 18:07:08.817139 |

<!-- section: commerce__commerce__06_next_item_rees46 -->
## Model Next Item Rees46

### candidate_results

40 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | recall_at_5 | 0.0 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_5 | 0.0 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | recall_at_10 | 0.0026 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_10 | 0.0007 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | recall_at_20 | 0.0051 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | ndcg_at_20 | 0.0013 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | mrr | 0.0014 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | train_recall_at_10 | 0.0427 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | train_ndcg_at_10 | 0.0182 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_transformer | sequence | candidate | validation | train_mrr | 0.0222 | 392 | ok | seconds=2.7 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | recall_at_5 | 0.0051 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | ndcg_at_5 | 0.0023 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | recall_at_10 | 0.0051 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | ndcg_at_10 | 0.0023 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | recall_at_20 | 0.0153 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | ndcg_at_20 | 0.0048 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | mrr | 0.0027 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | train_recall_at_10 | 0.0352 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | train_ndcg_at_10 | 0.015 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | sequence_gru | sequence | candidate | validation | train_mrr | 0.0188 | 392 | ok | seconds=92.3 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | recall_at_5 | 0.0077 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | ndcg_at_5 | 0.0042 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | recall_at_10 | 0.0128 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | ndcg_at_10 | 0.0059 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | recall_at_20 | 0.0255 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | ndcg_at_20 | 0.0089 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | mrr | 0.0052 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | train_recall_at_10 | 0.0729 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | train_ndcg_at_10 | 0.0377 | 392 | ok | seconds=34.8 |
| next_item_rees46.next_item | popularity | baseline | baseline | validation | train_mrr | 0.038 | 392 | ok | seconds=34.8 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| next_item_rees46.next_item | started | True | 14 |  | 2026-10-23 18:19:40.839836 |
| next_item_rees46.next_item | data_ready | True | 14 |  | 2026-10-23 18:19:52.358336 |
| next_item_rees46.next_item | finished | True | 14 | baseline_ok=True | 2026-10-23 18:24:22.142356 |

<!-- section: commerce__commerce__07_rl_session_sequences -->
## Model Session Policy

### candidate_results

40 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | action_agreement | 0.9484 | 349 | ok | seconds=1.6 |
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | reward_when_agree | 0.0 | 349 | ok | seconds=1.6 |
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | reward_when_disagree | 65.1392 | 349 | ok | seconds=1.6 |
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | reward_weighted_agreement | 0.0 | 349 | ok | seconds=1.6 |
| rl_session_sequences.policy | sequence_gru_reward_weighted | offline_rl | candidate | validation | train_action_agreement | 0.9747 | 349 | ok | seconds=1.6 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | action_agreement | 0.9484 | 349 | ok | seconds=89.0 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | reward_when_agree | 0.0 | 349 | ok | seconds=89.0 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | reward_when_disagree | 65.1392 | 349 | ok | seconds=89.0 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | reward_weighted_agreement | 0.0 | 349 | ok | seconds=89.0 |
| rl_session_sequences.policy | sequence_gru | offline_rl | candidate | validation | train_action_agreement | 0.9747 | 349 | ok | seconds=89.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | action_agreement | 0.9341 | 349 | ok | seconds=17.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_when_agree | 0.0 | 349 | ok | seconds=17.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_when_disagree | 50.9785 | 349 | ok | seconds=17.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_weighted_agreement | 0.0 | 349 | ok | seconds=17.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | train_action_agreement | 1.0 | 349 | ok | seconds=17.0 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | action_agreement | 0.9198 | 349 | ok | seconds=5.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_when_agree | 0.0 | 349 | ok | seconds=5.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_when_disagree | 41.8752 | 349 | ok | seconds=5.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_weighted_agreement | 0.0 | 349 | ok | seconds=5.1 |
| rl_session_sequences.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | train_action_agreement | 1.0 | 349 | ok | seconds=5.1 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | action_agreement | 0.9341 | 349 | ok | seconds=12.4 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_when_agree | 0.0 | 349 | ok | seconds=12.4 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_when_disagree | 50.9785 | 349 | ok | seconds=12.4 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_weighted_agreement | 0.0 | 349 | ok | seconds=12.4 |
| rl_session_sequences.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | train_action_agreement | 1.0 | 349 | ok | seconds=12.4 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | action_agreement | 0.9198 | 349 | ok | seconds=3.1 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_when_agree | 0.0 | 349 | ok | seconds=3.1 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_when_disagree | 41.8752 | 349 | ok | seconds=3.1 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_weighted_agreement | 0.0 | 349 | ok | seconds=3.1 |
| rl_session_sequences.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | train_action_agreement | 1.0 | 349 | ok | seconds=3.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| rl_session_sequences.policy | started | True | 9 |  | 2026-10-23 18:20:01.049501 |
| rl_session_sequences.policy | data_ready | True | 9 |  | 2026-10-23 18:20:12.152112 |
| rl_session_sequences.policy | finished | True | 9 | baseline_ok=True | 2026-10-23 18:25:08.526337 |
