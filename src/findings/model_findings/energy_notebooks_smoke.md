# ENERGY NOTEBOOK FINDINGS (SMOKE RUN)

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-10-23T18:30Z_

<!-- section: energy__energy__01_price_daily -->
## Model Price Daily

### candidate_results

70 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| price_daily.price | seasonal_naive | baseline | baseline | validation | mae | 28.0429 | 364 | ok | seconds=45.7 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | rmse | 42.9023 | 364 | ok | seconds=45.7 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | mean_error | -1.5877 | 364 | ok | seconds=45.7 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | mean_error_relative | -0.0202 | 364 | ok | seconds=45.7 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | pred_std_ratio | 1.0185 | 364 | ok | seconds=45.7 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | pinball_q50 | 14.0215 | 364 | ok | seconds=45.7 |
| price_daily.price | empirical_quantiles | baseline | baseline | validation | pinball_q10 | 5.8028 | 364 | ok | seconds=1.1 |
| price_daily.price | empirical_quantiles | baseline | baseline | validation | pinball_q50 | 14.5833 | 364 | ok | seconds=1.1 |
| price_daily.price | empirical_quantiles | baseline | baseline | validation | pinball_q90 | 16.8015 | 364 | ok | seconds=1.1 |
| price_daily.price | empirical_quantiles | baseline | baseline | validation | interval_coverage_80 | 0.9313 | 364 | ok | seconds=1.1 |
| price_daily.price | ridge | tabular | candidate | validation | mae | 15.6577 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | rmse | 22.5344 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | mean_error | 5.9908 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | mean_error_relative | 0.0763 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | pred_std_ratio | 0.9139 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | skill_mae | 0.4417 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | skill_rmse | 0.4748 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | pinball_q50 | 7.8288 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | train_mae | 19.9278 | 364 | ok | seconds=3.4 |
| price_daily.price | ridge | tabular | candidate | validation | train_rmse | 31.1918 | 364 | ok | seconds=3.4 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | mae | 12.579 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | rmse | 21.0109 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | mean_error | -5.8403 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0743 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9344 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.5514 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.5103 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | pinball_q50 | 6.2895 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | train_mae | 2.2952 | 364 | ok | seconds=324.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | train_rmse | 4.2779 | 364 | ok | seconds=324.0 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| price_daily.price | started | True | 0 |  | 2026-10-23 17:12:52.275537 |
| price_daily.price | data_ready | True | 0 |  | 2026-10-23 17:13:16.608014 |
| price_daily.price | finished | True | 0 | baseline_ok=True | 2026-10-23 17:39:27.161519 |

<!-- section: energy__energy__02_price_quarter_hour -->
## Model Price Quarter Hour

### candidate_results

104 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| price_quarter_hour.price | persistence | baseline | baseline | validation | mae | 33.1411 | 4000 | ok | seconds=41.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | rmse | 51.3919 | 4000 | ok | seconds=41.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | mean_error | -1.051 | 4000 | ok | seconds=41.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | mean_error_relative | -0.012 | 4000 | ok | seconds=41.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | pred_std_ratio | 0.9641 | 4000 | ok | seconds=41.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | pinball_q50 | 16.5706 | 4000 | ok | seconds=41.2 |
| price_quarter_hour.price | empirical_quantiles | baseline | baseline | validation | pinball_q10 | 7.947 | 4000 | ok | seconds=0.9 |
| price_quarter_hour.price | empirical_quantiles | baseline | baseline | validation | pinball_q50 | 15.7143 | 4000 | ok | seconds=0.9 |
| price_quarter_hour.price | empirical_quantiles | baseline | baseline | validation | pinball_q90 | 9.6238 | 4000 | ok | seconds=0.9 |
| price_quarter_hour.price | empirical_quantiles | baseline | baseline | validation | interval_coverage_80 | 0.8315 | 4000 | ok | seconds=0.9 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | mae | 21.7749 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | rmse | 31.337 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | mean_error | -1.3566 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | mean_error_relative | -0.0154 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | pred_std_ratio | 0.9157 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | skill_mae | 0.343 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | skill_rmse | 0.3902 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | pinball_q50 | 10.8875 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | train_mae | 22.5355 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | train_rmse | 36.4006 | 4000 | ok | seconds=2.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | mae | 18.7556 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | rmse | 28.1927 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | mean_error | -3.9786 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0452 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9635 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.4341 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.4514 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | pinball_q50 | 9.3778 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | train_mae | 6.2658 | 4000 | ok | seconds=233.2 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | train_rmse | 10.5043 | 4000 | ok | seconds=233.2 |

### task_run_context

6 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| price_quarter_hour.price | started | True | 0 |  | 2026-10-23 17:12:54.130230 |
| price_quarter_hour.price | data_ready | True | 0 |  | 2026-10-23 17:13:21.647772 |
| price_quarter_hour.negative_price | data_ready | True | 0 |  | 2026-10-23 17:36:11.261299 |
| price_quarter_hour.negative_price | finished | True | 0 | baseline_ok=True | 2026-10-23 17:45:36.701299 |
| price_quarter_hour.price | finished | True | 0 | baseline_ok=True | 2026-10-23 17:35:59.801699 |
| price_quarter_hour.negative_price | started | True | 0 |  | 2026-10-23 17:13:01.508012 |

<!-- section: energy__energy__03_load -->
## Model Load

### candidate_results

32 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| load.load | seasonal_mean | baseline | baseline | validation | mae | 58805.4664 | 2184 | ok | seconds=34.3 |
| load.load | seasonal_mean | baseline | baseline | validation | rmse | 112838.5335 | 2184 | ok | seconds=34.3 |
| load.load | seasonal_mean | baseline | baseline | validation | mean_error | 41287.7855 | 2184 | ok | seconds=34.3 |
| load.load | seasonal_mean | baseline | baseline | validation | mean_error_relative | 0.0753 | 2184 | ok | seconds=34.3 |
| load.load | seasonal_mean | baseline | baseline | validation | pred_std_ratio | 1.0451 | 2184 | ok | seconds=34.3 |
| load.load | ridge | tabular | candidate | validation | mae | 67635.9765 | 2184 | ok | seconds=8.8 |
| load.load | ridge | tabular | candidate | validation | rmse | 89416.5796 | 2184 | ok | seconds=8.8 |
| load.load | ridge | tabular | candidate | validation | mean_error | -2657.1911 | 2184 | ok | seconds=8.8 |
| load.load | ridge | tabular | candidate | validation | mean_error_relative | -0.0048 | 2184 | ok | seconds=8.8 |
| load.load | ridge | tabular | candidate | validation | pred_std_ratio | 0.9288 | 2184 | ok | seconds=8.8 |
| load.load | ridge | tabular | candidate | validation | skill_mae | -0.1502 | 2184 | ok | seconds=8.8 |
| load.load | ridge | tabular | candidate | validation | skill_rmse | 0.2076 | 2184 | ok | seconds=8.8 |
| load.load | ridge | tabular | candidate | validation | train_mae | 71354.1707 | 2184 | ok | seconds=8.8 |
| load.load | ridge | tabular | candidate | validation | train_rmse | 92177.3261 | 2184 | ok | seconds=8.8 |
| load.load | gbt_lightgbm | tabular | candidate | validation | mae | 26902.3333 | 2184 | ok | seconds=373.7 |
| load.load | gbt_lightgbm | tabular | candidate | validation | rmse | 44573.0971 | 2184 | ok | seconds=373.7 |
| load.load | gbt_lightgbm | tabular | candidate | validation | mean_error | -1188.2926 | 2184 | ok | seconds=373.7 |
| load.load | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0022 | 2184 | ok | seconds=373.7 |
| load.load | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9884 | 2184 | ok | seconds=373.7 |
| load.load | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.5425 | 2184 | ok | seconds=373.7 |
| load.load | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.605 | 2184 | ok | seconds=373.7 |
| load.load | gbt_lightgbm | tabular | candidate | validation | train_mae | 14400.0 | 2184 | ok | seconds=373.7 |
| load.load | gbt_lightgbm | tabular | candidate | validation | train_rmse | 20591.5842 | 2184 | ok | seconds=373.7 |
| load.load | gbt_sklearn | tabular | candidate | validation | mae | 26209.3071 | 2184 | ok | seconds=704.7 |
| load.load | gbt_sklearn | tabular | candidate | validation | rmse | 43413.5761 | 2184 | ok | seconds=704.7 |
| load.load | gbt_sklearn | tabular | candidate | validation | mean_error | -60.1096 | 2184 | ok | seconds=704.7 |
| load.load | gbt_sklearn | tabular | candidate | validation | mean_error_relative | -0.0001 | 2184 | ok | seconds=704.7 |
| load.load | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.9915 | 2184 | ok | seconds=704.7 |
| load.load | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.5543 | 2184 | ok | seconds=704.7 |
| load.load | gbt_sklearn | tabular | candidate | validation | skill_rmse | 0.6153 | 2184 | ok | seconds=704.7 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| load.load | started | True | 0 |  | 2026-10-23 17:12:52.341367 |
| load.load | data_ready | True | 0 |  | 2026-10-23 17:13:28.002366 |
| load.load | finished | True | 0 | baseline_ok=True | 2026-10-23 17:32:38.749223 |

<!-- section: energy__energy__04_bias -->
## Model Forecast Bias

### candidate_results

73 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae | 93123.7858 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | rmse | 196269.0249 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mean_error | 76322.5458 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | 0.815 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.0521 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.0056 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.0005 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | skill_mae_vs_scope_mean | 0.0098 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__offshore_wind | 9236.6618 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__onshore_wind | 18402.6454 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__other | 426925.7119 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__photovoltaic | 12585.329 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__total | 90465.7142 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__wind_and_photovoltaic | 26191.4981 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | train_mae | 8876.1435 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | train_rmse | 12293.7293 | 1819 | ok | seconds=218.6 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae | 93467.5898 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | rmse | 197582.3646 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mean_error | 77640.2562 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mean_error_relative | 0.829 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.0545 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.0019 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | skill_rmse | -0.0062 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | skill_mae_vs_scope_mean | 0.0062 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__offshore_wind | 9138.8513 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__onshore_wind | 18213.7127 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__other | 429659.7227 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__photovoltaic | 12697.5031 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__total | 88214.8142 | 1819 | ok | seconds=563.8 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__wind_and_photovoltaic | 25754.7924 | 1819 | ok | seconds=563.8 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| bias.bias | data_ready | True | 0 |  | 2026-10-23 17:28:22.552489 |
| bias.bias | finished | True | 0 | baseline_ok=True | 2026-10-23 17:46:07.011258 |
| bias.bias | started | True | 0 |  | 2026-10-23 17:28:00.658352 |

<!-- section: energy__energy__05_zone_generation -->
## Model Zone Generation

### candidate_results

32 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| zone_generation.generation | capacity_factor | baseline | baseline | validation | mae | 25994.6528 | 3640 | ok | seconds=122.3 |
| zone_generation.generation | capacity_factor | baseline | baseline | validation | rmse | 41929.7855 | 3640 | ok | seconds=122.3 |
| zone_generation.generation | capacity_factor | baseline | baseline | validation | mean_error | -671.5326 | 3640 | ok | seconds=122.3 |
| zone_generation.generation | capacity_factor | baseline | baseline | validation | mean_error_relative | -0.0122 | 3640 | ok | seconds=122.3 |
| zone_generation.generation | capacity_factor | baseline | baseline | validation | pred_std_ratio | 0.8145 | 3640 | ok | seconds=122.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | mae | 30388.3844 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | ridge | tabular | candidate | validation | rmse | 42447.6957 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | ridge | tabular | candidate | validation | mean_error | -2966.6956 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | ridge | tabular | candidate | validation | mean_error_relative | -0.054 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | ridge | tabular | candidate | validation | pred_std_ratio | 0.7348 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | ridge | tabular | candidate | validation | skill_mae | -0.169 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | ridge | tabular | candidate | validation | skill_rmse | -0.0124 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | ridge | tabular | candidate | validation | train_mae | 27212.2569 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | ridge | tabular | candidate | validation | train_rmse | 37771.5915 | 3640 | ok | seconds=2.2 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | mae | 12971.9062 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | rmse | 20031.4597 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | mean_error | 1181.5458 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | 0.0215 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 1.0121 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.501 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.5223 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | train_mae | 4601.6634 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | train_rmse | 6699.5581 | 3640 | ok | seconds=277.8 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | mae | 13737.7794 | 3640 | ok | seconds=582.6 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | rmse | 18865.0004 | 3640 | ok | seconds=582.6 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | mean_error | 1854.4998 | 3640 | ok | seconds=582.6 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | mean_error_relative | 0.0337 | 3640 | ok | seconds=582.6 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.9742 | 3640 | ok | seconds=582.6 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.4715 | 3640 | ok | seconds=582.6 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | skill_rmse | 0.5501 | 3640 | ok | seconds=582.6 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| zone_generation.generation | data_ready | True | 0 |  | 2026-10-23 17:32:22.150398 |
| zone_generation.generation | finished | True | 0 | baseline_ok=True | 2026-10-23 17:49:16.625998 |
| zone_generation.generation | started | True | 0 |  | 2026-10-23 17:32:10.017650 |

<!-- section: energy__energy__06_honda_forecast -->
## Model Honda Forecast

### candidate_results

32 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| honda_forecast.increment | persistence | baseline | baseline | validation | mae | 23.7196 | 4000 | ok | seconds=198.2 |
| honda_forecast.increment | persistence | baseline | baseline | validation | rmse | 49.605 | 4000 | ok | seconds=198.2 |
| honda_forecast.increment | persistence | baseline | baseline | validation | mean_error | -0.3695 | 4000 | ok | seconds=198.2 |
| honda_forecast.increment | persistence | baseline | baseline | validation | mean_error_relative | -0.0081 | 4000 | ok | seconds=198.2 |
| honda_forecast.increment | persistence | baseline | baseline | validation | pred_std_ratio | 0.9969 | 4000 | ok | seconds=198.2 |
| honda_forecast.increment | ridge | tabular | candidate | validation | mae | 25.2111 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | ridge | tabular | candidate | validation | rmse | 42.7658 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | ridge | tabular | candidate | validation | mean_error | 8.2666 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | ridge | tabular | candidate | validation | mean_error_relative | 0.1819 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | ridge | tabular | candidate | validation | pred_std_ratio | 0.8173 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | ridge | tabular | candidate | validation | skill_mae | -0.0629 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | ridge | tabular | candidate | validation | skill_rmse | 0.1379 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | ridge | tabular | candidate | validation | train_mae | 25.8553 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | ridge | tabular | candidate | validation | train_rmse | 43.4236 | 4000 | ok | seconds=1.9 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | mae | 31.8936 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | rmse | 52.4897 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | mean_error | 16.6028 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | 0.3654 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.7661 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | skill_mae | -0.3446 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | skill_rmse | -0.0582 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | train_mae | 14.4084 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | train_rmse | 20.274 | 4000 | ok | seconds=303.7 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | mae | 31.5393 | 4000 | ok | seconds=521.8 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | rmse | 51.878 | 4000 | ok | seconds=521.8 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | mean_error | 17.0582 | 4000 | ok | seconds=521.8 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | mean_error_relative | 0.3755 | 4000 | ok | seconds=521.8 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.7385 | 4000 | ok | seconds=521.8 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | skill_mae | -0.3297 | 4000 | ok | seconds=521.8 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | skill_rmse | -0.0458 | 4000 | ok | seconds=521.8 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| honda_forecast.increment | started | True | 0 |  | 2026-10-23 17:33:31.108630 |
| honda_forecast.increment | data_ready | True | 0 |  | 2026-10-23 17:33:43.861044 |
| honda_forecast.increment | finished | True | 0 | baseline_ok=True | 2026-10-23 17:51:35.987639 |

<!-- section: energy__energy__07_honda_anomaly -->
## Model Honda Anomaly

### candidate_results

30 row(s)

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | precision | 0.2012 | 4000 | ok | seconds=316.3 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | recall | 0.7462 | 4000 | ok | seconds=316.3 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | flag_rate | 0.0978 | 4000 | ok | seconds=316.3 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | pr_auc | 0.2192 | 4000 | ok | seconds=316.3 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | threshold | 2.2475 | 4000 | ok | seconds=316.3 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | train_flag_rate | 0.01 | 4000 | ok | seconds=316.3 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | precision | 0.2016 | 4000 | ok | seconds=52.7 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | recall | 0.7769 | 4000 | ok | seconds=52.7 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | flag_rate | 0.1032 | 4000 | ok | seconds=52.7 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | pr_auc | 0.7553 | 4000 | ok | seconds=52.7 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | threshold | 4.5105 | 4000 | ok | seconds=52.7 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | train_flag_rate | 0.01 | 4000 | ok | seconds=52.7 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | precision | 0.2122 | 4000 | ok | seconds=97.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | recall | 0.7769 | 4000 | ok | seconds=97.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | flag_rate | 0.097 | 4000 | ok | seconds=97.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | pr_auc | 0.7694 | 4000 | ok | seconds=97.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | threshold | 4.6026 | 4000 | ok | seconds=97.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | train_flag_rate | 0.01 | 4000 | ok | seconds=97.1 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | precision | 0.0811 | 4000 | ok | seconds=2.1 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | recall | 0.0231 | 4000 | ok | seconds=2.1 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | flag_rate | 0.0085 | 4000 | ok | seconds=2.1 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | pr_auc | 0.0597 | 4000 | ok | seconds=2.1 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | threshold | 0.5825 | 4000 | ok | seconds=2.1 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | train_flag_rate | 0.01 | 4000 | ok | seconds=2.1 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | precision | 0.0 | 4000 | ok | seconds=358.1 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | recall | 0.0 | 4000 | ok | seconds=358.1 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | flag_rate | 0.0018 | 4000 | ok | seconds=358.1 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | pr_auc | 0.0795 | 4000 | ok | seconds=358.1 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | threshold | 10.055 | 4000 | ok | seconds=358.1 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | train_flag_rate | 0.01 | 4000 | ok | seconds=358.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| honda_anomaly.anomaly | started | True | 0 |  | 2026-10-23 17:40:30.310525 |
| honda_anomaly.anomaly | data_ready | True | 0 |  | 2026-10-23 17:40:45.319920 |
| honda_anomaly.anomaly | finished | True | 0 | baseline_ok=True | 2026-10-23 18:06:06.943797 |

<!-- section: energy__energy__08_ccpp -->
## Model Ccpp

### candidate_results

41 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| ccpp.output | train_mean | baseline | baseline | validation | mae | 16.7486 | 1369 | ok | seconds=47.0 |
| ccpp.output | train_mean | baseline | baseline | validation | rmse | 19.7783 | 1369 | ok | seconds=47.0 |
| ccpp.output | train_mean | baseline | baseline | validation | mean_error | 10.1082 | 1369 | ok | seconds=47.0 |
| ccpp.output | train_mean | baseline | baseline | validation | mean_error_relative | 0.0222 | 1369 | ok | seconds=47.0 |
| ccpp.output | train_mean | baseline | baseline | validation | pred_std_ratio | 0.0 | 1369 | ok | seconds=47.0 |
| ccpp.output | ridge | tabular | candidate | validation | mae | 3.8553 | 1369 | ok | seconds=1.4 |
| ccpp.output | ridge | tabular | candidate | validation | rmse | 5.1253 | 1369 | ok | seconds=1.4 |
| ccpp.output | ridge | tabular | candidate | validation | mean_error | -1.2741 | 1369 | ok | seconds=1.4 |
| ccpp.output | ridge | tabular | candidate | validation | mean_error_relative | -0.0028 | 1369 | ok | seconds=1.4 |
| ccpp.output | ridge | tabular | candidate | validation | pred_std_ratio | 1.0519 | 1369 | ok | seconds=1.4 |
| ccpp.output | ridge | tabular | candidate | validation | skill_mae | 0.7698 | 1369 | ok | seconds=1.4 |
| ccpp.output | ridge | tabular | candidate | validation | skill_rmse | 0.7409 | 1369 | ok | seconds=1.4 |
| ccpp.output | ridge | tabular | candidate | validation | train_mae | 3.4122 | 1369 | ok | seconds=1.4 |
| ccpp.output | ridge | tabular | candidate | validation | train_rmse | 4.4139 | 1369 | ok | seconds=1.4 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | mae | 3.906 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | rmse | 5.5337 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | mean_error | 2.1555 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | 0.0047 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.8373 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.7668 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.7202 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | train_mae | 1.6161 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | train_rmse | 2.3627 | 1369 | ok | seconds=263.8 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | mae | 4.029 | 1369 | ok | seconds=723.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | rmse | 5.7228 | 1369 | ok | seconds=723.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | mean_error | 2.2987 | 1369 | ok | seconds=723.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | mean_error_relative | 0.0051 | 1369 | ok | seconds=723.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.8279 | 1369 | ok | seconds=723.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.7594 | 1369 | ok | seconds=723.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | skill_rmse | 0.7107 | 1369 | ok | seconds=723.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| ccpp.output | started | True | 0 |  | 2026-10-23 17:12:52.354094 |
| ccpp.output | data_ready | True | 0 |  | 2026-10-23 17:13:14.417536 |
| ccpp.output | finished | True | 0 | baseline_ok=True | 2026-10-23 17:31:19.211304 |

<!-- section: energy__energy__09_capacity_additions -->
## Model Capacity Additions

### candidate_results

32 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | mae | 22.001 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | rmse | 71.9581 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | mean_error | 10.4542 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | mean_error_relative | 0.3948 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 1.1162 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | skill_mae | -0.0749 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | skill_rmse | -0.298 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | train_mae | 7.5278 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | train_rmse | 21.1253 | 189 | ok | seconds=203.0 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | mae | 19.8307 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | rmse | 62.9842 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | mean_error | 7.9473 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | mean_error_relative | 0.3001 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 1.085 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | skill_mae | 0.0311 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | skill_rmse | -0.1361 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | train_mae | 5.8456 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | train_rmse | 15.588 | 189 | ok | seconds=516.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | mae | 47.6619 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | ridge | tabular | candidate | validation | rmse | 83.2732 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | ridge | tabular | candidate | validation | mean_error | 5.5242 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | ridge | tabular | candidate | validation | mean_error_relative | 0.2086 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | ridge | tabular | candidate | validation | pred_std_ratio | 0.2392 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | ridge | tabular | candidate | validation | skill_mae | -1.3287 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | ridge | tabular | candidate | validation | skill_rmse | -0.5021 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | ridge | tabular | candidate | validation | train_mae | 37.7486 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | ridge | tabular | candidate | validation | train_rmse | 79.1263 | 189 | ok | seconds=1.5 |
| capacity_additions.additions | seasonal_mean | baseline | baseline | validation | mae | 20.4674 | 189 | ok | seconds=48.6 |
| capacity_additions.additions | seasonal_mean | baseline | baseline | validation | rmse | 55.4368 | 189 | ok | seconds=48.6 |
| capacity_additions.additions | seasonal_mean | baseline | baseline | validation | mean_error | -1.2842 | 189 | ok | seconds=48.6 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| capacity_additions.additions | started | True | 0 |  | 2026-10-23 17:12:52.272195 |
| capacity_additions.additions | data_ready | True | 0 |  | 2026-10-23 17:13:12.806908 |
| capacity_additions.additions | finished | True | 0 | baseline_ok=True | 2026-10-23 17:26:51.227378 |

<!-- section: energy__energy__10_redispatch -->
## Model Redispatch

### candidate_results

66 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| redispatch.any_event | base_rate | baseline | baseline | validation | pr_auc | 0.6749 | 1452 | ok | seconds=112.4 |
| redispatch.any_event | base_rate | baseline | baseline | validation | log_loss | 0.6769 | 1452 | ok | seconds=112.4 |
| redispatch.any_event | base_rate | baseline | baseline | validation | calibration_error | 0.1499 | 1452 | ok | seconds=112.4 |
| redispatch.any_event | base_rate | baseline | baseline | validation | base_rate | 0.6749 | 1452 | ok | seconds=112.4 |
| redispatch.any_event | base_rate | baseline | baseline | validation | pred_mean | 0.525 | 1452 | ok | seconds=112.4 |
| redispatch.any_event | base_rate | baseline | baseline | validation | pred_positive_share | 1.0 | 1452 | ok | seconds=112.4 |
| redispatch.any_event | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 1452 | ok | seconds=112.4 |
| redispatch.any_event | logistic | tabular | candidate | validation | pr_auc | 0.8804 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | logistic | tabular | candidate | validation | log_loss | 0.5247 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | logistic | tabular | candidate | validation | calibration_error | 0.0476 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | logistic | tabular | candidate | validation | base_rate | 0.6749 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | logistic | tabular | candidate | validation | pred_mean | 0.6436 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | logistic | tabular | candidate | validation | pred_positive_share | 0.7424 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | logistic | tabular | candidate | validation | pr_auc_lift | 1.3045 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | logistic | tabular | candidate | validation | train_pr_auc | 0.7803 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | logistic | tabular | candidate | validation | train_log_loss | 0.5718 | 1452 | ok | seconds=2.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.8649 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.5386 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0442 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.6749 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.6448 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.7094 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 1.2815 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.9184 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.433 | 1452 | ok | seconds=153.8 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.8606 | 1452 | ok | seconds=229.9 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | log_loss | 0.5446 | 1452 | ok | seconds=229.9 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0411 | 1452 | ok | seconds=229.9 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | base_rate | 0.6749 | 1452 | ok | seconds=229.9 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.6402 | 1452 | ok | seconds=229.9 |

### task_run_context

6 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| redispatch.any_event | started | True | 2 |  | 2026-10-23 17:46:40.431244 |
| redispatch.event_energy | started | True | 2 |  | 2026-10-23 17:46:45.214963 |
| redispatch.any_event | data_ready | True | 2 |  | 2026-10-23 17:46:55.717231 |
| redispatch.event_energy | data_ready | True | 2 |  | 2026-10-23 17:55:45.559042 |
| redispatch.event_energy | finished | True | 2 | baseline_ok=True | 2026-10-23 18:05:55.077920 |
| redispatch.any_event | finished | True | 2 | baseline_ok=True | 2026-10-23 17:55:34.372501 |

<!-- section: energy__energy__11_survival -->
## Model Unit Survival

### candidate_results

39 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | concordance | 0.6732 | 4000 | ok | seconds=1.2 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | brier_1y | 0.0 | 4000 | ok | seconds=1.2 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | calibration_gap_1y | 0.0 | 4000 | ok | seconds=1.2 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | brier_3y | 0.0003 | 4000 | ok | seconds=1.2 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | calibration_gap_3y | 0.0003 | 4000 | ok | seconds=1.2 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | brier_5y | 0.0005 | 4000 | ok | seconds=1.2 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | calibration_gap_5y | 0.0003 | 4000 | ok | seconds=1.2 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | train_concordance | 0.6771 | 4000 | ok | seconds=1.2 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | concordance | 0.8984 | 4000 | ok | seconds=2.3 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | brier_1y | 0.0 | 4000 | ok | seconds=2.3 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | calibration_gap_1y | 0.0 | 4000 | ok | seconds=2.3 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | brier_3y | 0.0003 | 4000 | ok | seconds=2.3 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | calibration_gap_3y | 0.0003 | 4000 | ok | seconds=2.3 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | brier_5y | 0.0005 | 4000 | ok | seconds=2.3 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | calibration_gap_5y | 0.0002 | 4000 | ok | seconds=2.3 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | train_concordance | 0.9281 | 4000 | ok | seconds=2.3 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | concordance | 0.9454 | 4000 | ok | seconds=1.9 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | brier_1y | 0.0 | 4000 | ok | seconds=1.9 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | calibration_gap_1y | 0.0 | 4000 | ok | seconds=1.9 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | brier_3y | 0.0003 | 4000 | ok | seconds=1.9 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | calibration_gap_3y | 0.0003 | 4000 | ok | seconds=1.9 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | brier_5y | 0.0005 | 4000 | ok | seconds=1.9 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | calibration_gap_5y | 0.0004 | 4000 | ok | seconds=1.9 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | train_concordance | 0.9847 | 4000 | ok | seconds=1.9 |
| survival.unit_lifetime | survival_forest | survival | candidate | validation | concordance | 0.9273 | 4000 | ok | seconds=44.6 |
| survival.unit_lifetime | survival_forest | survival | candidate | validation | brier_1y | 0.0 | 4000 | ok | seconds=44.6 |
| survival.unit_lifetime | survival_forest | survival | candidate | validation | calibration_gap_1y | 0.0 | 4000 | ok | seconds=44.6 |
| survival.unit_lifetime | survival_forest | survival | candidate | validation | brier_3y | 0.0003 | 4000 | ok | seconds=44.6 |
| survival.unit_lifetime | survival_forest | survival | candidate | validation | calibration_gap_3y | 0.0003 | 4000 | ok | seconds=44.6 |
| survival.unit_lifetime | survival_forest | survival | candidate | validation | brier_5y | 0.0005 | 4000 | ok | seconds=44.6 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| survival.unit_lifetime | data_ready | True | 0 |  | 2026-10-23 18:27:13.972651 |
| survival.unit_lifetime | finished | True | 0 | baseline_ok=True | 2026-10-23 18:29:39.835923 |
| survival.unit_lifetime | started | True | 0 |  | 2026-10-23 18:27:04.638355 |

<!-- section: energy__energy__12_redispatch_matching -->
## Model Redispatch Matching

### candidate_results

65 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | accuracy | 0.9517 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | precision__exact_normalised |  | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | recall__exact_normalised |  | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | n_true__exact_normalised | 4.0 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | estimable__exact_normalised | 0.0 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | precision__unmatched | 0.9718 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | recall__unmatched | 0.9787 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | n_true__unmatched | 141.0 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | estimable__unmatched | 1.0 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | estimable_tiers | 1.0 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | balanced_accuracy | 0.4894 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | train_accuracy | 1.0 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | train_balanced_accuracy | 1.0 | 145 | ok | seconds=18.7 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | accuracy | 0.9517 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | precision__exact_normalised |  | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | recall__exact_normalised |  | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | n_true__exact_normalised | 4.0 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | estimable__exact_normalised | 0.0 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | precision__unmatched | 0.9718 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | recall__unmatched | 0.9787 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | n_true__unmatched | 141.0 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | estimable__unmatched | 1.0 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | estimable_tiers | 1.0 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | balanced_accuracy | 0.4894 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | train_accuracy | 1.0 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | train_balanced_accuracy | 1.0 | 145 | ok | seconds=26.5 |
| redispatch_matching.tier | char_ngram_logistic | matching | candidate | validation | accuracy | 0.9586 | 145 | ok | seconds=1.7 |
| redispatch_matching.tier | char_ngram_logistic | matching | candidate | validation | precision__exact_normalised |  | 145 | ok | seconds=1.7 |
| redispatch_matching.tier | char_ngram_logistic | matching | candidate | validation | recall__exact_normalised |  | 145 | ok | seconds=1.7 |
| redispatch_matching.tier | char_ngram_logistic | matching | candidate | validation | n_true__exact_normalised | 4.0 | 145 | ok | seconds=1.7 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| redispatch_matching.tier | started | True | 2 |  | 2026-10-23 17:47:24.059763 |
| redispatch_matching.tier | data_ready | True | 2 |  | 2026-10-23 17:47:34.945930 |
| redispatch_matching.tier | finished | True | 2 | baseline_ok=True | 2026-10-23 17:50:14.721697 |

<!-- section: energy__energy__13_weak_supervision -->
## Model Weak Supervision

### candidate_results

18 row(s)

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | coverage__series | 1.0 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | conflict_rate__series | 0.0 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | agreement_rate__series | 1.0 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | resolved_rate__series | 1.0 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | coverage__unit | 1.0 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | resolved_rate__unit | 1.0 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | coverage | 1.0 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | train_coverage | 1.0 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | accuracy__series__declared_gap_present | 0.99 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | accuracy__series__reconciliation_status | 0.7 | 3809 | ok | seconds=1.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | coverage__series | 1.0 | 3809 | ok | seconds=72.7 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | conflict_rate__series | 0.0 | 3809 | ok | seconds=72.7 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | agreement_rate__series | 1.0 | 3809 | ok | seconds=72.7 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | resolved_rate__series | 1.0 | 3809 | ok | seconds=72.7 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | coverage__unit | 1.0 | 3809 | ok | seconds=72.7 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | resolved_rate__unit | 1.0 | 3809 | ok | seconds=72.7 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | coverage | 1.0 | 3809 | ok | seconds=72.7 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | train_coverage | 1.0 | 3809 | ok | seconds=72.7 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| weak_supervision.labels | started | True | 2 |  | 2026-10-23 17:50:06.177260 |
| weak_supervision.labels | data_ready | True | 2 |  | 2026-10-23 17:50:16.980866 |
| weak_supervision.labels | finished | True | 2 | baseline_ok=True | 2026-10-23 17:51:37.965310 |

<!-- section: energy__energy__14_weather_imputation -->
## Model Weather Imputation

### candidate_results

159 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__air_temperature | 3.3215 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__air_temperature | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__air_temperature |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__dew_point_temperature | 2.2225 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__dew_point_temperature | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__dew_point_temperature |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__relative_humidity | 13.5171 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__relative_humidity | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__relative_humidity |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__pressure_station | 3.5532 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__pressure_station | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__pressure_station |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__pressure_sea_level | 4.3894 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__pressure_sea_level | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__pressure_sea_level |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__wind_speed | 1.6622 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__wind_speed | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__wind_speed |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__wind_direction | 54.4733 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__wind_direction | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__wind_direction |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__cloud_cover | 26.9392 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__cloud_cover | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__cloud_cover |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__visibility | 16243.1209 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__visibility | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__visibility |  | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae__weather__soil_temperature | 3.3058 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__weather__soil_temperature | 0.0 | 10 | ok | seconds=58.8 |
| weather_imputation.reconstruction | causal_baseline | baseline | baseline | validation | mae_heldout__weather__soil_temperature |  | 10 | ok | seconds=58.8 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| weather_imputation.reconstruction | data_ready | True | 2 |  | 2026-10-23 17:51:17.357143 |
| weather_imputation.reconstruction | finished | True | 2 | baseline_ok=True | 2026-10-23 18:17:48.276080 |
| weather_imputation.reconstruction | started | True | 2 |  | 2026-10-23 17:51:04.130183 |

<!-- section: energy__energy__15_self_supervised_other -->
## Model Self Supervised Series

### candidate_results

29 row(s)

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | mae__daily_load__series_value | 99276.6453 | 2 | ok | seconds=187.1 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | skill_mae__daily_load__series_value | 0.0531 | 2 | ok | seconds=187.1 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | mae__honda_hourly__series_value | 52.6894 | 2 | ok | seconds=187.1 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | skill_mae__honda_hourly__series_value | 0.0003 | 2 | ok | seconds=187.1 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | skill_mae_mean | 0.0267 | 2 | ok | seconds=187.1 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | train_skill_mae_mean | 0.598 | 2 | ok | seconds=187.1 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__daily_load__series_value | 96520.4998 | 2 | ok | seconds=78.7 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__daily_load__series_value | 0.0794 | 2 | ok | seconds=78.7 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__honda_hourly__series_value | 49.8741 | 2 | ok | seconds=78.7 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__honda_hourly__series_value | 0.0537 | 2 | ok | seconds=78.7 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae_mean | 0.0665 | 2 | ok | seconds=78.7 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | train_skill_mae_mean | 0.594 | 2 | ok | seconds=78.7 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | mae__daily_load__series_value | 104845.1037 | 2 | ok | seconds=59.4 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__daily_load__series_value | 0.0 | 2 | ok | seconds=59.4 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | mae__honda_hourly__series_value | 52.7035 | 2 | ok | seconds=59.4 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__honda_hourly__series_value | 0.0 | 2 | ok | seconds=59.4 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae_mean | 0.0 | 2 | ok | seconds=59.4 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | mae__daily_load__series_value | 334403.5762 | 2 | ok | seconds=200.5 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | skill_mae__daily_load__series_value | -2.1895 | 2 | ok | seconds=200.5 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | mae__honda_hourly__series_value | 102.2978 | 2 | ok | seconds=200.5 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | skill_mae__honda_hourly__series_value | -0.941 | 2 | ok | seconds=200.5 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | skill_mae_mean | -1.5653 | 2 | ok | seconds=200.5 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | train_skill_mae_mean | -3.2118 | 2 | ok | seconds=200.5 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | mae__daily_load__series_value | 96829.8447 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | skill_mae__daily_load__series_value | 0.0764 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | mae__honda_hourly__series_value | 57.1232 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | skill_mae__honda_hourly__series_value | -0.0839 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | skill_mae_mean | -0.0037 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | train_skill_mae_mean | -0.0217 | 2 | ok | seconds=1.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| self_supervised_other.reconstruction | data_ready | True | 2 |  | 2026-10-23 17:52:40.927279 |
| self_supervised_other.reconstruction | finished | True | 2 | baseline_ok=True | 2026-10-23 18:06:28.024074 |
| self_supervised_other.reconstruction | started | True | 2 |  | 2026-10-23 17:52:26.118029 |

<!-- section: energy__energy__16_rl_pumped_storage -->
## Model Pumped Storage Policy

### candidate_results

66 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | action_mae | 1074.4033 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_policy | 100508.7368 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_timing | 41845.0715 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_logged | 13149.0532 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | sign_agreement | 0.5507 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_gain | 87359.6836 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_timing_gain | 28696.0184 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | episode_net_abs_policy | 20546.6859 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | episode_net_abs_logged | 3467.0023 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | train_action_mae | 1050.0115 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | train_sign_agreement | 0.5885 | 4000 | ok | seconds=175.4 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | action_mae | 291.5813 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_policy | 20071.3518 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_timing | 14883.2657 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_logged | 13149.0532 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | sign_agreement | 0.8045 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_gain | 6922.2986 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_timing_gain | 1734.2126 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | episode_net_abs_policy | 4275.1888 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | episode_net_abs_logged | 3467.0023 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | train_action_mae | 180.2607 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | train_sign_agreement | 0.8988 | 4000 | ok | seconds=38.7 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | action_mae | 291.9433 | 4000 | ok | seconds=109.3 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_policy | 19635.2741 | 4000 | ok | seconds=109.3 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_timing | 14790.358 | 4000 | ok | seconds=109.3 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_logged | 13149.0532 | 4000 | ok | seconds=109.3 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | sign_agreement | 0.803 | 4000 | ok | seconds=109.3 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_gain | 6486.221 | 4000 | ok | seconds=109.3 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_timing_gain | 1641.3049 | 4000 | ok | seconds=109.3 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | episode_net_abs_policy | 4270.8028 | 4000 | ok | seconds=109.3 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| rl_pumped_storage.policy | data_ready | True | 2 |  | 2026-10-23 17:52:36.071932 |
| rl_pumped_storage.policy | finished | True | 2 | baseline_ok=True | 2026-10-23 18:06:16.876652 |
| rl_pumped_storage.policy | started | True | 2 |  | 2026-10-23 17:52:26.103072 |

<!-- section: energy__energy__17_rl_redispatch -->
## Model Redispatch Policy

### candidate_results

30 row(s)

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| rl_redispatch.policy | majority_action | baseline | baseline | validation | action_agreement | 0.419 | 4000 | ok | seconds=33.9 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | reward_when_agree | -1870.8523 | 4000 | ok | seconds=33.9 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | reward_when_disagree | -1769.8792 | 4000 | ok | seconds=33.9 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | reward_weighted_agreement | 0.4326 | 4000 | ok | seconds=33.9 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | train_action_agreement | 0.6242 | 4000 | ok | seconds=33.9 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | action_agreement | 0.6325 | 4000 | ok | seconds=1.6 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | reward_when_agree | -1987.1377 | 4000 | ok | seconds=1.6 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | reward_when_disagree | -1511.0811 | 4000 | ok | seconds=1.6 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | reward_weighted_agreement | 0.6936 | 4000 | ok | seconds=1.6 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | train_action_agreement | 0.7157 | 4000 | ok | seconds=1.6 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | action_agreement | 0.601 | 4000 | ok | seconds=42.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_when_agree | -2048.9582 | 4000 | ok | seconds=42.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_when_disagree | -1455.5465 | 4000 | ok | seconds=42.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_weighted_agreement | 0.6795 | 4000 | ok | seconds=42.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | train_action_agreement | 0.894 | 4000 | ok | seconds=42.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | action_agreement | 0.6055 | 4000 | ok | seconds=101.5 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_when_agree | -2041.3561 | 4000 | ok | seconds=101.5 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_when_disagree | -1460.4457 | 4000 | ok | seconds=101.5 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_weighted_agreement | 0.6821 | 4000 | ok | seconds=101.5 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | train_action_agreement | 0.8942 | 4000 | ok | seconds=101.5 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | action_agreement | 0.5938 | 4000 | ok | seconds=40.9 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_when_agree | -2044.2179 | 4000 | ok | seconds=40.9 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_when_disagree | -1473.0648 | 4000 | ok | seconds=40.9 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_weighted_agreement | 0.6698 | 4000 | ok | seconds=40.9 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | train_action_agreement | 0.888 | 4000 | ok | seconds=40.9 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | action_agreement | 0.6005 | 4000 | ok | seconds=102.7 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_when_agree | -2050.1038 | 4000 | ok | seconds=102.7 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_when_disagree | -1454.5673 | 4000 | ok | seconds=102.7 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_weighted_agreement | 0.6793 | 4000 | ok | seconds=102.7 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | train_action_agreement | 0.8888 | 4000 | ok | seconds=102.7 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| rl_redispatch.policy | data_ready | True | 2 |  | 2026-10-23 18:07:01.636952 |
| rl_redispatch.policy | finished | True | 2 | baseline_ok=True | 2026-10-23 18:12:48.535757 |
| rl_redispatch.policy | started | True | 2 |  | 2026-10-23 18:06:50.993270 |
