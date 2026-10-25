# ENERGY NOTEBOOK FINDINGS

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-10-25T17:42Z_

<!-- section: energy__energy__01_price_daily -->
## Model Price Daily

### candidate_results

70 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| price_daily.price | seasonal_naive | baseline | baseline | validation | mae | 28.0429 | 364 | ok | seconds=31.4 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | rmse | 42.9023 | 364 | ok | seconds=31.4 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | mean_error | -1.5877 | 364 | ok | seconds=31.4 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | mean_error_relative | -0.0202 | 364 | ok | seconds=31.4 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | pred_std_ratio | 1.0185 | 364 | ok | seconds=31.4 |
| price_daily.price | seasonal_naive | baseline | baseline | validation | pinball_q50 | 14.0215 | 364 | ok | seconds=31.4 |
| price_daily.price | empirical_quantiles | baseline | baseline | validation | pinball_q10 | 5.8028 | 364 | ok | seconds=1.0 |
| price_daily.price | empirical_quantiles | baseline | baseline | validation | pinball_q50 | 14.5833 | 364 | ok | seconds=1.0 |
| price_daily.price | empirical_quantiles | baseline | baseline | validation | pinball_q90 | 16.8015 | 364 | ok | seconds=1.0 |
| price_daily.price | empirical_quantiles | baseline | baseline | validation | interval_coverage_80 | 0.9313 | 364 | ok | seconds=1.0 |
| price_daily.price | ridge | tabular | candidate | validation | mae | 15.6577 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | rmse | 22.5344 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | mean_error | 5.9908 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | mean_error_relative | 0.0763 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | pred_std_ratio | 0.9139 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | skill_mae | 0.4417 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | skill_rmse | 0.4748 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | pinball_q50 | 7.8288 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | train_mae | 19.9278 | 364 | ok | seconds=2.0 |
| price_daily.price | ridge | tabular | candidate | validation | train_rmse | 31.1918 | 364 | ok | seconds=2.0 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | mae | 12.579 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | rmse | 21.0109 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | mean_error | -5.8403 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0743 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9344 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.5514 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.5103 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | pinball_q50 | 6.2895 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | train_mae | 2.2952 | 364 | ok | seconds=299.7 |
| price_daily.price | gbt_lightgbm | tabular | candidate | validation | train_rmse | 4.2779 | 364 | ok | seconds=299.7 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| price_daily.price | started | False | 0 |  | 2026-10-23 18:31:32.837596 |
| price_daily.price | data_ready | False | 0 |  | 2026-10-23 18:32:03.792603 |
| price_daily.price | finished | False | 0 | baseline_ok=True | 2026-10-23 18:57:41.536234 |

<!-- section: energy__energy__02_price_quarter_hour -->
## Model Price Quarter Hour

### candidate_results

104 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| price_quarter_hour.price | persistence | baseline | baseline | validation | mae | 33.1459 | 17476 | ok | seconds=31.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | rmse | 51.6481 | 17476 | ok | seconds=31.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | mean_error | -0.1679 | 17476 | ok | seconds=31.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | mean_error_relative | -0.0019 | 17476 | ok | seconds=31.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | pred_std_ratio | 1.0087 | 17476 | ok | seconds=31.2 |
| price_quarter_hour.price | persistence | baseline | baseline | validation | pinball_q50 | 16.5729 | 17476 | ok | seconds=31.2 |
| price_quarter_hour.price | empirical_quantiles | baseline | baseline | validation | pinball_q10 | 8.1447 | 17476 | ok | seconds=1.7 |
| price_quarter_hour.price | empirical_quantiles | baseline | baseline | validation | pinball_q50 | 15.8123 | 17476 | ok | seconds=1.7 |
| price_quarter_hour.price | empirical_quantiles | baseline | baseline | validation | pinball_q90 | 9.4806 | 17476 | ok | seconds=1.7 |
| price_quarter_hour.price | empirical_quantiles | baseline | baseline | validation | interval_coverage_80 | 0.8276 | 17476 | ok | seconds=1.7 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | mae | 22.0039 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | rmse | 31.5311 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | mean_error | -0.6512 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | mean_error_relative | -0.0074 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | pred_std_ratio | 0.9243 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | skill_mae | 0.3361 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | skill_rmse | 0.3895 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | pinball_q50 | 11.002 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | train_mae | 22.3594 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | ridge | tabular | candidate | validation | train_rmse | 36.9604 | 17476 | ok | seconds=2.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | mae | 18.2978 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | rmse | 27.7973 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | mean_error | -4.3582 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0493 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9631 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.448 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.4618 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | pinball_q50 | 9.1489 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | train_mae | 7.4614 | 17476 | ok | seconds=265.0 |
| price_quarter_hour.price | gbt_lightgbm | tabular | candidate | validation | train_rmse | 10.1037 | 17476 | ok | seconds=265.0 |

### task_run_context

6 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| price_quarter_hour.price | started | False | 0 |  | 2026-10-23 18:31:32.832727 |
| price_quarter_hour.negative_price | started | False | 0 |  | 2026-10-23 18:31:42.176990 |
| price_quarter_hour.price | data_ready | False | 0 |  | 2026-10-23 18:32:08.793658 |
| price_quarter_hour.negative_price | data_ready | False | 0 |  | 2026-10-23 18:57:02.660012 |
| price_quarter_hour.negative_price | finished | False | 0 | baseline_ok=True | 2026-10-23 19:05:27.994203 |
| price_quarter_hour.price | finished | False | 0 | baseline_ok=True | 2026-10-23 18:56:51.172906 |

<!-- section: energy__energy__03_load -->
## Model Load

### candidate_results

32 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| load.load | gbt_lightgbm | tabular | candidate | validation | mae | 25128.0502 | 2184 | ok | seconds=345.5 |
| load.load | gbt_lightgbm | tabular | candidate | validation | rmse | 42362.1348 | 2184 | ok | seconds=345.5 |
| load.load | gbt_lightgbm | tabular | candidate | validation | mean_error | -1145.2497 | 2184 | ok | seconds=345.5 |
| load.load | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0021 | 2184 | ok | seconds=345.5 |
| load.load | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9894 | 2184 | ok | seconds=345.5 |
| load.load | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.5592 | 2184 | ok | seconds=345.5 |
| load.load | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.6155 | 2184 | ok | seconds=345.5 |
| load.load | gbt_lightgbm | tabular | candidate | validation | train_mae | 16390.5642 | 2184 | ok | seconds=345.5 |
| load.load | gbt_lightgbm | tabular | candidate | validation | train_rmse | 24133.3261 | 2184 | ok | seconds=345.5 |
| load.load | gbt_sklearn | tabular | candidate | validation | mae | 24860.3053 | 2184 | ok | seconds=798.1 |
| load.load | gbt_sklearn | tabular | candidate | validation | rmse | 43168.5635 | 2184 | ok | seconds=798.1 |
| load.load | gbt_sklearn | tabular | candidate | validation | mean_error | -88.1932 | 2184 | ok | seconds=798.1 |
| load.load | gbt_sklearn | tabular | candidate | validation | mean_error_relative | -0.0002 | 2184 | ok | seconds=798.1 |
| load.load | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.9923 | 2184 | ok | seconds=798.1 |
| load.load | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.5639 | 2184 | ok | seconds=798.1 |
| load.load | gbt_sklearn | tabular | candidate | validation | skill_rmse | 0.6082 | 2184 | ok | seconds=798.1 |
| load.load | gbt_sklearn | tabular | candidate | validation | train_mae | 13290.6646 | 2184 | ok | seconds=798.1 |
| load.load | gbt_sklearn | tabular | candidate | validation | train_rmse | 19397.9908 | 2184 | ok | seconds=798.1 |
| load.load | ridge | tabular | candidate | validation | mae | 46012.0074 | 2184 | ok | seconds=6.0 |
| load.load | ridge | tabular | candidate | validation | rmse | 73768.4553 | 2184 | ok | seconds=6.0 |
| load.load | ridge | tabular | candidate | validation | mean_error | -6051.0696 | 2184 | ok | seconds=6.0 |
| load.load | ridge | tabular | candidate | validation | mean_error_relative | -0.011 | 2184 | ok | seconds=6.0 |
| load.load | ridge | tabular | candidate | validation | pred_std_ratio | 0.9429 | 2184 | ok | seconds=6.0 |
| load.load | ridge | tabular | candidate | validation | skill_mae | 0.1928 | 2184 | ok | seconds=6.0 |
| load.load | ridge | tabular | candidate | validation | skill_rmse | 0.3304 | 2184 | ok | seconds=6.0 |
| load.load | ridge | tabular | candidate | validation | train_mae | 44386.677 | 2184 | ok | seconds=6.0 |
| load.load | ridge | tabular | candidate | validation | train_rmse | 70823.4553 | 2184 | ok | seconds=6.0 |
| load.load | seasonal_mean | baseline | baseline | validation | mae | 57004.7223 | 2184 | ok | seconds=30.7 |
| load.load | seasonal_mean | baseline | baseline | validation | rmse | 110171.2595 | 2184 | ok | seconds=30.7 |
| load.load | seasonal_mean | baseline | baseline | validation | mean_error | 41525.3611 | 2184 | ok | seconds=30.7 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| load.load | started | False | 0 |  | 2026-10-23 18:31:34.336851 |
| load.load | data_ready | False | 0 |  | 2026-10-23 18:32:13.354139 |
| load.load | finished | False | 0 | baseline_ok=True | 2026-10-23 18:52:25.739330 |

<!-- section: energy__energy__04_bias -->
## Model Forecast Bias

### candidate_results

73 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae | 15334.1454 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | rmse | 22876.0163 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mean_error | -725.6553 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0077 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9986 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.8363 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.8835 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | skill_mae_vs_scope_mean | 0.7306 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__offshore_wind | 9083.9362 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__onshore_wind | 17791.6715 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__other | 9484.7847 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__photovoltaic | 12121.9179 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__total | 41134.678 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | mae__forecast_scope__wind_and_photovoltaic | 25650.7896 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | train_mae | 9676.7033 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_lightgbm | tabular | candidate | validation | train_rmse | 13423.4218 | 1819 | ok | seconds=327.5 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae | 15426.0679 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | rmse | 23049.328 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mean_error | -803.8658 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mean_error_relative | -0.0086 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.998 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.8353 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | skill_rmse | 0.8826 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | skill_mae_vs_scope_mean | 0.729 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__offshore_wind | 8764.9198 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__onshore_wind | 18221.0418 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__other | 9975.9846 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__photovoltaic | 12019.3999 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__total | 37633.9376 | 1819 | ok | seconds=802.4 |
| bias.bias | gbt_sklearn | tabular | candidate | validation | mae__forecast_scope__wind_and_photovoltaic | 25930.5025 | 1819 | ok | seconds=802.4 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| bias.bias | started | False | 0 |  | 2026-10-23 18:31:32.833934 |
| bias.bias | data_ready | False | 0 |  | 2026-10-23 18:31:58.803461 |
| bias.bias | finished | False | 0 | baseline_ok=True | 2026-10-23 18:51:56.210148 |

<!-- section: energy__energy__05_zone_generation -->
## Model Zone Generation

### candidate_results

32 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | mae | 12541.1318 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | rmse | 17979.34 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | mean_error | 1011.1667 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | mean_error_relative | 0.0184 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.9519 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.5123 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | skill_rmse | 0.5663 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | train_mae | 7291.3985 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_sklearn | tabular | candidate | validation | train_rmse | 11198.8487 | 3640 | ok | seconds=749.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | mae | 10930.1759 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | rmse | 15874.3664 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | mean_error | -959.2833 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0174 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9577 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.5749 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.6171 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | train_mae | 6160.6622 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | gbt_lightgbm | tabular | candidate | validation | train_rmse | 9307.2215 | 3640 | ok | seconds=337.7 |
| zone_generation.generation | ridge | tabular | candidate | validation | mae | 30564.8148 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | rmse | 42428.0971 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | mean_error | -1567.1179 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | mean_error_relative | -0.0285 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | pred_std_ratio | 0.7339 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | skill_mae | -0.1887 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | skill_rmse | -0.0234 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | train_mae | 27280.6977 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | ridge | tabular | candidate | validation | train_rmse | 37705.3963 | 3640 | ok | seconds=1.3 |
| zone_generation.generation | capacity_factor | baseline | baseline | validation | mae | 25712.8807 | 3640 | ok | seconds=39.7 |
| zone_generation.generation | capacity_factor | baseline | baseline | validation | rmse | 41456.8247 | 3640 | ok | seconds=39.7 |
| zone_generation.generation | capacity_factor | baseline | baseline | validation | mean_error | -1616.1512 | 3640 | ok | seconds=39.7 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| zone_generation.generation | started | False | 0 |  | 2026-10-23 18:31:32.828823 |
| zone_generation.generation | data_ready | False | 0 |  | 2026-10-23 18:31:54.916055 |
| zone_generation.generation | finished | False | 0 | baseline_ok=True | 2026-10-23 18:51:24.282179 |

<!-- section: energy__energy__06_honda_forecast -->
## Model Honda Forecast

### candidate_results

32 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| honda_forecast.increment | persistence | baseline | baseline | validation | mae | 20.3038 | 131040 | ok | seconds=62.2 |
| honda_forecast.increment | persistence | baseline | baseline | validation | rmse | 46.0149 | 131040 | ok | seconds=62.2 |
| honda_forecast.increment | persistence | baseline | baseline | validation | mean_error | 0.0363 | 131040 | ok | seconds=62.2 |
| honda_forecast.increment | persistence | baseline | baseline | validation | mean_error_relative | 0.0009 | 131040 | ok | seconds=62.2 |
| honda_forecast.increment | persistence | baseline | baseline | validation | pred_std_ratio | 0.9996 | 131040 | ok | seconds=62.2 |
| honda_forecast.increment | ridge | tabular | candidate | validation | mae | 19.3798 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | ridge | tabular | candidate | validation | rmse | 37.9991 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | ridge | tabular | candidate | validation | mean_error | 0.6467 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | ridge | tabular | candidate | validation | mean_error_relative | 0.0165 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | ridge | tabular | candidate | validation | pred_std_ratio | 0.8825 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | ridge | tabular | candidate | validation | skill_mae | 0.0455 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | ridge | tabular | candidate | validation | skill_rmse | 0.1742 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | ridge | tabular | candidate | validation | train_mae | 19.0181 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | ridge | tabular | candidate | validation | train_rmse | 35.6326 | 131040 | ok | seconds=3.1 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | mae | 17.9655 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | rmse | 35.2969 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | mean_error | 1.544 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | 0.0394 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9021 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.1152 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.2329 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | train_mae | 16.3918 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_lightgbm | tabular | candidate | validation | train_rmse | 30.6822 | 131040 | ok | seconds=251.4 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | mae | 18.0254 | 131040 | ok | seconds=615.1 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | rmse | 35.3867 | 131040 | ok | seconds=615.1 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | mean_error | 1.5708 | 131040 | ok | seconds=615.1 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | mean_error_relative | 0.0401 | 131040 | ok | seconds=615.1 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.9024 | 131040 | ok | seconds=615.1 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.1122 | 131040 | ok | seconds=615.1 |
| honda_forecast.increment | gbt_sklearn | tabular | candidate | validation | skill_rmse | 0.231 | 131040 | ok | seconds=615.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| honda_forecast.increment | data_ready | False | 0 |  | 2026-10-23 18:53:12.069491 |
| honda_forecast.increment | finished | False | 0 | baseline_ok=True | 2026-10-23 19:09:04.791561 |
| honda_forecast.increment | started | False | 0 |  | 2026-10-23 18:52:57.364794 |

<!-- section: energy__energy__07_honda_anomaly -->
## Model Honda Anomaly

### candidate_results

30 row(s)

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | precision | 0.389 | 26208 | ok | seconds=54.4 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | recall | 0.6555 | 26208 | ok | seconds=54.4 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | flag_rate | 0.0213 | 26208 | ok | seconds=54.4 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | pr_auc | 0.442 | 26208 | ok | seconds=54.4 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | threshold | 2.5459 | 26208 | ok | seconds=54.4 |
| honda_anomaly.anomaly | seasonal_zscore | baseline | baseline | validation | train_flag_rate | 0.01 | 26208 | ok | seconds=54.4 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | precision | 0.3192 | 26208 | ok | seconds=61.1 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | recall | 0.6853 | 26208 | ok | seconds=61.1 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | flag_rate | 0.0302 | 26208 | ok | seconds=61.1 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | pr_auc | 0.6483 | 26208 | ok | seconds=61.1 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | threshold | 4.3431 | 26208 | ok | seconds=61.1 |
| honda_anomaly.anomaly | forecast_residual_lightgbm | anomaly | candidate | validation | train_flag_rate | 0.01 | 26208 | ok | seconds=61.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | precision | 0.3112 | 26208 | ok | seconds=129.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | recall | 0.6909 | 26208 | ok | seconds=129.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | flag_rate | 0.0316 | 26208 | ok | seconds=129.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | pr_auc | 0.6517 | 26208 | ok | seconds=129.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | threshold | 4.3232 | 26208 | ok | seconds=129.1 |
| honda_anomaly.anomaly | forecast_residual_sklearn | anomaly | candidate | validation | train_flag_rate | 0.01 | 26208 | ok | seconds=129.1 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | precision | 0.0029 | 26208 | ok | seconds=3.6 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | recall | 0.0019 | 26208 | ok | seconds=3.6 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | flag_rate | 0.0132 | 26208 | ok | seconds=3.6 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | pr_auc | 0.029 | 26208 | ok | seconds=3.6 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | threshold | 0.5806 | 26208 | ok | seconds=3.6 |
| honda_anomaly.anomaly | isolation_forest | anomaly | candidate | validation | train_flag_rate | 0.01 | 26208 | ok | seconds=3.6 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | precision | 0.3409 | 26208 | ok | seconds=403.3 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | recall | 0.2533 | 26208 | ok | seconds=403.3 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | flag_rate | 0.0102 | 26208 | ok | seconds=403.3 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | pr_auc | 0.276 | 26208 | ok | seconds=403.3 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | threshold | 0.3632 | 26208 | ok | seconds=403.3 |
| honda_anomaly.anomaly | autoencoder | anomaly | candidate | validation | train_flag_rate | 0.01 | 26208 | ok | seconds=403.3 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| honda_anomaly.anomaly | data_ready | False | 0 |  | 2026-10-23 18:53:32.355673 |
| honda_anomaly.anomaly | finished | False | 0 | baseline_ok=True | 2026-10-23 19:15:25.716149 |
| honda_anomaly.anomaly | started | False | 0 |  | 2026-10-23 18:53:21.126088 |

<!-- section: energy__energy__08_ccpp -->
## Model Ccpp

### candidate_results

41 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | mae | 2.5034 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | rmse | 3.4474 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | mean_error | -0.0459 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | mean_error_relative | -0.0001 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 0.9721 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | skill_mae | 0.8308 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | skill_rmse | 0.7974 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | train_mae | 1.9068 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_lightgbm | tabular | candidate | validation | train_rmse | 2.6341 | 1369 | ok | seconds=212.3 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | mae | 2.4962 | 1369 | ok | seconds=539.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | rmse | 3.463 | 1369 | ok | seconds=539.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | mean_error | -0.0737 | 1369 | ok | seconds=539.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | mean_error_relative | -0.0002 | 1369 | ok | seconds=539.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 0.9714 | 1369 | ok | seconds=539.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | skill_mae | 0.8313 | 1369 | ok | seconds=539.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | skill_rmse | 0.7965 | 1369 | ok | seconds=539.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | train_mae | 1.9099 | 1369 | ok | seconds=539.1 |
| ccpp.output | gbt_sklearn | tabular | candidate | validation | train_rmse | 2.6383 | 1369 | ok | seconds=539.1 |
| ccpp.output | mlp | tabular | candidate | validation | mae | 3.1729 | 1369 | ok | seconds=16.5 |
| ccpp.output | mlp | tabular | candidate | validation | rmse | 4.1343 | 1369 | ok | seconds=16.5 |
| ccpp.output | mlp | tabular | candidate | validation | mean_error | 0.0678 | 1369 | ok | seconds=16.5 |
| ccpp.output | mlp | tabular | candidate | validation | mean_error_relative | 0.0001 | 1369 | ok | seconds=16.5 |
| ccpp.output | mlp | tabular | candidate | validation | pred_std_ratio | 0.9574 | 1369 | ok | seconds=16.5 |
| ccpp.output | mlp | tabular | candidate | validation | skill_mae | 0.7855 | 1369 | ok | seconds=16.5 |
| ccpp.output | mlp | tabular | candidate | validation | skill_rmse | 0.757 | 1369 | ok | seconds=16.5 |
| ccpp.output | mlp | tabular | candidate | validation | train_mae | 2.8995 | 1369 | ok | seconds=16.5 |
| ccpp.output | mlp | tabular | candidate | validation | train_rmse | 3.8339 | 1369 | ok | seconds=16.5 |
| ccpp.output | ridge | tabular | candidate | validation | mae | 3.7516 | 1369 | ok | seconds=1.0 |
| ccpp.output | ridge | tabular | candidate | validation | rmse | 4.6925 | 1369 | ok | seconds=1.0 |
| ccpp.output | ridge | tabular | candidate | validation | mean_error | 0.0136 | 1369 | ok | seconds=1.0 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| ccpp.output | data_ready | False | 0 |  | 2026-10-23 18:58:33.162324 |
| ccpp.output | finished | False | 0 | baseline_ok=True | 2026-10-23 19:12:47.017782 |
| ccpp.output | started | False | 0 |  | 2026-10-23 18:58:25.113441 |

<!-- section: energy__energy__09_capacity_additions -->
## Model Capacity Additions

### candidate_results

32 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| capacity_additions.additions | seasonal_mean | baseline | baseline | validation | mae | 20.4674 | 189 | ok | seconds=124.8 |
| capacity_additions.additions | seasonal_mean | baseline | baseline | validation | rmse | 55.4368 | 189 | ok | seconds=124.8 |
| capacity_additions.additions | seasonal_mean | baseline | baseline | validation | mean_error | -1.2842 | 189 | ok | seconds=124.8 |
| capacity_additions.additions | seasonal_mean | baseline | baseline | validation | mean_error_relative | -0.0485 | 189 | ok | seconds=124.8 |
| capacity_additions.additions | seasonal_mean | baseline | baseline | validation | pred_std_ratio | 0.6675 | 189 | ok | seconds=124.8 |
| capacity_additions.additions | ridge | tabular | candidate | validation | mae | 47.6619 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | rmse | 83.2732 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | mean_error | 5.5242 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | mean_error_relative | 0.2086 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | pred_std_ratio | 0.2392 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | skill_mae | -1.3287 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | skill_rmse | -0.5021 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | train_mae | 37.7486 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | ridge | tabular | candidate | validation | train_rmse | 79.1263 | 189 | ok | seconds=1.2 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | mae | 22.001 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | rmse | 71.9581 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | mean_error | 10.4542 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | mean_error_relative | 0.3948 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | pred_std_ratio | 1.1162 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | skill_mae | -0.0749 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | skill_rmse | -0.298 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | train_mae | 7.5278 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_lightgbm | tabular | candidate | validation | train_rmse | 21.1253 | 189 | ok | seconds=154.0 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | mae | 19.8307 | 189 | ok | seconds=332.1 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | rmse | 62.9842 | 189 | ok | seconds=332.1 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | mean_error | 7.9473 | 189 | ok | seconds=332.1 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | mean_error_relative | 0.3001 | 189 | ok | seconds=332.1 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | pred_std_ratio | 1.085 | 189 | ok | seconds=332.1 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | skill_mae | 0.0311 | 189 | ok | seconds=332.1 |
| capacity_additions.additions | poisson_gbt_sklearn | tabular | candidate | validation | skill_rmse | -0.1361 | 189 | ok | seconds=332.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| capacity_additions.additions | data_ready | False | 0 |  | 2026-10-23 19:03:31.715019 |
| capacity_additions.additions | finished | False | 0 | baseline_ok=True | 2026-10-23 19:14:12.105041 |
| capacity_additions.additions | started | False | 0 |  | 2026-10-23 19:03:19.594176 |

<!-- section: energy__energy__10_redispatch -->
## Model Redispatch

### candidate_results

66 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| redispatch.any_event | base_rate | baseline | baseline | validation | pr_auc | 0.6749 | 1452 | ok | seconds=118.7 |
| redispatch.any_event | base_rate | baseline | baseline | validation | log_loss | 0.633 | 1452 | ok | seconds=118.7 |
| redispatch.any_event | base_rate | baseline | baseline | validation | calibration_error | 0.033 | 1452 | ok | seconds=118.7 |
| redispatch.any_event | base_rate | baseline | baseline | validation | base_rate | 0.6749 | 1452 | ok | seconds=118.7 |
| redispatch.any_event | base_rate | baseline | baseline | validation | pred_mean | 0.6419 | 1452 | ok | seconds=118.7 |
| redispatch.any_event | base_rate | baseline | baseline | validation | pred_positive_share | 1.0 | 1452 | ok | seconds=118.7 |
| redispatch.any_event | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 1452 | ok | seconds=118.7 |
| redispatch.any_event | logistic | tabular | candidate | validation | pr_auc | 0.8828 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | logistic | tabular | candidate | validation | log_loss | 0.5223 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | logistic | tabular | candidate | validation | calibration_error | 0.045 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | logistic | tabular | candidate | validation | base_rate | 0.6749 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | logistic | tabular | candidate | validation | pred_mean | 0.6763 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | logistic | tabular | candidate | validation | pred_positive_share | 0.7596 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | logistic | tabular | candidate | validation | pr_auc_lift | 1.308 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | logistic | tabular | candidate | validation | train_pr_auc | 0.8488 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | logistic | tabular | candidate | validation | train_log_loss | 0.53 | 1452 | ok | seconds=2.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | pr_auc | 0.8793 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.5215 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0345 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.6749 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.6728 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.7355 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 1.3028 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 0.9341 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.4304 | 1452 | ok | seconds=206.5 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | pr_auc | 0.8762 | 1452 | ok | seconds=182.0 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | log_loss | 0.524 | 1452 | ok | seconds=182.0 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0336 | 1452 | ok | seconds=182.0 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | base_rate | 0.6749 | 1452 | ok | seconds=182.0 |
| redispatch.any_event | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.6721 | 1452 | ok | seconds=182.0 |

### task_run_context

6 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| redispatch.any_event | started | False | 2 |  | 2026-10-23 19:06:43.065298 |
| redispatch.event_energy | started | False | 2 |  | 2026-10-23 19:06:48.394907 |
| redispatch.any_event | data_ready | False | 2 |  | 2026-10-23 19:06:57.996291 |
| redispatch.any_event | finished | False | 2 | baseline_ok=True | 2026-10-23 19:15:45.905904 |
| redispatch.event_energy | data_ready | False | 2 |  | 2026-10-23 19:15:54.110622 |
| redispatch.event_energy | finished | False | 2 | baseline_ok=True | 2026-10-23 19:17:18.436610 |

<!-- section: energy__energy__11_survival -->
## Model Unit Survival

### candidate_results

39 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| survival.unit_lifetime | kaplan_meier | baseline | baseline | validation | concordance | 0.504 | 5243 | ok | seconds=61.0 |
| survival.unit_lifetime | kaplan_meier | baseline | baseline | validation | brier_1y | 0.0008 | 5243 | ok | seconds=61.0 |
| survival.unit_lifetime | kaplan_meier | baseline | baseline | validation | calibration_gap_1y | 0.0004 | 5243 | ok | seconds=61.0 |
| survival.unit_lifetime | kaplan_meier | baseline | baseline | validation | brier_3y | 0.0026 | 5243 | ok | seconds=61.0 |
| survival.unit_lifetime | kaplan_meier | baseline | baseline | validation | calibration_gap_3y | 0.0011 | 5243 | ok | seconds=61.0 |
| survival.unit_lifetime | kaplan_meier | baseline | baseline | validation | brier_5y | 0.003 | 5243 | ok | seconds=61.0 |
| survival.unit_lifetime | kaplan_meier | baseline | baseline | validation | calibration_gap_5y | 0.0012 | 5243 | ok | seconds=61.0 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | concordance | 0.911 | 5243 | ok | seconds=11.2 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | brier_1y | 0.0007 | 5243 | ok | seconds=11.2 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | calibration_gap_1y | 0.0007 | 5243 | ok | seconds=11.2 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | brier_3y | 0.0025 | 5243 | ok | seconds=11.2 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | calibration_gap_3y | 0.0008 | 5243 | ok | seconds=11.2 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | brier_5y | 0.0029 | 5243 | ok | seconds=11.2 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | calibration_gap_5y | 0.0006 | 5243 | ok | seconds=11.2 |
| survival.unit_lifetime | cox_lifelines | survival | candidate | validation | train_concordance | 0.9036 | 5243 | ok | seconds=11.2 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | concordance | 0.7254 | 5243 | ok | seconds=3.3 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | brier_1y | 0.0008 | 5243 | ok | seconds=3.3 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | calibration_gap_1y | 0.0004 | 5243 | ok | seconds=3.3 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | brier_3y | 0.0026 | 5243 | ok | seconds=3.3 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | calibration_gap_3y | 0.0011 | 5243 | ok | seconds=3.3 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | brier_5y | 0.003 | 5243 | ok | seconds=3.3 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | calibration_gap_5y | 0.0012 | 5243 | ok | seconds=3.3 |
| survival.unit_lifetime | commissioning_year_cox | survival | diagnostic | validation | train_concordance | 0.7111 | 5243 | ok | seconds=3.3 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | concordance | 0.9606 | 5243 | ok | seconds=3.1 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | brier_1y | 0.0008 | 5243 | ok | seconds=3.1 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | calibration_gap_1y | 0.0007 | 5243 | ok | seconds=3.1 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | brier_3y | 0.0018 | 5243 | ok | seconds=3.1 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | calibration_gap_3y | 0.0007 | 5243 | ok | seconds=3.1 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | brier_5y | 0.0018 | 5243 | ok | seconds=3.1 |
| survival.unit_lifetime | boosted_cox_xgboost | survival | candidate | validation | calibration_gap_5y | 0.0004 | 5243 | ok | seconds=3.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| survival.unit_lifetime | started | False | 0 |  | 2026-10-23 19:11:03.215945 |
| survival.unit_lifetime | data_ready | False | 0 |  | 2026-10-23 19:11:24.316839 |
| survival.unit_lifetime | finished | False | 0 | baseline_ok=True | 2026-10-23 19:27:03.392286 |

<!-- section: energy__energy__12_redispatch_matching -->
## Model Redispatch Matching

### candidate_results

65 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | accuracy | 0.9517 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | precision__exact_normalised |  | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | recall__exact_normalised |  | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | n_true__exact_normalised | 4.0 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | estimable__exact_normalised | 0.0 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | precision__unmatched | 0.9718 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | recall__unmatched | 0.9787 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | n_true__unmatched | 141.0 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | estimable__unmatched | 1.0 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | estimable_tiers | 1.0 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | balanced_accuracy | 0.4894 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | train_accuracy | 1.0 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_lightgbm | matching | candidate | validation | train_balanced_accuracy | 1.0 | 145 | ok | seconds=14.0 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | accuracy | 0.9517 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | precision__exact_normalised |  | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | recall__exact_normalised |  | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | n_true__exact_normalised | 4.0 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | estimable__exact_normalised | 0.0 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | precision__unmatched | 0.9718 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | recall__unmatched | 0.9787 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | n_true__unmatched | 141.0 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | estimable__unmatched | 1.0 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | estimable_tiers | 1.0 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | balanced_accuracy | 0.4894 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | train_accuracy | 1.0 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | gbt_sklearn | matching | candidate | validation | train_balanced_accuracy | 1.0 | 145 | ok | seconds=15.2 |
| redispatch_matching.tier | char_ngram_logistic | matching | candidate | validation | accuracy | 0.9586 | 145 | ok | seconds=1.5 |
| redispatch_matching.tier | char_ngram_logistic | matching | candidate | validation | precision__exact_normalised |  | 145 | ok | seconds=1.5 |
| redispatch_matching.tier | char_ngram_logistic | matching | candidate | validation | recall__exact_normalised |  | 145 | ok | seconds=1.5 |
| redispatch_matching.tier | char_ngram_logistic | matching | candidate | validation | n_true__exact_normalised | 4.0 | 145 | ok | seconds=1.5 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| redispatch_matching.tier | data_ready | False | 2 |  | 2026-10-23 19:13:52.484523 |
| redispatch_matching.tier | finished | False | 2 | baseline_ok=True | 2026-10-23 19:15:42.042006 |
| redispatch_matching.tier | started | False | 2 |  | 2026-10-23 19:13:42.469516 |

<!-- section: energy__energy__13_weak_supervision -->
## Model Weak Supervision

### candidate_results

18 row(s)

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | coverage__series | 1.0 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | conflict_rate__series | 0.0 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | agreement_rate__series | 1.0 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | resolved_rate__series | 1.0 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | coverage__unit | 1.0 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | resolved_rate__unit | 1.0 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | coverage | 1.0 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | train_coverage | 1.0 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | accuracy__series__declared_gap_present | 0.99 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | label_model | weak_supervision | candidate | validation | accuracy__series__reconciliation_status | 0.99 | 25837 | ok | seconds=1.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | coverage__series | 1.0 | 25837 | ok | seconds=47.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | conflict_rate__series | 0.0 | 25837 | ok | seconds=47.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | agreement_rate__series | 1.0 | 25837 | ok | seconds=47.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | resolved_rate__series | 1.0 | 25837 | ok | seconds=47.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | coverage__unit | 1.0 | 25837 | ok | seconds=47.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | resolved_rate__unit | 1.0 | 25837 | ok | seconds=47.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | coverage | 1.0 | 25837 | ok | seconds=47.0 |
| weak_supervision.labels | majority_vote | baseline | baseline | validation | train_coverage | 1.0 | 25837 | ok | seconds=47.0 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| weak_supervision.labels | started | False | 2 |  | 2026-10-23 19:15:00.620704 |
| weak_supervision.labels | data_ready | False | 2 |  | 2026-10-23 19:15:10.760474 |
| weak_supervision.labels | finished | False | 2 | baseline_ok=True | 2026-10-23 19:16:07.438625 |

<!-- section: energy__energy__14_weather_imputation -->
## Model Weather Imputation

### candidate_results

159 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__air_temperature | 0.1965 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__air_temperature | 0.939 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__air_temperature | 0.1856 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__dew_point_temperature | 0.1387 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__dew_point_temperature | 0.9393 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__dew_point_temperature | 0.1088 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__relative_humidity | 1.0925 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__relative_humidity | 0.9155 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__relative_humidity | 1.0673 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__pressure_station | 3.7668 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__pressure_station | 0.1727 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__pressure_station | 3.4699 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__pressure_sea_level | 3.331 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__pressure_sea_level | 0.2819 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__pressure_sea_level | 4.047 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__wind_speed | 1.4336 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__wind_speed | 0.2236 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__wind_speed | 1.2826 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__wind_direction | 55.866 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__wind_direction | 0.0214 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__wind_direction | 60.252 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__cloud_cover | 25.0479 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__cloud_cover | 0.0989 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__cloud_cover | 26.4422 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__visibility | 11001.8053 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__visibility | 0.3934 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__visibility | 12142.984 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__weather__soil_temperature | 1.0233 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__weather__soil_temperature | 0.6919 | 10 | ok | seconds=292.9 |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae_heldout__weather__soil_temperature | 1.1198 | 10 | ok | seconds=292.9 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| weather_imputation.reconstruction | data_ready | False | 2 |  | 2026-10-23 19:16:49.880473 |
| weather_imputation.reconstruction | finished | False | 2 | baseline_ok=True | 2026-10-23 19:34:09.608939 |
| weather_imputation.reconstruction | started | False | 2 |  | 2026-10-23 19:16:27.369891 |

<!-- section: energy__energy__15_self_supervised_other -->
## Model Self Supervised Series

### candidate_results

29 row(s)

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | mae__daily_load__series_value | 52436.594 | 2 | ok | seconds=141.5 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | skill_mae__daily_load__series_value | 0.0869 | 2 | ok | seconds=141.5 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | mae__honda_hourly__series_value | 48.7697 | 2 | ok | seconds=141.5 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | skill_mae__honda_hourly__series_value | 0.0291 | 2 | ok | seconds=141.5 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | skill_mae_mean | 0.058 | 2 | ok | seconds=141.5 |
| self_supervised_other.reconstruction | gbt_cross_variable_sklearn | reconstruction | candidate | validation | train_skill_mae_mean | 0.3024 | 2 | ok | seconds=141.5 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__daily_load__series_value | 51950.3928 | 2 | ok | seconds=12.9 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__daily_load__series_value | 0.0953 | 2 | ok | seconds=12.9 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | mae__honda_hourly__series_value | 48.5061 | 2 | ok | seconds=12.9 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae__honda_hourly__series_value | 0.0343 | 2 | ok | seconds=12.9 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | skill_mae_mean | 0.0648 | 2 | ok | seconds=12.9 |
| self_supervised_other.reconstruction | gbt_cross_variable_lightgbm | reconstruction | candidate | validation | train_skill_mae_mean | 0.3309 | 2 | ok | seconds=12.9 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | mae__daily_load__series_value | 57425.8102 | 2 | ok | seconds=41.6 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__daily_load__series_value | 0.0 | 2 | ok | seconds=41.6 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | mae__honda_hourly__series_value | 50.2315 | 2 | ok | seconds=41.6 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae__honda_hourly__series_value | 0.0 | 2 | ok | seconds=41.6 |
| self_supervised_other.reconstruction | causal_baseline | baseline | baseline | validation | skill_mae_mean | 0.0 | 2 | ok | seconds=41.6 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | mae__daily_load__series_value | 82801.9956 | 2 | ok | seconds=369.0 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | skill_mae__daily_load__series_value | -0.4419 | 2 | ok | seconds=369.0 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | mae__honda_hourly__series_value | 73.0901 | 2 | ok | seconds=369.0 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | skill_mae__honda_hourly__series_value | -0.4551 | 2 | ok | seconds=369.0 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | skill_mae_mean | -0.4485 | 2 | ok | seconds=369.0 |
| self_supervised_other.reconstruction | masked_sequence_model | reconstruction | candidate | validation | train_skill_mae_mean | -0.4341 | 2 | ok | seconds=369.0 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | mae__daily_load__series_value | 59022.8055 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | skill_mae__daily_load__series_value | -0.0278 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | mae__honda_hourly__series_value | 53.3765 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | skill_mae__honda_hourly__series_value | -0.0626 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | skill_mae_mean | -0.0452 | 2 | ok | seconds=1.1 |
| self_supervised_other.reconstruction | ridge_cross_variable | reconstruction | candidate | validation | train_skill_mae_mean | -0.0221 | 2 | ok | seconds=1.1 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| self_supervised_other.reconstruction | data_ready | False | 2 |  | 2026-10-23 19:16:39.624582 |
| self_supervised_other.reconstruction | finished | False | 2 | baseline_ok=True | 2026-10-23 19:30:43.083764 |
| self_supervised_other.reconstruction | started | False | 2 |  | 2026-10-23 19:16:27.401913 |

<!-- section: energy__energy__16_rl_pumped_storage -->
## Model Pumped Storage Policy

### candidate_results

66 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | action_mae | 528.2648 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_policy | 56372.3129 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_timing | 24594.4061 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_logged | 14830.7681 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | sign_agreement | 0.7208 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_gain | 41541.5447 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | reward_timing_gain | 9763.6379 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | episode_net_abs_policy | 40226.8278 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | episode_net_abs_logged | 11265.9143 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | train_action_mae | 525.6799 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | conservative_q | offline_rl | candidate | validation | train_sign_agreement | 0.7725 | 17476 | ok | seconds=152.3 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | action_mae | 293.4382 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_policy | 23647.0254 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_timing | 16466.4402 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_logged | 14830.7681 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | sign_agreement | 0.8016 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_gain | 8816.2573 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | reward_timing_gain | 1635.6721 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | episode_net_abs_policy | 17459.1605 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | episode_net_abs_logged | 11265.9143 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | train_action_mae | 209.7609 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | offline_rl | candidate | validation | train_sign_agreement | 0.8798 | 17476 | ok | seconds=35.8 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | action_mae | 290.273 | 17476 | ok | seconds=57.8 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_policy | 23155.4571 | 17476 | ok | seconds=57.8 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_timing | 16174.8195 | 17476 | ok | seconds=57.8 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_logged | 14830.7681 | 17476 | ok | seconds=57.8 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | sign_agreement | 0.805 | 17476 | ok | seconds=57.8 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_gain | 8324.6889 | 17476 | ok | seconds=57.8 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | reward_timing_gain | 1344.0513 | 17476 | ok | seconds=57.8 |
| rl_pumped_storage.policy | reward_weighted_regression_sklearn | offline_rl | candidate | validation | episode_net_abs_policy | 17127.0042 | 17476 | ok | seconds=57.8 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| rl_pumped_storage.policy | data_ready | False | 2 |  | 2026-10-23 19:18:12.653260 |
| rl_pumped_storage.policy | finished | False | 2 | baseline_ok=True | 2026-10-23 19:28:30.207328 |
| rl_pumped_storage.policy | started | False | 2 |  | 2026-10-23 19:18:05.543863 |

<!-- section: energy__energy__17_rl_redispatch -->
## Model Redispatch Policy

### candidate_results

30 row(s)

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | action_agreement | 0.6752 | 5207 | ok | seconds=18.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_when_agree | -1695.993 | 5207 | ok | seconds=18.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_when_disagree | -1663.8142 | 5207 | ok | seconds=18.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | reward_weighted_agreement | 0.6794 | 5207 | ok | seconds=18.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_lightgbm | offline_rl | candidate | validation | train_action_agreement | 0.8438 | 5207 | ok | seconds=18.9 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | action_agreement | 0.682 | 5207 | ok | seconds=15.4 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_when_agree | -1689.5365 | 5207 | ok | seconds=15.4 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_when_disagree | -1676.9789 | 5207 | ok | seconds=15.4 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | reward_weighted_agreement | 0.6836 | 5207 | ok | seconds=15.4 |
| rl_redispatch.policy | behaviour_cloning_gbt_sklearn | offline_rl | candidate | validation | train_action_agreement | 0.843 | 5207 | ok | seconds=15.4 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | action_agreement | 0.6941 | 5207 | ok | seconds=4.2 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_when_agree | -1684.9552 | 5207 | ok | seconds=4.2 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_when_disagree | -1686.8757 | 5207 | ok | seconds=4.2 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | reward_weighted_agreement | 0.6938 | 5207 | ok | seconds=4.2 |
| rl_redispatch.policy | reward_weighted_gbt_lightgbm | offline_rl | candidate | validation | train_action_agreement | 0.8441 | 5207 | ok | seconds=4.2 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | action_agreement | 0.6962 | 5207 | ok | seconds=14.9 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_when_agree | -1677.5564 | 5207 | ok | seconds=14.9 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_when_disagree | -1703.8426 | 5207 | ok | seconds=14.9 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | reward_weighted_agreement | 0.6929 | 5207 | ok | seconds=14.9 |
| rl_redispatch.policy | reward_weighted_gbt_sklearn | offline_rl | candidate | validation | train_action_agreement | 0.8436 | 5207 | ok | seconds=14.9 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | action_agreement | 0.55 | 5207 | ok | seconds=2.5 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | reward_when_agree | -1624.9918 | 5207 | ok | seconds=2.5 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | reward_when_disagree | -1759.558 | 5207 | ok | seconds=2.5 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | reward_weighted_agreement | 0.5303 | 5207 | ok | seconds=2.5 |
| rl_redispatch.policy | behaviour_cloning_logistic | offline_rl | candidate | validation | train_action_agreement | 0.6368 | 5207 | ok | seconds=2.5 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | action_agreement | 0.527 | 5207 | ok | seconds=46.8 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | reward_when_agree | -1601.6433 | 5207 | ok | seconds=46.8 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | reward_when_disagree | -1779.0141 | 5207 | ok | seconds=46.8 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | reward_weighted_agreement | 0.5008 | 5207 | ok | seconds=46.8 |
| rl_redispatch.policy | majority_action | baseline | baseline | validation | train_action_agreement | 0.5497 | 5207 | ok | seconds=46.8 |

### task_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| rl_redispatch.policy | data_ready | False | 2 |  | 2026-10-23 19:29:22.384043 |
| rl_redispatch.policy | finished | False | 2 | baseline_ok=True | 2026-10-23 19:31:24.091624 |
| rl_redispatch.policy | started | False | 2 |  | 2026-10-23 19:29:14.581264 |

<!-- section: energy__evaluate__00_evaluation_setup -->
## Evaluation Setup

### evaluation_spec

_no rows for this run_

<!-- section: energy__evaluate__01_tabular_energy -->
## Evaluate Tabular Models Energy

### evaluation_results

1125 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| price_daily.price | seasonal_naive | baseline | test | mae | 28.6236 | 28.0429 | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | rmse | 38.4733 | 42.9023 | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mean_error | -0.0951 | -1.5877 | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mean_error_relative | -0.001 | -0.0202 | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | pred_std_ratio | 0.9938 | 1.0185 | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | pinball_q50 | 14.3118 | 14.0215 | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | primary_change_relative | 0.0207 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__market_area_code__de_lu | 28.6236 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-01 | 51.8913 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-03 | 27.4197 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-04 | 18.0803 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-05 | 19.1765 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-06 | 21.5253 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-07 | 11.6842 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-08 | 15.3887 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-09 | 35.4757 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-10 | 38.3406 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-11 | 29.5123 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2025-12 | 23.8787 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2026-01 | 23.6926 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2026-03 | 38.681 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2026-04 | 42.6217 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2026-05 | 29.7623 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2026-06 | 31.2587 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2026-07 | 32.7994 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | mae__month__2026-08 | 27.3252 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | pred_finite_share | 1.0 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | pred_std | 33.7505 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | pred_outside_range_share | 0.0 |  | 613 | ok | on test |
| price_daily.price | seasonal_naive | baseline | test | reproduced_validation_value | 28.0429 |  | 613 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

33 row(s), first 30 shown

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| price_daily.price | started | False | 0 |  | 2026-10-25 14:03:43.443995 |
| price_daily.price | data_ready | False | 0 |  | 2026-10-25 14:04:57.999690 |
| price_daily.price | finished | False | 0 | baseline_ok=True | 2026-10-25 14:15:35.358070 |
| price_quarter_hour.price | started | False | 0 |  | 2026-10-25 14:15:59.463865 |
| price_quarter_hour.price | data_ready | False | 0 |  | 2026-10-25 14:16:10.148217 |
| price_quarter_hour.price | finished | False | 0 | baseline_ok=True | 2026-10-25 14:16:30.082622 |
| price_quarter_hour.negative_price | started | False | 0 |  | 2026-10-25 14:16:43.897696 |
| price_quarter_hour.negative_price | data_ready | False | 0 |  | 2026-10-25 14:16:54.009785 |
| price_quarter_hour.negative_price | finished | False | 0 | baseline_ok=True | 2026-10-25 14:17:41.771801 |
| load.load | started | False | 0 |  | 2026-10-25 14:18:01.303836 |
| load.load | data_ready | False | 0 |  | 2026-10-25 14:18:10.770752 |
| load.load | finished | False | 0 | baseline_ok=True | 2026-10-25 14:18:38.563149 |
| bias.bias | started | False | 0 |  | 2026-10-25 14:19:03.646382 |
| bias.bias | data_ready | False | 0 |  | 2026-10-25 14:19:17.381269 |
| bias.bias | finished | False | 0 | baseline_ok=True | 2026-10-25 14:19:33.216209 |
| zone_generation.generation | started | False | 0 |  | 2026-10-25 14:19:45.635835 |
| zone_generation.generation | data_ready | False | 0 |  | 2026-10-25 14:19:54.510587 |
| zone_generation.generation | finished | False | 0 | baseline_ok=True | 2026-10-25 14:50:07.422559 |
| honda_forecast.increment | started | False | 0 |  | 2026-10-25 14:50:26.929783 |
| honda_forecast.increment | data_ready | False | 0 |  | 2026-10-25 14:50:36.644330 |
| honda_forecast.increment | finished | False | 0 | baseline_ok=True | 2026-10-25 14:51:07.533184 |
| ccpp.output | started | False | 0 |  | 2026-10-25 14:51:19.870754 |
| ccpp.output | data_ready | False | 0 |  | 2026-10-25 14:51:29.552951 |
| capacity_additions.additions | data_ready | False | 0 |  | 2026-10-25 14:59:46.836405 |
| redispatch.event_energy | data_ready | False | 2 |  | 2026-10-25 15:01:44.465852 |
| redispatch.any_event | data_ready | False | 2 |  | 2026-10-25 15:01:06.314024 |
| capacity_additions.additions | finished | False | 0 | baseline_ok=True | 2026-10-25 15:00:44.807663 |
| redispatch.event_energy | finished | False | 2 | baseline_ok=True | 2026-10-25 15:01:58.740436 |
| redispatch.any_event | finished | False | 2 | baseline_ok=True | 2026-10-25 15:01:19.762647 |
| capacity_additions.additions | started | False | 0 |  | 2026-10-25 14:59:35.942894 |

<!-- section: energy__evaluate__02_survival_energy -->
## Evaluate Survival Models Energy

### evaluation_results

55 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | concordance | 0.959 | 0.9606 | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | brier_1y | 0.0008 | 0.0008 | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | calibration_gap_1y | 0.0001 | 0.0007 | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | brier_3y | 0.0011 | 0.0018 | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | calibration_gap_3y | 0.0001 | 0.0007 | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | brier_5y | 0.001 | 0.0018 | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | calibration_gap_5y | 0.0006 | 0.0004 | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | primary_change_relative | 0.0017 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | skill_primary | 0.4559 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | skill_primary_ci_low | 0.4496 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | skill_primary_ci_high | 0.4626 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | bootstrap_resamples_valid | 200.0 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | bootstrap_blocks | 5244.0 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | bootstrap_rows_used | 5244.0 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | concordance__is_offshore__False | 0.9587 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | concordance__is_offshore__True |  |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | pred_finite_share | 1.0 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | pred_std | 417510.7784 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | reproduced_validation_value | 0.9606 |  | 5244 | ok | on test |
| survival.unit_lifetime | boosted_cox_xgboost | candidate | test | reproduction_gap_relative | 0.0 |  | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | concordance | 0.9626 | 0.9641 | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | brier_1y | 0.0009 | 0.0008 | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | calibration_gap_1y | 0.0002 | 0.0008 | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | brier_3y | 0.0012 | 0.0018 | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | calibration_gap_3y | 0.0026 | 0.0021 | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | brier_5y | 0.001 | 0.0017 | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | calibration_gap_5y | 0.0035 | 0.0026 | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | primary_change_relative | 0.0016 |  | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | skill_primary | 0.4594 |  | 5244 | ok | on test |
| survival.unit_lifetime | survival_forest | candidate | test | skill_primary_ci_low | 0.4516 |  | 5244 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| survival.unit_lifetime | data_ready | False | 0 |  | 2026-10-25 15:20:37.386588 |
| survival.unit_lifetime | finished | False | 0 | baseline_ok=True | 2026-10-25 15:27:59.712916 |
| survival.unit_lifetime | started | False | 0 |  | 2026-10-25 15:19:27.397409 |

<!-- section: energy__evaluate__03_anomaly_energy -->
## Evaluate Anomaly Models Energy

### evaluation_results

85 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | precision | 0.3191 | 0.3192 | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | recall | 0.6383 | 0.6853 | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | flag_rate | 0.0304 | 0.0302 | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc | 0.5568 | 0.6483 | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | threshold | 4.3431 | 4.3431 | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | injected_rows | 575.0 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | primary_change_relative | 0.1412 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | skill_primary | 0.1998 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | skill_primary_ci_low | 0.0806 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | skill_primary_ci_high | 0.3062 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | bootstrap_resamples_valid | 200.0 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | bootstrap_blocks | 13.0 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | bootstrap_rows_used | 26277.0 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__channel__combined_heat_and_power | 0.5997 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__channel__solar_photovoltaic | 0.7072 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__channel__total | 0.4345 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-01 | 0.7921 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-02 | 0.6361 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-03 | 0.1061 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-04 | 0.5997 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-05 | 0.8953 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-06 | 0.638 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-07 | 0.4717 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-08 | 0.8529 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-09 | 0.6851 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-10 | 0.6344 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-11 | 0.0446 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pr_auc__month__2023-12 | 0.5788 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pred_finite_share | 1.0 |  | 26277 | ok | on test |
| honda_anomaly.anomaly | forecast_residual_lightgbm | candidate | test | pred_std | 1.6594 |  | 26277 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| honda_anomaly.anomaly | started | False | 0 |  | 2026-10-25 14:03:44.571033 |
| honda_anomaly.anomaly | data_ready | False | 0 |  | 2026-10-25 14:04:51.586945 |
| honda_anomaly.anomaly | finished | False | 0 | baseline_ok=True | 2026-10-25 14:13:38.802216 |

<!-- section: energy__evaluate__04_reconstruction_energy -->
## Evaluate Reconstruction Models Energy

### evaluation_results

139 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__air_temperature | 0.2353 | 0.1965 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__air_temperature | 0.9316 | 0.939 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__air_temperature | 0.2319 | 0.1856 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__dew_point_temperature | 0.1547 | 0.1387 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__dew_point_temperature | 0.934 | 0.9393 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__dew_point_temperature | 0.131 | 0.1088 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__relative_humidity | 1.206 | 1.0925 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__relative_humidity | 0.9167 | 0.9155 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__relative_humidity | 1.1476 | 1.0673 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__pressure_station | 3.5489 | 3.7668 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__pressure_station | 0.0915 | 0.1727 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__pressure_station | 3.7663 | 3.4699 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__pressure_sea_level | 3.1807 | 3.331 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__pressure_sea_level | 0.2395 | 0.2819 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__pressure_sea_level | 3.1603 | 4.047 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__wind_speed | 1.2742 | 1.4336 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__wind_speed | 0.223 | 0.2236 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__wind_speed | 1.2924 | 1.2826 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__wind_direction | 59.1239 | 55.866 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__wind_direction | -0.0778 | 0.0214 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__wind_direction | 58.5011 | 60.252 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__cloud_cover | 27.1722 | 25.0479 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__cloud_cover | 0.1107 | 0.0989 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__cloud_cover | 26.2151 | 26.4422 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__visibility | 10405.5081 | 11001.8053 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__visibility | 0.3967 | 0.3934 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__visibility | 10710.8767 | 12142.984 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae__weather__soil_temperature | 1.0587 | 1.0233 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | skill_mae__weather__soil_temperature | 0.701 | 0.6919 | 297986 | ok | on test |
| weather_imputation.reconstruction | gbt_cross_variable_lightgbm | candidate | test | mae_heldout__weather__soil_temperature | 1.0934 | 1.1198 | 297986 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

6 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| weather_imputation.reconstruction | started | False | 2 |  | 2026-10-25 14:03:47.524304 |
| weather_imputation.reconstruction | data_ready | False | 2 |  | 2026-10-25 14:04:48.592788 |
| self_supervised_other.reconstruction | data_ready | False | 2 |  | 2026-10-25 14:14:04.470748 |
| self_supervised_other.reconstruction | finished | False | 2 | baseline_ok=True | 2026-10-25 14:14:16.182431 |
| weather_imputation.reconstruction | finished | False | 2 | baseline_ok=True | 2026-10-25 14:13:38.812478 |
| self_supervised_other.reconstruction | started | False | 2 |  | 2026-10-25 14:13:55.684206 |

<!-- section: energy__evaluate__05_weak_supervision_energy -->
## Evaluate Weak Supervision Energy

### evaluation_results

28 row(s)

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| weak_supervision.labels | label_model | candidate | test | coverage__series | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | conflict_rate__series | 0.0 | 0.0 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | agreement_rate__series | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | resolved_rate__series | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | coverage__unit | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | resolved_rate__unit | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | coverage | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | accuracy__series__declared_gap_present | 0.99 | 0.99 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | accuracy__series__reconciliation_status | 0.99 | 0.99 | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | primary_change_relative | 0.0 |  | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | skill_primary | 0.0 |  | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | skill_primary_ci_low | 0.0 |  | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | skill_primary_ci_high | 0.0 |  | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | bootstrap_resamples_valid | 200.0 |  | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | bootstrap_blocks | 25650.0 |  | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | bootstrap_rows_used | 25846.0 |  | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | reproduced_validation_value | 1.0 |  | 25846 | ok | on test |
| weak_supervision.labels | label_model | candidate | test | reproduction_gap_relative | 0.0 |  | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | coverage__series | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | conflict_rate__series | 0.0 | 0.0 | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | agreement_rate__series | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | resolved_rate__series | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | coverage__unit | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | resolved_rate__unit | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | coverage | 1.0 | 1.0 | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | primary_change_relative | 0.0 |  | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | reproduced_validation_value | 1.0 |  | 25846 | ok | on test |
| weak_supervision.labels | majority_vote | baseline | test | reproduction_gap_relative | 0.0 |  | 25846 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| weak_supervision.labels | data_ready | False | 2 |  | 2026-10-25 14:05:05.676330 |
| weak_supervision.labels | finished | False | 2 | baseline_ok=True | 2026-10-25 14:05:24.735965 |
| weak_supervision.labels | started | False | 2 |  | 2026-10-25 14:03:43.825218 |

<!-- section: energy__evaluate__06_matching_energy -->
## Evaluate Matching Models Energy

### evaluation_results

74 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | accuracy | 0.972 | 0.9724 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | precision__exact_normalised |  |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | recall__exact_normalised |  |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | n_true__exact_normalised | 4.0 | 4.0 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | estimable__exact_normalised | 0.0 | 0.0 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | precision__unmatched | 0.9787 | 0.979 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | recall__unmatched | 0.9928 | 0.9929 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | n_true__unmatched | 139.0 | 141.0 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | estimable__unmatched | 1.0 | 1.0 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | estimable_tiers | 1.0 | 1.0 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | balanced_accuracy | 0.6214 | 0.6215 | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | primary_change_relative | 0.0001 |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | skill_primary | 0.1214 |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | skill_primary_ci_low | -0.0073 |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | skill_primary_ci_high | 0.4895 |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | bootstrap_resamples_valid | 200.0 |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | bootstrap_blocks | 143.0 |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | bootstrap_rows_used | 143.0 |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | reproduced_validation_value | 0.6215 |  | 143 | ok | on test |
| redispatch_matching.tier | tabicl_v2_in_context | candidate | test | reproduction_gap_relative | 0.0 |  | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | accuracy | 0.9371 | 0.9586 | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | precision__exact_normalised |  |  | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | recall__exact_normalised |  |  | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | n_true__exact_normalised | 4.0 | 4.0 | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | estimable__exact_normalised | 0.0 | 0.0 | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | precision__unmatched | 0.971 | 0.9787 | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | recall__unmatched | 0.964 | 0.9787 | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | n_true__unmatched | 139.0 | 141.0 | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | estimable__unmatched | 1.0 | 1.0 | 143 | ok | on test |
| redispatch_matching.tier | char_ngram_logistic | candidate | test | estimable_tiers | 1.0 | 1.0 | 143 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

3 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| redispatch_matching.tier | data_ready | False | 2 |  | 2026-10-25 14:07:21.435348 |
| redispatch_matching.tier | finished | False | 2 | baseline_ok=True | 2026-10-25 14:10:26.187760 |
| redispatch_matching.tier | started | False | 2 |  | 2026-10-25 14:06:32.801648 |

<!-- section: energy__evaluate__07_offline_rl_energy -->
## Evaluate Offline Policies Energy

### evaluation_results

140 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | action_mae | 323.3327 | 293.4382 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | reward_policy | 50568.9748 | 23647.0254 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | reward_timing | 34143.4852 | 16466.4402 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | reward_logged | 30807.4353 | 14830.7681 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | sign_agreement | 0.8349 | 0.8016 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | reward_gain | 19761.5395 | 8816.2573 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | reward_timing_gain | 3336.0499 | 1635.6721 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | episode_net_abs_policy | 21178.9139 | 17459.1605 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | episode_net_abs_logged | 12596.207 | 11265.9143 | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | primary_change_relative | -1.0735 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | skill_primary | -7444.2772 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | skill_primary_ci_low | -9141.2429 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | skill_primary_ci_high | -6103.4485 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | bootstrap_resamples_valid | 200.0 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | bootstrap_blocks | 247.0 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | bootstrap_rows_used | 23693.0 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | pred_finite_share | 1.0 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | pred_std | 785.4829 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | pred_outside_range_share | 0.0 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | reproduced_validation_value | 16466.4402 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | reward_weighted_regression_lightgbm | candidate | test | reproduction_gap_relative | 0.0 |  | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | action_mae | 104.0161 | 102.9072 | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | reward_policy | 33502.1725 | 19639.9796 | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | reward_timing | 30453.455 | 15115.6474 | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | reward_logged | 30807.4353 | 14830.7681 | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | sign_agreement | 0.9267 | 0.8856 | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | reward_gain | 2694.7372 | 4809.2115 | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | reward_timing_gain | -353.9803 | 284.8793 | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | episode_net_abs_policy | 11708.9273 | 9478.522 | 23693 | ok | on test |
| rl_pumped_storage.policy | decision_transformer | candidate | test | episode_net_abs_logged | 12596.207 | 11265.9143 | 23693 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

6 row(s)

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| rl_pumped_storage.policy | started | False | 2 |  | 2026-10-25 14:11:22.637056 |
| rl_pumped_storage.policy | data_ready | False | 2 |  | 2026-10-25 14:12:11.754515 |
| rl_redispatch.policy | data_ready | False | 2 |  | 2026-10-25 14:15:23.806884 |
| rl_pumped_storage.policy | finished | False | 2 | baseline_ok=True | 2026-10-25 14:14:58.078147 |
| rl_redispatch.policy | finished | False | 2 | baseline_ok=True | 2026-10-25 14:15:50.145330 |
| rl_redispatch.policy | started | False | 2 |  | 2026-10-25 14:15:12.272554 |

<!-- section: energy__split__01_weak_supervision_split -->
## Weak Supervision Split

### evaluation_split_manifest

| dataset_id | partition | rows |
|---|---|---|
| redispatch_matching | test | 143 |
| redispatch_matching | train | 431 |
| redispatch_matching | validation | 145 |
| weak_supervision | test | 26046 |
| weak_supervision | train | 121682 |
| weak_supervision | validation | 26028 |

<!-- section: energy__split__02_redispatch_matching_split -->
## Redispatch Matching Split

### evaluation_split_manifest

| dataset_id | partition | rows |
|---|---|---|
| redispatch_matching | test | 143 |
| redispatch_matching | train | 431 |
| redispatch_matching | validation | 145 |
| weak_supervision | test | 26046 |
| weak_supervision | train | 121682 |
| weak_supervision | validation | 26028 |

<!-- section: energy__split__03_evaluation_spec -->
## Evaluation Specification

### evaluation_spec

15 row(s)

| dataset_id | spec_key | spec_value |
|---|---|---|
| weak_supervision | split_percent | (70, 15, 15) |
| weak_supervision | group | place for series entities, unit for unit entities |
| weak_supervision | stratum | entity_type |
| weak_supervision | min_series_entities | 10 |
| redispatch_matching | split_percent | (60, 20, 20) |
| redispatch_matching | group | normalised asset identity |
| redispatch_matching | stratum | match tier |
| redispatch_matching | min_tier_rows | 10 |
| survival | horizons_years | (1, 3, 5) |
| price_daily | quantiles | (0.1, 0.5, 0.9) |
| price_quarter_hour | quantiles | (0.1, 0.5, 0.9) |
| honda_anomaly | injection | mask_specification |
| honda_anomaly | flag_quantile | 0.99 |
| weather_imputation | masks | mask_specification |
| self_supervised_other | masks | mask_specification |
