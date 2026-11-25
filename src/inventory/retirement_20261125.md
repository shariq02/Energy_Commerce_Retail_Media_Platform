# Layer retirement plan

_Generated: 2026-11-25T13:05Z_

Switches: tables and views False, Volumes False, schemas False

## Schemas to drop

| layer | schema | tables | views | volumes | decision |
| --- | --- | --- | --- | --- | --- |
| gold and analytics (old tables) | energy_gold | 0 | 0 | 0 | drop |
| gold and analytics (old tables) | commerce_gold | 0 | 0 | 0 | drop |
| gold and analytics (old tables) | genomics_gold | 0 | 0 | 0 | drop |
| gold and analytics (old tables) | shared_conformed | 0 | 0 | 0 | drop |
| gold and analytics (old tables) | energy_analytics | 0 | 0 | 0 | drop |
| gold and analytics (old tables) | commerce_analytics | 0 | 0 | 0 | drop |
| gold and analytics (old tables) | genomics_analytics | 0 | 0 | 0 | drop |

## Successor check

| layer | successor schema | tables | problem | passes |
| --- | --- | --- | --- | --- |
| gold and analytics (old tables) | energy_silver | 17 |  | 1 |
| gold and analytics (old tables) | energy_silver_reference | 12 |  | 1 |
| gold and analytics (old tables) | commerce_silver | 6 |  | 1 |
| gold and analytics (old tables) | genomics_silver | 28 |  | 1 |
| gold and analytics (old tables) | genomics_silver_reference | 12 |  | 1 |

## Tables by last write (cutoff 2026-11-19T21:00:00Z)

| schema | table | last write | provenance column | decision |
| --- | --- | --- | --- | --- |
| energy_gold | balancing_area | 2026-11-25 12:59 | yes | keep (written after the cutoff or never) |
| energy_gold | capacity_plan | 2026-11-25 13:00 | yes | keep (written after the cutoff or never) |
| energy_gold | channel_reading | 2026-11-25 10:05 | yes | keep (written after the cutoff or never) |
| energy_gold | energy_component | 2026-11-25 10:05 | yes | keep (written after the cutoff or never) |
| energy_gold | entity_link | 2026-11-25 10:06 | yes | keep (written after the cutoff or never) |
| energy_gold | forecast_actual | 2026-11-25 10:05 | yes | keep (written after the cutoff or never) |
| energy_gold | generation_unit | 2026-11-25 13:00 | yes | keep (written after the cutoff or never) |
| energy_gold | grid_connection_point | 2026-11-25 13:00 | yes | keep (written after the cutoff or never) |
| energy_gold | grid_intervention_event | 2026-11-25 10:05 | yes | keep (written after the cutoff or never) |
| energy_gold | grid_location | 2026-11-25 12:59 | yes | keep (written after the cutoff or never) |
| energy_gold | grid_network | 2026-11-25 12:59 | yes | keep (written after the cutoff or never) |
| energy_gold | iot_device | 2026-11-25 10:04 | yes | keep (written after the cutoff or never) |
| energy_gold | market_actor | 2026-11-25 12:59 | yes | keep (written after the cutoff or never) |
| energy_gold | market_actor_role | 2026-11-25 13:00 | yes | keep (written after the cutoff or never) |
| energy_gold | market_price | 2026-11-25 10:05 | yes | keep (written after the cutoff or never) |
| energy_gold | plant_sensor_observation | 2026-11-25 10:04 | yes | keep (written after the cutoff or never) |
| energy_gold | power_plant | 2026-11-25 13:00 | yes | keep (written after the cutoff or never) |
| energy_gold | storage_plant | 2026-11-25 12:59 | yes | keep (written after the cutoff or never) |
| energy_gold | unit_event | 2026-11-25 13:00 | yes | keep (written after the cutoff or never) |
| energy_gold | weather_daily | 2026-11-25 10:04 | yes | keep (written after the cutoff or never) |
| energy_gold | weather_observation | 2026-11-25 10:06 | yes | keep (written after the cutoff or never) |
| commerce_gold | commerce_event | 2026-11-25 09:47 | yes | keep (written after the cutoff or never) |
| commerce_gold | commerce_event_item | 2026-11-25 09:48 | yes | keep (written after the cutoff or never) |
| commerce_gold | commerce_product | 2026-11-25 09:45 | yes | keep (written after the cutoff or never) |
| commerce_gold | commerce_session | 2026-11-25 09:45 | yes | keep (written after the cutoff or never) |
| commerce_gold | retail_trade_index | 2026-11-25 09:49 | yes | keep (written after the cutoff or never) |
| genomics_gold | benchmark_variant | 2026-11-25 10:26 | yes | keep (written after the cutoff or never) |
| genomics_gold | case_assay_record | 2026-11-25 10:21 | yes | keep (written after the cutoff or never) |
| genomics_gold | cell_line_profile | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| genomics_gold | clinical_trial_profile | 2026-11-25 10:09 | yes | keep (written after the cutoff or never) |
| genomics_gold | clinvar_classification_change | 2026-11-25 10:12 | yes | keep (written after the cutoff or never) |
| genomics_gold | disease_profile | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| genomics_gold | dms_assay | 2026-11-25 10:26 | yes | keep (written after the cutoff or never) |
| genomics_gold | dms_assay_mutant | 2026-11-25 10:26 | yes | keep (written after the cutoff or never) |
| genomics_gold | drug_profile | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| genomics_gold | embedding_split | 2026-11-25 10:27 | yes | keep (written after the cutoff or never) |
| genomics_gold | entity_identity_record | 2026-11-25 10:22 | yes | keep (written after the cutoff or never) |
| genomics_gold | epitope_profile | 2026-11-25 10:09 | yes | keep (written after the cutoff or never) |
| genomics_gold | gene_disease_profile | 2026-11-19 22:00 | yes | keep (written after the cutoff or never) |
| genomics_gold | gene_measurement | 2026-11-25 10:18 | yes | keep (written after the cutoff or never) |
| genomics_gold | gene_profile | 2026-11-25 09:51 | yes | keep (written after the cutoff or never) |
| genomics_gold | genetic_test_profile | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| genomics_gold | multi_value_attribute | 2026-11-25 09:50 | yes | keep (written after the cutoff or never) |
| genomics_gold | multi_value_score | 2026-11-25 09:52 | yes | keep (written after the cutoff or never) |
| genomics_gold | protein_profile | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| genomics_gold | protein_variant_record | 2026-11-25 10:19 | yes | keep (written after the cutoff or never) |
| genomics_gold | relation_record | 2026-11-25 10:10 | yes | keep (written after the cutoff or never) |
| genomics_gold | somatic_mutation_profile | 2026-11-25 10:09 | yes | keep (written after the cutoff or never) |
| genomics_gold | somatic_site_profile | 2026-11-25 10:12 | yes | keep (written after the cutoff or never) |
| genomics_gold | structural_variant_profile | 2026-11-25 10:11 | yes | keep (written after the cutoff or never) |
| genomics_gold | transcript_profile | 2026-11-25 10:07 | yes | keep (written after the cutoff or never) |
| genomics_gold | tumor_sample_profile | 2026-11-25 10:09 | yes | keep (written after the cutoff or never) |
| genomics_gold | variant_profile | 2026-11-25 10:04 | yes | keep (written after the cutoff or never) |
| shared_conformed | dim_date |  | no | keep (no column) |
| shared_conformed | dim_geography |  | no | keep (no column) |
| shared_conformed | dim_place | 2026-11-25 09:47 | yes | keep (written after the cutoff or never) |
| shared_conformed | dim_time |  | no | keep (no column) |
| shared_conformed | dim_weather_context |  | no | keep (no column) |
| shared_conformed | geo_plz_gemeinde_xref |  | no | keep (no column) |
| shared_conformed | metric_definition | 2026-11-25 09:47 | yes | keep (written after the cutoff or never) |
| shared_conformed | ref_energy_carrier | 2026-11-25 09:45 | yes | keep (written after the cutoff or never) |
| shared_conformed | ref_energy_carrier_map | 2026-11-25 09:45 | yes | keep (written after the cutoff or never) |
| shared_conformed | ref_measure | 2026-11-25 09:45 | yes | keep (written after the cutoff or never) |
| shared_conformed | series_coverage | 2026-11-25 09:47 | yes | keep (written after the cutoff or never) |
| energy_analytics | capacity_margin_yearly | 2026-11-25 10:11 | yes | keep (written after the cutoff or never) |
| energy_analytics | capacity_registry | 2026-11-25 10:10 | yes | keep (written after the cutoff or never) |
| energy_analytics | capacity_utilisation_monthly | 2026-11-25 10:11 | yes | keep (written after the cutoff or never) |
| energy_analytics | cross_border_trade | 2026-11-25 10:09 | yes | keep (written after the cutoff or never) |
| energy_analytics | extreme_weather_stress_event | 2026-11-25 10:16 | yes | keep (written after the cutoff or never) |
| energy_analytics | fleet_lifecycle_event | 2026-11-25 10:11 | yes | keep (written after the cutoff or never) |
| energy_analytics | forecast_accuracy | 2026-11-25 10:09 | yes | keep (written after the cutoff or never) |
| energy_analytics | generation_by_carrier_daily | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| energy_analytics | grid_intervention_daily | 2026-11-25 10:09 | yes | keep (written after the cutoff or never) |
| energy_analytics | market_price_daily | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| energy_analytics | market_timeseries | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| energy_analytics | negative_price_event | 2026-11-25 10:08 | yes | keep (written after the cutoff or never) |
| energy_analytics | operator_concentration | 2026-11-25 10:11 | yes | keep (written after the cutoff or never) |
| energy_analytics | regional_fleet_monthly | 2026-11-25 10:11 | yes | keep (written after the cutoff or never) |
| energy_analytics | site_energy_weather_daily | 2026-11-25 10:12 | yes | keep (written after the cutoff or never) |
| energy_analytics | support_scheme_buildout | 2026-11-25 10:12 | yes | keep (written after the cutoff or never) |
| energy_analytics | weather_energy_daily | 2026-11-25 10:12 | yes | keep (written after the cutoff or never) |
| commerce_analytics | customer_journey_daily | 2026-11-25 09:56 | yes | keep (written after the cutoff or never) |
| commerce_analytics | ga4_demand_daily | 2026-11-25 09:52 | yes | keep (written after the cutoff or never) |
| commerce_analytics | product_demand_daily | 2026-11-25 10:00 | yes | keep (written after the cutoff or never) |
| commerce_analytics | retail_energy_monthly | 2026-11-25 10:02 | yes | keep (written after the cutoff or never) |
| genomics_analytics | classification_change_summary | 2026-11-25 10:28 | yes | keep (written after the cutoff or never) |
| genomics_analytics | disease_overview | 2026-11-25 10:27 | yes | keep (written after the cutoff or never) |
| genomics_analytics | gene_prioritization | 2026-11-25 10:27 | yes | keep (written after the cutoff or never) |
| genomics_analytics | genetic_test_panel | 2026-11-25 10:28 | yes | keep (written after the cutoff or never) |
| genomics_analytics | population_risk | 2026-11-25 10:28 | yes | keep (written after the cutoff or never) |
| genomics_analytics | variant_review_queue | 2026-11-25 10:28 | yes | keep (written after the cutoff or never) |
