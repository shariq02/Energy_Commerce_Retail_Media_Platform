# Catalog inventory: energy_commerce_retail_media

Generated: 2026-11-16T18:24:09Z

Size is the data files of the current table version (older versions kept for time travel are not counted). A view has no size. `tables` counts tables and views; `views` is the part that are views.

## Guard lines

- WARNING metastore: 892 tables and views in all catalogs (guard 400)

## Overall

| schemas | tables | views | rows | files | size_bytes | size_gb | volumes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 26 | 300 | 0 | 2,407,318,758 | 2,364 | 125,839,468,600 | 117.197 | 5 |

## Catalogs

| catalog | tables_and_views | error |
| --- | --- | --- |
| energy_commerce_retail_media | 300 |  |
| samples | 124 |  |
| system | 468 |  |

## Quota limits and usage

| quota | parent | parent_type | used | limit | used_percent |
| --- | --- | --- | --- | --- | --- |
| table-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 98 | 100 | 98.0 |
| table-quota | energy_commerce_retail_media.energy_ml_datasets | SecurableType.SCHEMA | 91 | 100 | 91.0 |
| table-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 349 | 500 | 69.8 |
| table-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 62 | 100 | 62.0 |
| schema-quota | energy_commerce_retail_media | SecurableType.CATALOG | 26 | 50 | 52.0 |
| table-quota | energy_commerce_retail_media.commerce_ml_datasets | SecurableType.SCHEMA | 46 | 100 | 46.0 |
| table-quota | energy_commerce_retail_media.genomics_gold | SecurableType.SCHEMA | 32 | 100 | 32.0 |
| table-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.genomics_silver | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 25 | 100 | 25.0 |
| table-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 20 | 100 | 20.0 |
| volume-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 8 | 50 | 16.0 |
| table-quota | energy_commerce_retail_media.energy_silver | SecurableType.SCHEMA | 15 | 100 | 15.0 |
| table-quota | energy_commerce_retail_media.energy_silver_reference | SecurableType.SCHEMA | 12 | 100 | 12.0 |
| table-quota | energy_commerce_retail_media.genomics_silver_reference | SecurableType.SCHEMA | 11 | 100 | 11.0 |
| table-quota | energy_commerce_retail_media.shared_conformed | SecurableType.SCHEMA | 10 | 100 | 10.0 |
| table-quota | energy_commerce_retail_media.ml_datasets_shared | SecurableType.SCHEMA | 9 | 100 | 9.0 |
| table-quota | energy_commerce_retail_media.commerce_gold | SecurableType.SCHEMA | 7 | 100 | 7.0 |
| catalog-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 3 | 50 | 6.0 |
| table-quota | energy_commerce_retail_media.genomics_analytics | SecurableType.SCHEMA | 6 | 100 | 6.0 |
| table-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 6 | 100 | 6.0 |
| table-quota | energy_commerce_retail_media.commerce_silver | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| table-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| table-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| privilege-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 9 | 200 | 4.5 |
| schema-quota | system | SecurableType.CATALOG | 2 | 50 | 4.0 |
| volume-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 2 | 50 | 4.0 |
| volume-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 2 | 50 | 4.0 |
| privilege-quota | energy_commerce_retail_media | SecurableType.CATALOG | 6 | 200 | 3.0 |
| privilege-quota | samples | SecurableType.CATALOG | 5 | 200 | 2.5 |
| provider-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 1 | 50 | 2.0 |
| volume-quota | samples.sec | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| privilege-quota | system.ai | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| privilege-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| privilege-quota | system.__internal_ai | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| volume-quota | system.__internal_ai | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| volume-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| volume-quota | samples.databricks | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| privilege-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| privilege-quota | system | SecurableType.CATALOG | 2 | 200 | 1.0 |
| privilege-quota | system.lakeflow | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.sharing | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.lakeflow_pipeline_events_preview | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | energy_commerce_retail_media.energy_semantic | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| table-quota | energy_commerce_retail_media.energy_semantic | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| table-quota | system.__internal_data_quality_monitoring | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| privilege-quota | system.access | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| table-quota | energy_commerce_retail_media.commerce_silver_reference | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| privilege-quota | system.tags | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | energy_commerce_retail_media.commerce_semantic | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| table-quota | energy_commerce_retail_media.commerce_semantic | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| privilege-quota | system.query | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.storage | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.billing | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.mlflow | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.compute | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.ai_gateway | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| table-quota | system.data_quality_monitoring | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| privilege-quota | system.alert | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.serving | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.data_quality_monitoring | SecurableType.SCHEMA | 1 | 200 | 0.5 |
| schema-quota | samples | SecurableType.CATALOG | 0 | 50 | 0.0 |
| external_location-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 50 | 0.0 |
| recipient-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 50 | 0.0 |
| registered-model-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 100 | 0.0 |
| share-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 50 | 0.0 |
| storage_credential-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 1,000 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.lakeflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.lakeflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.lakeflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.accuweather | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.accuweather | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.accuweather | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.sec | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.sec | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.sec | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.lakeflow_pipeline_events_preview | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.lakeflow_pipeline_events_preview | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.lakeflow_pipeline_events_preview | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.__internal_functions | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.__internal_functions | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.__internal_functions | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_semantic | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_semantic | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.bakehouse | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.bakehouse | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.bakehouse | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.__internal_data_quality_monitoring | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.__internal_data_quality_monitoring | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.access | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.access | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.access | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.tags | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.tags | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.tags | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpcds_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpcds_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpcds_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.default | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.default | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | energy_commerce_retail_media.default | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_semantic | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_semantic | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.query | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.query | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.query | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.storage | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.storage | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.storage | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch_sf100 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch_sf100 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch_sf100 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.billing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.billing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.billing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.shared_conformed | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.shared_conformed | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.mlflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.mlflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.mlflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch_sf10 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch_sf10 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch_sf10 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.wanderbricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.wanderbricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.wanderbricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.healthverity | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.healthverity | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.healthverity | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| volume-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 0 | 50 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.ml_datasets_shared | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.ml_datasets_shared | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.compute | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.compute | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.compute | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.__internal_ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.__internal_ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.__internal_ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.ai_gateway | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.ai_gateway | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.ai_gateway | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpcds_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpcds_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpcds_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.data_quality_monitoring | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.data_quality_monitoring | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.alert | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.alert | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.alert | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.serving | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.serving | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.serving | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.nyctaxi | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.nyctaxi | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.nyctaxi | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.databricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.databricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.databricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| privilege-quota | energy_commerce_retail_media.quality.quality_audit_log | SecurableType.TABLE | 0 | 100 | 0.0 |
| privilege-quota | system.data_quality_monitoring.table_results | SecurableType.TABLE | 0 | 100 | 0.0 |

## Per schema

| schema | tables | views | rows | files | size_bytes | size_mb | size_gb |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bronze | 33 | 0 | 340,080,547 | 102 | 3,257,552,131 | 3106.64 | 3.034 |
| commerce_analytics | 5 | 0 | 13,788 | 5 | 185,954 | 0.18 | 0.0 |
| commerce_gold | 7 | 0 | 139,443,955 | 156 | 24,886,832,497 | 23733.93 | 23.178 |
| commerce_ml_datasets | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| commerce_ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| commerce_semantic | 1 | 0 | 3 | 1 | 4,260 | 0.0 | 0.0 |
| commerce_silver | 5 | 0 | 143,426,687 | 311 | 16,388,339,362 | 15629.14 | 15.263 |
| commerce_silver_reference | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| default | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_analytics | 18 | 0 | 3,755,732 | 30 | 78,660,041 | 75.02 | 0.073 |
| energy_gold | 25 | 0 | 210,311,542 | 95 | 10,068,482,486 | 9602.05 | 9.377 |
| energy_ml_datasets | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_semantic | 1 | 0 | 5 | 1 | 4,468 | 0.0 | 0.0 |
| energy_silver | 15 | 0 | 66,546,182 | 107 | 5,421,034,552 | 5169.9 | 5.049 |
| energy_silver_reference | 12 | 0 | 24,109,625 | 42 | 819,693,152 | 781.72 | 0.763 |
| genomics_analytics | 6 | 0 | 3,916,302 | 12 | 51,138,141 | 48.77 | 0.048 |
| genomics_bronze | 82 | 0 | 250,870,769 | 243 | 5,956,966,677 | 5681.01 | 5.548 |
| genomics_gold | 32 | 0 | 88,529,775 | 125 | 3,509,988,705 | 3347.39 | 3.269 |
| genomics_silver | 27 | 0 | 417,095,479 | 330 | 19,402,764,863 | 18503.92 | 18.07 |
| genomics_silver_reference | 11 | 0 | 684,244,195 | 611 | 29,749,664,855 | 28371.49 | 27.707 |
| knowledge | 6 | 0 | 27,195,278 | 107 | 5,978,870,204 | 5701.89 | 5.568 |
| ml_datasets_shared | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| quality | 4 | 0 | 1,895,534 | 73 | 39,314,612 | 37.49 | 0.037 |
| shared_conformed | 10 | 0 | 5,883,360 | 13 | 229,971,640 | 219.32 | 0.214 |

## Per table

| schema | table | type | rows | files | size_bytes | size_mb | last_modified | error |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bronze | dwd_device_instrument | MANAGED | 4,008 | 1 | 32,485 | 0.03 | 2026-09-05 10:46:54 |  |
| bronze | dwd_measurement | MANAGED | 177,413,542 | 9 | 453,628,845 | 432.61 | 2026-11-14 01:10:49 |  |
| bronze | dwd_missing_value_periods | MANAGED | 5,897,409 | 7 | 21,224,003 | 20.24 | 2026-09-05 10:47:07 |  |
| bronze | dwd_parameter_unit | MANAGED | 5,418 | 1 | 24,771 | 0.02 | 2026-09-05 10:46:59 |  |
| bronze | dwd_station_geography | MANAGED | 222 | 1 | 6,526 | 0.01 | 2026-09-05 10:46:45 |  |
| bronze | dwd_station_name_history | MANAGED | 106 | 1 | 3,067 | 0.0 | 2026-09-05 10:46:49 |  |
| bronze | energy_charts_physical_flows | MANAGED | 853,584 | 3 | 1,910,744 | 1.82 | 2026-11-12 12:18:49 |  |
| bronze | ga4_events | MANAGED | 779,485 | 1 | 25,902,201 | 24.7 | 2026-09-15 15:58:48 |  |
| bronze | honda_iot_meter | MANAGED | 20,507,677 | 4 | 221,410,704 | 211.15 | 2026-11-12 14:20:54 |  |
| bronze | honda_iot_weather | MANAGED | 3,419,507 | 3 | 41,263,292 | 39.35 | 2026-08-27 13:33:35 |  |
| bronze | mastr_anlagen_eeg | MANAGED | 66,718 | 4 | 2,563,630 | 2.44 | 2026-11-12 14:20:45 |  |
| bronze | mastr_anlagen_kwk | MANAGED | 92,514 | 1 | 2,726,455 | 2.6 | 2026-09-05 23:07:40 |  |
| bronze | mastr_bilanzierungsgebiete | MANAGED | 1,118 | 1 | 17,096 | 0.02 | 2026-09-05 23:09:11 |  |
| bronze | mastr_code_lookup | MANAGED | 1,789 | 5 | 31,440 | 0.03 | 2026-11-12 14:20:57 |  |
| bronze | mastr_einheiten | MANAGED | 170,974 | 6 | 13,736,216 | 13.1 | 2026-11-12 14:20:40 |  |
| bronze | mastr_einheiten_aenderung_netzbetreiberzuordnungen | MANAGED | 226,405 | 1 | 5,739,794 | 5.47 | 2026-09-05 23:09:28 |  |
| bronze | mastr_einheiten_genehmigung | MANAGED | 37,722 | 1 | 1,679,203 | 1.6 | 2026-09-05 23:09:15 |  |
| bronze | mastr_ertuechtigungen | MANAGED | 1,681 | 1 | 33,971 | 0.03 | 2026-09-05 23:09:31 |  |
| bronze | mastr_geloeschte_deaktivierte_einheiten | MANAGED | 244,295 | 1 | 2,923,502 | 2.79 | 2026-09-05 23:09:19 |  |
| bronze | mastr_geloeschte_deaktivierte_marktakteure | MANAGED | 285,315 | 1 | 3,242,833 | 3.09 | 2026-09-05 23:09:23 |  |
| bronze | mastr_katalogkategorien | MANAGED | 123 | 1 | 2,399 | 0.0 | 2026-09-05 23:09:37 |  |
| bronze | mastr_lokationen | MANAGED | 6,776,394 | 4 | 225,606,612 | 215.16 | 2026-11-12 19:32:00 |  |
| bronze | mastr_marktakteure | MANAGED | 5,616,357 | 2 | 120,078,875 | 114.52 | 2026-11-12 19:35:41 |  |
| bronze | mastr_marktakteure_und_rollen | MANAGED | 15,689 | 1 | 338,068 | 0.32 | 2026-09-05 23:08:25 |  |
| bronze | mastr_netzanschlusspunkte | MANAGED | 5,867,232 | 4 | 190,773,766 | 181.94 | 2026-11-12 19:36:05 |  |
| bronze | mastr_netze | MANAGED | 1,740 | 1 | 52,014 | 0.05 | 2026-09-05 23:08:50 |  |
| bronze | power_plant_capacity_additions | MANAGED | 17 | 1 | 3,093 | 0.0 | 2026-09-25 17:49:03 |  |
| bronze | power_plant_list | MANAGED | 2,613 | 1 | 98,131 | 0.09 | 2026-09-22 00:22:21 |  |
| bronze | redispatch_measures | MANAGED | 40,118 | 1 | 392,398 | 0.37 | 2026-09-05 11:15:45 |  |
| bronze | rees46_events | MANAGED | 109,950,743 | 29 | 1,913,065,542 | 1824.44 | 2026-09-06 20:43:12 |  |
| bronze | smard_balancing_energy | MANAGED | 71,136 | 1 | 354,586 | 0.34 | 2026-11-12 12:07:36 |  |
| bronze | smard_commercial_exchanges | MANAGED | 71,136 | 1 | 2,966,729 | 2.83 | 2026-11-12 12:07:42 |  |
| bronze | smard_energy_timeseries | MANAGED | 1,657,760 | 2 | 5,719,140 | 5.45 | 2026-11-12 12:07:30 |  |
| commerce_analytics | customer_journey_daily | MANAGED | 155 | 1 | 14,271 | 0.01 | 2026-11-15 22:35:44 |  |
| commerce_analytics | ga4_demand_daily | MANAGED | 7,408 | 1 | 38,955 | 0.04 | 2026-11-15 22:31:53 |  |
| commerce_analytics | metric_definition | MANAGED | 5 | 1 | 4,185 | 0.0 | 2026-11-15 22:36:59 |  |
| commerce_analytics | product_demand_daily | MANAGED | 3,728 | 1 | 98,933 | 0.09 | 2026-11-15 22:36:50 |  |
| commerce_analytics | rees46_demand_daily | MANAGED | 2,492 | 1 | 29,610 | 0.03 | 2026-11-15 22:33:14 |  |
| commerce_gold | commerce_cart_event | MANAGED | 3,828,439 | 12 | 713,476,424 | 680.42 | 2026-11-16 00:37:28 |  |
| commerce_gold | commerce_product | MANAGED | 208,268 | 1 | 21,392,234 | 20.4 | 2026-11-15 22:19:38 |  |
| commerce_gold | commerce_purchase_event | MANAGED | 1,659,703 | 6 | 310,549,023 | 296.16 | 2026-11-16 00:37:45 |  |
| commerce_gold | commerce_session | MANAGED | 23,186,564 | 27 | 3,184,216,870 | 3036.71 | 2026-11-15 22:19:14 |  |
| commerce_gold | commerce_user | MANAGED | 5,449,658 | 6 | 472,584,918 | 450.69 | 2026-11-15 22:19:25 |  |
| commerce_gold | commerce_view_event | MANAGED | 104,331,838 | 103 | 20,099,238,406 | 19168.13 | 2026-11-15 22:21:43 |  |
| commerce_gold | commerce_web_event | MANAGED | 779,485 | 1 | 85,374,622 | 81.42 | 2026-11-15 22:25:18 |  |
| commerce_semantic | metric_definition | MANAGED | 3 | 1 | 4,260 | 0.0 | 2026-09-28 15:25:22 |  |
| commerce_silver | commerce_event | MANAGED | 110,599,465 | 207 | 12,537,632,320 | 11956.82 | 2026-11-15 12:46:50 |  |
| commerce_silver | commerce_product | MANAGED | 208,268 | 17 | 22,019,649 | 21.0 | 2026-11-15 12:54:43 |  |
| commerce_silver | commerce_session | MANAGED | 23,186,564 | 79 | 3,185,096,619 | 3037.54 | 2026-11-15 12:52:13 |  |
| commerce_silver | commerce_user | MANAGED | 5,449,658 | 6 | 472,581,855 | 450.69 | 2026-11-15 12:53:32 |  |
| commerce_silver | ga4_event_item | MANAGED | 3,982,732 | 2 | 171,008,919 | 163.09 | 2026-11-15 12:48:33 |  |
| energy_analytics | capacity_margin_yearly | MANAGED | 12 | 1 | 3,799 | 0.0 | 2026-11-16 17:54:12 |  |
| energy_analytics | capacity_registry | MANAGED | 120,395 | 2 | 2,278,453 | 2.17 | 2026-11-16 17:53:48 |  |
| energy_analytics | capacity_utilisation_monthly | MANAGED | 3,057 | 1 | 43,895 | 0.04 | 2026-11-16 17:54:03 |  |
| energy_analytics | cross_border_trade | MANAGED | 1,066,971 | 1 | 13,137,314 | 12.53 | 2026-11-16 16:13:29 |  |
| energy_analytics | extreme_weather_stress_event | MANAGED | 96,767 | 8 | 563,433 | 0.54 | 2026-11-16 17:55:13 |  |
| energy_analytics | fleet_lifecycle_event | MANAGED | 997,022 | 4 | 42,087,687 | 40.14 | 2026-11-16 17:53:57 |  |
| energy_analytics | forecast_accuracy | MANAGED | 19,000 | 1 | 422,078 | 0.4 | 2026-11-16 16:13:42 |  |
| energy_analytics | generation_by_carrier_daily | MANAGED | 89,706 | 1 | 587,528 | 0.56 | 2026-11-16 16:12:49 |  |
| energy_analytics | grid_intervention_daily | MANAGED | 11,666 | 1 | 104,766 | 0.1 | 2026-11-16 16:13:35 |  |
| energy_analytics | market_price_daily | MANAGED | 13,840 | 1 | 514,341 | 0.49 | 2026-11-16 16:12:55 |  |
| energy_analytics | market_timeseries | MANAGED | 335,197 | 1 | 5,358,574 | 5.11 | 2026-11-16 16:13:04 |  |
| energy_analytics | metric_definition | MANAGED | 10 | 1 | 5,209 | 0.0 | 2026-11-16 16:13:47 |  |
| energy_analytics | negative_price_event | MANAGED | 2,116 | 1 | 48,224 | 0.05 | 2026-11-16 16:13:20 |  |
| energy_analytics | operator_concentration | MANAGED | 881,624 | 2 | 11,493,171 | 10.96 | 2026-11-16 17:54:20 |  |
| energy_analytics | price_formation_daily | MANAGED | 14,582 | 1 | 214,679 | 0.2 | 2026-11-16 16:13:12 |  |
| energy_analytics | site_energy_weather_daily | MANAGED | 50,952 | 1 | 577,404 | 0.55 | 2026-11-16 17:54:34 |  |
| energy_analytics | support_scheme_buildout | MANAGED | 3,923 | 1 | 39,076 | 0.04 | 2026-11-16 17:54:26 |  |
| energy_analytics | weather_energy_daily | MANAGED | 48,892 | 1 | 1,180,410 | 1.13 | 2026-11-16 17:54:48 |  |
| energy_gold | balancing_area | MANAGED | 1,118 | 1 | 60,403 | 0.06 | 2026-11-16 17:50:44 |  |
| energy_gold | capacity_plan | MANAGED | 52 | 1 | 6,508 | 0.01 | 2026-11-16 17:50:56 |  |
| energy_gold | channel_reading | MANAGED | 63,243,582 | 5 | 547,869,554 | 522.49 | 2026-11-16 15:31:10 |  |
| energy_gold | entity_link | MANAGED | 33,963,083 | 18 | 1,033,355,050 | 985.48 | 2026-11-16 15:33:17 |  |
| energy_gold | forecast_actual | MANAGED | 156,746 | 1 | 18,951,130 | 18.07 | 2026-11-16 15:31:50 |  |
| energy_gold | generation_unit | MANAGED | 170,974 | 1 | 19,174,129 | 18.29 | 2026-11-16 17:50:02 |  |
| energy_gold | grid_connection_point | MANAGED | 5,867,232 | 6 | 360,710,132 | 344.0 | 2026-11-16 18:07:06 |  |
| energy_gold | grid_intervention_event | MANAGED | 39,550 | 1 | 3,938,551 | 3.76 | 2026-11-16 15:30:40 |  |
| energy_gold | grid_location | MANAGED | 6,776,394 | 4 | 249,505,031 | 237.95 | 2026-11-16 17:50:31 |  |
| energy_gold | grid_network | MANAGED | 1,740 | 1 | 69,789 | 0.07 | 2026-11-16 17:50:38 |  |
| energy_gold | iot_device | MANAGED | 198,164 | 1 | 18,116,378 | 17.28 | 2026-11-16 15:30:28 |  |
| energy_gold | market_actor | MANAGED | 5,616,357 | 2 | 137,322,372 | 130.96 | 2026-11-16 17:50:23 |  |
| energy_gold | market_actor_role | MANAGED | 15,689 | 1 | 1,521,251 | 1.45 | 2026-11-16 17:51:03 |  |
| energy_gold | market_energy_component | MANAGED | 3,793,321 | 3 | 191,285,107 | 182.42 | 2026-11-16 15:31:18 |  |
| energy_gold | market_price | MANAGED | 335,197 | 1 | 25,020,984 | 23.86 | 2026-11-16 15:30:47 |  |
| energy_gold | plant_sensor_observation | MANAGED | 9,568 | 1 | 740,896 | 0.71 | 2026-11-16 15:30:34 |  |
| energy_gold | power_plant | MANAGED | 2,613 | 1 | 213,544 | 0.2 | 2026-11-16 17:50:14 |  |
| energy_gold | site_energy_component | MANAGED | 12,823,261 | 8 | 927,596,920 | 884.63 | 2026-11-16 15:31:35 |  |
| energy_gold | support_registration | MANAGED | 159,232 | 1 | 10,912,900 | 10.41 | 2026-11-16 17:50:08 |  |
| energy_gold | unit_event | MANAGED | 795,418 | 3 | 38,556,787 | 36.77 | 2026-11-16 17:50:51 |  |
| energy_gold | weather_air_state | MANAGED | 20,855,956 | 7 | 1,840,152,722 | 1754.91 | 2026-11-16 15:32:11 |  |
| energy_gold | weather_daily | MANAGED | 2,144,456 | 3 | 150,132,921 | 143.18 | 2026-11-16 15:30:21 |  |
| energy_gold | weather_ground_sun | MANAGED | 23,696,000 | 8 | 2,051,201,236 | 1956.18 | 2026-11-16 15:33:00 |  |
| energy_gold | weather_sky | MANAGED | 13,007,447 | 11 | 1,109,062,801 | 1057.68 | 2026-11-16 15:32:43 |  |
| energy_gold | weather_wind_precipitation | MANAGED | 16,638,392 | 5 | 1,333,005,390 | 1271.25 | 2026-11-16 15:32:27 |  |
| energy_semantic | metric_definition | MANAGED | 5 | 1 | 4,468 | 0.0 | 2026-09-28 15:25:45 |  |
| energy_silver | device_telemetry_snapshot | MANAGED | 198,164 | 1 | 18,116,109 | 17.28 | 2026-11-16 15:20:45 |  |
| energy_silver | energy_component | MANAGED | 16,616,582 | 12 | 1,119,023,209 | 1067.18 | 2026-11-15 14:10:46 |  |
| energy_silver | energy_observation | MANAGED | 3,753,157 | 8 | 510,034,690 | 486.41 | 2026-11-15 14:09:57 |  |
| energy_silver | generation_unit | MANAGED | 170,974 | 6 | 19,492,254 | 18.59 | 2026-11-16 17:36:22 |  |
| energy_silver | grid_intervention_event | MANAGED | 39,550 | 1 | 3,938,261 | 3.76 | 2026-11-16 15:20:26 |  |
| energy_silver | plant_operating_sample | MANAGED | 9,568 | 1 | 740,630 | 0.71 | 2026-11-16 15:20:02 |  |
| energy_silver | power_plant_capacity_plan | MANAGED | 13 | 1 | 6,400 | 0.01 | 2026-11-16 15:19:45 |  |
| energy_silver | power_plant_register | MANAGED | 2,613 | 1 | 213,255 | 0.2 | 2026-11-16 15:19:32 |  |
| energy_silver | register_change_event | MANAGED | 756,015 | 3 | 36,615,493 | 34.92 | 2026-11-16 17:40:11 |  |
| energy_silver | register_link | MANAGED | 15,358,350 | 12 | 717,669,589 | 684.42 | 2026-11-16 17:41:18 |  |
| energy_silver | support_registration | MANAGED | 159,232 | 5 | 11,028,626 | 10.52 | 2026-11-16 17:37:40 |  |
| energy_silver | unit_authorisation | MANAGED | 37,722 | 1 | 1,833,843 | 1.75 | 2026-11-16 17:37:56 |  |
| energy_silver | unit_repowering | MANAGED | 1,681 | 1 | 35,922 | 0.03 | 2026-11-16 17:38:08 |  |
| energy_silver | weather_daily | MANAGED | 2,144,456 | 3 | 150,131,277 | 143.18 | 2026-11-15 13:55:27 |  |
| energy_silver | weather_observation | MANAGED | 27,298,105 | 51 | 2,832,154,994 | 2700.95 | 2026-11-15 20:05:01 |  |
| energy_silver_reference | balancing_area | MANAGED | 1,118 | 1 | 60,138 | 0.06 | 2026-11-16 17:39:51 |  |
| energy_silver_reference | energy_carrier_map | MANAGED | 100 | 8 | 36,129 | 0.03 | 2026-11-16 17:46:14 |  |
| energy_silver_reference | grid_connection_point | MANAGED | 5,867,232 | 4 | 202,269,309 | 192.9 | 2026-11-16 17:39:07 |  |
| energy_silver_reference | grid_location | MANAGED | 6,776,394 | 4 | 249,502,363 | 237.94 | 2026-11-16 17:39:38 |  |
| energy_silver_reference | grid_network | MANAGED | 1,740 | 1 | 69,536 | 0.07 | 2026-11-16 17:39:20 |  |
| energy_silver_reference | market_actor | MANAGED | 5,616,357 | 2 | 137,320,035 | 130.96 | 2026-11-16 17:38:35 |  |
| energy_silver_reference | market_actor_role | MANAGED | 15,689 | 1 | 435,942 | 0.42 | 2026-11-16 17:38:51 |  |
| energy_silver_reference | reference_code_list | MANAGED | 1,967 | 9 | 131,271 | 0.13 | 2026-11-16 17:30:01 |  |
| energy_silver_reference | weather_location | MANAGED | 80 | 4 | 30,836 | 0.03 | 2026-11-15 12:39:05 |  |
| energy_silver_reference | weather_missing_value_period | MANAGED | 5,817,858 | 3 | 229,352,841 | 218.73 | 2026-11-15 12:48:12 |  |
| energy_silver_reference | weather_missingness_reconciliation | MANAGED | 1,391 | 1 | 61,923 | 0.06 | 2026-11-15 12:52:14 |  |
| energy_silver_reference | weather_station_period | MANAGED | 9,699 | 4 | 422,829 | 0.4 | 2026-11-15 12:40:54 |  |
| genomics_analytics | classification_change_summary | MANAGED | 192 | 1 | 5,222 | 0.0 | 2026-11-15 23:09:21 |  |
| genomics_analytics | disease_overview | MANAGED | 266,194 | 1 | 14,408,521 | 13.74 | 2026-11-15 23:08:45 |  |
| genomics_analytics | gene_prioritization | MANAGED | 228,118 | 1 | 2,632,599 | 2.51 | 2026-11-15 23:08:38 |  |
| genomics_analytics | genetic_test_panel | MANAGED | 305,239 | 1 | 1,454,982 | 1.39 | 2026-11-15 23:08:51 |  |
| genomics_analytics | population_risk | MANAGED | 533,351 | 7 | 1,569,707 | 1.5 | 2026-11-15 23:09:14 |  |
| genomics_analytics | variant_review_queue | MANAGED | 2,583,208 | 1 | 31,067,110 | 29.63 | 2026-11-15 23:09:06 |  |
| genomics_bronze | aact_browse_terms | MANAGED | 7,003,546 | 12 | 32,237,197 | 30.74 | 2026-11-12 14:08:24 |  |
| genomics_bronze | aact_eligibilities | MANAGED | 610,240 | 6 | 292,237,780 | 278.7 | 2026-11-13 12:12:18 |  |
| genomics_bronze | aact_interventions | MANAGED | 1,034,143 | 4 | 70,770,522 | 67.49 | 2026-11-12 14:08:49 |  |
| genomics_bronze | aact_studies | MANAGED | 611,233 | 2 | 84,971,495 | 81.04 | 2026-11-15 01:28:25 |  |
| genomics_bronze | aact_study_terms | MANAGED | 2,757,633 | 4 | 44,009,529 | 41.97 | 2026-11-12 14:08:33 |  |
| genomics_bronze | acmg_secondary_findings_genes | MANAGED | 94 | 1 | 4,691 | 0.0 | 2026-11-12 14:09:02 |  |
| genomics_bronze | alphafold_residue_confidence | MANAGED | 15,088,388 | 7 | 30,155,041 | 28.76 | 2026-11-12 14:09:10 |  |
| genomics_bronze | alphamissense_variant_scores | MANAGED | 2,534,806 | 4 | 21,012,309 | 20.04 | 2026-11-04 22:34:22 |  |
| genomics_bronze | biogrid_interactions | MANAGED | 1,441,421 | 1 | 35,129,924 | 33.5 | 2026-11-13 12:13:03 |  |
| genomics_bronze | cadd_scores | MANAGED | 9,230,587 | 4 | 32,062,401 | 30.58 | 2026-11-04 20:34:09 |  |
| genomics_bronze | chembl_compounds | MANAGED | 2,897,819 | 5 | 264,141,214 | 251.9 | 2026-11-12 19:34:53 |  |
| genomics_bronze | chembl_uniprot_mapping | MANAGED | 17,257 | 1 | 253,378 | 0.24 | 2026-11-04 20:34:23 |  |
| genomics_bronze | clingen_dosage_sensitivity | MANAGED | 1,539 | 1 | 60,798 | 0.06 | 2026-11-04 20:34:29 |  |
| genomics_bronze | clingen_gene_validity | MANAGED | 3,717 | 1 | 200,789 | 0.19 | 2026-11-04 20:34:33 |  |
| genomics_bronze | clinvar_variant_summary | MANAGED | 23,468,593 | 21 | 1,371,309,896 | 1307.78 | 2026-11-15 00:11:51 |  |
| genomics_bronze | clinvar_variation_allele | MANAGED | 4,694,205 | 3 | 20,007,696 | 19.08 | 2026-11-12 14:10:10 |  |
| genomics_bronze | collectri_regulons | MANAGED | 43,175 | 1 | 556,897 | 0.53 | 2026-11-12 14:10:15 |  |
| genomics_bronze | cpic_pairs | MANAGED | 574 | 1 | 13,556 | 0.01 | 2026-11-04 20:35:21 |  |
| genomics_bronze | dbvar_variants | MANAGED | 222,568 | 2 | 10,821,942 | 10.32 | 2026-11-04 20:35:26 |  |
| genomics_bronze | ddinter_interactions | MANAGED | 222,383 | 1 | 928,085 | 0.89 | 2026-11-12 14:10:19 |  |
| genomics_bronze | depmap_gene_effect | MANAGED | 25,088,674 | 5 | 225,352,043 | 214.91 | 2026-11-12 16:05:13 |  |
| genomics_bronze | depmap_models | MANAGED | 2,554 | 1 | 136,572 | 0.13 | 2026-11-12 14:10:37 |  |
| genomics_bronze | encode_ccre_registry | MANAGED | 2,348,854 | 3 | 30,035,097 | 28.64 | 2026-11-12 14:10:43 |  |
| genomics_bronze | encode_enhancer_gene_links | MANAGED | 66,102,715 | 28 | 1,264,427,746 | 1205.85 | 2026-11-12 15:57:24 |  |
| genomics_bronze | ensembl_compara_homologues | MANAGED | 246,152 | 2 | 4,006,502 | 3.82 | 2026-11-12 14:11:31 |  |
| genomics_bronze | faers_case_code | MANAGED | 618,006 | 2 | 2,965,999 | 2.83 | 2026-11-12 14:21:30 |  |
| genomics_bronze | faers_demo | MANAGED | 819,683 | 3 | 16,258,571 | 15.51 | 2026-11-04 20:35:41 |  |
| genomics_bronze | faers_drug | MANAGED | 3,330,435 | 6 | 38,126,836 | 36.36 | 2026-11-04 20:35:48 |  |
| genomics_bronze | faers_indi | MANAGED | 2,264,273 | 3 | 7,771,809 | 7.41 | 2026-11-04 20:35:55 |  |
| genomics_bronze | faers_reac | MANAGED | 2,725,426 | 3 | 10,041,343 | 9.58 | 2026-11-04 20:36:09 |  |
| genomics_bronze | faers_ther | MANAGED | 730,973 | 1 | 4,967,308 | 4.74 | 2026-11-04 20:36:18 |  |
| genomics_bronze | fda_pgx_biomarkers | MANAGED | 692 | 1 | 11,451 | 0.01 | 2026-11-04 20:36:22 |  |
| genomics_bronze | gene_ages_consensus | MANAGED | 20,331 | 1 | 235,259 | 0.22 | 2026-11-12 19:16:00 |  |
| genomics_bronze | genome_annotation | MANAGED | 633,133 | 2 | 21,693,604 | 20.69 | 2026-11-12 14:21:27 |  |
| genomics_bronze | gnomad_mtdna_variants | MANAGED | 18,164 | 1 | 168,860 | 0.16 | 2026-11-12 14:11:35 |  |
| genomics_bronze | gnomad_variants | MANAGED | 2,723,660 | 2 | 90,630,659 | 86.43 | 2026-11-04 22:19:05 |  |
| genomics_bronze | gtex_gene_median_tpm | MANAGED | 59,033 | 1 | 14,032,223 | 13.38 | 2026-11-04 20:36:52 |  |
| genomics_bronze | gtr_test_condition_gene | MANAGED | 512,630 | 1 | 6,034,927 | 5.76 | 2026-11-04 20:36:58 |  |
| genomics_bronze | hpo_genes_to_phenotype | MANAGED | 333,983 | 1 | 1,823,754 | 1.74 | 2026-11-04 20:37:02 |  |
| genomics_bronze | hpo_phenotype_annotations | MANAGED | 286,651 | 1 | 1,543,954 | 1.47 | 2026-11-04 20:37:07 |  |
| genomics_bronze | hpo_term_relations | MANAGED | 24,436 | 1 | 132,252 | 0.13 | 2026-11-04 20:37:16 |  |
| genomics_bronze | hpo_terms | MANAGED | 20,482 | 1 | 880,437 | 0.84 | 2026-11-04 20:37:11 |  |
| genomics_bronze | iedb_epitopes | MANAGED | 2,910,311 | 13 | 66,999,420 | 63.9 | 2026-11-12 14:11:43 |  |
| genomics_bronze | iedb_mhc_ligands | MANAGED | 5,777,975 | 2 | 115,813,362 | 110.45 | 2026-11-12 16:05:22 |  |
| genomics_bronze | long_range_benchmark_causal_eqtl | MANAGED | 97,922 | 1 | 1,261,892 | 1.2 | 2026-11-04 20:37:21 |  |
| genomics_bronze | long_range_benchmark_pathogenic | MANAGED | 2,361,125 | 2 | 64,806,212 | 61.8 | 2026-11-12 14:21:04 |  |
| genomics_bronze | maps_embeddings_metadata | MANAGED | 132,565 | 13 | 707,381,343 | 674.61 | 2026-11-04 20:37:40 |  |
| genomics_bronze | medgen_concepts | MANAGED | 753,372 | 2 | 16,167,188 | 15.42 | 2026-11-04 20:37:46 |  |
| genomics_bronze | medgen_relations | MANAGED | 1,159,163 | 2 | 13,214,044 | 12.6 | 2026-11-04 20:37:52 |  |
| genomics_bronze | ncbi_gene_gene2refseq | MANAGED | 849,265 | 2 | 12,248,195 | 11.68 | 2026-11-04 20:37:58 |  |
| genomics_bronze | ncbi_gene_gene_info | MANAGED | 193,962 | 1 | 4,718,514 | 4.5 | 2026-11-04 20:38:03 |  |
| genomics_bronze | ncbi_gene_mim2gene_medgen | MANAGED | 28,709 | 1 | 213,466 | 0.2 | 2026-11-04 20:38:07 |  |
| genomics_bronze | opentargets_association_direct | MANAGED | 880,000 | 4 | 195,240,545 | 186.2 | 2026-11-12 14:21:10 |  |
| genomics_bronze | opentargets_disease | MANAGED | 45,896 | 1 | 6,483,652 | 6.18 | 2026-11-04 20:38:31 |  |
| genomics_bronze | opentargets_drug_mechanism_of_action | MANAGED | 6,092 | 1 | 267,810 | 0.26 | 2026-11-04 20:38:35 |  |
| genomics_bronze | opentargets_drug_molecule | MANAGED | 19,170 | 1 | 6,969,746 | 6.65 | 2026-11-04 20:38:40 |  |
| genomics_bronze | opentargets_evidence_gwas_credible_sets | MANAGED | 3,445,546 | 4 | 220,180,404 | 209.98 | 2026-11-13 00:10:34 |  |
| genomics_bronze | opentargets_target | MANAGED | 78,733 | 6 | 28,025,565 | 26.73 | 2026-11-04 20:38:57 |  |
| genomics_bronze | pharmgkb_drugs | MANAGED | 3,770 | 1 | 631,590 | 0.6 | 2026-11-04 20:39:01 |  |
| genomics_bronze | pharmgkb_genes | MANAGED | 25,041 | 1 | 2,792,070 | 2.66 | 2026-11-04 20:39:06 |  |
| genomics_bronze | pharmgkb_relationships | MANAGED | 246,714 | 1 | 2,198,300 | 2.1 | 2026-11-04 20:39:10 |  |
| genomics_bronze | pharmgkb_variants | MANAGED | 7,692 | 1 | 961,390 | 0.92 | 2026-11-04 20:39:14 |  |
| genomics_bronze | proteingym_reference | MANAGED | 2,621 | 2 | 1,129,178 | 1.08 | 2026-11-12 14:21:14 |  |
| genomics_bronze | proteingym_substitutions | MANAGED | 548,557 | 2 | 4,871,643 | 4.65 | 2026-11-12 14:21:18 |  |
| genomics_bronze | reactome_pathway_mappings | MANAGED | 320,280 | 1 | 877,930 | 0.84 | 2026-11-04 20:39:43 |  |
| genomics_bronze | reactome_pathway_relations | MANAGED | 2,905 | 1 | 18,123 | 0.02 | 2026-11-04 20:39:47 |  |
| genomics_bronze | reactome_pathways | MANAGED | 2,889 | 1 | 50,087 | 0.05 | 2026-11-04 20:39:38 |  |
| genomics_bronze | spectralbio_variants | MANAGED | 867 | 1 | 14,103 | 0.01 | 2026-11-04 20:39:56 |  |
| genomics_bronze | string_protein_aliases | MANAGED | 3,889,207 | 4 | 15,118,314 | 14.42 | 2026-11-04 20:40:03 |  |
| genomics_bronze | string_protein_info | MANAGED | 19,699 | 1 | 1,747,819 | 1.67 | 2026-11-04 20:40:07 |  |
| genomics_bronze | string_protein_links | MANAGED | 10,826,054 | 1 | 64,706,278 | 61.71 | 2026-11-15 14:35:47 |  |
| genomics_bronze | tahoe_cell_line_metadata | MANAGED | 1,000 | 1 | 12,002 | 0.01 | 2026-11-04 20:40:21 |  |
| genomics_bronze | tahoe_drug_metadata | MANAGED | 379 | 1 | 25,237 | 0.02 | 2026-11-04 20:40:25 |  |
| genomics_bronze | tahoe_gene_metadata | MANAGED | 62,710 | 1 | 523,565 | 0.5 | 2026-11-04 20:40:29 |  |
| genomics_bronze | tahoe_pseudobulk_differential_expression | MANAGED | 10,772,362 | 3 | 143,116,168 | 136.49 | 2026-11-13 00:10:26 |  |
| genomics_bronze | tahoe_sample_metadata | MANAGED | 1,344 | 1 | 59,022 | 0.06 | 2026-11-04 20:40:47 |  |
| genomics_bronze | tcga_mc3_somatic_mutations | MANAGED | 3,600,963 | 2 | 108,634,227 | 103.6 | 2026-11-11 23:04:15 |  |
| genomics_bronze | thousand_genomes_variants | MANAGED | 544,361 | 2 | 9,622,124 | 9.18 | 2026-11-12 14:12:01 |  |
| genomics_bronze | ucsc_conservation_100way | MANAGED | 8,110,308 | 2 | 32,131,458 | 30.64 | 2026-11-12 14:21:22 |  |
| genomics_bronze | uniprot_entries | MANAGED | 20,431 | 1 | 372,643 | 0.36 | 2026-11-04 20:41:17 |  |
| genomics_bronze | uniprot_features | MANAGED | 283,500 | 1 | 4,209,336 | 4.01 | 2026-11-04 20:41:21 |  |
| genomics_bronze | variant_benchmark_sets | MANAGED | 12,042,448 | 2 | 50,990,366 | 48.63 | 2026-11-12 14:21:38 |  |
| genomics_gold | benchmark_coding_variant | MANAGED | 153,410 | 1 | 8,594,924 | 8.2 | 2026-11-15 23:06:58 |  |
| genomics_gold | benchmark_population_variant | MANAGED | 12,124,849 | 6 | 538,853,554 | 513.89 | 2026-11-15 23:06:45 |  |
| genomics_gold | benchmark_regulatory_variant | MANAGED | 2,419,395 | 2 | 146,137,398 | 139.37 | 2026-11-15 23:06:52 |  |
| genomics_gold | cell_line_profile | MANAGED | 3,450 | 1 | 317,489 | 0.3 | 2026-11-15 23:01:47 |  |
| genomics_gold | clinical_trial_profile | MANAGED | 611,233 | 6 | 375,134,802 | 357.76 | 2026-11-15 23:35:40 |  |
| genomics_gold | clinvar_classification_change | MANAGED | 519,421 | 4 | 3,914,910 | 3.73 | 2026-11-15 23:05:04 |  |
| genomics_gold | disease_profile | MANAGED | 266,194 | 1 | 14,068,818 | 13.42 | 2026-11-15 23:01:21 |  |
| genomics_gold | dms_assay | MANAGED | 2,621 | 1 | 1,230,944 | 1.17 | 2026-11-15 23:07:04 |  |
| genomics_gold | dms_assay_mutant | MANAGED | 548,557 | 1 | 24,362,507 | 23.23 | 2026-11-15 23:07:10 |  |
| genomics_gold | drug_profile | MANAGED | 2,903,856 | 11 | 363,135,894 | 346.31 | 2026-11-15 23:01:37 |  |
| genomics_gold | embedding_split | MANAGED | 2 | 1 | 27,866 | 0.03 | 2026-11-15 23:07:15 |  |
| genomics_gold | epitope_profile | MANAGED | 2,910,311 | 6 | 108,522,455 | 103.5 | 2026-11-15 23:02:31 |  |
| genomics_gold | gene_cancer_profile | MANAGED | 20,862 | 1 | 373,042 | 0.36 | 2026-11-15 22:43:18 |  |
| genomics_gold | gene_clinical_profile | MANAGED | 228,120 | 1 | 2,469,880 | 2.36 | 2026-11-15 22:42:44 |  |
| genomics_gold | gene_disease_profile | MANAGED | 4,621,959 | 2 | 326,467,568 | 311.34 | 2026-11-15 23:03:16 |  |
| genomics_gold | gene_drug_profile | MANAGED | 228,115 | 1 | 1,484,881 | 1.42 | 2026-11-15 22:43:08 |  |
| genomics_gold | gene_expression_profile | MANAGED | 58,492 | 1 | 1,589,223 | 1.52 | 2026-11-15 22:42:55 |  |
| genomics_gold | gene_profile | MANAGED | 228,118 | 1 | 15,020,190 | 14.32 | 2026-11-15 22:42:25 |  |
| genomics_gold | genetic_test_profile | MANAGED | 512,638 | 1 | 19,829,213 | 18.91 | 2026-11-15 23:02:00 |  |
| genomics_gold | multi_value_attribute | MANAGED | 52 | 26 | 138,691 | 0.13 | 2026-11-15 22:41:55 |  |
| genomics_gold | multi_value_score | MANAGED | 60,138 | 16 | 2,738,704 | 2.61 | 2026-11-15 22:51:22 |  |
| genomics_gold | protein_profile | MANAGED | 461,200 | 1 | 18,386,446 | 17.53 | 2026-11-15 23:00:58 |  |
| genomics_gold | somatic_mutation_profile | MANAGED | 3,600,920 | 5 | 270,067,270 | 257.56 | 2026-11-16 02:35:59 |  |
| genomics_gold | somatic_site_profile | MANAGED | 3,091,452 | 4 | 33,092,662 | 31.56 | 2026-11-15 23:04:52 |  |
| genomics_gold | structural_variant_profile | MANAGED | 7,974,145 | 2 | 126,543,970 | 120.68 | 2026-11-16 02:36:10 |  |
| genomics_gold | transcript_profile | MANAGED | 663,754 | 2 | 33,131,148 | 31.6 | 2026-11-15 23:00:44 |  |
| genomics_gold | tumor_sample_profile | MANAGED | 10,295 | 1 | 201,749 | 0.19 | 2026-11-15 23:02:40 |  |
| genomics_gold | variant_clinical_profile | MANAGED | 4,661,766 | 2 | 126,063,136 | 120.22 | 2026-11-16 01:36:36 |  |
| genomics_gold | variant_frequency_profile | MANAGED | 2,744,901 | 3 | 133,563,555 | 127.38 | 2026-11-16 01:36:43 |  |
| genomics_gold | variant_impact_profile | MANAGED | 4,661,766 | 2 | 90,678,174 | 86.48 | 2026-11-16 01:36:24 |  |
| genomics_gold | variant_profile | MANAGED | 22,212,522 | 9 | 572,001,958 | 545.5 | 2026-11-16 01:36:54 |  |
| genomics_gold | variant_score_profile | MANAGED | 10,025,261 | 3 | 151,845,684 | 144.81 | 2026-11-16 01:36:14 |  |
| genomics_silver | benchmark_variant | MANAGED | 14,697,654 | 15 | 680,151,658 | 648.64 | 2026-11-15 22:27:54 |  |
| genomics_silver | clinical_trial | MANAGED | 611,233 | 6 | 353,459,254 | 337.09 | 2026-11-15 14:14:38 |  |
| genomics_silver | dms_assay | MANAGED | 2,621 | 1 | 1,230,939 | 1.17 | 2026-11-15 14:04:31 |  |
| genomics_silver | dms_assay_mutant | MANAGED | 548,557 | 1 | 24,362,436 | 23.23 | 2026-11-15 14:04:47 |  |
| genomics_silver | embedding_split | MANAGED | 2 | 1 | 27,855 | 0.03 | 2026-11-15 14:04:16 |  |
| genomics_silver | entity_relation | MANAGED | 29,634,775 | 25 | 1,450,789,410 | 1383.58 | 2026-11-15 16:32:37 |  |
| genomics_silver | epitope | MANAGED | 2,910,311 | 1 | 84,985,428 | 81.05 | 2026-11-15 14:18:24 |  |
| genomics_silver | epitope_mhc_assay | MANAGED | 5,777,975 | 2 | 121,376,473 | 115.75 | 2026-11-16 13:14:06 |  |
| genomics_silver | faers_case | MANAGED | 819,683 | 1 | 50,668,624 | 48.32 | 2026-11-15 14:12:47 |  |
| genomics_silver | faers_case_drug | MANAGED | 3,328,534 | 4 | 156,116,686 | 148.88 | 2026-11-15 14:13:12 |  |
| genomics_silver | faers_case_item | MANAGED | 6,235,415 | 2 | 259,535,973 | 247.51 | 2026-11-15 14:13:44 |  |
| genomics_silver | gene_dependency | MANAGED | 24,274,029 | 16 | 994,117,652 | 948.06 | 2026-11-15 14:15:49 |  |
| genomics_silver | gene_disease_score_year | MANAGED | 17,316,246 | 10 | 819,287,111 | 781.33 | 2026-11-15 14:09:14 |  |
| genomics_silver | gene_expression_median | MANAGED | 4,014,244 | 1 | 150,248,347 | 143.29 | 2026-11-15 14:09:43 |  |
| genomics_silver | locus_score | MANAGED | 20,025,911 | 13 | 792,341,977 | 755.64 | 2026-11-15 13:59:00 |  |
| genomics_silver | perturbation_expression | MANAGED | 10,772,362 | 10 | 619,777,434 | 591.07 | 2026-11-15 14:12:24 |  |
| genomics_silver | perturbation_sample | MANAGED | 1,344 | 1 | 113,036 | 0.11 | 2026-11-15 14:11:46 |  |
| genomics_silver | protein_residue_score | MANAGED | 15,088,388 | 8 | 532,719,207 | 508.04 | 2026-11-15 14:15:12 |  |
| genomics_silver | regulatory_element_gene_link | MANAGED | 64,167,549 | 64 | 3,750,268,951 | 3576.54 | 2026-11-15 14:17:43 |  |
| genomics_silver | somatic_mutation | MANAGED | 3,600,920 | 8 | 267,201,924 | 254.82 | 2026-11-16 14:32:10 |  |
| genomics_silver | tumor_sample | MANAGED | 10,295 | 1 | 230,381 | 0.22 | 2026-11-15 14:10:46 |  |
| genomics_silver | variant | MANAGED | 22,212,522 | 9 | 630,988,536 | 601.76 | 2026-11-15 13:20:13 |  |
| genomics_silver | variant_classification | MANAGED | 14,214,237 | 13 | 633,342,107 | 604.0 | 2026-11-15 13:58:04 |  |
| genomics_silver | variant_condition | MANAGED | 29,039,994 | 29 | 1,379,639,959 | 1315.73 | 2026-11-15 13:54:38 |  |
| genomics_silver | variant_feature_overlap | MANAGED | 79,803,253 | 55 | 3,444,415,504 | 3284.85 | 2026-11-15 14:02:06 |  |
| genomics_silver | variant_hgvs | MANAGED | 7,832,977 | 9 | 459,349,102 | 438.07 | 2026-11-15 13:28:13 |  |
| genomics_silver | variant_population_frequency | MANAGED | 40,154,448 | 24 | 1,746,018,899 | 1665.13 | 2026-11-15 14:00:19 |  |
| genomics_silver_reference | cell_line | MANAGED | 3,450 | 1 | 322,655 | 0.31 | 2026-11-15 13:12:38 |  |
| genomics_silver_reference | entity_claim | MANAGED | 79,060,972 | 67 | 3,729,531,344 | 3556.76 | 2026-11-15 13:09:17 |  |
| genomics_silver_reference | entity_coordinate | MANAGED | 4,043,423 | 2 | 175,548,706 | 167.42 | 2026-11-15 13:10:17 |  |
| genomics_silver_reference | entity_derived_attribute | MANAGED | 541,705,210 | 476 | 22,933,318,821 | 21870.92 | 2026-11-15 20:58:44 |  |
| genomics_silver_reference | entity_id_crosswalk | MANAGED | 35,792,769 | 32 | 1,558,460,248 | 1486.26 | 2026-11-15 12:52:44 |  |
| genomics_silver_reference | entity_master | MANAGED | 4,523,119 | 14 | 466,378,201 | 444.77 | 2026-11-15 13:11:24 |  |
| genomics_silver_reference | entity_name | MANAGED | 15,446,406 | 14 | 705,348,916 | 672.67 | 2026-11-15 12:56:05 |  |
| genomics_silver_reference | genetic_test | MANAGED | 512,638 | 1 | 20,513,643 | 19.56 | 2026-11-15 13:13:18 |  |
| genomics_silver_reference | ontology_relation | MANAGED | 2,115,093 | 1 | 93,405,635 | 89.08 | 2026-11-15 13:13:42 |  |
| genomics_silver_reference | protein_feature | MANAGED | 550,016 | 2 | 47,168,597 | 44.98 | 2026-11-15 13:12:20 |  |
| genomics_silver_reference | protein_gene_link | MANAGED | 491,099 | 1 | 19,668,089 | 18.76 | 2026-11-15 13:11:51 |  |
| knowledge | rerank_scores | MANAGED | 5,453 | 5 | 210,864 | 0.2 | 2026-10-27 22:32:22 |  |
| knowledge | wikipedia_article | MANAGED | 4,428,568 | 86 | 5,140,253,845 | 4902.13 | 2026-11-15 13:51:19 |  |
| knowledge | wikipedia_chunks | MANAGED | 209,100 | 6 | 358,528,560 | 341.92 | 2026-10-13 23:27:05 |  |
| knowledge | wikipedia_clickstream | MANAGED | 22,509,883 | 8 | 416,598,429 | 397.3 | 2026-11-15 17:39:26 |  |
| knowledge | wikipedia_edges | MANAGED | 8,831 | 1 | 173,401 | 0.17 | 2026-10-13 18:37:21 |  |
| knowledge | wikipedia_selected | MANAGED | 33,443 | 1 | 63,105,105 | 60.18 | 2026-11-15 15:31:35 |  |
| quality | field_class_registry | MANAGED | 3,901 | 4 | 41,270 | 0.04 | 2026-11-15 12:37:39 |  |
| quality | pipeline_watermarks | MANAGED | 2,040 | 22 | 102,635 | 0.1 | 2026-11-16 17:55:16 |  |
| quality | quality_audit_log | MANAGED | 5,543 | 31 | 181,536 | 0.17 | 2026-11-16 17:55:47 |  |
| quality | quarantine | MANAGED | 1,884,050 | 16 | 38,989,171 | 37.18 | 2026-11-16 17:33:56 |  |
| shared_conformed | dim_date | MANAGED | 50,768 | 1 | 13,696 | 0.01 | 2026-11-16 17:48:17 |  |
| shared_conformed | dim_geography | MANAGED | 17 | 1 | 3,301 | 0.0 | 2026-11-15 22:17:13 |  |
| shared_conformed | dim_place | MANAGED | 80 | 1 | 15,325 | 0.01 | 2026-11-16 17:48:23 |  |
| shared_conformed | dim_time | MANAGED | 1,440 | 2 | 5,387 | 0.01 | 2026-11-15 22:17:10 |  |
| shared_conformed | dim_weather_context | MANAGED | 0 | 0 | 0 | 0.0 | 2026-11-15 19:42:21 |  |
| shared_conformed | geo_plz_gemeinde_xref | MANAGED | 0 | 0 | 0 | 0.0 | 2026-11-15 19:42:20 |  |
| shared_conformed | ref_energy_carrier | MANAGED | 40 | 1 | 3,204 | 0.0 | 2026-11-16 17:48:07 |  |
| shared_conformed | ref_energy_carrier_map | MANAGED | 100 | 1 | 9,601 | 0.01 | 2026-11-16 17:48:01 |  |
| shared_conformed | ref_measure | MANAGED | 1,967 | 1 | 96,739 | 0.09 | 2026-11-16 17:48:12 |  |
| shared_conformed | series_coverage | MANAGED | 5,828,948 | 5 | 229,824,387 | 219.18 | 2026-11-16 17:48:32 |  |

## Volumes

| schema | volume | type |
| --- | --- | --- |
| bronze | landing | MANAGED |
| energy_ml_models | foundation_models | MANAGED |
| energy_ml_models | model_libraries | MANAGED |
| knowledge | models | MANAGED |
| knowledge | rerank | MANAGED |

## Errors (0)

None.
