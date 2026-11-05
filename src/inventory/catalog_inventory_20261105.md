# Catalog inventory: energy_commerce_retail_media

Generated: 2026-11-05T23:10:43Z

Size is the data files of the current table version (older versions kept for time travel are not counted). A view has no size. `tables` counts tables and views; `views` is the part that are views.

## Guard lines

- WARNING metastore: 1154 tables and views in all catalogs (guard 400)

## Overall

| schemas | tables | views | rows | files | size_bytes | size_gb | volumes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 24 | 430 | 42 | 2,761,949,360 | 2,019 | 112,373,378,579 | 104.656 | 4 |

## Catalogs

| catalog | tables_and_views | error |
| --- | --- | --- |
| energy_commerce_retail_media | 430 |  |
| samples | 125 |  |
| system | 599 |  |

## Quota limits and usage

| quota | parent | parent_type | used | limit | used_percent |
| --- | --- | --- | --- | --- | --- |
| table-quota | energy_commerce_retail_media.energy_ml_datasets | SecurableType.SCHEMA | 91 | 100 | 91.0 |
| table-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 431 | 500 | 86.2 |
| table-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 72 | 100 | 72.0 |
| table-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 55 | 100 | 55.0 |
| schema-quota | energy_commerce_retail_media | SecurableType.CATALOG | 24 | 50 | 48.0 |
| table-quota | energy_commerce_retail_media.commerce_ml_datasets | SecurableType.SCHEMA | 46 | 100 | 46.0 |
| table-quota | energy_commerce_retail_media.energy_silver | SecurableType.SCHEMA | 34 | 100 | 34.0 |
| table-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 30 | 100 | 30.0 |
| table-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.energy_silver_reference | SecurableType.SCHEMA | 15 | 100 | 15.0 |
| volume-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 7 | 50 | 14.0 |
| table-quota | energy_commerce_retail_media.shared_conformed | SecurableType.SCHEMA | 14 | 100 | 14.0 |
| table-quota | energy_commerce_retail_media.genomics_silver_reference | SecurableType.SCHEMA | 14 | 100 | 14.0 |
| table-quota | energy_commerce_retail_media.commerce_gold | SecurableType.SCHEMA | 10 | 100 | 10.0 |
| table-quota | energy_commerce_retail_media.genomics_silver | SecurableType.SCHEMA | 10 | 100 | 10.0 |
| table-quota | energy_commerce_retail_media.ml_datasets_shared | SecurableType.SCHEMA | 9 | 100 | 9.0 |
| catalog-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 3 | 50 | 6.0 |
| table-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 6 | 100 | 6.0 |
| table-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| table-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| privilege-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 9 | 200 | 4.5 |
| schema-quota | system | SecurableType.CATALOG | 2 | 50 | 4.0 |
| volume-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 2 | 50 | 4.0 |
| volume-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 2 | 50 | 4.0 |
| privilege-quota | energy_commerce_retail_media | SecurableType.CATALOG | 6 | 200 | 3.0 |
| table-quota | energy_commerce_retail_media.commerce_silver | SecurableType.SCHEMA | 3 | 100 | 3.0 |
| privilege-quota | samples | SecurableType.CATALOG | 5 | 200 | 2.5 |
| provider-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 1 | 50 | 2.0 |
| volume-quota | samples.sec | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| privilege-quota | system.ai | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| table-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 2 | 100 | 2.0 |
| privilege-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 4 | 200 | 2.0 |
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
| function-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.clickbench | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.clickbench | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.clickbench | SecurableType.SCHEMA | 0 | 100 | 0.0 |
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
| bronze | 55 | 0 | 338,682,835 | 141 | 3,248,704,476 | 3098.21 | 3.026 |
| commerce_analytics | 2 | 0 | 9,881 | 2 | 69,223 | 0.07 | 0.0 |
| commerce_gold | 10 | 3 | 143,431,474 | 41 | 1,407,419,136 | 1342.22 | 1.311 |
| commerce_ml_datasets | 37 | 7 | 678,393,280 | 341 | 22,078,998,224 | 21056.17 | 20.563 |
| commerce_ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| commerce_semantic | 1 | 0 | 3 | 1 | 4,260 | 0.0 | 0.0 |
| commerce_silver | 3 | 0 | 114,582,197 | 209 | 12,700,673,848 | 12112.31 | 11.828 |
| commerce_silver_reference | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| default | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_analytics | 5 | 0 | 138,871 | 5 | 946,395 | 0.9 | 0.001 |
| energy_gold | 30 | 12 | 169,733,068 | 211 | 10,268,599,394 | 9792.9 | 9.563 |
| energy_ml_datasets | 65 | 19 | 9,254,726 | 70 | 140,451,834 | 133.95 | 0.131 |
| energy_ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_semantic | 1 | 0 | 5 | 1 | 4,468 | 0.0 | 0.0 |
| energy_silver | 34 | 0 | 179,010,526 | 131 | 13,102,224,761 | 12495.26 | 12.202 |
| energy_silver_reference | 15 | 0 | 24,109,525 | 34 | 816,801,064 | 778.96 | 0.761 |
| genomics_bronze | 72 | 0 | 111,159,279 | 176 | 3,045,678,636 | 2904.59 | 2.837 |
| genomics_silver | 25 | 0 | 545,274,130 | 252 | 24,221,582,910 | 23099.5 | 22.558 |
| genomics_silver_reference | 15 | 0 | 201,022,916 | 114 | 9,128,840,295 | 8705.94 | 8.502 |
| knowledge | 6 | 0 | 27,195,278 | 107 | 5,978,870,204 | 5701.89 | 5.568 |
| ml_datasets_shared | 9 | 0 | 208,705,881 | 66 | 5,927,241,586 | 5652.66 | 5.52 |
| ml_models | 27 | 0 | 3,012,222 | 52 | 37,480,751 | 35.74 | 0.035 |
| quality | 4 | 0 | 1,889,402 | 43 | 39,142,343 | 37.33 | 0.036 |
| shared_conformed | 14 | 1 | 6,343,861 | 22 | 229,644,771 | 219.01 | 0.214 |

## Per table

| schema | table | type | rows | files | size_bytes | size_mb | last_modified | error |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bronze | dwd_air_temperature | MANAGED | 17,336,388 | 1 | 42,040,783 | 40.09 | 2026-09-05 12:43:38 |  |
| bronze | dwd_cloud_type | MANAGED | 13,101,463 | 1 | 48,541,877 | 46.29 | 2026-09-05 13:09:11 |  |
| bronze | dwd_cloudiness | MANAGED | 12,973,316 | 8 | 10,905,182 | 10.4 | 2026-09-05 10:44:08 |  |
| bronze | dwd_device_instrument | MANAGED | 4,008 | 1 | 32,485 | 0.03 | 2026-09-05 10:46:54 |  |
| bronze | dwd_dew_point | MANAGED | 13,524,783 | 1 | 37,493,076 | 35.76 | 2026-09-05 13:08:45 |  |
| bronze | dwd_extreme_wind | MANAGED | 6,480,621 | 4 | 8,735,436 | 8.33 | 2026-09-05 10:46:18 |  |
| bronze | dwd_missing_value_periods | MANAGED | 5,897,409 | 7 | 21,224,003 | 20.24 | 2026-09-05 10:47:07 |  |
| bronze | dwd_moisture | MANAGED | 12,449,858 | 2 | 99,508,690 | 94.9 | 2026-09-05 11:34:35 |  |
| bronze | dwd_parameter_unit | MANAGED | 5,418 | 1 | 24,771 | 0.02 | 2026-09-05 10:46:59 |  |
| bronze | dwd_precipitation | MANAGED | 7,154,301 | 5 | 5,527,700 | 5.27 | 2026-09-05 10:44:32 |  |
| bronze | dwd_pressure | MANAGED | 13,227,922 | 1 | 34,549,225 | 32.95 | 2026-09-06 12:38:50 |  |
| bronze | dwd_soil_temperature | MANAGED | 7,183,226 | 8 | 30,237,188 | 28.84 | 2026-09-05 10:45:29 |  |
| bronze | dwd_solar | MANAGED | 5,809,249 | 8 | 24,926,282 | 23.77 | 2026-09-05 10:46:41 |  |
| bronze | dwd_station_geography | MANAGED | 222 | 1 | 6,526 | 0.01 | 2026-09-05 10:46:45 |  |
| bronze | dwd_station_name_history | MANAGED | 106 | 1 | 3,067 | 0.0 | 2026-09-05 10:46:49 |  |
| bronze | dwd_sun | MANAGED | 12,650,564 | 8 | 10,055,692 | 9.59 | 2026-09-05 10:44:52 |  |
| bronze | dwd_visibility | MANAGED | 13,124,582 | 1 | 20,084,903 | 19.15 | 2026-09-05 13:08:52 |  |
| bronze | dwd_weather_phenomena | MANAGED | 12,915,288 | 1 | 17,912,130 | 17.08 | 2026-09-05 13:08:59 |  |
| bronze | dwd_wind | MANAGED | 15,957,198 | 1 | 31,203,087 | 29.76 | 2026-09-05 12:43:28 |  |
| bronze | dwd_wind_synop | MANAGED | 13,524,783 | 1 | 29,537,612 | 28.17 | 2026-09-05 13:08:36 |  |
| bronze | ga4_events | MANAGED | 779,485 | 1 | 25,902,201 | 24.7 | 2026-09-15 15:58:48 |  |
| bronze | honda_iot_cooling | MANAGED | 6,835,855 | 2 | 50,501,340 | 48.16 | 2026-09-21 13:54:28 |  |
| bronze | honda_iot_electricity | MANAGED | 6,835,916 | 2 | 120,150,032 | 114.58 | 2026-09-21 13:54:16 |  |
| bronze | honda_iot_heating | MANAGED | 6,835,906 | 2 | 51,072,493 | 48.71 | 2026-09-21 13:54:22 |  |
| bronze | honda_iot_weather | MANAGED | 3,419,507 | 3 | 41,263,292 | 39.35 | 2026-08-27 13:33:35 |  |
| bronze | mastr_anlagen_eeg_biomasse | MANAGED | 15,644 | 1 | 747,895 | 0.71 | 2026-09-05 23:07:28 |  |
| bronze | mastr_anlagen_eeg_geothermie_gsgk | MANAGED | 132 | 1 | 10,498 | 0.01 | 2026-09-05 23:07:35 |  |
| bronze | mastr_anlagen_eeg_wasser | MANAGED | 7,485 | 1 | 303,236 | 0.29 | 2026-09-05 23:07:31 |  |
| bronze | mastr_anlagen_eeg_wind | MANAGED | 43,457 | 1 | 1,483,125 | 1.41 | 2026-09-05 23:07:23 |  |
| bronze | mastr_anlagen_kwk | MANAGED | 92,514 | 1 | 2,726,455 | 2.6 | 2026-09-05 23:07:40 |  |
| bronze | mastr_bilanzierungsgebiete | MANAGED | 1,118 | 1 | 17,096 | 0.02 | 2026-09-05 23:09:11 |  |
| bronze | mastr_code_lookup | MANAGED | 52 | 4 | 6,336 | 0.01 | 2026-09-21 13:54:32 |  |
| bronze | mastr_einheiten_aenderung_netzbetreiberzuordnungen | MANAGED | 226,405 | 1 | 5,739,794 | 5.47 | 2026-09-05 23:09:28 |  |
| bronze | mastr_einheiten_biomasse | MANAGED | 24,334 | 1 | 2,302,674 | 2.2 | 2026-09-07 02:35:28 |  |
| bronze | mastr_einheiten_genehmigung | MANAGED | 37,722 | 1 | 1,679,203 | 1.6 | 2026-09-05 23:09:15 |  |
| bronze | mastr_einheiten_geothermie_gsgk | MANAGED | 336 | 1 | 48,501 | 0.05 | 2026-09-17 13:42:15 |  |
| bronze | mastr_einheiten_kernkraft | MANAGED | 6 | 1 | 13,789 | 0.01 | 2026-09-07 00:19:17 |  |
| bronze | mastr_einheiten_verbrennung | MANAGED | 94,027 | 1 | 6,779,898 | 6.47 | 2026-09-17 12:45:45 |  |
| bronze | mastr_einheiten_wasser | MANAGED | 8,814 | 1 | 798,504 | 0.76 | 2026-09-17 12:11:05 |  |
| bronze | mastr_einheiten_wind | MANAGED | 43,457 | 1 | 3,733,949 | 3.56 | 2026-09-07 00:19:08 |  |
| bronze | mastr_ertuechtigungen | MANAGED | 1,681 | 1 | 33,971 | 0.03 | 2026-09-05 23:09:31 |  |
| bronze | mastr_geloeschte_deaktivierte_einheiten | MANAGED | 244,295 | 1 | 2,923,502 | 2.79 | 2026-09-05 23:09:19 |  |
| bronze | mastr_geloeschte_deaktivierte_marktakteure | MANAGED | 285,315 | 1 | 3,242,833 | 3.09 | 2026-09-05 23:09:23 |  |
| bronze | mastr_katalogkategorien | MANAGED | 123 | 1 | 2,399 | 0.0 | 2026-09-05 23:09:37 |  |
| bronze | mastr_katalogwerte | MANAGED | 1,737 | 1 | 24,359 | 0.02 | 2026-09-05 23:09:40 |  |
| bronze | mastr_lokationen | MANAGED | 6,776,394 | 4 | 225,606,612 | 215.16 | 2026-11-01 05:09:14 |  |
| bronze | mastr_marktakteure | MANAGED | 5,616,357 | 2 | 120,078,875 | 114.52 | 2026-11-01 05:09:37 |  |
| bronze | mastr_marktakteure_und_rollen | MANAGED | 15,689 | 1 | 338,068 | 0.32 | 2026-09-05 23:08:25 |  |
| bronze | mastr_netzanschlusspunkte | MANAGED | 5,867,232 | 4 | 190,773,766 | 181.94 | 2026-11-01 05:09:44 |  |
| bronze | mastr_netze | MANAGED | 1,740 | 1 | 52,014 | 0.05 | 2026-09-05 23:08:50 |  |
| bronze | power_plant_capacity_additions | MANAGED | 17 | 1 | 3,093 | 0.0 | 2026-09-25 17:49:03 |  |
| bronze | power_plant_list | MANAGED | 2,613 | 1 | 98,131 | 0.09 | 2026-09-22 00:22:21 |  |
| bronze | redispatch_measures | MANAGED | 40,118 | 1 | 392,398 | 0.37 | 2026-09-05 11:15:45 |  |
| bronze | rees46_events | MANAGED | 109,950,743 | 29 | 1,913,065,542 | 1824.44 | 2026-09-06 20:43:12 |  |
| bronze | smard_energy_timeseries | MANAGED | 1,255,904 | 2 | 4,238,887 | 4.04 | 2026-09-05 10:52:37 |  |
| commerce_analytics | ga4_demand_daily | MANAGED | 7,389 | 1 | 39,713 | 0.04 | 2026-09-28 15:24:36 |  |
| commerce_analytics | rees46_demand_daily | MANAGED | 2,492 | 1 | 29,510 | 0.03 | 2026-09-28 15:24:56 |  |
| commerce_gold | ga4_event | VIEW | 779,485 |  |  |  |  |  |
| commerce_gold | ga4_event_item | VIEW | 3,982,732 |  |  |  |  |  |
| commerce_gold | ga4_product | MANAGED | 1,393 | 1 | 53,914 | 0.05 | 2026-09-28 10:57:48 |  |
| commerce_gold | ga4_purchase | MANAGED | 4,786 | 1 | 452,322 | 0.43 | 2026-09-28 11:03:50 |  |
| commerce_gold | ga4_session | MANAGED | 168,963 | 1 | 6,735,536 | 6.42 | 2026-09-28 10:49:50 |  |
| commerce_gold | ga4_user | MANAGED | 133,010 | 1 | 2,937,909 | 2.8 | 2026-09-28 10:51:21 |  |
| commerce_gold | rees46_event | VIEW | 109,819,980 |  |  |  |  |  |
| commerce_gold | rees46_product | MANAGED | 206,876 | 11 | 7,031,106 | 6.71 | 2026-09-28 11:07:38 |  |
| commerce_gold | rees46_session | MANAGED | 23,017,601 | 21 | 1,299,758,367 | 1239.55 | 2026-11-01 05:09:21 |  |
| commerce_gold | rees46_user | MANAGED | 5,316,648 | 5 | 90,449,982 | 86.26 | 2026-10-05 23:06:14 |  |
| commerce_ml_datasets | assembled_lapse_ga4 | VIEW | 73,601 |  |  |  |  |  |
| commerce_ml_datasets | assembled_lapse_rees46 | VIEW | 3,022,289 |  |  |  |  |  |
| commerce_ml_datasets | assembled_next_item_ga4 | VIEW | 3,769,789 |  |  |  |  |  |
| commerce_ml_datasets | assembled_next_item_rees46 | VIEW | 73,916,883 |  |  |  |  |  |
| commerce_ml_datasets | assembled_rl_session_sequences | VIEW | 109,819,980 |  |  |  |  |  |
| commerce_ml_datasets | assembled_session_purchase_ga4 | VIEW | 17,218 |  |  |  |  |  |
| commerce_ml_datasets | assembled_session_purchase_rees46 | VIEW | 14,543,421 |  |  |  |  |  |
| commerce_ml_datasets | dataset_lapse_ga4 | MANAGED | 33,593 | 1 | 825,367 | 0.79 | 2026-10-20 16:16:31 |  |
| commerce_ml_datasets | dataset_lapse_ga4_frozen_v0 | MANAGED | 33,593 | 1 | 825,367 | 0.79 | 2026-10-19 16:38:14 |  |
| commerce_ml_datasets | dataset_lapse_rees46 | MANAGED | 3,022,289 | 3 | 59,826,792 | 57.06 | 2026-10-20 16:16:31 |  |
| commerce_ml_datasets | dataset_lapse_rees46_frozen_v0 | MANAGED | 3,022,289 | 3 | 59,826,792 | 57.06 | 2026-10-19 16:38:20 |  |
| commerce_ml_datasets | dataset_next_item_ga4 | MANAGED | 3,769,789 | 4 | 217,680,692 | 207.6 | 2026-10-30 12:55:43 |  |
| commerce_ml_datasets | dataset_next_item_rees46 | MANAGED | 33,581,598 | 43 | 2,540,451,315 | 2422.76 | 2026-10-30 12:55:35 |  |
| commerce_ml_datasets | dataset_rl_session_sequences | MANAGED | 49,989,095 | 86 | 4,989,186,306 | 4758.06 | 2026-10-30 14:08:37 |  |
| commerce_ml_datasets | dataset_session_purchase_ga4 | MANAGED | 17,218 | 2 | 1,013,985 | 0.97 | 2026-10-20 16:16:29 |  |
| commerce_ml_datasets | dataset_session_purchase_ga4_frozen_v0 | MANAGED | 17,218 | 2 | 1,013,985 | 0.97 | 2026-10-19 16:38:00 |  |
| commerce_ml_datasets | dataset_session_purchase_rees46 | MANAGED | 6,700,427 | 9 | 541,500,853 | 516.42 | 2026-10-20 16:16:29 |  |
| commerce_ml_datasets | dataset_session_purchase_rees46_frozen_v1 | MANAGED | 6,700,427 | 9 | 541,500,853 | 516.42 | 2026-10-19 16:38:08 |  |
| commerce_ml_datasets | features_item_sequence_ga4 | MANAGED | 3,852,091 | 2 | 30,867,850 | 29.44 | 2026-09-30 19:28:13 |  |
| commerce_ml_datasets | features_item_sequence_rees46 | MANAGED | 109,819,980 | 86 | 5,209,540,926 | 4968.21 | 2026-10-07 21:29:43 |  |
| commerce_ml_datasets | features_lapse_ga4 | MANAGED | 73,601 | 2 | 1,083,727 | 1.03 | 2026-09-30 19:22:30 |  |
| commerce_ml_datasets | features_lapse_rees46 | MANAGED | 3,022,289 | 4 | 44,658,868 | 42.59 | 2026-09-30 19:22:05 |  |
| commerce_ml_datasets | features_product_asof_ga4 | MANAGED | 51,188 | 1 | 196,959 | 0.19 | 2026-09-30 19:22:37 |  |
| commerce_ml_datasets | features_product_asof_rees46 | MANAGED | 5,001,492 | 10 | 13,701,638 | 13.07 | 2026-09-30 19:22:02 |  |
| commerce_ml_datasets | features_session_prefix_ga4 | MANAGED | 17,452 | 1 | 723,125 | 0.69 | 2026-09-30 19:22:10 |  |
| commerce_ml_datasets | features_session_prefix_rees46 | MANAGED | 14,747,521 | 14 | 793,483,515 | 756.72 | 2026-10-07 21:29:19 |  |
| commerce_ml_datasets | features_user_asof_ga4 | MANAGED | 168,963 | 1 | 3,984,698 | 3.8 | 2026-09-30 19:22:53 |  |
| commerce_ml_datasets | features_user_asof_rees46 | MANAGED | 23,017,601 | 13 | 738,096,882 | 703.9 | 2026-10-07 21:29:31 |  |
| commerce_ml_datasets | target_basket_ga4 | MANAGED | 4,446 | 1 | 144,125 | 0.14 | 2026-09-30 19:35:14 |  |
| commerce_ml_datasets | target_basket_rees46 | MANAGED | 1,402,758 | 2 | 43,650,706 | 41.63 | 2026-09-30 19:35:06 |  |
| commerce_ml_datasets | target_lapse_ga4 | MANAGED | 73,601 | 2 | 717,156 | 0.68 | 2026-09-30 19:30:22 |  |
| commerce_ml_datasets | target_lapse_rees46 | MANAGED | 3,022,289 | 2 | 13,193,354 | 12.58 | 2026-09-30 19:30:02 |  |
| commerce_ml_datasets | target_next_item_ga4 | MANAGED | 3,769,789 | 1 | 6,757,247 | 6.44 | 2026-09-30 19:34:55 |  |
| commerce_ml_datasets | target_next_item_rees46 | MANAGED | 73,916,883 | 9 | 537,945,785 | 513.03 | 2026-10-07 21:30:51 |  |
| commerce_ml_datasets | target_rl_session_rees46 | MANAGED | 109,819,980 | 18 | 5,287,014,008 | 5042.09 | 2026-09-30 19:31:20 |  |
| commerce_ml_datasets | target_session_purchase_ga4 | MANAGED | 17,218 | 2 | 439,244 | 0.42 | 2026-09-30 19:34:44 |  |
| commerce_ml_datasets | target_session_purchase_rees46 | MANAGED | 14,543,421 | 7 | 399,146,104 | 380.66 | 2026-10-07 21:31:03 |  |
| commerce_semantic | metric_definition | MANAGED | 3 | 1 | 4,260 | 0.0 | 2026-09-28 15:25:22 |  |
| commerce_silver | ga4_event | MANAGED | 779,485 | 2 | 69,319,275 | 66.11 | 2026-10-03 20:51:19 |  |
| commerce_silver | ga4_event_item | MANAGED | 3,982,732 | 2 | 170,994,682 | 163.07 | 2026-10-27 13:53:12 |  |
| commerce_silver | rees46_event | MANAGED | 109,819,980 | 205 | 12,460,359,891 | 11883.13 | 2026-10-27 13:54:23 |  |
| energy_analytics | generation_by_carrier_daily | MANAGED | 99,847 | 1 | 472,922 | 0.45 | 2026-09-28 15:22:24 |  |
| energy_analytics | market_price_daily | MANAGED | 2,897 | 1 | 45,855 | 0.04 | 2026-09-28 15:22:24 |  |
| energy_analytics | site_energy_daily | MANAGED | 16,984 | 1 | 126,308 | 0.12 | 2026-09-28 15:22:59 |  |
| energy_analytics | site_energy_weather_daily | MANAGED | 16,984 | 1 | 247,528 | 0.24 | 2026-09-28 15:24:02 |  |
| energy_analytics | site_weather_daily | MANAGED | 2,159 | 1 | 53,782 | 0.05 | 2026-09-28 15:23:30 |  |
| energy_gold | balancing_area | VIEW | 1,118 |  |  |  |  |  |
| energy_gold | capacity_plan | MANAGED | 13 | 1 | 6,702 | 0.01 | 2026-09-28 10:51:27 |  |
| energy_gold | channel_reading | MANAGED | 63,243,582 | 41 | 2,278,291,230 | 2172.75 | 2026-10-30 22:43:16 |  |
| energy_gold | energy_balance_component | MANAGED | 13,973,351 | 3 | 221,784,893 | 211.51 | 2026-09-30 12:31:35 |  |
| energy_gold | energy_channel | MANAGED | 8 | 1 | 3,018 | 0.0 | 2026-09-28 11:06:40 |  |
| energy_gold | entity_link | MANAGED | 33,963,083 | 45 | 999,891,239 | 953.57 | 2026-10-29 14:04:40 |  |
| energy_gold | forecast_actual | MANAGED | 17,142 | 4 | 541,644 | 0.52 | 2026-09-28 11:06:37 |  |
| energy_gold | generation_forecast | VIEW | 2,897 |  |  |  |  |  |
| energy_gold | generation_unit | MANAGED | 170,974 | 1 | 19,177,378 | 18.29 | 2026-10-02 13:23:01 |  |
| energy_gold | grid_connection_point | VIEW | 5,867,232 |  |  |  |  |  |
| energy_gold | grid_intervention_event | VIEW | 39,550 |  |  |  |  |  |
| energy_gold | grid_location | MANAGED | 6,776,394 | 2 | 249,467,624 | 237.91 | 2026-10-29 14:04:32 |  |
| energy_gold | grid_location_coordinate_conflict | VIEW | 48,904 |  |  |  |  |  |
| energy_gold | grid_network | MANAGED | 1,740 | 1 | 65,665 | 0.06 | 2026-09-28 10:51:22 |  |
| energy_gold | iot_device | MANAGED | 198,164 | 1 | 11,739,516 | 11.2 | 2026-09-28 11:05:53 |  |
| energy_gold | iot_device_observation | MANAGED | 198,164 | 1 | 13,817,628 | 13.18 | 2026-09-28 10:59:09 |  |
| energy_gold | market_actor | MANAGED | 5,616,357 | 2 | 137,302,016 | 130.94 | 2026-10-05 23:05:53 |  |
| energy_gold | market_actor_role | VIEW | 15,689 |  |  |  |  |  |
| energy_gold | market_price | VIEW | 72,689 |  |  |  |  |  |
| energy_gold | place_instrument_period | MANAGED | 3,965 | 1 | 178,016 | 0.17 | 2026-09-28 11:22:09 |  |
| energy_gold | place_parameter_period | MANAGED | 5,412 | 1 | 206,979 | 0.2 | 2026-09-28 11:22:16 |  |
| energy_gold | plant_sensor_observation | VIEW | 9,568 |  |  |  |  |  |
| energy_gold | power_plant | MANAGED | 2,613 | 1 | 213,474 | 0.2 | 2026-09-28 10:57:48 |  |
| energy_gold | registry_change_event | MANAGED | 756,015 | 1 | 23,290,821 | 22.21 | 2026-09-28 10:59:11 |  |
| energy_gold | support_registration | VIEW | 159,232 |  |  |  |  |  |
| energy_gold | unit_authorisation | VIEW | 37,722 |  |  |  |  |  |
| energy_gold | unit_repowering | VIEW | 1,681 |  |  |  |  |  |
| energy_gold | weather_daily | VIEW | 2,144,456 |  |  |  |  |  |
| energy_gold | weather_observation | MANAGED | 27,251,632 | 88 | 5,537,479,343 | 5280.95 | 2026-11-01 05:09:31 |  |
| energy_gold | weather_observation_radiation | MANAGED | 9,153,721 | 16 | 775,142,208 | 739.23 | 2026-11-01 05:09:07 |  |
| energy_ml_datasets | assembled_bias | VIEW | 16,208 |  |  |  |  |  |
| energy_ml_datasets | assembled_capacity_additions | VIEW | 7,371 |  |  |  |  |  |
| energy_ml_datasets | assembled_ccpp | VIEW | 9,527 |  |  |  |  |  |
| energy_ml_datasets | assembled_honda_anomaly | VIEW | 144,691 |  |  |  |  |  |
| energy_ml_datasets | assembled_honda_forecast | VIEW | 723,470 |  |  |  |  |  |
| energy_ml_datasets | assembled_load | VIEW | 22,844 |  |  |  |  |  |
| energy_ml_datasets | assembled_price_daily | VIEW | 2,897 |  |  |  |  |  |
| energy_ml_datasets | assembled_price_quarter_hour | VIEW | 69,792 |  |  |  |  |  |
| energy_ml_datasets | assembled_redispatch | VIEW | 11,324 |  |  |  |  |  |
| energy_ml_datasets | assembled_redispatch_matching | VIEW | 719 |  |  |  |  |  |
| energy_ml_datasets | assembled_rl_pumped_storage | VIEW | 69,681 |  |  |  |  |  |
| energy_ml_datasets | assembled_rl_redispatch | VIEW | 39,440 |  |  |  |  |  |
| energy_ml_datasets | assembled_self_supervised_other | VIEW | 167,535 |  |  |  |  |  |
| energy_ml_datasets | assembled_survival | VIEW | 34,956 |  |  |  |  |  |
| energy_ml_datasets | assembled_weak_supervision | VIEW | 173,756 |  |  |  |  |  |
| energy_ml_datasets | assembled_weather_imputation | VIEW | 1,943,510 |  |  |  |  |  |
| energy_ml_datasets | assembled_zone_generation | VIEW | 42,643 |  |  |  |  |  |
| energy_ml_datasets | dataset_bias | MANAGED | 16,187 | 1 | 2,891,336 | 2.76 | 2026-10-20 16:16:19 |  |
| energy_ml_datasets | dataset_capacity_additions | MANAGED | 1,419 | 1 | 17,699 | 0.02 | 2026-10-20 16:16:20 |  |
| energy_ml_datasets | dataset_ccpp | MANAGED | 9,527 | 1 | 742,926 | 0.71 | 2026-10-20 16:16:22 |  |
| energy_ml_datasets | dataset_honda_anomaly | MANAGED | 144,547 | 1 | 8,217,791 | 7.84 | 2026-10-20 16:16:18 |  |
| energy_ml_datasets | dataset_honda_forecast | MANAGED | 722,750 | 1 | 34,607,977 | 33.0 | 2026-10-20 16:16:26 |  |
| energy_ml_datasets | dataset_load | MANAGED | 17,344 | 1 | 2,112,170 | 2.01 | 2026-10-20 16:16:27 |  |
| energy_ml_datasets | dataset_price_daily | MANAGED | 2,893 | 1 | 843,039 | 0.8 | 2026-10-20 16:16:27 |  |
| energy_ml_datasets | dataset_price_quarter_hour | MANAGED | 69,408 | 1 | 6,690,792 | 6.38 | 2026-10-20 16:16:24 |  |
| energy_ml_datasets | dataset_redispatch | MANAGED | 11,308 | 1 | 465,792 | 0.44 | 2026-10-20 16:16:21 |  |
| energy_ml_datasets | dataset_redispatch_matching | MANAGED | 719 | 1 | 25,437 | 0.02 | 2026-10-20 16:16:25 |  |
| energy_ml_datasets | dataset_rl_pumped_storage | MANAGED | 69,297 | 1 | 2,382,833 | 2.27 | 2026-10-20 16:16:20 |  |
| energy_ml_datasets | dataset_rl_redispatch | MANAGED | 39,314 | 1 | 2,177,745 | 2.08 | 2026-10-20 16:16:22 |  |
| energy_ml_datasets | dataset_self_supervised_other | MANAGED | 161,891 | 2 | 1,233,807 | 1.18 | 2026-10-20 16:16:24 |  |
| energy_ml_datasets | dataset_survival | MANAGED | 34,956 | 1 | 1,869,386 | 1.78 | 2026-10-20 16:16:23 |  |
| energy_ml_datasets | dataset_weak_supervision | MANAGED | 173,756 | 1 | 2,263,833 | 2.16 | 2026-10-20 16:16:28 |  |
| energy_ml_datasets | dataset_weather_imputation | MANAGED | 1,940,822 | 3 | 32,326,966 | 30.83 | 2026-10-20 16:16:17 |  |
| energy_ml_datasets | dataset_zone_generation | MANAGED | 28,910 | 1 | 1,163,426 | 1.11 | 2026-10-20 16:16:17 |  |
| energy_ml_datasets | derived_pv_forecast | VIEW | 2,897 |  |  |  |  |  |
| energy_ml_datasets | features_calendar_market_area | MANAGED | 32,868 | 1 | 117,608 | 0.11 | 2026-09-30 17:09:33 |  |
| energy_ml_datasets | features_honda_site | MANAGED | 723,470 | 1 | 23,824,653 | 22.72 | 2026-09-30 17:11:14 |  |
| energy_ml_datasets | features_hub_height_fill | MANAGED | 35,266 | 8 | 286,996 | 0.27 | 2026-09-30 17:09:48 |  |
| energy_ml_datasets | features_market_daily | MANAGED | 2,897 | 1 | 463,363 | 0.44 | 2026-09-30 17:14:11 |  |
| energy_ml_datasets | features_market_quarter_hour | MANAGED | 69,792 | 1 | 2,809,214 | 2.68 | 2026-09-30 17:15:26 |  |
| energy_ml_datasets | features_pv_forecast_daily | MANAGED | 2,897 | 1 | 43,684 | 0.04 | 2026-09-30 17:11:39 |  |
| energy_ml_datasets | features_redispatch_daily | MANAGED | 11,324 | 1 | 209,177 | 0.2 | 2026-09-30 17:15:25 |  |
| energy_ml_datasets | features_redispatch_match | MANAGED | 719 | 1 | 18,123 | 0.02 | 2026-09-30 17:16:15 |  |
| energy_ml_datasets | features_station_day_radiation | MANAGED | 43,383 | 1 | 202,994 | 0.19 | 2026-09-30 17:10:19 |  |
| energy_ml_datasets | features_station_day_wind | MANAGED | 80,300 | 1 | 553,320 | 0.53 | 2026-09-30 17:09:43 |  |
| energy_ml_datasets | features_unit_static | MANAGED | 35,266 | 1 | 1,193,865 | 1.14 | 2026-09-30 17:12:56 |  |
| energy_ml_datasets | features_zone_capacity_asof | MANAGED | 17,382 | 1 | 56,393 | 0.05 | 2026-09-30 17:10:41 |  |
| energy_ml_datasets | features_zone_weather | MANAGED | 46,184 | 5 | 516,889 | 0.49 | 2026-09-30 17:19:28 |  |
| energy_ml_datasets | gap_limits | MANAGED | 19 | 1 | 2,914 | 0.0 | 2026-09-30 17:36:45 |  |
| energy_ml_datasets | target_capacity_additions | MANAGED | 7,371 | 1 | 64,016 | 0.06 | 2026-09-30 17:41:29 |  |
| energy_ml_datasets | target_ccpp | MANAGED | 9,527 | 1 | 348,224 | 0.33 | 2026-09-30 17:41:30 |  |
| energy_ml_datasets | target_forecast_bias | MANAGED | 16,208 | 6 | 241,778 | 0.23 | 2026-09-30 17:39:06 |  |
| energy_ml_datasets | target_honda_increment | MANAGED | 723,470 | 1 | 3,274,084 | 3.12 | 2026-09-30 17:40:36 |  |
| energy_ml_datasets | target_load | MANAGED | 22,844 | 1 | 153,275 | 0.15 | 2026-09-30 17:38:32 |  |
| energy_ml_datasets | target_price_daily | MANAGED | 2,897 | 1 | 16,787 | 0.02 | 2026-09-30 17:38:33 |  |
| energy_ml_datasets | target_price_quarter_hour | MANAGED | 69,792 | 1 | 138,781 | 0.13 | 2026-09-30 17:38:33 |  |
| energy_ml_datasets | target_redispatch_daily | MANAGED | 11,324 | 4 | 100,245 | 0.1 | 2026-09-30 17:40:14 |  |
| energy_ml_datasets | target_redispatch_match | MANAGED | 719 | 1 | 9,651 | 0.01 | 2026-09-30 17:41:10 |  |
| energy_ml_datasets | target_rl_pumped_storage | MANAGED | 69,681 | 1 | 2,178,917 | 2.08 | 2026-09-30 17:42:15 |  |
| energy_ml_datasets | target_rl_redispatch_events | MANAGED | 39,440 | 1 | 1,714,057 | 1.63 | 2026-09-30 17:41:51 |  |
| energy_ml_datasets | target_unit_survival | MANAGED | 34,956 | 1 | 503,877 | 0.48 | 2026-09-30 17:40:09 |  |
| energy_ml_datasets | target_weak_labels | MANAGED | 173,756 | 3 | 1,117,601 | 1.07 | 2026-09-30 17:41:27 |  |
| energy_ml_datasets | target_zone_generation | MANAGED | 42,643 | 1 | 254,945 | 0.24 | 2026-09-30 17:40:19 |  |
| energy_ml_datasets | zone_code_map | VIEW | 4 |  |  |  |  |  |
| energy_ml_datasets | zone_weather_parameters | MANAGED | 18 | 1 | 3,448 | 0.0 | 2026-09-30 17:17:06 |  |
| energy_semantic | metric_definition | MANAGED | 5 | 1 | 4,468 | 0.0 | 2026-09-28 15:25:45 |  |
| energy_silver | actor_deletion_event | MANAGED | 285,315 | 1 | 4,745,238 | 4.53 | 2026-09-26 14:35:32 |  |
| energy_silver | device_telemetry_snapshot | MANAGED | 198,164 | 1 | 18,116,157 | 17.28 | 2026-09-26 12:58:03 |  |
| energy_silver | electricity_balance | MANAGED | 3,577,595 | 3 | 322,915,430 | 307.96 | 2026-10-28 13:44:24 |  |
| energy_silver | electricity_generation_forecast | MANAGED | 2,897 | 1 | 313,064 | 0.3 | 2026-09-27 19:19:40 |  |
| energy_silver | electricity_price | MANAGED | 72,689 | 1 | 5,030,194 | 4.8 | 2026-09-27 19:19:21 |  |
| energy_silver | energy_meter_reading | MANAGED | 3,417,957 | 2 | 361,720,841 | 344.96 | 2026-10-28 13:44:01 |  |
| energy_silver | generation_unit | MANAGED | 170,974 | 6 | 19,425,640 | 18.53 | 2026-09-29 17:59:13 |  |
| energy_silver | grid_intervention_event | MANAGED | 39,550 | 1 | 3,938,911 | 3.76 | 2026-09-30 17:53:20 |  |
| energy_silver | grid_location_coordinate_conflict | MANAGED | 48,904 | 1 | 635,351 | 0.61 | 2026-09-27 10:45:08 |  |
| energy_silver | grid_operator_change_event | MANAGED | 226,405 | 1 | 13,428,001 | 12.81 | 2026-09-26 14:35:48 |  |
| energy_silver | plant_operating_sample | MANAGED | 9,568 | 1 | 740,682 | 0.71 | 2026-09-26 12:52:05 |  |
| energy_silver | power_plant_capacity_plan | MANAGED | 13 | 1 | 5,894 | 0.01 | 2026-09-26 14:28:12 |  |
| energy_silver | power_plant_register | MANAGED | 2,613 | 1 | 210,911 | 0.2 | 2026-09-27 18:58:58 |  |
| energy_silver | register_link | MANAGED | 15,358,350 | 6 | 706,589,925 | 673.86 | 2026-10-27 13:54:30 |  |
| energy_silver | support_registration | MANAGED | 159,232 | 5 | 11,027,671 | 10.52 | 2026-09-26 14:27:59 |  |
| energy_silver | thermal_energy | MANAGED | 3,417,960 | 2 | 249,399,955 | 237.85 | 2026-10-28 13:44:18 |  |
| energy_silver | unit_authorisation | MANAGED | 37,722 | 1 | 1,834,114 | 1.75 | 2026-09-26 14:28:25 |  |
| energy_silver | unit_deletion_event | MANAGED | 244,295 | 1 | 4,278,051 | 4.08 | 2026-09-26 14:35:11 |  |
| energy_silver | unit_repowering | MANAGED | 1,681 | 1 | 35,971 | 0.03 | 2026-09-26 14:28:47 |  |
| energy_silver | weather_cloud | MANAGED | 12,649,094 | 8 | 992,542,765 | 946.56 | 2026-10-27 13:54:43 |  |
| energy_silver | weather_daily | MANAGED | 2,144,456 | 4 | 150,064,863 | 143.11 | 2026-10-27 13:54:55 |  |
| energy_silver | weather_humidity | MANAGED | 17,468,859 | 9 | 1,318,829,562 | 1257.73 | 2026-10-27 13:53:34 |  |
| energy_silver | weather_longwave_radiation | MANAGED | 1,764,083 | 2 | 131,355,806 | 125.27 | 2026-10-27 13:54:49 |  |
| energy_silver | weather_precipitation | MANAGED | 6,954,742 | 4 | 488,285,381 | 465.67 | 2026-10-27 13:54:36 |  |
| energy_silver | weather_present_weather | MANAGED | 7,866,901 | 5 | 578,828,202 | 552.01 | 2026-10-27 13:52:56 |  |
| energy_silver | weather_pressure | MANAGED | 12,354,629 | 8 | 930,428,302 | 887.33 | 2026-10-27 13:53:42 |  |
| energy_silver | weather_soil_temperature | MANAGED | 6,986,583 | 5 | 536,408,602 | 511.56 | 2026-10-27 13:54:03 |  |
| energy_silver | weather_solar_geometry | MANAGED | 5,809,249 | 3 | 437,833,423 | 417.55 | 2026-10-27 13:53:49 |  |
| energy_silver | weather_solar_radiation | MANAGED | 9,035,706 | 6 | 672,275,718 | 641.13 | 2026-10-27 13:53:26 |  |
| energy_silver | weather_sunshine_duration | MANAGED | 18,292,694 | 7 | 1,317,002,992 | 1255.99 | 2026-10-27 13:53:05 |  |
| energy_silver | weather_temperature | MANAGED | 20,854,453 | 15 | 1,627,833,103 | 1552.42 | 2026-10-27 13:53:19 |  |
| energy_silver | weather_uv_index | MANAGED | 6,727 | 2 | 474,823 | 0.45 | 2026-09-26 13:33:42 |  |
| energy_silver | weather_visibility | MANAGED | 12,927,956 | 6 | 932,407,131 | 889.21 | 2026-10-27 13:53:56 |  |
| energy_silver | weather_wind | MANAGED | 16,622,510 | 10 | 1,263,262,087 | 1204.74 | 2026-10-27 13:52:46 |  |
| energy_silver_reference | balancing_area | MANAGED | 1,118 | 1 | 56,473 | 0.05 | 2026-09-27 10:44:58 |  |
| energy_silver_reference | grid_connection_point | MANAGED | 5,867,232 | 4 | 202,269,297 | 192.9 | 2026-10-28 13:44:30 |  |
| energy_silver_reference | grid_location | MANAGED | 6,776,394 | 4 | 246,863,118 | 235.43 | 2026-10-28 13:44:09 |  |
| energy_silver_reference | grid_network | MANAGED | 1,740 | 1 | 61,120 | 0.06 | 2026-09-27 10:44:48 |  |
| energy_silver_reference | market_actor | MANAGED | 5,616,357 | 2 | 137,132,014 | 130.78 | 2026-10-27 13:54:10 |  |
| energy_silver_reference | market_actor_role | MANAGED | 15,689 | 1 | 436,069 | 0.42 | 2026-09-26 14:33:12 |  |
| energy_silver_reference | mastr_code_list | MANAGED | 1,912 | 1 | 92,369 | 0.09 | 2026-09-26 12:47:16 |  |
| energy_silver_reference | weather_location | MANAGED | 80 | 4 | 30,818 | 0.03 | 2026-09-26 12:47:38 |  |
| energy_silver_reference | weather_location_validity | MANAGED | 222 | 1 | 18,690 | 0.02 | 2026-09-26 12:48:09 |  |
| energy_silver_reference | weather_missing_value_period | MANAGED | 5,817,858 | 3 | 229,352,829 | 218.73 | 2026-10-27 13:54:17 |  |
| energy_silver_reference | weather_missingness_reconciliation | MANAGED | 1,391 | 1 | 61,867 | 0.06 | 2026-09-28 12:37:51 |  |
| energy_silver_reference | weather_parameter_catalog | MANAGED | 55 | 8 | 32,218 | 0.03 | 2026-09-26 12:50:10 |  |
| energy_silver_reference | weather_parameter_period | MANAGED | 5,412 | 1 | 206,739 | 0.2 | 2026-09-26 12:49:53 |  |
| energy_silver_reference | weather_station_instrument | MANAGED | 3,965 | 1 | 177,774 | 0.17 | 2026-09-26 12:49:36 |  |
| energy_silver_reference | weather_station_name_history | MANAGED | 100 | 1 | 9,669 | 0.01 | 2026-09-26 12:49:21 |  |
| genomics_bronze | alphamissense_variant_scores | MANAGED | 2,534,806 | 4 | 21,012,309 | 20.04 | 2026-11-04 22:34:22 |  |
| genomics_bronze | biogrid_interactions | MANAGED | 1,441,421 | 11 | 40,620,759 | 38.74 | 2026-11-04 20:33:57 |  |
| genomics_bronze | cadd_scores | MANAGED | 9,230,587 | 4 | 32,062,401 | 30.58 | 2026-11-04 20:34:09 |  |
| genomics_bronze | chembl_compounds | MANAGED | 2,897,819 | 5 | 264,141,214 | 251.9 | 2026-11-05 19:07:02 |  |
| genomics_bronze | chembl_uniprot_mapping | MANAGED | 17,257 | 1 | 253,378 | 0.24 | 2026-11-04 20:34:23 |  |
| genomics_bronze | clingen_dosage_sensitivity | MANAGED | 1,539 | 1 | 60,798 | 0.06 | 2026-11-04 20:34:29 |  |
| genomics_bronze | clingen_gene_validity | MANAGED | 3,717 | 1 | 200,789 | 0.19 | 2026-11-04 20:34:33 |  |
| genomics_bronze | clinvar_variant_summary | MANAGED | 11,839,175 | 12 | 699,431,506 | 667.03 | 2026-11-04 22:11:57 |  |
| genomics_bronze | clinvar_variation_allele | MANAGED | 4,694,205 | 3 | 20,007,696 | 19.08 | 2026-11-04 20:35:11 |  |
| genomics_bronze | clinvar_vs_benign_variants | MANAGED | 50,164 | 1 | 1,134,360 | 1.08 | 2026-11-04 20:35:16 |  |
| genomics_bronze | cpic_pairs | MANAGED | 574 | 1 | 13,556 | 0.01 | 2026-11-04 20:35:21 |  |
| genomics_bronze | dbvar_variants | MANAGED | 222,568 | 2 | 10,821,942 | 10.32 | 2026-11-04 20:35:26 |  |
| genomics_bronze | encode_ccre_registry | MANAGED | 2,348,854 | 3 | 30,035,097 | 28.64 | 2026-11-04 20:35:34 |  |
| genomics_bronze | faers_demo | MANAGED | 819,683 | 3 | 16,258,571 | 15.51 | 2026-11-04 20:35:41 |  |
| genomics_bronze | faers_drug | MANAGED | 3,330,435 | 6 | 38,126,836 | 36.36 | 2026-11-04 20:35:48 |  |
| genomics_bronze | faers_indi | MANAGED | 2,264,273 | 3 | 7,771,809 | 7.41 | 2026-11-04 20:35:55 |  |
| genomics_bronze | faers_outc | MANAGED | 595,285 | 1 | 2,860,143 | 2.73 | 2026-11-04 20:36:00 |  |
| genomics_bronze | faers_reac | MANAGED | 2,725,426 | 3 | 10,041,343 | 9.58 | 2026-11-04 20:36:09 |  |
| genomics_bronze | faers_rpsr | MANAGED | 22,721 | 1 | 104,813 | 0.1 | 2026-11-04 20:36:13 |  |
| genomics_bronze | faers_ther | MANAGED | 730,973 | 1 | 4,967,308 | 4.74 | 2026-11-04 20:36:18 |  |
| genomics_bronze | fda_pgx_biomarkers | MANAGED | 692 | 1 | 11,451 | 0.01 | 2026-11-04 20:36:22 |  |
| genomics_bronze | gencode_annotation | MANAGED | 425,844 | 4 | 17,131,847 | 16.34 | 2026-11-04 20:36:29 |  |
| genomics_bronze | gnomad_balanced_variants | MANAGED | 11,992,284 | 6 | 49,856,905 | 47.55 | 2026-11-04 20:36:46 |  |
| genomics_bronze | gnomad_variants | MANAGED | 2,723,660 | 2 | 90,630,659 | 86.43 | 2026-11-04 22:19:05 |  |
| genomics_bronze | gtex_gene_median_tpm | MANAGED | 59,033 | 1 | 14,032,223 | 13.38 | 2026-11-04 20:36:52 |  |
| genomics_bronze | gtr_test_condition_gene | MANAGED | 512,630 | 1 | 6,034,927 | 5.76 | 2026-11-04 20:36:58 |  |
| genomics_bronze | hpo_genes_to_phenotype | MANAGED | 333,983 | 1 | 1,823,754 | 1.74 | 2026-11-04 20:37:02 |  |
| genomics_bronze | hpo_phenotype_annotations | MANAGED | 286,651 | 1 | 1,543,954 | 1.47 | 2026-11-04 20:37:07 |  |
| genomics_bronze | hpo_term_relations | MANAGED | 24,436 | 1 | 132,252 | 0.13 | 2026-11-04 20:37:16 |  |
| genomics_bronze | hpo_terms | MANAGED | 20,482 | 1 | 880,437 | 0.84 | 2026-11-04 20:37:11 |  |
| genomics_bronze | long_range_benchmark_causal_eqtl | MANAGED | 97,922 | 1 | 1,261,892 | 1.2 | 2026-11-04 20:37:21 |  |
| genomics_bronze | long_range_benchmark_pathogenic_coding | MANAGED | 39,652 | 1 | 737,065 | 0.7 | 2026-11-04 20:37:25 |  |
| genomics_bronze | long_range_benchmark_pathogenic_non_coding | MANAGED | 2,321,473 | 8 | 65,713,590 | 62.67 | 2026-11-04 20:37:31 |  |
| genomics_bronze | maps_embeddings_metadata | MANAGED | 132,565 | 13 | 707,381,343 | 674.61 | 2026-11-04 20:37:40 |  |
| genomics_bronze | medgen_concepts | MANAGED | 753,372 | 2 | 16,167,188 | 15.42 | 2026-11-04 20:37:46 |  |
| genomics_bronze | medgen_relations | MANAGED | 1,159,163 | 2 | 13,214,044 | 12.6 | 2026-11-04 20:37:52 |  |
| genomics_bronze | ncbi_gene_gene2refseq | MANAGED | 849,265 | 2 | 12,248,195 | 11.68 | 2026-11-04 20:37:58 |  |
| genomics_bronze | ncbi_gene_gene_info | MANAGED | 193,962 | 1 | 4,718,514 | 4.5 | 2026-11-04 20:38:03 |  |
| genomics_bronze | ncbi_gene_mim2gene_medgen | MANAGED | 28,709 | 1 | 213,466 | 0.2 | 2026-11-04 20:38:07 |  |
| genomics_bronze | opentargets_association_by_datasource_direct | MANAGED | 440,000 | 2 | 93,665,363 | 89.33 | 2026-11-04 22:18:59 |  |
| genomics_bronze | opentargets_association_overall_direct | MANAGED | 440,000 | 2 | 96,109,928 | 91.66 | 2026-11-04 22:19:11 |  |
| genomics_bronze | opentargets_disease | MANAGED | 45,896 | 1 | 6,483,652 | 6.18 | 2026-11-04 20:38:31 |  |
| genomics_bronze | opentargets_drug_mechanism_of_action | MANAGED | 6,092 | 1 | 267,810 | 0.26 | 2026-11-04 20:38:35 |  |
| genomics_bronze | opentargets_drug_molecule | MANAGED | 19,170 | 1 | 6,969,746 | 6.65 | 2026-11-04 20:38:40 |  |
| genomics_bronze | opentargets_evidence_gwas_credible_sets | MANAGED | 3,445,546 | 4 | 220,180,404 | 209.98 | 2026-11-05 23:08:58 |  |
| genomics_bronze | opentargets_target | MANAGED | 78,733 | 6 | 28,025,565 | 26.73 | 2026-11-04 20:38:57 |  |
| genomics_bronze | pharmgkb_drugs | MANAGED | 3,770 | 1 | 631,590 | 0.6 | 2026-11-04 20:39:01 |  |
| genomics_bronze | pharmgkb_genes | MANAGED | 25,041 | 1 | 2,792,070 | 2.66 | 2026-11-04 20:39:06 |  |
| genomics_bronze | pharmgkb_relationships | MANAGED | 246,714 | 1 | 2,198,300 | 2.1 | 2026-11-04 20:39:10 |  |
| genomics_bronze | pharmgkb_variants | MANAGED | 7,692 | 1 | 961,390 | 0.92 | 2026-11-04 20:39:14 |  |
| genomics_bronze | proteingym_clinical_reference | MANAGED | 2,525 | 1 | 1,068,180 | 1.02 | 2026-11-04 20:39:20 |  |
| genomics_bronze | proteingym_clinical_substitutions | MANAGED | 62,727 | 1 | 308,074 | 0.29 | 2026-11-04 20:39:24 |  |
| genomics_bronze | proteingym_dms_reference | MANAGED | 96 | 1 | 52,100 | 0.05 | 2026-11-04 20:39:28 |  |
| genomics_bronze | proteingym_dms_substitutions | MANAGED | 485,830 | 1 | 4,562,048 | 4.35 | 2026-11-04 20:39:33 |  |
| genomics_bronze | reactome_pathway_mappings | MANAGED | 320,280 | 1 | 877,930 | 0.84 | 2026-11-04 20:39:43 |  |
| genomics_bronze | reactome_pathway_relations | MANAGED | 2,905 | 1 | 18,123 | 0.02 | 2026-11-04 20:39:47 |  |
| genomics_bronze | reactome_pathways | MANAGED | 2,889 | 1 | 50,087 | 0.05 | 2026-11-04 20:39:38 |  |
| genomics_bronze | refseq_annotation | MANAGED | 207,289 | 1 | 5,143,190 | 4.9 | 2026-11-04 20:39:52 |  |
| genomics_bronze | spectralbio_variants | MANAGED | 867 | 1 | 14,103 | 0.01 | 2026-11-04 20:39:56 |  |
| genomics_bronze | string_protein_aliases | MANAGED | 3,889,207 | 4 | 15,118,314 | 14.42 | 2026-11-04 20:40:03 |  |
| genomics_bronze | string_protein_info | MANAGED | 19,699 | 1 | 1,747,819 | 1.67 | 2026-11-04 20:40:07 |  |
| genomics_bronze | string_protein_links | MANAGED | 10,826,054 | 9 | 65,640,604 | 62.6 | 2026-11-04 20:40:17 |  |
| genomics_bronze | tahoe_cell_line_metadata | MANAGED | 1,000 | 1 | 12,002 | 0.01 | 2026-11-04 20:40:21 |  |
| genomics_bronze | tahoe_drug_metadata | MANAGED | 379 | 1 | 25,237 | 0.02 | 2026-11-04 20:40:25 |  |
| genomics_bronze | tahoe_gene_metadata | MANAGED | 62,710 | 1 | 523,565 | 0.5 | 2026-11-04 20:40:29 |  |
| genomics_bronze | tahoe_pseudobulk_differential_expression | MANAGED | 10,772,362 | 3 | 143,116,168 | 136.49 | 2026-11-05 23:08:53 |  |
| genomics_bronze | tahoe_sample_metadata | MANAGED | 1,344 | 1 | 59,022 | 0.06 | 2026-11-04 20:40:47 |  |
| genomics_bronze | tcga_mc3_somatic_mutations | MANAGED | 3,600,963 | 2 | 108,634,227 | 103.6 | 2026-11-04 22:19:17 |  |
| genomics_bronze | ucsc_conservation_phastcons_100way | MANAGED | 4,055,154 | 2 | 13,755,146 | 13.12 | 2026-11-04 20:41:05 |  |
| genomics_bronze | ucsc_conservation_phylop_100way | MANAGED | 4,055,154 | 2 | 18,424,566 | 17.57 | 2026-11-04 20:41:12 |  |
| genomics_bronze | uniprot_entries | MANAGED | 20,431 | 1 | 372,643 | 0.36 | 2026-11-04 20:41:17 |  |
| genomics_bronze | uniprot_features | MANAGED | 283,500 | 1 | 4,209,336 | 4.01 | 2026-11-04 20:41:21 |  |
| genomics_silver | benchmark_variant | MANAGED | 14,697,654 | 13 | 672,588,777 | 641.43 | 2026-11-05 23:03:19 |  |
| genomics_silver | dms_assay | MANAGED | 2,621 | 1 | 1,228,412 | 1.17 | 2026-11-05 22:40:44 |  |
| genomics_silver | dms_assay_mutant | MANAGED | 548,557 | 1 | 24,350,687 | 23.22 | 2026-11-05 22:41:00 |  |
| genomics_silver | drug_gene_relation | MANAGED | 26,959 | 1 | 1,303,594 | 1.24 | 2026-11-05 22:42:53 |  |
| genomics_silver | embedding_split | MANAGED | 2 | 1 | 27,803 | 0.03 | 2026-11-05 22:40:29 |  |
| genomics_silver | faers_case | MANAGED | 819,683 | 2 | 53,282,309 | 50.81 | 2026-11-05 22:48:11 |  |
| genomics_silver | faers_case_drug | MANAGED | 3,328,534 | 3 | 147,028,449 | 140.22 | 2026-11-05 22:48:31 |  |
| genomics_silver | gene_disease_association | MANAGED | 4,532,366 | 5 | 266,277,342 | 253.94 | 2026-11-05 22:42:09 |  |
| genomics_silver | gene_expression_median | MANAGED | 4,014,244 | 1 | 150,305,224 | 143.34 | 2026-11-05 22:44:43 |  |
| genomics_silver | pathway_membership | MANAGED | 320,280 | 1 | 12,509,356 | 11.93 | 2026-11-05 22:43:58 |  |
| genomics_silver | perturbation_expression | MANAGED | 10,772,362 | 7 | 595,153,025 | 567.58 | 2026-11-05 22:47:26 |  |
| genomics_silver | perturbation_sample | MANAGED | 1,344 | 1 | 112,999 | 0.11 | 2026-11-05 22:47:06 |  |
| genomics_silver | pharmacogenomic_association | MANAGED | 225,864 | 1 | 10,849,260 | 10.35 | 2026-11-05 22:43:09 |  |
| genomics_silver | phenotype_annotation | MANAGED | 605,426 | 1 | 26,063,096 | 24.86 | 2026-11-05 22:42:30 |  |
| genomics_silver | position_score | MANAGED | 8,110,026 | 2 | 300,650,022 | 286.72 | 2026-11-05 22:34:47 |  |
| genomics_silver | protein_interaction | MANAGED | 12,267,475 | 8 | 556,002,751 | 530.25 | 2026-11-05 23:03:16 |  |
| genomics_silver | somatic_mutation | MANAGED | 3,600,920 | 9 | 262,506,286 | 250.35 | 2026-11-05 22:46:49 |  |
| genomics_silver | tumor_sample | MANAGED | 10,295 | 1 | 230,328 | 0.22 | 2026-11-05 22:45:37 |  |
| genomics_silver | variant | MANAGED | 22,174,902 | 10 | 604,566,115 | 576.56 | 2026-11-05 23:03:44 |  |
| genomics_silver | variant_classification | MANAGED | 8,245,025 | 10 | 386,753,083 | 368.84 | 2026-11-05 23:03:27 |  |
| genomics_silver | variant_condition | MANAGED | 7,247,815 | 8 | 367,643,124 | 350.61 | 2026-11-05 23:03:13 |  |
| genomics_silver | variant_feature_overlap | MANAGED | 387,797,108 | 144 | 17,311,302,118 | 16509.34 | 2026-11-05 22:37:45 |  |
| genomics_silver | variant_hgvs | MANAGED | 7,174,993 | 7 | 404,299,585 | 385.57 | 2026-11-05 23:03:42 |  |
| genomics_silver | variant_population_frequency | MANAGED | 36,833,790 | 11 | 1,582,095,130 | 1508.8 | 2026-11-05 22:35:25 |  |
| genomics_silver | variant_score | MANAGED | 11,915,885 | 3 | 484,454,035 | 462.01 | 2026-11-05 22:34:25 |  |
| genomics_silver_reference | cell_line | MANAGED | 99 | 1 | 13,731 | 0.01 | 2026-11-05 22:17:49 |  |
| genomics_silver_reference | disease | MANAGED | 266,194 | 1 | 7,229,652 | 6.89 | 2026-11-05 22:17:03 |  |
| genomics_silver_reference | drug | MANAGED | 2,903,856 | 2 | 296,559,416 | 282.82 | 2026-11-05 22:17:30 |  |
| genomics_silver_reference | entity_claim | MANAGED | 43,826,104 | 29 | 2,157,811,976 | 2057.85 | 2026-11-05 22:13:32 |  |
| genomics_silver_reference | entity_coordinate | MANAGED | 4,043,423 | 2 | 173,895,544 | 165.84 | 2026-11-05 22:14:08 |  |
| genomics_silver_reference | entity_derived_attribute | MANAGED | 123,758,692 | 49 | 5,437,923,754 | 5186.01 | 2026-11-05 22:53:56 |  |
| genomics_silver_reference | entity_id_crosswalk | MANAGED | 15,569,705 | 16 | 643,764,522 | 613.94 | 2026-11-05 22:10:26 |  |
| genomics_silver_reference | entity_name | MANAGED | 7,270,767 | 7 | 288,837,520 | 275.46 | 2026-11-05 23:03:14 |  |
| genomics_silver_reference | gene | MANAGED | 236,181 | 1 | 8,965,456 | 8.55 | 2026-11-05 22:14:59 |  |
| genomics_silver_reference | genetic_test | MANAGED | 63,461 | 1 | 2,809,522 | 2.68 | 2026-11-05 22:18:03 |  |
| genomics_silver_reference | ontology_relation | MANAGED | 1,184,771 | 1 | 46,675,507 | 44.51 | 2026-11-05 22:18:21 |  |
| genomics_silver_reference | protein | MANAGED | 461,200 | 1 | 5,996,480 | 5.72 | 2026-11-05 22:15:37 |  |
| genomics_silver_reference | protein_feature | MANAGED | 283,500 | 1 | 20,112,512 | 19.18 | 2026-11-05 22:16:06 |  |
| genomics_silver_reference | protein_gene_link | MANAGED | 491,209 | 1 | 19,669,111 | 18.76 | 2026-11-05 22:15:52 |  |
| genomics_silver_reference | transcript | MANAGED | 663,754 | 1 | 18,575,592 | 17.72 | 2026-11-05 22:15:20 |  |
| knowledge | rerank_scores | MANAGED | 5,453 | 5 | 210,864 | 0.2 | 2026-10-27 22:32:22 |  |
| knowledge | wikipedia_article | MANAGED | 4,428,568 | 86 | 5,140,253,845 | 4902.13 | 2026-10-26 21:16:59 |  |
| knowledge | wikipedia_chunks | MANAGED | 209,100 | 6 | 358,528,560 | 341.92 | 2026-10-13 23:27:05 |  |
| knowledge | wikipedia_clickstream | MANAGED | 22,509,883 | 8 | 416,598,429 | 397.3 | 2026-10-20 18:55:18 |  |
| knowledge | wikipedia_edges | MANAGED | 8,831 | 1 | 173,401 | 0.17 | 2026-10-13 18:37:21 |  |
| knowledge | wikipedia_selected | MANAGED | 33,443 | 1 | 63,105,105 | 60.18 | 2026-10-16 14:31:27 |  |
| ml_datasets_shared | dataset_manifest | MANAGED | 26 | 2 | 11,777 | 0.01 | 2026-10-28 13:34:13 |  |
| ml_datasets_shared | feature_contract | MANAGED | 532 | 2 | 8,918 | 0.01 | 2026-10-28 13:34:17 |  |
| ml_datasets_shared | gate_results | MANAGED | 377 | 2 | 9,596 | 0.01 | 2026-10-28 13:34:21 |  |
| ml_datasets_shared | imputer_parameters | MANAGED | 865 | 2 | 51,599 | 0.05 | 2026-10-28 13:34:25 |  |
| ml_datasets_shared | mask_specification | MANAGED | 14 | 1 | 4,062 | 0.0 | 2026-10-28 13:37:33 |  |
| ml_datasets_shared | null_class_registry | MANAGED | 398 | 2 | 8,774 | 0.01 | 2026-10-28 13:34:32 |  |
| ml_datasets_shared | null_rate_profile | MANAGED | 401 | 1 | 5,461 | 0.01 | 2026-10-28 13:37:36 |  |
| ml_datasets_shared | partition_manifest | MANAGED | 208,703,244 | 52 | 5,927,130,928 | 5652.55 | 2026-10-28 13:35:15 |  |
| ml_datasets_shared | split_specification | MANAGED | 24 | 2 | 10,471 | 0.01 | 2026-10-28 13:35:19 |  |
| ml_models | approval_spec | MANAGED | 32 | 2 | 7,416 | 0.01 | 2026-10-28 12:55:23 |  |
| ml_models | candidate_results | MANAGED | 2,348 | 2 | 43,989 | 0.04 | 2026-10-28 12:55:27 |  |
| ml_models | candidate_selection | MANAGED | 77 | 2 | 11,975 | 0.01 | 2026-10-28 12:55:30 |  |
| ml_models | evaluation_flags | MANAGED | 49 | 2 | 9,004 | 0.01 | 2026-10-28 13:09:06 |  |
| ml_models | evaluation_results | MANAGED | 3,898 | 2 | 58,635 | 0.06 | 2026-10-28 12:55:37 |  |
| ml_models | evaluation_run_context | MANAGED | 377 | 2 | 20,784 | 0.02 | 2026-10-28 12:55:40 |  |
| ml_models | evaluation_spec | MANAGED | 43 | 2 | 6,009 | 0.01 | 2026-10-28 12:55:43 |  |
| ml_models | evaluation_split_manifest | MANAGED | 174,475 | 1 | 2,133,448 | 2.03 | 2026-10-28 13:02:33 |  |
| ml_models | foundation_contamination | MANAGED | 18 | 2 | 8,654 | 0.01 | 2026-10-28 13:09:40 |  |
| ml_models | foundation_model_register | MANAGED | 26 | 2 | 16,708 | 0.02 | 2026-10-28 12:55:53 |  |
| ml_models | foundation_registry_entry | MANAGED | 3 | 1 | 9,171 | 0.01 | 2026-10-28 13:02:37 |  |
| ml_models | foundation_results | MANAGED | 1,151 | 2 | 33,313 | 0.03 | 2026-10-28 12:55:59 |  |
| ml_models | foundation_run_context | MANAGED | 192 | 2 | 19,157 | 0.02 | 2026-10-28 12:56:02 |  |
| ml_models | foundation_selection | MANAGED | 23 | 2 | 9,966 | 0.01 | 2026-10-28 12:56:06 |  |
| ml_models | library_availability | MANAGED | 32 | 2 | 4,038 | 0.0 | 2026-10-28 12:56:09 |  |
| ml_models | model_approval | MANAGED | 100 | 2 | 29,656 | 0.03 | 2026-10-28 12:56:12 |  |
| ml_models | model_predictions | MANAGED | 2,824,374 | 2 | 34,831,003 | 33.22 | 2026-10-28 12:56:17 |  |
| ml_models | model_registry | MANAGED | 100 | 2 | 28,761 | 0.03 | 2026-10-28 12:56:21 |  |
| ml_models | monitoring_flags | MANAGED | 59 | 2 | 10,131 | 0.01 | 2026-10-28 12:56:24 |  |
| ml_models | monitoring_results | MANAGED | 1,282 | 2 | 20,469 | 0.02 | 2026-10-28 12:56:27 |  |
| ml_models | monitoring_spec | MANAGED | 12 | 2 | 5,262 | 0.01 | 2026-10-28 12:56:31 |  |
| ml_models | monitoring_triggers | MANAGED | 12 | 2 | 9,496 | 0.01 | 2026-10-28 12:56:34 |  |
| ml_models | reference_profile | MANAGED | 209 | 2 | 29,423 | 0.03 | 2026-10-28 12:56:37 |  |
| ml_models | registry_check | MANAGED | 48 | 2 | 16,452 | 0.02 | 2026-10-28 12:56:40 |  |
| ml_models | scoring_requests | MANAGED | 2,718 | 2 | 76,393 | 0.07 | 2026-10-28 12:56:43 |  |
| ml_models | task_registry | MANAGED | 26 | 2 | 9,199 | 0.01 | 2026-10-28 12:56:47 |  |
| ml_models | task_run_context | MANAGED | 538 | 2 | 22,239 | 0.02 | 2026-10-28 12:56:51 |  |
| quality | field_class_registry | MANAGED | 3,683 | 4 | 37,758 | 0.04 | 2026-11-05 20:52:44 |  |
| quality | pipeline_watermarks | MANAGED | 1,173 | 26 | 102,239 | 0.1 | 2026-11-05 22:54:04 |  |
| quality | quality_audit_log | MANAGED | 4,175 | 5 | 69,335 | 0.07 | 2026-11-05 22:54:06 |  |
| quality | quarantine | MANAGED | 1,880,371 | 8 | 38,933,011 | 37.13 | 2026-09-27 19:18:42 |  |
| shared_conformed | data_gap_period | MANAGED | 5,817,858 | 3 | 229,350,234 | 218.73 | 2026-10-29 14:04:25 |  |
| shared_conformed | dim_date | MANAGED | 50,768 | 8 | 70,358 | 0.07 | 2026-09-28 10:47:24 |  |
| shared_conformed | dim_geography | MANAGED | 17 | 1 | 3,300 | 0.0 | 2026-09-28 10:45:45 |  |
| shared_conformed | dim_place | MANAGED | 80 | 1 | 15,623 | 0.01 | 2026-09-28 10:48:10 |  |
| shared_conformed | dim_place_validity | MANAGED | 322 | 2 | 30,168 | 0.03 | 2026-09-28 10:48:19 |  |
| shared_conformed | dim_time | MANAGED | 1,440 | 2 | 5,383 | 0.01 | 2026-09-28 10:45:41 |  |
| shared_conformed | dim_weather_context | MANAGED | 0 | 0 | 0 | 0.0 | 2026-09-27 17:23:55 |  |
| shared_conformed | geo_plz_gemeinde_xref | MANAGED | 0 | 0 | 0 | 0.0 | 2026-09-27 17:23:53 |  |
| shared_conformed | quality_exception | VIEW | 469,839 |  |  |  |  |  |
| shared_conformed | ref_energy_carrier | MANAGED | 84 | 1 | 4,200 | 0.0 | 2026-09-28 12:27:48 |  |
| shared_conformed | ref_energy_carrier_map | MANAGED | 95 | 1 | 5,573 | 0.01 | 2026-09-28 12:27:56 |  |
| shared_conformed | ref_measure | MANAGED | 55 | 1 | 6,210 | 0.01 | 2026-09-28 10:46:44 |  |
| shared_conformed | ref_vocabulary | MANAGED | 1,912 | 1 | 92,600 | 0.09 | 2026-09-28 10:46:43 |  |
| shared_conformed | series_coverage | MANAGED | 1,391 | 1 | 61,122 | 0.06 | 2026-09-28 12:38:51 |  |

## Volumes

| schema | volume | type |
| --- | --- | --- |
| energy_ml_models | foundation_models | MANAGED |
| energy_ml_models | model_libraries | MANAGED |
| knowledge | models | MANAGED |
| knowledge | rerank | MANAGED |

## Errors (0)

None.
