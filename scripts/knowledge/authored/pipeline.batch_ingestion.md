# Pipeline: batch ingestion

Acquisition by source:
- SMARD, DWD (28 curated stations), MaStR, power plant list and redispatch: scripted download of public data.
- Honda IoT: manual download of `reduced_data.zip` from Dryad.
- REES46 (commerce): manual download from Kaggle, which needs an account and token.
- GA4 (commerce): BigQuery query and export, not a plain file download.

Each file source follows one path: download or BigQuery export, raw landing in `data/raw/{source}/`, validation against the data contract, extraction or chunking into `data/staging/{source}/`, upload to Unity Catalog, then the Bronze, Silver, Gold and Analytical Processing notebooks.

Raw data is never uploaded directly. Only the staging output reaches Unity Catalog. Each source has its own data contract.
