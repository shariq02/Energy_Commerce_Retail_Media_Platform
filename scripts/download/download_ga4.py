# GA4 (Google Analytics 4 sample ecommerce export) data download
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: September 2026
#
# Purpose: acquire every daily table of the public GA4 BigQuery-export sample
# (bigquery-public-data.ga4_obfuscated_sample_ecommerce), all columns, all
# days present. Local download only -- query each source table with no
# destination table, job location US, stream the full result to a local
# newline-delimited JSON file. Loading the result anywhere else (a BigQuery
# acquisition dataset, Databricks Unity Catalog) is a separate, manual step
# outside this script.
#
# Every daily local file is skipped if it already exists -- safe to re-run.

import json
import sys
from pathlib import Path

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, GCP_CONFIG, get_logger

logger = get_logger(__name__)

SOURCE_PROJECT = "bigquery-public-data"
SOURCE_DATASET = "ga4_obfuscated_sample_ecommerce"
SOURCE_LOCATION = "US"

QUERY_PROJECT = GCP_CONFIG["project_id"]  # billed project for the query jobs

OUTPUT_DIR = DATA_RAW_DIR / "ga4"


def list_source_tables(client: bigquery.Client) -> list[str]:
    """Every events_YYYYMMDD table in the source dataset, discovered live --
    never hardcoded, so this tracks whatever days Google's sample actually
    covers rather than an assumed count."""
    dataset_ref = bigquery.DatasetReference(SOURCE_PROJECT, SOURCE_DATASET)
    table_ids = sorted(
        t.table_id
        for t in client.list_tables(dataset_ref)
        if t.table_id.startswith("events_") and t.table_id[len("events_") :].isdigit()
    )
    return table_ids


def download_table(client: bigquery.Client, table_id: str) -> Path:
    """Query one daily source table (all columns), job location US, no
    destination table -- stream the full result to a local JSONL file."""
    dest = OUTPUT_DIR / f"{table_id}.jsonl"
    if dest.exists():
        logger.info(f"GA4 {dest} already downloaded, skipping")
        return dest

    logger.info(f"Querying {SOURCE_PROJECT}.{SOURCE_DATASET}.{table_id}")
    query = f"SELECT * FROM `{SOURCE_PROJECT}.{SOURCE_DATASET}.{table_id}`"
    query_job = client.query(query, location=SOURCE_LOCATION)
    rows = query_job.result()

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".jsonl.tmp")
    row_count = 0
    with open(tmp, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(dict(row.items()), default=str))
            fh.write("\n")
            row_count += 1
    tmp.rename(dest)

    logger.info(f"Wrote {row_count} rows to {dest}")
    return dest


def main() -> None:
    client = bigquery.Client(project=QUERY_PROJECT)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    table_ids = list_source_tables(client)
    logger.info(f"Found {len(table_ids)} GA4 daily source tables")

    for table_id in table_ids:
        download_table(client, table_id)

    logger.info(f"GA4 download complete -- files in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
