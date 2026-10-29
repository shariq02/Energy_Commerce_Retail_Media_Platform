# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # TOOL SERVER PRINCIPALS -- GRANTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** each tool server that reads Databricks has its own service principal
# MAGIC (`ecrmap_tools_energy`, `ecrmap_tools_commerce`, `ecrmap_tools_ml`,
# MAGIC `ecrmap_tools_dq`). This notebook gives each one `SELECT` on the schemas of its
# MAGIC server and on nothing else, so Databricks refuses a read of any other layer.
# MAGIC The energy and commerce principals also read the one table
# MAGIC `quality.quality_audit_log`.
# MAGIC
# MAGIC The principals, the permission to use tokens and the permission to use the SQL
# MAGIC warehouse are set by hand in the workspace. The notebook finds the Application ID of
# MAGIC each principal by its name; nothing is stored in the repository. The grants are the same
# MAGIC as the scope lists of `ai/mcp_servers/servers.yml` (a test compares them).
# MAGIC
# MAGIC The writes are switched off in the configuration cell (`APPLY`). Run once with
# MAGIC `APPLY = False` to read the plan, then with `APPLY = True`. A `GRANT` that is
# MAGIC run again changes nothing.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
GRANTS = {
    "energy": {
        "schemas": ["energy_analytics", "energy_semantic"],
        "tables": ["quality.quality_audit_log"],
    },
    "commerce": {
        "schemas": ["commerce_analytics", "commerce_semantic"],
        "tables": ["quality.quality_audit_log"],
    },
    "ml": {"schemas": ["ml_models"], "tables": []},
    "dq": {"schemas": ["quality"], "tables": []},
}
APPLY = False  # True

print(f"Catalog: {CATALOG}")
print(f"Principals: {list(GRANTS)}")
print(f"Apply: {APPLY}")

# COMMAND ----------

# DBTITLE 1,Find the Application IDs by name
from databricks.sdk import WorkspaceClient

client = WorkspaceClient()
application_ids = {}
for name in GRANTS:
    display_name = f"ecrmap_tools_{name}"
    found = list(
        client.service_principals.list(filter=f'displayName eq "{display_name}"')
    )
    if len(found) != 1:
        raise ValueError(
            f"{display_name}: found {len(found)} service principals, expected 1"
        )
    application_ids[name] = found[0].application_id
    print(f"OK  {display_name}: {application_ids[name]}")
print(f"OK  {len(application_ids)} of {len(GRANTS)} Application IDs found")

# COMMAND ----------

# DBTITLE 1,The statements
statements = {}
for name, app_id in application_ids.items():
    wanted = [f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{app_id}`"]
    for schema in GRANTS[name]["schemas"]:
        wanted.append(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `{app_id}`")
        wanted.append(f"GRANT SELECT ON SCHEMA {CATALOG}.{schema} TO `{app_id}`")
    for table in GRANTS[name]["tables"]:
        schema = table.split(".")[0]
        wanted.append(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `{app_id}`")
        wanted.append(f"GRANT SELECT ON TABLE {CATALOG}.{table} TO `{app_id}`")
    statements[name] = wanted
    print(f"{name}: {len(wanted)} statement(s)")
    for text in wanted:
        print("   ", text)

# COMMAND ----------

# DBTITLE 1,Run the statements
for name, wanted in statements.items():
    for text in wanted:
        if not APPLY:
            print(f"SKIP  {text}: APPLY is False")
            continue
        spark.sql(text)
        print(f"OK  {text}")

# COMMAND ----------

# DBTITLE 1,What each principal may read now
for name, app_id in application_ids.items():
    for schema in GRANTS[name]["schemas"]:
        print(f"== {name}: {CATALOG}.{schema}")
        display(spark.sql(f"SHOW GRANTS `{app_id}` ON SCHEMA {CATALOG}.{schema}"))
    for table in GRANTS[name]["tables"]:
        print(f"== {name}: {CATALOG}.{table}")
        display(spark.sql(f"SHOW GRANTS `{app_id}` ON TABLE {CATALOG}.{table}"))

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("TOOL SERVER PRINCIPALS SUMMARY")
print("=" * 70)
print(f"Principals with an Application ID : {sorted(application_ids)}")
print(f"Statements applied                : {APPLY}")
print("Not set here: the permission to use tokens and the SQL warehouse (workspace UI)")
print("=" * 70)
