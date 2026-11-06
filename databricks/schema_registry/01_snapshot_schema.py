# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SCHEMA SNAPSHOT & VERSIONING (ALL LAYERS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** Capture the current schema (schema, table, column, datatype,
# MAGIC nullable, ordinal position) of every table in each layer and store it as a
# MAGIC versioned Markdown file. One notebook covers Bronze, Silver, Gold, ML and
# MAGIC models. Schemas are found live with `SHOW SCHEMAS` and `SHOW TABLES`. A
# MAGIC layer with no schema is skipped, so a layer built later needs no new
# MAGIC notebook. Each series has its own versions: `v001` is the baseline; an
# MAGIC unchanged schema writes nothing; a changed schema is written as the next
# MAGIC version with a short summary of what changed. Old versions are never
# MAGIC overwritten.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt
import hashlib as _hashlib
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SHARED_CONFORMED_SCHEMA = "shared_conformed"
SCHEMAS_SUBDIR = "src/schemas"

# series -> (folder, file prefix, hash tag, title, which schemas belong to it)
SERIES = {
    "bronze": (
        "bronze_registry",
        "bronze_schema",
        "bronze-schema-hash",
        "Bronze",
        lambda s: s == "bronze",
    ),
    "genomics_bronze": (
        "bronze_registry",
        "genomics_bronze_schema",
        "bronze-schema-hash",
        "Genomics Bronze",
        lambda s: s == "genomics_bronze",
    ),
    "silver": (
        "silver_registry",
        "silver_schema",
        "silver-schema-hash",
        "Silver",
        lambda s: s.endswith(("_silver", "_silver_reference")),
    ),
    "gold": (
        "gold_registry",
        "gold_schema",
        "gold-schema-hash",
        "Gold",
        lambda s: s.endswith("_gold") or s == SHARED_CONFORMED_SCHEMA,
    ),
    "ml": (
        "ml_registry",
        "ml_schema",
        "ml-schema-hash",
        "ML",
        lambda s: s == "ml_datasets_shared" or s.endswith("_ml_datasets"),
    ),
    "model": (
        "model_registry",
        "model_schema",
        "model-schema-hash",
        "Model",
        lambda s: s == "ml_models" or s.endswith("_ml_models"),
    ),
}

# COMMAND ----------

# DBTITLE 1,Repo-root discovery


def _repo_root():
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    try:
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception as exc:
        raise RuntimeError(
            "repo root not found -- run from inside the repo's Databricks Git folder"
        ) from exc
    i = wp.rfind("/databricks/")
    if i > 0:
        for cand in (wp[:i], "/Workspace" + wp[:i]):
            if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                return cand
    raise RuntimeError(
        "repo root not found -- run from inside the repo's Databricks Git folder"
    )


SCHEMAS_DIR = _os.path.join(_repo_root(), SCHEMAS_SUBDIR)
print(f"OK  schemas directory: {SCHEMAS_DIR}")

# COMMAND ----------

# DBTITLE 1,Discover the schemas of the catalog
ALL_SCHEMAS = sorted(
    r["databaseName"] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
)
print(f"catalog has {len(ALL_SCHEMAS)} schema(s)")

# COMMAND ----------

# DBTITLE 1,Define the schema capture


def capture_rows(schemas):
    """(schema, table, ordinal, column, datatype, nullable) for every table."""
    out = []
    for schema in schemas:
        tables = sorted(
            r["tableName"]
            for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
            if not r["isTemporary"]
        )
        for t in tables:
            try:
                fields = spark.table(f"{CATALOG}.{schema}.{t}").schema.fields
            except Exception as exc:
                print(f"  SKIP {schema}.{t}: {exc}")
                continue
            for i, f in enumerate(fields, start=1):
                out.append(
                    (schema, t, i, f.name, f.dataType.simpleString(), bool(f.nullable))
                )
    return out


# COMMAND ----------

# DBTITLE 1,Define the schema fingerprint


def schema_hash(rows):
    text = "\n".join(
        f"{s}\t{t}\t{o}\t{c}\t{d}\t{int(n)}" for s, t, o, c, d, n in sorted(rows)
    )
    return _hashlib.sha256(text.encode("utf-8")).hexdigest()


# COMMAND ----------

# DBTITLE 1,Define the snapshot file readers


def version_files(folder, prefix):
    if not _os.path.isdir(folder):
        return []
    out = []
    for name in _os.listdir(folder):
        if name.startswith(f"{prefix}_v") and name.endswith(".md"):
            try:
                out.append((int(name[len(prefix) + 2 : -len(".md")]), name))
            except ValueError:
                continue
    return sorted(out)


def parse_snapshot(path, tag, default_schema):
    """Return (hash, rows) from a written snapshot file. Old Bronze files have
    five columns and no schema column; `default_schema` fills it."""
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    h = ""
    for line in text.splitlines():
        if line.startswith(f"<!-- {tag}:"):
            h = line.split(":", 1)[1].strip().removesuffix("-->").strip()
            break
    parsed = []
    for line in text.splitlines():
        if not line.startswith("| "):
            continue
        parts = [p.strip() for p in line.strip().strip("|").split("|")]
        if len(parts) == 5 and parts[1].isdigit():
            parts = [default_schema, *parts]
        if len(parts) != 6 or not parts[2].isdigit():
            continue
        parsed.append(
            (
                parts[0],
                parts[1],
                int(parts[2]),
                parts[3],
                parts[4],
                parts[5].lower() == "true",
            )
        )
    return h, parsed


# COMMAND ----------

# DBTITLE 1,Define the diff


def diff_schemas(prev, curr):
    prev_by = {}
    curr_by = {}
    for s, t, o, c, d, n in prev:
        prev_by.setdefault((s, t), {})[c] = (o, d, n)
    for s, t, o, c, d, n in curr:
        curr_by.setdefault((s, t), {})[c] = (o, d, n)
    added_t = sorted(set(curr_by) - set(prev_by))
    removed_t = sorted(set(prev_by) - set(curr_by))
    per_table = []
    for st in sorted(set(prev_by) & set(curr_by)):
        pc, cc = prev_by[st], curr_by[st]
        notes = []
        for col in sorted(set(cc) - set(pc)):
            o, d, n = cc[col]
            notes.append(
                f"column added: `{col}` ({d}, {'nullable' if n else 'not null'})"
            )
        for col in sorted(set(pc) - set(cc)):
            notes.append(f"column removed: `{col}`")
        for col in sorted(set(pc) & set(cc)):
            (_po, pd_, pn), (_co, cd, cn) = pc[col], cc[col]
            if pd_ != cd:
                notes.append(f"type changed: `{col}` {pd_} -> {cd}")
            if pn != cn:
                notes.append(
                    f"nullable changed: `{col}` {'nullable' if pn else 'not null'} -> "
                    f"{'nullable' if cn else 'not null'}"
                )
        moved = sum(1 for col in set(pc) & set(cc) if pc[col][0] != cc[col][0])
        if moved:
            notes.append(f"{moved} column(s) changed ordinal position")
        if notes:
            per_table.append((st, notes))
    return added_t, removed_t, per_table


# COMMAND ----------

# DBTITLE 1,Define the change summary


def build_summary(next_version, latest, diff):
    added_t, removed_t, per_table = diff
    if next_version == 1:
        return ["First snapshot -- baseline."]
    if not (added_t or removed_t or per_table):
        return [
            (
                f"Schema fingerprint differs from v{latest[0]:03d} but no table/column/"
                "type/nullable/ordinal difference was found."
            )
        ]
    summary = [
        f"Compared with v{latest[0]:03d}.",
        "",
        f"**Tables added:** {', '.join(f'{s}.{t}' for s, t in added_t) or '(none)'}",
        f"**Tables removed:** {', '.join(f'{s}.{t}' for s, t in removed_t) or '(none)'}",
    ]
    for (s, t), notes in per_table:
        summary += ["", f"**{s}.{t}**", *[f"- {nline}" for nline in notes]]
    return summary


# COMMAND ----------

# DBTITLE 1,Define the snapshot of one series


def snapshot(name):
    folder_name, prefix, tag, title, belongs = SERIES[name]
    schemas = [s for s in ALL_SCHEMAS if belongs(s)]
    if not schemas:
        print(f"{name}: no schema in the catalog -- skipped.")
        return "skipped:no schema"
    folder = _os.path.join(SCHEMAS_DIR, folder_name)
    rows = capture_rows(schemas)
    table_count = len({(r[0], r[1]) for r in rows})
    print(f"{name}: captured {len(rows)} column(s) across {table_count} table(s)")
    current_hash = schema_hash(rows)

    versions = version_files(folder, prefix)
    latest = versions[-1] if versions else None
    if latest:
        latest_hash, latest_rows = parse_snapshot(
            _os.path.join(folder, latest[1]), tag, schemas[0]
        )
        print(f"{name}: latest snapshot {latest[1]} (hash {latest_hash})")
    else:
        latest_hash, latest_rows = None, []
        print(f"{name}: no existing snapshot -- this run writes the v001 baseline")

    if latest and latest_hash == current_hash:
        print(f"{name}: schema unchanged vs {latest[1]} -- nothing written.")
        return f"unchanged:{latest[1]}"

    next_version = (latest[0] + 1) if latest else 1
    out_name = f"{prefix}_v{next_version:03d}.md"
    out_path = _os.path.join(folder, out_name)
    if _os.path.exists(out_path):
        raise RuntimeError(f"refusing to overwrite existing snapshot {out_path}")

    summary = build_summary(next_version, latest, diff_schemas(latest_rows, rows))
    captured = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"# {title} schema snapshot v{next_version:03d}",
        "",
        f"<!-- {tag}: {current_hash} -->",
        "",
        f"**Captured:** {captured}",
        f"**Catalog / schemas:** `{CATALOG}.{{{', '.join(schemas)}}}`",
        f"**Tables:** {table_count}  |  **Columns:** {len(rows)}",
        "",
        "## Change summary",
        "",
        *summary,
        "",
        "## Schema",
        "",
        "| schema | table | # | column | type | nullable |",
        "|---|---|---|---|---|---|",
    ]
    for s, t, o, c, d, n in sorted(rows):
        lines.append(f"| {s} | {t} | {o} | {c} | {d} | {'true' if n else 'false'} |")
    lines.append("")

    _os.makedirs(folder, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"{name}: wrote {out_path} ({table_count} tables, {len(rows)} columns)")
    return f"written:{out_name}"


# COMMAND ----------

# DBTITLE 1,Snapshot every series
results = {name: snapshot(name) for name in SERIES}

# COMMAND ----------

# DBTITLE 1,Summary
for name, status in results.items():
    print(f"{name:<16} {status}")
