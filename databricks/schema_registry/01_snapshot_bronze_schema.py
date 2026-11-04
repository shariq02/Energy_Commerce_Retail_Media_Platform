# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # BRONZE SCHEMA SNAPSHOT & VERSIONING
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Capture the current schema (table, column, datatype,
# MAGIC nullable, ordinal position) of every table in each Bronze schema (`bronze`
# MAGIC and `genomics_bronze`) and store it as a versioned Markdown file. Each
# MAGIC schema has its own series: `v001` is the baseline; a later run that finds an
# MAGIC unchanged schema does nothing, and a changed schema is written as the next
# MAGIC version (`v002`, ...) with a short summary of what changed. Previous
# MAGIC versions are never overwritten.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt
import hashlib as _hashlib
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
# Bronze schema -> file prefix of its own snapshot series
BRONZE_SERIES = {
    "bronze": "bronze_schema",
    "genomics_bronze": "genomics_bronze_schema",
}
REGISTRY_SUBDIR = "src/schemas/bronze_registry"

# COMMAND ----------

# DBTITLE 1,Repo-root discovery + registry directory


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


REGISTRY_DIR = _os.path.join(_repo_root(), REGISTRY_SUBDIR)
_os.makedirs(REGISTRY_DIR, exist_ok=True)
print(f"OK  registry directory: {REGISTRY_DIR}")

# COMMAND ----------

# DBTITLE 1,Define the schema capture


def capture_rows(schema):
    """(table, ordinal, column, datatype, nullable) for every table of the schema."""
    out = []
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
            out.append((t, i, f.name, f.dataType.simpleString(), bool(f.nullable)))
    return out


# COMMAND ----------

# DBTITLE 1,Define the schema fingerprint


def schema_hash(schema_rows):
    text = "\n".join(
        f"{t}\t{o}\t{c}\t{d}\t{int(n)}" for t, o, c, d, n in sorted(schema_rows)
    )
    return _hashlib.sha256(text.encode("utf-8")).hexdigest()


# COMMAND ----------

# DBTITLE 1,Define the snapshot file readers


def version_files(prefix):
    out = []
    for name in _os.listdir(REGISTRY_DIR):
        if name.startswith(f"{prefix}_v") and name.endswith(".md"):
            try:
                out.append((int(name[len(prefix) + 2 : -len(".md")]), name))
            except ValueError:
                continue
    return sorted(out)


def parse_snapshot(path):
    """Return (hash, [(table, ordinal, column, datatype, nullable)]) from a
    previously written snapshot Markdown file."""
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    h = ""
    for line in text.splitlines():
        if line.startswith("<!-- bronze-schema-hash:"):
            h = line.split(":", 1)[1].strip().removesuffix("-->").strip()
            break
    parsed = []
    for line in text.splitlines():
        if not line.startswith("| "):
            continue
        parts = [p.strip() for p in line.strip().strip("|").split("|")]
        if len(parts) != 5 or not parts[1].isdigit():
            continue
        parsed.append(
            (parts[0], int(parts[1]), parts[2], parts[3], parts[4].lower() == "true")
        )
    return h, parsed


# COMMAND ----------

# DBTITLE 1,Define the diff


def diff_schemas(prev, curr):
    prev_by_t = {}
    curr_by_t = {}
    for t, o, c, d, n in prev:
        prev_by_t.setdefault(t, {})[c] = (o, d, n)
    for t, o, c, d, n in curr:
        curr_by_t.setdefault(t, {})[c] = (o, d, n)
    added_t = sorted(set(curr_by_t) - set(prev_by_t))
    removed_t = sorted(set(prev_by_t) - set(curr_by_t))
    per_table = []
    for t in sorted(set(prev_by_t) & set(curr_by_t)):
        pc, cc = prev_by_t[t], curr_by_t[t]
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
            per_table.append((t, notes))
    return added_t, removed_t, per_table


# COMMAND ----------

# DBTITLE 1,Define the change summary


def build_summary(next_version, latest, diff):
    added_t, removed_t, per_table = diff
    if next_version == 1:
        return ["First snapshot -- baseline."]
    if not (added_t or removed_t or per_table):
        # hash differed but the structural diff found nothing (e.g. whitespace in a
        # prior file) -- record it plainly rather than inventing a change.
        return [
            (
                f"Schema fingerprint differs from v{latest[0]:03d} but no table/column/"
                "type/nullable/ordinal difference was found."
            )
        ]
    summary = [
        f"Compared with v{latest[0]:03d}.",
        "",
        f"**Tables added:** {', '.join(added_t) or '(none)'}",
        f"**Tables removed:** {', '.join(removed_t) or '(none)'}",
    ]
    for t, notes in per_table:
        summary += ["", f"**{t}**", *[f"- {nline}" for nline in notes]]
    return summary


# COMMAND ----------

# DBTITLE 1,Define the snapshot of one Bronze schema


def snapshot(schema, prefix):
    rows = capture_rows(schema)
    table_count = len({r[0] for r in rows})
    print(f"{schema}: captured {len(rows)} column(s) across {table_count} table(s)")
    current_hash = schema_hash(rows)

    versions = version_files(prefix)
    latest = versions[-1] if versions else None
    if latest:
        latest_hash, latest_rows = parse_snapshot(
            _os.path.join(REGISTRY_DIR, latest[1])
        )
        print(f"{schema}: latest snapshot {latest[1]} (hash {latest_hash})")
    else:
        latest_hash, latest_rows = None, []
        print(f"{schema}: no existing snapshot -- this run writes the v001 baseline")

    if latest and latest_hash == current_hash:
        print(f"{schema}: schema unchanged vs {latest[1]} -- nothing written.")
        return f"unchanged:{latest[1]}"

    next_version = (latest[0] + 1) if latest else 1
    out_name = f"{prefix}_v{next_version:03d}.md"
    out_path = _os.path.join(REGISTRY_DIR, out_name)
    if _os.path.exists(out_path):
        raise RuntimeError(f"refusing to overwrite existing snapshot {out_path}")

    summary = build_summary(next_version, latest, diff_schemas(latest_rows, rows))
    captured = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"# Bronze schema snapshot v{next_version:03d}",
        "",
        f"<!-- bronze-schema-hash: {current_hash} -->",
        "",
        f"**Captured:** {captured}",
        f"**Catalog / schema:** `{CATALOG}.{schema}`",
        f"**Tables:** {table_count}  |  **Columns:** {len(rows)}",
        "",
        "## Change summary",
        "",
        *summary,
        "",
        "## Schema",
        "",
        "| table | # | column | type | nullable |",
        "|---|---|---|---|---|",
    ]
    for t, o, c, d, n in sorted(rows):
        lines.append(f"| {t} | {o} | {c} | {d} | {'true' if n else 'false'} |")
    lines.append("")

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"{schema}: wrote {out_path} ({table_count} tables, {len(rows)} columns)")
    return f"written:{out_name}"


# COMMAND ----------

# DBTITLE 1,Snapshot each Bronze schema
results = {schema: snapshot(schema, prefix) for schema, prefix in BRONZE_SERIES.items()}

# COMMAND ----------

# DBTITLE 1,Summary
for schema, status in results.items():
    print(f"{schema:<18} {status}")
