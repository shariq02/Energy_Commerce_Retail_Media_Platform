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
# MAGIC nullable, ordinal position) of every table in the Bronze schema and
# MAGIC store it as a versioned Markdown file. `v001` is the baseline; a later
# MAGIC run that finds an unchanged schema does nothing, and a changed schema
# MAGIC is written as the next version (`v002`, ...) with a short summary of
# MAGIC what changed. Previous versions are never overwritten.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt
import hashlib as _hashlib
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
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

# DBTITLE 1,Capture the current Bronze schema
rows = []  # (table, ordinal, column, datatype, nullable)
tables = sorted(
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
    if not r["isTemporary"]
)
for t in tables:
    try:
        schema = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{t}").schema
    except Exception as exc:
        print(f"  SKIP {t}: {exc}")
        continue
    for i, f in enumerate(schema.fields, start=1):
        rows.append((t, i, f.name, f.dataType.simpleString(), bool(f.nullable)))

print(f"captured {len(rows)} column(s) across {len({r[0] for r in rows})} table(s)")

# COMMAND ----------

# DBTITLE 1,Schema fingerprint


def _canonical(schema_rows):
    return "\n".join(
        f"{t}\t{o}\t{c}\t{d}\t{int(n)}" for t, o, c, d, n in sorted(schema_rows)
    )


current_hash = _hashlib.sha256(_canonical(rows).encode("utf-8")).hexdigest()
print(f"current schema hash: {current_hash}")

# COMMAND ----------

# DBTITLE 1,Locate the latest existing snapshot


def _version_files():
    out = []
    for name in _os.listdir(REGISTRY_DIR):
        if name.startswith("bronze_schema_v") and name.endswith(".md"):
            try:
                out.append((int(name[len("bronze_schema_v") : -len(".md")]), name))
            except ValueError:
                continue
    return sorted(out)


def _parse_snapshot(path):
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


_versions = _version_files()
_latest = _versions[-1] if _versions else None
if _latest:
    _latest_hash, _latest_rows = _parse_snapshot(
        _os.path.join(REGISTRY_DIR, _latest[1])
    )
    print(f"latest snapshot: {_latest[1]}  (hash {_latest_hash})")
else:
    _latest_hash, _latest_rows = None, []
    print("no existing snapshot -- this run writes the v001 baseline")

# COMMAND ----------

# DBTITLE 1,Diff current vs latest


def _diff(prev, curr):
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


_added_t, _removed_t, _per_table = _diff(_latest_rows, rows)
_changed = bool(_added_t or _removed_t or _per_table)

# COMMAND ----------

# DBTITLE 1,Decide: no-op, baseline, or new version
if _latest and _latest_hash == current_hash:
    print(f"schema unchanged vs {_latest[1]} -- nothing written.")
    dbutils.notebook.exit(f"unchanged:{_latest[1]}")

next_version = (_latest[0] + 1) if _latest else 1
out_name = f"bronze_schema_v{next_version:03d}.md"
out_path = _os.path.join(REGISTRY_DIR, out_name)
if _os.path.exists(out_path):
    raise RuntimeError(f"refusing to overwrite existing snapshot {out_path}")

# COMMAND ----------

# DBTITLE 1,Build the change summary
_summary = []
if next_version == 1:
    _summary.append("First snapshot -- baseline.")
elif not _changed:
    # hash differed but the structural diff found nothing (e.g. whitespace in a
    # prior file) -- record it plainly rather than inventing a change.
    _summary.append(
        f"Schema fingerprint differs from v{_latest[0]:03d} but no table/column/"
        "type/nullable/ordinal difference was found."
    )
else:
    _summary.append(f"Compared with v{_latest[0]:03d}.")
    _summary.append("")
    _summary.append(f"**Tables added:** {', '.join(_added_t) or '(none)'}")
    _summary.append(f"**Tables removed:** {', '.join(_removed_t) or '(none)'}")
    for t, notes in _per_table:
        _summary.append("")
        _summary.append(f"**{t}**")
        for nline in notes:
            _summary.append(f"- {nline}")

# COMMAND ----------

# DBTITLE 1,Write the new snapshot Markdown
_captured = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
_table_count = len({r[0] for r in rows})

lines = [
    f"# Bronze schema snapshot v{next_version:03d}",
    "",
    f"<!-- bronze-schema-hash: {current_hash} -->",
    "",
    f"**Captured:** {_captured}",
    f"**Catalog / schema:** `{CATALOG}.{BRONZE_SCHEMA}`",
    f"**Tables:** {_table_count}  |  **Columns:** {len(rows)}",
    "",
    "## Change summary",
    "",
    *_summary,
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

print(f"wrote {out_path}  ({_table_count} tables, {len(rows)} columns)")
dbutils.notebook.exit(f"written:{out_name}")