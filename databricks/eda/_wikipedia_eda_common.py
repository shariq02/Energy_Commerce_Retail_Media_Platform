# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA SHARED LIBRARY -- TEXT AND LINK GRAPH SOURCES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** Profiling helpers for article text and weighted link edges:
# MAGIC length and paragraph statistics (word and character proxies), markup and
# MAGIC character checks, title forms, key overlap between two frames and degree
# MAGIC summaries. Pulled in with `%run ../_wikipedia_eda_common` after
# MAGIC `%run ../_eda_common` and `%run ../_samples_eda_common`. Definitions only --
# MAGIC no side effects at import; the caller owns `spark`.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Constants
# Words per paragraph above which a paragraph probably exceeds a 256-token chunk.
WORDS_PER_CHUNK_PROXY = 190
QUANTILE_PROBS = (0.01, 0.25, 0.5, 0.75, 0.95, 0.99)
MARKUP_PATTERNS = {
    "wiki_heading": r"(?m)^=+[^=]+=+\s*$",
    "wiki_link": r"\[\[",
    "template_brace": r"\{\{",
    "html_tag": r"<[a-zA-Z/][^>]*>",
    "url": r"https?://",
    "redirect": r"(?i)^\s*#redirect",
}

# COMMAND ----------

# DBTITLE 1,Column role helpers


def resolve_roles(columns, hints):
    # First column whose lower-case name equals one of the hints, per role.
    return {
        role: next((c for h in names for c in columns if c.lower() == h), None)
        for role, names in hints.items()
    }


def longest_text_col(df, cols, n=500):
    # Fallback when no name hint matches: the string column with the longest
    # average value in the first n rows.
    str_cols = [c for c in cols if dict(df.dtypes).get(c) == "string"]
    if not str_cols:
        return None
    r = (
        df.limit(n)
        .agg(*[F.avg(F.length(qcol(c))).alias(f"a{i}") for i, c in enumerate(str_cols)])
        .first()
        .asDict()
    )
    best = max(range(len(str_cols)), key=lambda i: r[f"a{i}"] or 0)
    return str_cols[best]


def title_norm(col):
    return F.lower(F.trim(F.regexp_replace(as_str(col), "_", " ")))


def word_count(text_col):
    # Whitespace-separated words; an empty or blank string has zero words.
    t = F.trim(text_col)
    return F.when(F.length(t) == 0, F.lit(0)).otherwise(F.size(F.split(t, r"\s+")))


# COMMAND ----------

# DBTITLE 1,Text length and paragraph statistics


def text_stats(df, col):
    txt = as_str(col)
    base = df.select(F.length(txt).alias("__chars"), word_count(txt).alias("__words"))
    probs = ",".join(str(p) for p in QUANTILE_PROBS)
    r = (
        base.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum((F.col("__words") == 0).cast("long")).alias("empty"),
            F.sum((F.col("__chars") < 200).cast("long")).alias("under_200_chars"),
            F.sum((F.col("__words") > WORDS_PER_CHUNK_PROXY).cast("long")).alias(
                "over_proxy_words"
            ),
            F.sum("__words").alias("total_words"),
            F.max("__chars").alias("max_chars"),
            F.expr(f"percentile_approx(__chars, array({probs}))").alias("chars_q"),
            F.expr(f"percentile_approx(__words, array({probs}))").alias("words_q"),
        )
        .first()
        .asDict()
    )
    r["chars_q"] = dict(zip(QUANTILE_PROBS, r["chars_q"], strict=True))
    r["words_q"] = dict(zip(QUANTILE_PROBS, r["words_q"], strict=True))
    return r


def paragraph_stats(df, col, fraction=1.0, seed=7):
    # Paragraphs are non-blank lines. Computed on a row sample when fraction < 1.
    src = df.select(as_str(col).alias("__t"))
    if fraction < 1.0:
        src = src.sample(fraction=fraction, seed=seed)
    paras = src.select(F.split(F.col("__t"), r"\n+").alias("__p"))
    per_article = (
        paras.agg(
            F.count(F.lit(1)).alias("articles"),
            F.expr("percentile_approx(size(__p), array(0.5, 0.95))").alias("per_q"),
            F.max(F.size("__p")).alias("per_max"),
        )
        .first()
        .asDict()
    )
    flat = (
        paras.select(F.explode("__p").alias("__x"))
        .where(F.trim(F.col("__x")) != "")
        .select(word_count(F.col("__x")).alias("__w"))
    )
    probs = ",".join(str(p) for p in QUANTILE_PROBS)
    r = (
        flat.agg(
            F.count(F.lit(1)).alias("paragraphs"),
            F.sum((F.col("__w") > WORDS_PER_CHUNK_PROXY).cast("long")).alias(
                "over_proxy"
            ),
            F.max("__w").alias("max_words"),
            F.expr(f"percentile_approx(__w, array({probs}))").alias("words_q"),
        )
        .first()
        .asDict()
    )
    r["words_q"] = dict(zip(QUANTILE_PROBS, r["words_q"], strict=True))
    r["sample_fraction"] = fraction
    r["sampled_articles"] = per_article["articles"]
    r["per_article_p50_p95"] = per_article["per_q"]
    r["per_article_max"] = per_article["per_max"]
    return r


def log2_histogram(df, value_col):
    # Counts per power-of-two bucket of a numeric Column expression.
    b = F.floor(F.log2(value_col + 1)).cast("int")
    rows = (
        df.select(b.alias("__b"))
        .where(F.col("__b").isNotNull())
        .groupBy("__b")
        .count()
        .orderBy("__b")
        .collect()
    )
    return [(f"2^{r['__b']}", r["count"]) for r in rows]


# COMMAND ----------

# DBTITLE 1,Markup, character and duplicate checks


def markup_indicators(df, col, patterns=None):
    # Rows containing each markup pattern at least once.
    patterns = patterns or MARKUP_PATTERNS
    txt = as_str(col)
    exprs = [F.count(F.lit(1)).alias("rows")]
    exprs += [F.sum(txt.rlike(p).cast("long")).alias(n) for n, p in patterns.items()]
    return df.agg(*exprs).first().asDict()


def char_issues(df, col):
    txt = as_str(col)
    non_ascii = F.length(txt) - F.length(F.regexp_replace(txt, r"[^\x00-\x7F]", ""))
    r = (
        df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(txt.rlike("�").cast("long")).alias("replacement_char_rows"),
            F.sum(txt.rlike(r"[\x00-\x08\x0B\x0C\x0E-\x1F]").cast("long")).alias(
                "control_char_rows"
            ),
            F.sum(txt.rlike(r"[^\x00-\x7F]").cast("long")).alias("non_ascii_rows"),
            F.sum(non_ascii).alias("non_ascii_chars"),
            F.sum(F.length(txt)).alias("chars"),
        )
        .first()
        .asDict()
    )
    r["non_ascii_char_share"] = (
        round(r["non_ascii_chars"] / r["chars"], 6) if r["chars"] else None
    )
    return r


def text_duplicates(df, col):
    # Exact duplicate texts through one 64-bit hash per row.
    per = df.select(F.xxhash64(as_str(col)).alias("__h")).groupBy("__h").count()
    return (
        per.agg(
            F.sum("count").alias("rows"),
            F.count(F.lit(1)).alias("distinct_texts"),
            F.sum((F.col("count") > 1).cast("long")).alias("dup_groups"),
            F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0)).alias(
                "rows_in_dup_groups"
            ),
        )
        .first()
        .asDict()
    )


def title_forms(df, col):
    t = as_str(col)
    r = (
        df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(is_missing(col).cast("long")).alias("empty"),
            F.sum(t.contains("_").cast("long")).alias("with_underscore"),
            F.sum((t != F.trim(t)).cast("long")).alias("edge_space"),
            F.sum(t.contains(":").cast("long")).alias("with_colon"),
            F.sum(t.rlike(r"(?i)\(disambiguation\)").cast("long")).alias(
                "disambiguation"
            ),
            F.sum(t.rlike(r"(?i)^list[ _]of").cast("long")).alias("list_titles"),
            F.sum(t.rlike(r"^\d+$").cast("long")).alias("digits_only"),
            F.sum(t.rlike(r"^[a-z]").cast("long")).alias("starts_lowercase"),
        )
        .first()
        .asDict()
    )
    coll = (
        df.select(title_norm(col).alias("__n"))
        .groupBy("__n")
        .count()
        .agg(
            F.sum((F.col("count") > 1).cast("long")).alias("collision_groups"),
            F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0)).alias(
                "collision_rows"
            ),
        )
        .first()
        .asDict()
    )
    return {**r, **coll}


# COMMAND ----------

# DBTITLE 1,Domain term and date hints


def domain_term_hits(df, col, terms_by_domain, fraction=1.0, seed=7):
    # Rows matching any term of each domain (whole words, case-insensitive).
    src = df.select(as_str(col).alias("__t"))
    if fraction < 1.0:
        src = src.sample(fraction=fraction, seed=seed)
    exprs = [F.count(F.lit(1)).alias("rows")]
    for name, terms in terms_by_domain.items():
        pat = r"(?i)\b(" + "|".join(terms) + r")\b"
        exprs.append(F.sum(F.col("__t").rlike(pat).cast("long")).alias(name))
    r = src.agg(*exprs).first().asDict()
    r["sample_fraction"] = fraction
    return r


def latest_year_hint(df, col, fraction=0.02, seed=7, top=8):
    # Per-article latest four-digit year mentioned: a hint of the snapshot date.
    src = df.select(as_str(col).alias("__t"))
    if fraction < 1.0:
        src = src.sample(fraction=fraction, seed=seed)
    years = F.regexp_extract_all(F.col("__t"), F.lit(r"\b(19\d\d|20\d\d)\b"), F.lit(1))
    latest = F.array_max(F.transform(years, lambda x: x.cast("int")))
    rows = (
        src.select(latest.alias("__y"))
        .where(F.col("__y").isNotNull())
        .groupBy("__y")
        .count()
        .orderBy(F.desc("__y"))
        .limit(top)
        .collect()
    )
    return [(r["__y"], r["count"]) for r in rows]


def title_term_mass(df, title_col, weight_col, terms_by_domain):
    # Total weight and weight on titles matching each domain's terms.
    t = as_str(title_col)
    w = to_double(weight_col)
    exprs = [F.sum(w).alias("weight")]
    for name, terms in terms_by_domain.items():
        pat = r"(?i)\b(" + "|".join(terms) + r")\b"
        exprs.append(F.sum(F.when(t.rlike(pat), w).otherwise(0.0)).alias(name))
    return df.agg(*exprs).first().asDict()


# COMMAND ----------

# DBTITLE 1,Key overlap between two frames


def key_overlap(a, col_a, b, col_b, normalise=True):
    # Distinct keys in a, in b and in both, from one tagged union.
    def key(col):
        return title_norm(col) if normalise else as_str(col)

    ka = a.select(key(col_a).alias("__k")).where(F.col("__k") != "").distinct()
    kb = b.select(key(col_b).alias("__k")).where(F.col("__k") != "").distinct()
    tagged = ka.select("__k", F.lit(1).alias("__a"), F.lit(0).alias("__b")).unionByName(
        kb.select("__k", F.lit(0).alias("__a"), F.lit(1).alias("__b"))
    )
    r = (
        tagged.groupBy("__k")
        .agg(F.max("__a").alias("a"), F.max("__b").alias("b"))
        .agg(
            F.sum("a").alias("a_distinct"),
            F.sum("b").alias("b_distinct"),
            F.sum(((F.col("a") + F.col("b")) == 2).cast("long")).alias("both"),
        )
        .first()
        .asDict()
    )
    r["a_share_in_b"] = (
        round(r["both"] / r["a_distinct"], 4) if r["a_distinct"] else None
    )
    r["b_share_in_a"] = (
        round(r["both"] / r["b_distinct"], 4) if r["b_distinct"] else None
    )
    return r


def weighted_coverage(edges, title_col, weight_col, known, known_col):
    # Share of edges and of edge weight whose title is a known (article) title.
    ref = (
        known.select(title_norm(known_col).alias("__k"))
        .distinct()
        .withColumn("__known", F.lit(1))
    )
    e = edges.select(
        title_norm(title_col).alias("__k"), to_double(weight_col).alias("__w")
    )
    hit = F.col("__known").isNotNull()
    r = (
        e.join(ref, "__k", "left")
        .agg(
            F.count(F.lit(1)).alias("edges"),
            F.sum(hit.cast("long")).alias("edges_matched"),
            F.sum("__w").alias("weight"),
            F.sum(F.when(hit, F.col("__w")).otherwise(0.0)).alias("weight_matched"),
        )
        .first()
        .asDict()
    )
    r["edge_share"] = round(r["edges_matched"] / r["edges"], 4) if r["edges"] else None
    r["weight_share"] = (
        round(r["weight_matched"] / r["weight"], 4) if r["weight"] else None
    )
    return r


# COMMAND ----------

# DBTITLE 1,Degree summaries


def degree_table(edges, key_col, weight_col):
    # One row per node: edge count and total weight.
    return edges.groupBy(as_str(key_col).alias("__k")).agg(
        F.count(F.lit(1)).alias("edges"), F.sum(to_double(weight_col)).alias("clicks")
    )


def degree_summary(tbl, top=10):
    r = (
        tbl.agg(
            F.count(F.lit(1)).alias("nodes"),
            F.expr("percentile_approx(edges, array(0.5, 0.9, 0.99))").alias("edges_q"),
            F.max("edges").alias("edges_max"),
            F.expr("percentile_approx(clicks, array(0.5, 0.9, 0.99))").alias(
                "clicks_q"
            ),
            F.max("clicks").alias("clicks_max"),
            F.sum("clicks").alias("clicks_total"),
            F.expr("percentile_approx(clicks, 0.99)").alias("clicks_p99"),
        )
        .first()
        .asDict()
    )
    top_mass = (
        tbl.where(F.col("clicks") >= F.lit(r["clicks_p99"]))
        .agg(F.sum("clicks").alias("m"))
        .first()["m"]
    )
    r["top1pct_click_share"] = (
        round(top_mass / r["clicks_total"], 4) if r["clicks_total"] else None
    )
    r["top"] = [
        (row["__k"], row["clicks"], row["edges"])
        for row in tbl.orderBy(F.desc("clicks")).limit(top).collect()
    ]
    return r
