# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CPU SPEED PROBE FOR MOMENT AND TIMESFM
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** measure how fast MOMENT and TimesFM run on this compute, so the size of
# MAGIC the evaluation sample comes from a measurement and not from an estimate. MOMENT
# MAGIC scores 128 windows. TimesFM forecasts 64 series with one series and with
# MAGIC `TIMESFM_BATCH` series per forward pass. Nothing is written to a table.
# MAGIC
# MAGIC **Worst case:** every step has a limit of 120 seconds and stops with an error when
# MAGIC the limit is reached. At most 5 limited steps run, so the notebook ends after 10
# MAGIC minutes at the latest. A healthy run takes 2 to 4 minutes.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../models/lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../models/lib/_model_metrics

# COMMAND ----------

# DBTITLE 1,Tabular model library
# MAGIC %run ../models/lib/_fit_tabular

# COMMAND ----------

# DBTITLE 1,Anomaly model library
# MAGIC %run ../models/lib/_fit_anomaly

# COMMAND ----------

# DBTITLE 1,Reconstruction model library
# MAGIC %run ../models/lib/_fit_reconstruction

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../models/lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Foundation anomaly and reconstruction library
# MAGIC %run ../models/lib/_fit_foundation_anomaly

# COMMAND ----------

# DBTITLE 1,Configuration
import os
import platform
import time

PROBE_LIMIT_SECONDS = 120
MOMENT_WARMUP_WINDOWS = 8
MOMENT_WINDOWS = 128
TIMESFM_WARMUP_SERIES = 4
TIMESFM_SERIES = 64
TIMESFM_CONTEXT = 512
TIMESFM_HORIZON = 2
TIMESFM_BATCHES = (1, TIMESFM_BATCH)
# sizes of the evaluation tasks, from the findings of 2026-10-25
WEATHER_VARIABLES = 10
ZONE_GENERATION_ROWS = 6110
SAMPLE_EVENTS_PER_VARIABLE = 500

# COMMAND ----------

# DBTITLE 1,Compute facts
import torch

print(f"processor type : {platform.machine()}")
print(f"cpu count      : {os.cpu_count()}")
print(f"torch threads  : {torch.get_num_threads()}")
print(f"cuda available : {torch.cuda.is_available()}")

# COMMAND ----------

# DBTITLE 1,Test data
rng = np.random.default_rng(MODEL_SEED)
steps = np.arange(RECON_WINDOW)
windows = np.sin(2 * np.pi * steps / 24)[None, :] + 0.1 * rng.standard_normal(
    (MOMENT_WARMUP_WINDOWS + MOMENT_WINDOWS, RECON_WINDOW)
)
windows = windows.astype("float32")
observed = np.ones_like(windows)
observed[:, -24:] = 0.0
contexts = [
    rng.standard_normal(TIMESFM_CONTEXT).cumsum()
    for _ in range(TIMESFM_WARMUP_SERIES + TIMESFM_SERIES)
]
print(f"OK  {len(windows)} MOMENT windows, {len(contexts)} TimesFM series")

# COMMAND ----------

# DBTITLE 1,MOMENT load and speed
moment = MomentBackend(model_entry("moment", verify=False)["dir"])
with candidate_guard("moment load", PROBE_LIMIT_SECONDS):
    started = time.monotonic()
    moment.load()
    moment_load_seconds = time.monotonic() - started
warm, timed = slice(0, MOMENT_WARMUP_WINDOWS), slice(MOMENT_WARMUP_WINDOWS, None)
with candidate_guard("moment warm-up", PROBE_LIMIT_SECONDS):
    moment.reconstruct(windows[warm], observed[warm])
with candidate_guard("moment speed", PROBE_LIMIT_SECONDS):
    started = time.monotonic()
    moment.reconstruct(windows[timed], observed[timed])
    moment_seconds = time.monotonic() - started
moment_rate = MOMENT_WINDOWS / moment_seconds
print(f"MOMENT load {moment_load_seconds:.1f} s")
print(f"MOMENT {MOMENT_WINDOWS} windows in {moment_seconds:.1f} s")
print(f"MOMENT speed {moment_rate:.2f} windows/s")

# COMMAND ----------

# DBTITLE 1,TimesFM speed per batch size
timesfm_rates = {}
for batch in TIMESFM_BATCHES:
    backend = TimesFMBackend(model_entry("timesfm25", verify=False)["dir"], batch=batch)
    with candidate_guard(f"timesfm warm-up, batch {batch}", PROBE_LIMIT_SECONDS):
        started = time.monotonic()
        backend.forecast(
            contexts[:TIMESFM_WARMUP_SERIES], TIMESFM_HORIZON, (0.5,), 86400
        )
        warm_seconds = time.monotonic() - started
    with candidate_guard(f"timesfm speed, batch {batch}", PROBE_LIMIT_SECONDS):
        started = time.monotonic()
        backend.forecast(
            contexts[TIMESFM_WARMUP_SERIES:], TIMESFM_HORIZON, (0.5,), 86400
        )
        seconds = time.monotonic() - started
    timesfm_rates[batch] = TIMESFM_SERIES / seconds
    print(
        f"TimesFM batch {batch}: warm-up {warm_seconds:.1f} s, "
        f"{TIMESFM_SERIES} series in {seconds:.1f} s = {timesfm_rates[batch]:.2f}/s"
    )

# COMMAND ----------

# DBTITLE 1,Projected times
full_events = EVAL_EVENTS_PER_VARIABLE * WEATHER_VARIABLES
sample_events = SAMPLE_EVENTS_PER_VARIABLE * WEATHER_VARIABLES
full_minutes = full_events / moment_rate / 60
sample_minutes = sample_events / moment_rate / 60
print(f"MOMENT, all {full_events} events: {full_minutes:.0f} min")
print(f"MOMENT, sample {sample_events} events: {sample_minutes:.0f} min")
for batch, rate in timesfm_rates.items():
    minutes = ZONE_GENERATION_ROWS / rate / 60
    print(f"TimesFM, {ZONE_GENERATION_ROWS} series, batch {batch}: {minutes:.0f} min")
