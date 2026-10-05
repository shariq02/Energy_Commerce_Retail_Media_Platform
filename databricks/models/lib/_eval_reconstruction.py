# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # RECONSTRUCTION EVALUATION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** scoring of the stored reconstruction models and the causal baseline
# MAGIC on mask events placed in the evaluated partition (new fixed seed, block lengths
# MAGIC from the stored mask specification), pulled in with
# MAGIC `%run ../lib/_eval_reconstruction` after `_fit_reconstruction` and `_eval_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Absolute error that keeps the missing values


def abs_error_full(variable, circular, truth, pred):
    """Absolute error per event step, circular where the variable is an angle; a
    missing value stays missing so the rows keep their series."""
    err = np.abs(np.asarray(truth, dtype="float64") - np.asarray(pred, dtype="float64"))
    if variable in circular:
        err = np.minimum(err, CIRCULAR_DEGREES - err)
    return err


# COMMAND ----------

# DBTITLE 1,Reconstruction evaluator


class ReconstructionEvaluator(Evaluator):
    """Masked-value error per variable and the skill over the causal baseline. The
    events are drawn again, so the validation result cannot be reproduced."""

    can_reproduce = False

    def _series(self, df, cfg):
        d = df
        if cfg.get("filter_col"):
            d = d.filter(F.col(cfg["filter_col"]) == cfg["filter_value"])
        cols = [cfg["id_col"], cfg["ts_col"], *cfg["variables"], "partition"]
        keys = [cfg["id_col"], cfg["ts_col"]]
        pdf, _ = to_training_frame(d, key_cols=keys, columns=cols)
        return build_series_set(
            cfg["name"],
            pdf,
            cfg["id_col"],
            cfg["ts_col"],
            cfg["variables"],
            cfg["freq"],
            cfg["period"],
            cfg.get("circular", ()),
        )

    def load(self, df_eval, df_validation, validation_fraction: float = 1.0):
        set_utc_session()
        lengths, weights, self.held = read_mask_spec(
            self.ctx.ecosystem, self.ctx.dataset_id
        )
        rng = np.random.default_rng(self.ctx.thresholds["reconstruction_event_seed"])
        self.sets, self.data = [], {}
        for cfg in self.spec["sets"]:
            steps = _lengths_in_steps(lengths, cfg["step_hours"])
            sset = self._series(df_eval, cfg)
            lens = [len(a) for a in sset.arrays]
            events = {}
            for j in range(len(cfg["variables"])):
                events[j] = sample_events(
                    rng,
                    steps,
                    weights,
                    lens,
                    np.arange(len(lens)),
                    EVAL_EVENTS_PER_VARIABLE,
                )
                self.data[(cfg["name"], j)] = build_examples(sset, j, events[j])
            self.sets.append((cfg, sset, events))
        total = sum(len(v[1]) for v in self.data.values())
        _FRAME_NOTES.append(
            {"read": {"partition": self.ctx.partition, "events": int(total)}}
        )
        self.frames["eval"] = pd.DataFrame({"events": [total]})

    def _predict(self, bundle, cfg, sset, events, j, x):
        name = cfg["name"]
        if isinstance(bundle, dict) and "kinds" in bundle:
            fallback = {j: bundle["fallbacks"][(name, j)]}
            return BaselineReconstructor(bundle["kinds"][name], fallback).predict(j, x)
        if hasattr(bundle, "medians"):
            key = (name, j)
            filled = np.where(np.isnan(x), bundle.medians[key], x) if bundle.fill else x
            return bundle.models[key].predict(filled)
        return bundle[name].predict(sset, events[j], [j] * len(events[j]))

    def score(self, bundle, part, bundles):
        base = bundles[self.base_name]
        out, skills, parts = {}, [], {}
        for cfg, sset, events in self.sets:
            circ = cfg.get("circular", ())
            for j, var in enumerate(cfg["variables"]):
                key = (cfg["name"], j)
                x, truth, sid = self.data[key]
                pred = np.asarray(self._predict(bundle, cfg, sset, events, j, x))
                ref = np.asarray(self._predict(base, cfg, sset, events, j, x))
                err = abs_error_full(var, circ, truth, pred)
                err_ref = abs_error_full(var, circ, truth, ref)
                mae_v, mae_ref = np.nanmean(err), np.nanmean(err_ref)
                label = f"{cfg['name']}__{var}"
                out[f"mae__{label}"] = float(mae_v)
                out[f"skill_mae__{label}"] = skill(float(mae_v), float(mae_ref))
                skills.append(out[f"skill_mae__{label}"])
                if self.held:
                    ids = np.array(sset.ids, dtype=object)[sid]
                    held = np.isin(ids, list(self.held))
                    if held.any():
                        out[f"mae_heldout__{label}"] = float(np.nanmean(err[held]))
                parts[key] = (err, err_ref, sid)
        out["skill_mae_mean"] = float(np.nanmean(skills)) if skills else float("nan")
        sc = Scored(out, sum(len(v[1]) for v in self.data.values()))
        sc.parts = parts
        return sc

    def can_interval(self, sc):
        return hasattr(sc, "parts")

    def interval(self, sc, base):
        """Interval of the mean skill over variables, resampling whole series."""
        seed = self.ctx.thresholds["bootstrap_seed"]
        samplers = {
            key: BlockSampler(sid.astype(str).astype(object), BOOTSTRAP_MAX_ROWS, seed)
            for key, (_, _, sid) in sc.parts.items()
        }

        def stat(rng):
            skills = []
            for key, (err, err_ref, _) in sc.parts.items():
                rows = samplers[key].draw(rng)
                skills.append(skill(np.nanmean(err[rows]), np.nanmean(err_ref[rows])))
            return float(np.nanmean(skills))

        low, high, n = bootstrap_interval(
            stat, self.ctx.thresholds["bootstrap_resamples"], seed
        )
        blocks = min(s.n_blocks for s in samplers.values())
        rows = sum(s.rows_used for s in samplers.values())
        return low, high, n, blocks, rows
