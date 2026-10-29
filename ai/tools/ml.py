"""Model tools: a model card, monitoring rows and stored predictions.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The card is a file of `src/model_cards/`; the path comes from the list of card files,
never from the argument. Monitoring and predictions are read from `ml_models` for
one task, newest first. The prediction table has no horizon column and no as-of time
of the request, so neither can be asked. Library only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from ai.tools import common as cm
from ai.tools import metrics as mt

SERVER = "ml_tools"
SCOPE = cm.load_scopes()[SERVER]
CARD_DIR = Path(__file__).resolve().parents[2] / "src" / "model_cards"
CARD_LIMIT = 20_000
LIMIT_MAX = 200

MONITORING_SQL = {
    "results": (
        "SELECT task_id, model_name, registry_version, window_id, check_kind, subject, "
        "value, warning_threshold, flag_threshold, level, n_reference, n_window, "
        "detail, measured_at FROM [[ml_models.monitoring_results]] "
        "WHERE task_id = :task_id AND (:window_id = '*' OR window_id = :window_id) "
        "ORDER BY measured_at DESC"
    ),
    "flags": (
        "SELECT task_id, model_name, registry_version, window_id, check_kind, subject, "
        "level, value, threshold, detail, flagged_at "
        "FROM [[ml_models.monitoring_flags]] "
        "WHERE task_id = :task_id AND (:window_id = '*' OR window_id = :window_id) "
        "ORDER BY flagged_at DESC"
    ),
    "triggers": (
        "SELECT task_id, model_name, registry_version, trigger_kind, source, "
        "condition, detail, value, threshold, raised_at "
        "FROM [[ml_models.monitoring_triggers]] WHERE task_id = :task_id "
        "ORDER BY raised_at DESC"
    ),
}
PREDICTION_SQL = (
    "SELECT task_id, model_name, registry_version, window_id, row_key, prediction, "
    "prediction_label, scored_at FROM [[ml_models.model_predictions]] "
    "WHERE task_id = :task_id AND (:row_key = '*' OR row_key = :row_key) "
    "AND (:window_id = '*' OR window_id = :window_id) "
    "AND (:model_version < 0 OR registry_version = :model_version) "
    "ORDER BY scored_at DESC"
)
MONITORING_T = {
    kind: SCOPE.register(
        cm.SqlTemplate(
            f"monitoring {kind}",
            sql,
            ("task_id",) if kind == "triggers" else ("task_id", "window_id"),
        )
    )
    for kind, sql in MONITORING_SQL.items()
}
PREDICTION_T = SCOPE.register(
    cm.SqlTemplate(
        "predictions",
        PREDICTION_SQL,
        ("task_id", "row_key", "window_id", "model_version"),
    )
)
TIME_COLUMNS = ("measured_at", "flagged_at", "raised_at", "scored_at")


class CardText(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str
    ecosystem: str
    text: str


class CardResult(cm.ToolResult):
    data: CardText


def card_paths() -> dict[str, Path]:
    """Task id -> card file, from the files on disk."""
    return {path.stem: path for path in sorted(CARD_DIR.glob("*/*.md"))}


def _task(task_id: str) -> str:
    known = card_paths()
    if task_id not in known:
        raise cm.ToolFailure(
            "invalid_argument",
            f"{task_id} is not a task with a model card",
            f"the tasks are {sorted(known)}",
        )
    return task_id


@cm.guarded
def read_model_card(task_id: str) -> CardResult:
    path = card_paths()[_task(task_id)]
    text = path.read_text(encoding="utf-8")
    notes = []
    if len(text) > CARD_LIMIT:
        text, notes = text[:CARD_LIMIT], [f"the card is cut at {CARD_LIMIT} characters"]
    return CardResult(
        data=CardText(task_id=task_id, ecosystem=path.parent.name, text=text),
        source_tables=[str(path.relative_to(CARD_DIR.parents[1]))],
        row_count=1,
        truncated=bool(notes),
        warnings=notes,
    )


def _rows(
    template: cm.SqlTemplate,
    params: dict[str, Any],
    table: str,
    limit: int,
) -> cm.RowsResult:
    if not 1 <= limit <= LIMIT_MAX:
        raise cm.ToolFailure(
            "invalid_argument", f"limit is outside 1 to {LIMIT_MAX}", ""
        )
    result = cm.get_warehouse().run(SCOPE, template, params, row_cap=limit)
    rows = [{k: cm.json_value(v) for k, v in raw.items()} for raw in result.rows]
    stamps = [str(r[c]) for r in rows for c in TIME_COLUMNS if r.get(c)]
    notes = []
    if result.truncated:
        notes.append(f"the result is cut at {limit} rows; narrow the filter")
    if not rows:
        notes.append("no rows for this task and filter")
    return cm.RowsResult(
        data=rows,
        source_tables=[table],
        as_of=max(stamps) if stamps else None,
        row_count=len(rows),
        truncated=result.truncated,
        warnings=notes,
    )


@cm.guarded
def read_monitoring(
    task_id: str,
    kind: Literal["results", "flags", "triggers"] = "results",
    window_id: str = "*",
    limit: int = 50,
) -> cm.RowsResult:
    template = MONITORING_T.get(kind)
    if template is None:
        raise cm.ToolFailure(
            "invalid_argument",
            f"kind {kind} is not known",
            "use results, flags or triggers",
        )
    params = {"task_id": _task(task_id)}
    if kind != "triggers":
        params["window_id"] = mt.check_text("window_id", window_id)
    return _rows(template, params, f"ml_models.monitoring_{kind}", limit)


@cm.guarded
def read_predictions(
    task_id: str,
    row_key: str = "*",
    window_id: str = "*",
    model_version: int = -1,
    limit: int = 50,
) -> cm.RowsResult:
    params = {
        "task_id": _task(task_id),
        "row_key": mt.check_text("row_key", row_key),
        "window_id": mt.check_text("window_id", window_id),
        "model_version": int(model_version),
    }
    return _rows(PREDICTION_T, params, "ml_models.model_predictions", limit)
