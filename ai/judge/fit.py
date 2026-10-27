"""Fit the small calibrated models of the local judge (numpy only).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Three parts, each fitted on the tuning questions of one corpus and each refused when
the data is too small: a calibration of the reranker score into a probability that a
chunk is relevant (Platt scaling), a logistic regression on the top scores of the
retrievers that tells a question the corpus can answer from one it cannot, and a
domain router (the nearest centroid of the chunk vectors of each ecosystem, with a
fitted temperature). Nothing in here names an ecosystem or a retriever; the labels
and the feature names come from the data. Library only.
"""

from __future__ import annotations

import math
from itertools import pairwise

import numpy as np

MIN_POSITIVE = 30
MIN_NEGATIVE = 10
TEMPERATURES = (0.01, 0.02, 0.03, 0.05, 0.08, 0.12, 0.2, 0.3, 0.5)


class NotEnoughData(ValueError):
    """The labelled data is too small to fit a part."""


def sigmoid(z):
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30.0, 30.0)))


def standardise(x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    mean = x.mean(axis=0)
    scale = x.std(axis=0)
    scale[scale == 0] = 1.0
    return mean, scale


def fit_logistic(x: np.ndarray, y: np.ndarray, l2: float = 1.0, steps: int = 60):
    """(coefficients, intercept) by Newton steps on the L2-penalised log loss; the
    intercept is not penalised."""
    n, d = x.shape
    design = np.hstack([x, np.ones((n, 1))])
    weights = np.zeros(d + 1)
    penalty = np.eye(d + 1) * l2
    penalty[-1, -1] = 0.0
    for _ in range(steps):
        p = sigmoid(design @ weights)
        gradient = design.T @ (p - y) + penalty @ weights
        hessian = (design.T * (p * (1 - p))) @ design + penalty + 1e-9 * np.eye(d + 1)
        step = np.linalg.solve(hessian, gradient)
        weights -= step
        if np.max(np.abs(step)) < 1e-9:
            break
    return weights[:-1], float(weights[-1])


def log_loss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.clip(p, 1e-9, 1 - 1e-9)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def auc(scores: np.ndarray, y: np.ndarray) -> float | None:
    """Share of (positive, negative) pairs that the score orders correctly."""
    positive, negative = scores[y == 1], scores[y == 0]
    if not len(positive) or not len(negative):
        return None
    wins = (positive[:, None] > negative[None, :]).sum()
    ties = (positive[:, None] == negative[None, :]).sum()
    return float((wins + 0.5 * ties) / (len(positive) * len(negative)))


def _check_counts(y: np.ndarray, minimum_positive: int, minimum_negative: int) -> None:
    positive, negative = int((y == 1).sum()), int((y == 0).sum())
    if positive < minimum_positive or negative < minimum_negative:
        raise NotEnoughData(
            f"{positive} positive and {negative} negative label(s); at least "
            f"{minimum_positive} and {minimum_negative} are needed"
        )


def fit_platt(
    scores,
    labels,
    minimum_positive: int = MIN_POSITIVE,
    minimum_negative: int = MIN_POSITIVE,
) -> dict:
    """p = sigmoid(a * score + b) for one raw score."""
    s = np.asarray(scores, dtype=float).reshape(-1, 1)
    y = np.asarray(labels, dtype=float)
    _check_counts(y, minimum_positive, minimum_negative)
    mean, scale = standardise(s)
    coefficient, intercept = fit_logistic((s - mean) / scale, y, l2=1e-3)
    a = float(coefficient[0] / scale[0])
    b = float(intercept - coefficient[0] * mean[0] / scale[0])
    p = sigmoid(a * s[:, 0] + b)
    return {
        "a": a,
        "b": b,
        "n_positive": int((y == 1).sum()),
        "n_negative": int((y == 0).sum()),
        "log_loss": log_loss(p, y),
        "auc": auc(s[:, 0], y),
    }


def fit_gate(
    rows: list[dict],
    labels,
    minimum_positive: int = MIN_POSITIVE,
    minimum_negative: int = MIN_NEGATIVE,
) -> dict:
    """A logistic regression on the top score of each retriever. `rows` map the name
    of a retriever to its best score (a missing name counts as 0)."""
    names = sorted({name for row in rows for name in row})
    y = np.asarray(labels, dtype=float)
    _check_counts(y, minimum_positive, minimum_negative)
    x = np.array(
        [[row.get(name) or 0.0 for name in names] for row in rows], dtype=float
    )
    mean, scale = standardise(x)
    coefficient, intercept = fit_logistic((x - mean) / scale, y, l2=1.0)
    p = sigmoid(((x - mean) / scale) @ coefficient + intercept)
    return {
        "features": names,
        "mean": mean.tolist(),
        "scale": scale.tolist(),
        "coefficients": coefficient.tolist(),
        "intercept": intercept,
        "n_positive": int((y == 1).sum()),
        "n_negative": int((y == 0).sum()),
        "log_loss": log_loss(p, y),
        "auc": auc(p, y),
    }


def fit_router(
    centroids: dict[str, np.ndarray], vectors: np.ndarray, labels: list[str]
) -> dict:
    """The temperature of a softmax over the cosine to each ecosystem centroid, chosen
    on the labelled query vectors. Needs at least two ecosystems and 10 queries."""
    names = sorted(centroids)
    if len(names) < 2 or len(labels) < 10:
        raise NotEnoughData(
            f"{len(names)} ecosystem(s) and {len(labels)} labelled query vector(s)"
        )
    matrix = np.array([centroids[n] / np.linalg.norm(centroids[n]) for n in names])
    similarity = vectors @ matrix.T
    target = np.array([names.index(label) for label in labels])
    best = None
    for temperature in TEMPERATURES:
        z = similarity / temperature
        z = z - z.max(axis=1, keepdims=True)
        probs = np.exp(z) / np.exp(z).sum(axis=1, keepdims=True)
        loss = float(
            -np.mean(np.log(np.clip(probs[np.arange(len(target)), target], 1e-9, 1)))
        )
        if best is None or loss < best[0]:
            accuracy = float((probs.argmax(axis=1) == target).mean())
            best = (loss, temperature, accuracy)
    return {
        "labels": names,
        "centroids": matrix.tolist(),
        "temperature": best[1],
        "n": len(labels),
        "log_loss": best[0],
        "accuracy": best[2],
    }


def ece(p: np.ndarray, y: np.ndarray, bins: int = 5) -> float:
    """Expected calibration error over equal-width bins of the probability."""
    edges = np.linspace(0, 1, bins + 1)
    total = 0.0
    for low, high in pairwise(edges):
        mask = (p >= low) & (p < high if high < 1 else p <= high)
        if mask.any():
            total += mask.mean() * abs(p[mask].mean() - y[mask].mean())
    return float(total) if math.isfinite(total) else float("nan")
