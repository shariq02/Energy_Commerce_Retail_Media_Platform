# ====================================================================
# Shared Phase 2b physical chunk writer
# Energy Commerce & Retail Media Analytics Platform
# ====================================================================
# Purpose: write one logical Phase 2b dataset out as a sequence of
# physical chunk files, each capped at MAX_CHUNK_ROWS rows and
# MAX_CHUNK_BYTES bytes. A chunk is purely a physical-file/upload
# concern -- all chunks written by one ChunkedCSVWriter instance remain
# one logical dataset and one Volume upload unit; chunk count must
# never be read as dataset count.
#
# Each write() call is handed a DataFrame no larger than one processing
# batch (<=100,000 rows, per the memory guard). That batch is written to
# disk immediately -- if it would push the current physical chunk past
# either limit, the batch itself is sliced by row so no output file
# exceeds MAX_CHUNK_BYTES, and the current chunk is closed/rotated
# before the remainder is written. The full dataset is never held in
# RAM: only the one incoming batch, plus a running byte count for the
# currently-open chunk.
# ====================================================================

from pathlib import Path

import pandas as pd

MAX_CHUNK_ROWS = 100_000
MAX_CHUNK_BYTES = 50 * 1024 * 1024  # 50 MiB


class ChunkedCSVWriter:
    def __init__(
        self,
        out_dir: Path,
        source: str,
        dataset: str,
        max_rows: int = MAX_CHUNK_ROWS,
        max_bytes: int = MAX_CHUNK_BYTES,
        extension: str = "csv",
        sep: str = ",",
    ):
        self.out_dir = out_dir
        self.source = source
        self.dataset = dataset
        self.max_rows = max_rows
        self.max_bytes = max_bytes
        self.extension = extension
        self.sep = sep

        self.out_dir.mkdir(parents=True, exist_ok=True)

        self._chunk_index = 0        # last chunk number actually opened (1-based)
        self._current_path: Path | None = None
        self._current_rows = 0
        self._current_bytes = 0
        self._current_has_header = False

        self.total_rows = 0
        self.chunk_paths: list[Path] = []

    def _chunk_path(self, index: int) -> Path:
        return self.out_dir / f"{self.source}_{self.dataset}_chunk_{index:05d}.{self.extension}"

    def _open_new_chunk(self) -> None:
        self._chunk_index += 1
        self._current_path = self._chunk_path(self._chunk_index)
        self._current_rows = 0
        self._current_bytes = 0
        self._current_has_header = False
        self.chunk_paths.append(self._current_path)

    def _append(self, df: pd.DataFrame) -> None:
        if self._current_path is None:
            self._open_new_chunk()
        first_write = not self._current_has_header
        df.to_csv(
            self._current_path, mode="w" if first_write else "a",
            header=first_write, index=False, encoding="utf-8", sep=self.sep,
        )
        self._current_has_header = True
        self._current_rows += len(df)
        self._current_bytes = self._current_path.stat().st_size
        self.total_rows += len(df)

    def write(self, df: pd.DataFrame) -> None:
        """Write one processing batch (<=max_rows), splitting it across
        chunk-file boundaries as needed. Never buffers more than the
        batch handed in plus one row-slice of it."""
        if df.empty:
            return

        remaining = df
        while len(remaining) > 0:
            if self._current_path is None:
                self._open_new_chunk()

            room_rows = self.max_rows - self._current_rows
            if room_rows <= 0:
                self._open_new_chunk()
                continue

            head = remaining.iloc[:room_rows]
            tail = remaining.iloc[room_rows:]

            # Estimate whether `head` fits the remaining byte budget for
            # the current chunk; if not, binary-search down to the
            # largest row-prefix that does.
            est_bytes = len(head.to_csv(index=False, header=not self._current_has_header, sep=self.sep).encode("utf-8"))
            if self._current_bytes + est_bytes > self.max_bytes and self._current_rows > 0:
                self._open_new_chunk()
                continue

            if self._current_bytes + est_bytes > self.max_bytes:
                # Even an empty chunk can't fit the full head -- shrink
                # head by row count until it fits (guarantees progress
                # since a single row always eventually fits under any
                # sane per-row size, and this only runs on an empty
                # freshly-opened chunk).
                lo, hi = 1, len(head)
                fit = 1
                while lo <= hi:
                    mid = (lo + hi) // 2
                    trial = head.iloc[:mid]
                    trial_bytes = len(trial.to_csv(index=False, header=not self._current_has_header, sep=self.sep).encode("utf-8"))
                    if trial_bytes <= self.max_bytes:
                        fit = mid
                        lo = mid + 1
                    else:
                        hi = mid - 1
                tail = pd.concat([head.iloc[fit:], tail]) if fit < len(head) else tail
                head = head.iloc[:fit]

            self._append(head)
            del head
            remaining = tail
            del tail

    def close(self) -> None:
        self._current_path = None
        self._current_rows = 0
        self._current_bytes = 0
        self._current_has_header = False
