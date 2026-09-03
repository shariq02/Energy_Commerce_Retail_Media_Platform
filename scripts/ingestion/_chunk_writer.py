# Shared physical chunk writer
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: write one logical staging dataset out as a sequence of
# physical chunk files, each capped at MAX_CHUNK_ROWS rows and
# MAX_CHUNK_BYTES bytes. A chunk is purely a physical-file/upload
# concern -- all chunks written by one ChunkedCSVWriter instance remain
# one logical dataset and one Volume upload unit; chunk count must
# never be read as dataset count.
#
# Each write() call is handed a DataFrame no larger than one processing
# batch (<=300,000 rows, per the memory guard). The batch is serialised
# to CSV bytes exactly ONCE per chunk-append and written straight to the
# open chunk file; the byte budget is tracked with a running counter
# (the encoded length actually written), never a filesystem stat() and
# never a throwaway second serialisation just to measure size. The full
# dataset is never held in RAM: only the one incoming batch plus its
# single encoded copy.

from pathlib import Path

import pandas as pd

MAX_CHUNK_ROWS = 300_000
MAX_CHUNK_BYTES = 80 * 1024 * 1024  # 80 MiB


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

        self._chunk_index = 0  # last chunk number actually opened (1-based)
        self._current_path: Path | None = None
        self._current_rows = 0
        self._current_bytes = 0  # running count of bytes written to the open chunk
        self._current_has_header = False

        self.total_rows = 0
        self.chunk_paths: list[Path] = []

    def _chunk_path(self, index: int) -> Path:
        return (
            self.out_dir
            / f"{self.source}_{self.dataset}_chunk_{index:05d}.{self.extension}"
        )

    def _open_new_chunk(self) -> None:
        self._chunk_index += 1
        self._current_path = self._chunk_path(self._chunk_index)
        self._current_rows = 0
        self._current_bytes = 0
        self._current_has_header = False
        self.chunk_paths.append(self._current_path)

    def _encode(self, df: pd.DataFrame, header: bool) -> bytes:
        """Serialise one row-slice to CSV bytes. This is the ONLY place a
        frame is turned into CSV -- callers reuse the returned bytes for
        both the size check and the actual write."""
        return df.to_csv(index=False, header=header, sep=self.sep).encode("utf-8")

    def _append(self, data: bytes, rows: int) -> None:
        if self._current_path is None:
            self._open_new_chunk()
        # A fresh chunk (no header written yet) is created/truncated; an
        # existing one is appended to. `data` was encoded with header iff
        # this is the first write to the chunk.
        mode = "ab" if self._current_has_header else "wb"
        with open(self._current_path, mode) as f:
            f.write(data)
        self._current_has_header = True
        self._current_rows += rows
        self._current_bytes += len(data)
        self.total_rows += rows

    def write(self, df: pd.DataFrame) -> None:
        """Write one processing batch (<=max_rows), splitting it across
        chunk-file boundaries as needed. Holds only the batch handed in
        plus one encoded CSV copy of the slice being written."""
        if df.empty:
            return

        n = len(df)
        start = 0
        while start < n:
            if self._current_path is None:
                self._open_new_chunk()

            room_rows = self.max_rows - self._current_rows
            if room_rows <= 0:
                self._open_new_chunk()
                continue

            end = min(start + room_rows, n)
            head = df.iloc[start:end]
            need_header = not self._current_has_header
            data = self._encode(head, need_header)

            # If `head` would push the open chunk past the byte cap and the
            # chunk already holds rows, rotate and re-encode (with header)
            # on the next iteration.
            if (
                self._current_rows > 0
                and self._current_bytes + len(data) > self.max_bytes
            ):
                self._open_new_chunk()
                continue

            # Fresh/empty chunk still can't hold the whole slice: size a
            # prefix from the measured average row width, then trim if the
            # estimate ran over. Guarantees progress (fit >= 1).
            if self._current_bytes + len(data) > self.max_bytes:
                avg = max(1, len(data) // max(1, len(head)))
                fit = min(
                    len(head), max(1, (self.max_bytes - self._current_bytes) // avg)
                )
                data = self._encode(head.iloc[:fit], need_header)
                while fit > 1 and len(data) > self.max_bytes:
                    fit = max(1, int(fit * 0.9))
                    data = self._encode(head.iloc[:fit], need_header)
                head = head.iloc[:fit]
                end = start + fit

            self._append(data, len(head))
            start = end

    def close(self) -> None:
        self._current_path = None
        self._current_rows = 0
        self._current_bytes = 0
        self._current_has_header = False
