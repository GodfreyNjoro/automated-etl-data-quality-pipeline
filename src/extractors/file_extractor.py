"""File extractor for CSV, JSON and Excel sources.

Supported ``options``:

    path:            Path to the file (or glob for multiple CSV/JSON files).
    encoding:        Text encoding (CSV/JSON).
    delimiter:       CSV field delimiter.
    sheet_name:      Excel sheet name or index.
    json_orient:     Orientation for JSON (records|columns|...); default records.
    read_options:    Extra kwargs forwarded to the pandas reader.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any

import pandas as pd

from src.extractors.base import BaseExtractor, ExtractionResult


class FileExtractor(BaseExtractor):
    """Extract data from CSV, JSON or Excel files."""

    def _resolve_paths(self) -> list[str]:
        pattern = self.options["path"]
        matches = sorted(glob.glob(pattern)) if any(c in pattern for c in "*?[") else [pattern]
        missing = [p for p in matches if not Path(p).exists()]
        if not matches or missing:
            raise FileNotFoundError(f"No files matched path: {pattern}")
        return matches

    def _read_one(self, path: str) -> pd.DataFrame:
        read_options: dict[str, Any] = dict(self.options.get("read_options", {}))
        if self.config.type == "csv":
            return pd.read_csv(
                path,
                encoding=self.options.get("encoding", "utf-8"),
                sep=self.options.get("delimiter", ","),
                **read_options,
            )
        if self.config.type == "json":
            records_path = self.options.get("records_path")
            if records_path:
                import json

                with open(path, encoding=self.options.get("encoding", "utf-8")) as fh:
                    payload: Any = json.load(fh)
                for key in str(records_path).split("."):
                    payload = payload[key]
                return pd.json_normalize(payload)
            return pd.read_json(
                path,
                orient=self.options.get("json_orient", "records"),
                **read_options,
            )
        if self.config.type == "excel":
            return pd.read_excel(
                path,
                sheet_name=self.options.get("sheet_name", 0),
                **read_options,
            )
        raise ValueError(f"Unsupported file source type: {self.config.type}")

    def extract(self, since: Any | None = None) -> ExtractionResult:
        paths = self._resolve_paths()
        frames = [self._read_one(p) for p in paths]
        frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        # Optional incremental filter on a timestamp column.
        incremental_key = self.options.get("incremental_key")
        if since is not None and incremental_key and incremental_key in frame.columns:
            frame = frame[pd.to_datetime(frame[incremental_key]) > pd.to_datetime(since)]

        self.log.info(
            "File extraction complete",
            extra={"source": self.config.name, "rows": len(frame), "files": len(paths)},
        )
        return ExtractionResult.from_frame(self.config, frame, files=paths)

    def test_connection(self) -> bool:
        try:
            self._resolve_paths()
            return True
        except FileNotFoundError:
            return False
