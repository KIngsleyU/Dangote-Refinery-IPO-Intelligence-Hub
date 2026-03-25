# data_engineering/ingestion/run_all_ingestion.py

"""
run_all_ingestion.py

High-level orchestrator that runs a set of ingestion scripts under
`data_engineering/ingestion/` and snapshots their outputs into
`data_engineering/documents/`.

For each script run, it:
- Takes a snapshot of `data_engineering/ingestion/data/` (all files).
- Runs the script with `python data_engineering/ingestion/<script>.py`.
- Compares the data snapshot before/after to find new or modified files.
- Creates a new folder in `data_engineering/documents/` with an
  informative name like:
    `<timestamp>__<script_stem>/...`
- Copies the changed files into that folder, preserving relative
  paths under the `data_engineering/ingestion/data/` root.

This gives you a per-run, per-script “bundle” of outputs that your
RAG / analytics layer can inspect or archive, without changing the
behaviour of the individual ingestion scripts themselves.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import json
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INGESTION_DIR = BASE_DIR / "data_engineering" / "ingestion"
DATA_ROOT = INGESTION_DIR / "data"
DOCS_ROOT = BASE_DIR / "data_engineering" / "documents"


DOCS_ROOT.mkdir(parents=True, exist_ok=True)


@dataclass
class IngestionTask:
    script_name: str  # e.g. "macro_energy_extractor.py"
    label: str        # human-friendly label for folder names


# Scripts that produce finite outputs into data_engineering/ingestion/data.
# Long-running streams (e.g. ais_telemetry_stream.py) are intentionally
# not included; you can add them if you want them orchestrated as well.
TASKS: List[IngestionTask] = [
    IngestionTask("acled_events_extractor.py", "acled_events"),
    # IngestionTask("ais_telemetry_stream_v2.py", "ais_telemetry"),
    IngestionTask("dynamic_market_scraper.py", "black_market_USDT_NGN_fx_rates"),
    IngestionTask("geopolitics_extractor.py", "acled_hrp_trends"),
    IngestionTask("ingestion_data_client.py", "ingestion_data"),
    IngestionTask("macro_energy_extractor.py", "macro_energy"),
    # IngestionTask("market_data_client.py", "market_data"),
]


def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def snapshot_data_dir() -> Dict[Path, float]:
    """Return mapping of file -> mtime under DATA_ROOT."""
    snapshot: Dict[Path, float] = {}
    if not DATA_ROOT.exists():
        return snapshot
    for path in DATA_ROOT.rglob("*"):
        if path.is_file():
            try:
                snapshot[path] = path.stat().st_mtime
            except FileNotFoundError:
                continue
    return snapshot


def diff_snapshots(before: Dict[Path, float], after: Dict[Path, float]) -> List[Path]:
    """Return files that are new or modified between snapshots."""
    changed: List[Path] = []
    for path, mtime in after.items():
        old_mtime = before.get(path)
        if old_mtime is None or mtime > old_mtime + 1e-6:
            changed.append(path)
    return changed


def run_task(task: IngestionTask) -> None:
    script_path = INGESTION_DIR / task.script_name
    if not script_path.exists():
        print(f"[SKIP] {task.script_name} not found.")
        return

    print(f"\n=== Running ingestion script: {task.script_name} ===")
    before = snapshot_data_dir()

    start = time.time()
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
    )
    elapsed = time.time() - start

    if proc.stdout:
        print(f"[{task.label}] stdout:\n{proc.stdout}")
    if proc.stderr:
        print(f"[{task.label}] stderr:\n{proc.stderr}", file=sys.stderr)

    if proc.returncode != 0:
        print(f"[WARN] {task.script_name} exited with code {proc.returncode} after {elapsed:.1f}s")
    else:
        print(f"[OK] {task.script_name} completed in {elapsed:.1f}s")

    after = snapshot_data_dir()
    changed_files = diff_snapshots(before, after)

    if not changed_files:
        print(f"[INFO] No new or modified files detected for {task.label}.")
        return

    run_folder = DOCS_ROOT / f"{now_ts()}__{task.label}"
    run_folder.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Saving {len(changed_files)} outputs to {run_folder}")
    for src in changed_files:
        try:
            rel = src.relative_to(DATA_ROOT)
        except ValueError:
            # Shouldn't happen, but guard against copying outside DATA_ROOT
            rel = src.name
        dest = run_folder / rel
        dest.parent.mkdir(parents=True, exist_ok=True)

        # For specific heavy ACLED outputs, write trimmed views into the documents snapshot.
        if task.label == "acled_events" and src.suffix == ".jsonl":
            _copy_trimmed_acled_events(src, dest)
        elif task.label == "acled_hrp_trends" and src.suffix.lower() in {".xlsx", ".xls"}:
            _copy_trimmed_hrp_workbook(src, dest)
        else:
            shutil.copy2(src, dest)


def _copy_trimmed_acled_events(src: Path, dest: Path) -> None:
    """Write a filtered copy of ACLED events into snapshot.

    Rule: from the latest event_date in the source file, keep events from
    approximately the last 4 months. If that yields nothing (e.g. dates
    missing), fall back to the last 500 events. This NEVER mutates the
    original source file.
    """
    from datetime import datetime, timedelta

    events: list[dict] = []
    kept: list[dict] = []
    with src.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            events.append(ev)

    # If there are no events at all, just create an empty file.
    if not events:
        dest.write_text("", encoding="utf-8")
        return

    # Compute date range based on the data itself (ACLED may be lagged).
    dates = []
    for ev in events:
        d_str = ev.get("event_date")
        if not d_str:
            continue
        try:
            d = datetime.strptime(d_str, "%Y-%m-%d").date()
            dates.append(d)
        except Exception:
            continue

    if dates:
        latest = max(d for d in dates if d is not None)
        cutoff = latest - timedelta(days=120)  # ~last 4 months relative to latest event
        for ev, d in zip(events, dates):
            if d is not None and d >= cutoff:
                kept.append(ev)

    # If for some reason nothing passes the cutoff but we have data, keep a small tail as fallback.
    if not kept:
        kept = events[-500:]  # last 500 events as a reasonable slice

    with dest.open("w", encoding="utf-8") as f:
        for ev in kept:
            f.write(json.dumps(ev) + "\n")


def _copy_trimmed_hrp_workbook(src: Path, dest: Path) -> None:
    """Write a filtered copy of ACLED HRP monthly aggregates into snapshot.

    Rule: from the last recorded month-year in the sheet, keep only the
    last 4 months. If month information can't be reliably detected,
    fall back to keeping the last 4 rows. This NEVER mutates the
    original source file.
    """
    try:
        df = pd.read_excel(src)
        if df.empty:
            df.to_excel(dest, index=False)
            return

        # Try to detect a month column
        month_col = None
        for col in df.columns:
            col_str = str(col).lower()
            if "month-year" in col_str or "month_year" in col_str or "month" in col_str:
                month_col = col
                break

        if month_col is not None:
            s = df[month_col].astype(str)
            try:
                periods = pd.PeriodIndex(s, freq="M")
                df["_period"] = periods
                max_p = df["_period"].max()
                if pd.isna(max_p):
                    raise ValueError("Invalid period values")
                cutoff = max_p - 3  # last 4 months inclusive
                df = df[df["_period"] >= cutoff].drop(columns=["_period"])
            except Exception:
                # Fallback: if parsing fails, just take the last 4 rows
                if len(df) > 4:
                    df = df.tail(4)
        else:
            # No obvious month column; keep the last 4 rows
            if len(df) > 4:
                df = df.tail(4)

        df.to_excel(dest, index=False)
    except Exception:
        # Fallback: copy original if trimming fails
        shutil.copy2(src, dest)

def main() -> None:
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"DATA_ROOT: {DATA_ROOT}")
    print(f"DOCS_ROOT: {DOCS_ROOT}")
    for task in TASKS:
        run_task(task)
    print("\nAll configured ingestion tasks have finished.")


if __name__ == "__main__":
    main()

