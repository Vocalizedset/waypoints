"""File-based incremental processing for monthly Yellow Taxi datasets."""

import json
import shutil

from pyspark.sql import SparkSession

from waypoints.analytics.batch import run_batch_analytics
from waypoints.processing.cleaning import PROCESSED_DIR, RAW_DIR, run_cleaning_pipeline
from waypoints.processing.features import run_feature_engineering
from waypoints.processing.zones import run_zone_enrichment

MANIFEST_PATH = PROCESSED_DIR / "processing_manifest.json"
PREFIX = "yellow_tripdata_"
OUTPUT_SUFFIXES = ("_cleaned.parquet", "_enriched.parquet", "_features.parquet")


def _source_state() -> dict[str, dict]:
    """Map each source month to its file name, size and modification time."""
    state = {}
    for path in RAW_DIR.glob(f"{PREFIX}*.parquet"):
        stat = path.stat()
        state[path.stem.removeprefix(PREFIX)] = {
            "file": path.name,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    return state


def _read_manifest() -> dict[str, dict]:
    if not MANIFEST_PATH.exists():
        return {}
    return json.loads(MANIFEST_PATH.read_text()).get("files", {})


def _write_manifest(files: dict[str, dict]) -> None:
    MANIFEST_PATH.write_text(json.dumps({"files": files}, indent=2, sort_keys=True) + "\n")


def _remove_month_outputs(month: str) -> None:
    for suffix in OUTPUT_SUFFIXES:
        path = PROCESSED_DIR / f"{PREFIX}{month}{suffix}"
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()


def run_incremental_pipeline(spark: SparkSession) -> None:
    """Process new/changed months, then rebuild analytics from all months."""
    current = _source_state()
    previous = _read_manifest()
    changed = tuple(sorted(m for m, info in current.items() if previous.get(m) != info))
    stale = tuple(sorted(set(previous) - set(current)))

    for month in stale:
        _remove_month_outputs(month)
    if stale:
        print(f"Incremental: removed stale months ({', '.join(stale)})")

    if changed:
        print(f"Incremental: processing {len(changed)} months ({', '.join(changed)})")
        run_cleaning_pipeline(spark, changed)
        run_zone_enrichment(spark, changed)
        run_feature_engineering(spark, changed)
    else:
        print("Incremental: no new or modified months")

    run_batch_analytics(spark)
    _write_manifest(current)
