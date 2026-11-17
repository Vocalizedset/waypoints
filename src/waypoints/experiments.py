"""Batch versus incremental processing experiment.

Scenarios use only real TLC data. The "new month" is a byte-for-byte copy of
an existing monthly file under a new month name, and the "modified month" is
a controlled copy of another real monthly file. No trips or events are
synthesized, and all raw files are restored after the experiment.

Analytics outputs are hashed row-by-row with numeric values rounded to two
decimals (the precision used by the aggregation writers). This is the only
tolerance allowed in the correctness comparison.
"""

import csv
import shutil
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from waypoints.analytics.batch import (
    PREFIX,
    discover_months,
    run_batch_analytics,
)
from waypoints.incremental import (
    MANIFEST_PATH,
    OUTPUT_SUFFIXES,
    _read_manifest,
    _source_state,
    run_incremental_pipeline,
)
from waypoints.processing.cleaning import OUTPUT_DIR, PROCESSED_DIR, RAW_DIR
from waypoints.processing.cleaning import run_cleaning_pipeline
from waypoints.processing.features import run_feature_engineering
from waypoints.processing.zones import run_zone_enrichment

EXPERIMENT_CSV = OUTPUT_DIR / "batch_vs_incremental_experiment.csv"
SNAPSHOT_DIR = OUTPUT_DIR / "experiment_snapshots"
NEW_MONTH = "2026-05"
NEW_MONTH_SOURCE = "2026-04"
MODIFIED_MONTH = "2026-03"
MODIFIED_MONTH_SOURCE = "2026-01"
BACKUP_DIR = Path("/tmp/opencode/waypoints_experiment_backup")

FIELDS = (
    "scenario",
    "approach",
    "source_months",
    "changed_months",
    "source_records_processed",
    "execution_seconds",
    "throughput_records_per_second",
    "throughput_basis",
    "monthly_output_records",
    "analytics_output_records",
    "outputs_compared",
    "mismatched_outputs",
    "correctness_match",
    "correctness_tolerance",
    "notes",
)


def _raw_path(month: str) -> Path:
    return RAW_DIR / f"{PREFIX}{month}.parquet"


def _changed_months() -> tuple[str, ...]:
    current = _source_state()
    previous = _read_manifest()
    return tuple(sorted(m for m, info in current.items() if previous.get(m) != info))


def _source_records(spark: SparkSession, months: tuple[str, ...]) -> int:
    if not months:
        return 0
    return spark.read.parquet(*(str(_raw_path(m)) for m in months)).count()


def _run_full(spark: SparkSession, months: tuple[str, ...]) -> float:
    start = time.perf_counter()
    run_cleaning_pipeline(spark, months)
    run_zone_enrichment(spark, months)
    run_feature_engineering(spark, months)
    run_batch_analytics(spark)
    return time.perf_counter() - start


def _run_incremental(spark: SparkSession) -> float:
    start = time.perf_counter()
    run_incremental_pipeline(spark)
    return time.perf_counter() - start


def _snapshot(label: str) -> Path:
    destination = SNAPSHOT_DIR / label
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)
    for path in OUTPUT_DIR.glob("analytics_*.parquet"):
        if path.is_dir():
            shutil.copytree(path, destination / path.name)
    return destination


def _restore_snapshot(snapshot: Path) -> None:
    for path in OUTPUT_DIR.glob("analytics_*.parquet"):
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    for path in snapshot.glob("analytics_*.parquet"):
        shutil.copytree(path, OUTPUT_DIR / path.name)


def _backup_processed_month(month: str) -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    for suffix in OUTPUT_SUFFIXES:
        source = PROCESSED_DIR / f"{PREFIX}{month}{suffix}"
        destination = BACKUP_DIR / source.name
        if destination.exists():
            if destination.is_dir():
                shutil.rmtree(destination)
            else:
                destination.unlink()
        if source.is_dir():
            shutil.copytree(source, destination)
        elif source.exists():
            shutil.copy2(source, destination)


def _restore_processed_month(month: str) -> None:
    for suffix in OUTPUT_SUFFIXES:
        target = PROCESSED_DIR / f"{PREFIX}{month}{suffix}"
        source = BACKUP_DIR / target.name
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        if source.is_dir():
            shutil.copytree(source, target)
        elif source.exists():
            shutil.copy2(source, target)


def _output_records(spark: SparkSession, snapshot: Path) -> tuple[int, int]:
    monthly = discover_months()
    monthly_records = 0
    if monthly:
        monthly_records = spark.read.parquet(
            *(str(PROCESSED_DIR / f"{PREFIX}{m}_features.parquet") for m in monthly)
        ).count()
    analytics_records = sum(
        spark.read.parquet(str(path)).count()
        for path in snapshot.glob("analytics_*.parquet")
    )
    return monthly_records, analytics_records


def _fingerprint(df: DataFrame) -> DataFrame:
    columns = []
    for name, dtype in df.dtypes:
        numeric = dtype in ("int", "bigint", "float", "double") or dtype.startswith("decimal")
        if numeric:
            columns.append(F.round(F.col(name), 2).cast("string"))
        else:
            columns.append(F.col(name).cast("string"))
    hashed = df.select(F.sha2(F.concat_ws("||", *columns), 256).alias("row_hash"))
    return hashed.groupBy("row_hash").count()


def _compare(spark: SparkSession, batch: Path, incremental: Path) -> tuple[int, list[str]]:
    names = sorted({p.name for p in batch.glob("analytics_*.parquet")}
                   | {p.name for p in incremental.glob("analytics_*.parquet")})
    mismatches = []
    for name in names:
        left_path, right_path = batch / name, incremental / name
        if not left_path.exists() or not right_path.exists():
            mismatches.append(name)
            continue
        left, right = spark.read.parquet(str(left_path)), spark.read.parquet(str(right_path))
        if [f"{n}:{t}" for n, t in left.dtypes] != [f"{n}:{t}" for n, t in right.dtypes]:
            mismatches.append(name)
            continue
        joined = (
            _fingerprint(left).withColumnRenamed("count", "batch_count")
            .join(
                _fingerprint(right).withColumnRenamed("count", "incremental_count"),
                "row_hash", "full_outer",
            )
        )
        bad = joined.filter(
            F.col("batch_count").isNull()
            | F.col("incremental_count").isNull()
            | (F.col("batch_count") != F.col("incremental_count"))
        ).limit(1).count()
        if bad:
            mismatches.append(name)
    return len(names), mismatches


def _result_row(
    scenario: str,
    approach: str,
    source_months: tuple[str, ...],
    changed_months: tuple[str, ...],
    records: int,
    seconds: float,
    monthly_records: int,
    analytics_records: int,
    outputs_compared: int,
    mismatched_outputs: list[str],
    correctness_match: bool,
    correctness_tolerance: str,
    notes: str,
) -> dict:
    return {
        "scenario": scenario,
        "approach": approach,
        "source_months": ",".join(source_months),
        "changed_months": ",".join(changed_months),
        "source_records_processed": records,
        "execution_seconds": round(seconds, 3),
        "throughput_records_per_second": round(records / seconds, 1) if seconds else 0.0,
        "throughput_basis": "source_records_processed",
        "monthly_output_records": monthly_records,
        "analytics_output_records": analytics_records,
        "outputs_compared": outputs_compared,
        "mismatched_outputs": ",".join(mismatched_outputs),
        "correctness_match": correctness_match,
        "correctness_tolerance": correctness_tolerance,
        "notes": notes,
    }


def _scenario(
    spark: SparkSession,
    results: list[dict],
    scenario: str,
    months: tuple[str, ...],
    incremental_changed: tuple[str, ...],
    reset_manifest: bool,
    notes: str,
) -> None:
    if reset_manifest and MANIFEST_PATH.exists():
        MANIFEST_PATH.unlink()

    full_records = _source_records(spark, months)
    full_seconds = _run_full(spark, months)
    batch_snapshot = _snapshot(f"{scenario}_batch")
    batch_monthly, batch_analytics = _output_records(spark, batch_snapshot)

    incremental_records = _source_records(spark, incremental_changed)
    incremental_seconds = _run_incremental(spark)
    incremental_snapshot = _snapshot(f"{scenario}_incremental")
    incremental_monthly, incremental_analytics = _output_records(spark, incremental_snapshot)

    compared, mismatches = _compare(spark, batch_snapshot, incremental_snapshot)
    common = dict(
        scenario=scenario,
        outputs_compared=compared,
        mismatched_outputs=mismatches,
        correctness_match=not mismatches,
        correctness_tolerance="numeric rounded to 2 decimals before row-hash comparison",
    )
    results.append(_result_row(
        **common, approach="batch", source_months=months, changed_months=months,
        records=full_records, seconds=full_seconds, monthly_records=batch_monthly,
        analytics_records=batch_analytics, notes=notes,
    ))
    results.append(_result_row(
        **common, approach="incremental", source_months=months,
        changed_months=incremental_changed, records=incremental_records,
        seconds=incremental_seconds,
        monthly_records=incremental_monthly, analytics_records=incremental_analytics,
        notes=notes,
    ))


def run_experiment(spark: SparkSession) -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    manifest_backup = MANIFEST_PATH.read_text() if MANIFEST_PATH.exists() else None
    modified_backup = BACKUP_DIR / f"{PREFIX}{MODIFIED_MONTH}.parquet.backup"
    shutil.copy2(_raw_path(MODIFIED_MONTH), modified_backup)
    _backup_processed_month(MODIFIED_MONTH)
    baseline_snapshot = _snapshot("baseline")
    initial_months = tuple(sorted(_source_state()))
    new_path = _raw_path(NEW_MONTH)
    created_new_month = False
    results: list[dict] = []

    try:
        _scenario(
            spark, results, "initial_run", initial_months, initial_months, True,
            "First run: no manifest for incremental; both process all available months.",
        )

        shutil.copy2(_raw_path(NEW_MONTH_SOURCE), new_path)
        created_new_month = True
        new_changed = _changed_months()
        assert new_changed == (NEW_MONTH,), f"Expected only {NEW_MONTH}, got {new_changed}"
        months_with_new = tuple(sorted(_source_state()))
        _scenario(
            spark, results, "new_month", months_with_new, new_changed, False,
            "New month is a byte-for-byte copy of real 2026-04 TLC data under a new name.",
        )

        shutil.copy2(_raw_path(MODIFIED_MONTH_SOURCE), _raw_path(MODIFIED_MONTH))
        changed = _changed_months()
        assert changed == (MODIFIED_MONTH,), f"Expected only {MODIFIED_MONTH}, got {changed}"
        _scenario(
            spark, results, "modified_month", months_with_new, changed, False,
            "Modified month is a controlled copy of real 2026-01 TLC data over 2026-03.",
        )
    finally:
        if created_new_month and new_path.exists():
            new_path.unlink()
        shutil.copy2(modified_backup, _raw_path(MODIFIED_MONTH))
        for path in PROCESSED_DIR.glob(f"{PREFIX}{NEW_MONTH}_*"):
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        _restore_processed_month(MODIFIED_MONTH)
        _restore_snapshot(baseline_snapshot)
        if manifest_backup is None:
            if MANIFEST_PATH.exists():
                MANIFEST_PATH.unlink()
        else:
            MANIFEST_PATH.write_text(manifest_backup)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with EXPERIMENT_CSV.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(results)
    print(f"Experiment results: {EXPERIMENT_CSV}")
