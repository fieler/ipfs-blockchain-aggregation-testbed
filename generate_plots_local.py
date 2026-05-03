#!/usr/bin/env python3
"""
Local plot generation from per-scenario CSV directories.

Scans <plots-dir>/<SCENARIO>/ subdirectories, merges all batch_metrics.csv
and e2e_metrics.csv files into combined DataFrames, saves the merged master
CSVs, and generates all plots — all into the same <plots-dir>.

Usage:
    python generate_plots_local.py
    python generate_plots_local.py --plots-dir .tmp_plots
    python generate_plots_local.py --tex

Dependencies:  pip install -r requirements.txt
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Import plotting functions from the middleware role without installing it.
_MW_FILES = Path(__file__).parent / "roles" / "middleware" / "files"
sys.path.insert(0, str(_MW_FILES))

try:
    from generate_boxplots import (
        setup_style,
        parse_label,
        plot_metric,
        plot_line_with_errorbars,
        plot_tx_comparison,
        print_write_reduction_table,
        SCENARIO_ORDER,
        RATE_ORDER,
    )
except ImportError as exc:
    print(f"Error: could not import generate_boxplots.py from {_MW_FILES}: {exc}")
    sys.exit(1)


_BATCH_COLS = [
    "timestamp", "scenario_label", "batching_strategy", "batch_param",
    "batch_size", "ipfs_duration_s", "chain_duration_s", "batch_duration_s",
    "tx_hash", "ipfs_cid",
    "cpu_percent", "memory_usage_bytes", "memory_limit_bytes", "throughput_msg_per_s",
]

_E2E_COLS = [
    "timestamp", "scenario_label", "tx_hash", "batch_size",
    "propagation_delay_s", "e2e_finality_min_s", "e2e_finality_max_s",
    "e2e_finality_mean_s", "nodes_total", "nodes_synced",
]


def discover_scenario_dirs(plots_dir: Path) -> list[Path]:
    """Return subdirectories of plots_dir that contain at least one metrics CSV."""
    dirs = []
    for subdir in sorted(plots_dir.iterdir()):
        if not subdir.is_dir():
            continue
        if (subdir / "batch_metrics.csv").exists() or (subdir / "e2e_metrics.csv").exists():
            dirs.append(subdir)
    return dirs


def merge_and_prepare(plots_dir: Path):
    """Discover, load, merge, and parse all scenario CSVs.

    Returns (batch_df, e2e_df) with scenario/rate/run columns added.
    Both DataFrames have unlabeled warmup rows removed.
    """
    scenario_dirs = discover_scenario_dirs(plots_dir)

    if not scenario_dirs:
        print(f"  No scenario subdirectories found in {plots_dir}")
        return None, None

    found_scenarios = {d.name for d in scenario_dirs}
    missing = [s for s in SCENARIO_ORDER if s not in found_scenarios]
    if missing:
        print(f"  Warning: missing scenarios: {', '.join(missing)}")

    print(f"  Found: {', '.join(d.name for d in scenario_dirs)}\n")

    raw_batch, raw_e2e = [], []

    for subdir in scenario_dirs:
        batch_path = subdir / "batch_metrics.csv"
        e2e_path = subdir / "e2e_metrics.csv"

        if batch_path.exists():
            df = pd.read_csv(batch_path, names=_BATCH_COLS, skiprows=1, engine="python")
            raw_batch.append(df)
            print(f"  {subdir.name}/batch_metrics.csv  — {len(df)} rows")
        else:
            print(f"  {subdir.name}/batch_metrics.csv  — not found, skipping")

        if e2e_path.exists():
            df = pd.read_csv(e2e_path)
            raw_e2e.append(df)
            print(f"  {subdir.name}/e2e_metrics.csv    — {len(df)} rows")
        else:
            print(f"  {subdir.name}/e2e_metrics.csv    — not found, skipping")

    batch_df = _prepare_batch(pd.concat(raw_batch, ignore_index=True)) if raw_batch else None
    e2e_df   = _prepare_e2e(pd.concat(raw_e2e,   ignore_index=True)) if raw_e2e   else None

    return batch_df, e2e_df


def _prepare_batch(df: pd.DataFrame) -> pd.DataFrame:
    unlabeled = (df["scenario_label"] == "unlabeled").sum()
    df = df[df["scenario_label"] != "unlabeled"].copy()
    if unlabeled:
        print(f"\n  Filtered {unlabeled} unlabeled batch rows (warmup)")

    parsed = df["scenario_label"].apply(parse_label)
    valid = parsed.notna()
    if not valid.all():
        bad = df.loc[~valid, "scenario_label"].unique().tolist()
        print(f"  Warning: skipping {(~valid).sum()} batch rows with unparseable labels: {bad}")
    df = df[valid].copy()

    df["scenario"] = parsed[valid].apply(lambda x: x[0])
    df["rate"]     = parsed[valid].apply(lambda x: x[1])
    df["run"]      = parsed[valid].apply(lambda x: x[2])

    for col in ["ipfs_duration_s", "chain_duration_s", "batch_duration_s"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["batch_size", "cpu_percent", "memory_usage_bytes",
                "memory_limit_bytes", "throughput_msg_per_s"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"  Total labeled batch rows: {len(df)}")
    return df


def _prepare_e2e(df: pd.DataFrame) -> pd.DataFrame:
    unlabeled = (df["scenario_label"] == "unlabeled").sum()
    df = df[df["scenario_label"] != "unlabeled"].copy()
    if unlabeled:
        print(f"  Filtered {unlabeled} unlabeled e2e rows (warmup)")

    parsed = df["scenario_label"].apply(parse_label)
    valid = parsed.notna()
    if not valid.all():
        bad = df.loc[~valid, "scenario_label"].unique().tolist()
        print(f"  Warning: skipping {(~valid).sum()} e2e rows with unparseable labels: {bad}")
    df = df[valid].copy()

    df["scenario"] = parsed[valid].apply(lambda x: x[0])
    df["rate"]     = parsed[valid].apply(lambda x: x[1])

    for col in ["propagation_delay_s", "e2e_finality_min_s",
                "e2e_finality_max_s", "e2e_finality_mean_s"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"  Total labeled e2e rows:   {len(df)}")
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Merge per-scenario CSVs and generate all thesis plots locally"
    )
    parser.add_argument(
        "--plots-dir", default=".tmp_plots",
        help="Directory containing per-scenario subdirectories; merged CSVs and "
             "PNGs are written here too (default: .tmp_plots)",
    )
    parser.add_argument(
        "--tex", action="store_true",
        help="Enable LaTeX rendering (requires a TeX installation)",
    )
    args = parser.parse_args()

    plots_dir = Path(args.plots_dir)

    if not plots_dir.exists():
        print(f"Error: --plots-dir '{plots_dir}' does not exist.")
        sys.exit(1)

    setup_style(args.tex)

    print(f"Scanning {plots_dir.resolve()} ...\n")
    batch_df, e2e_df = merge_and_prepare(plots_dir)

    if batch_df is None and e2e_df is None:
        print("No data found. Exiting.")
        sys.exit(1)

    # Save merged master CSVs into the same directory
    if batch_df is not None:
        out = plots_dir / "master_batch_metrics.csv"
        batch_df.to_csv(out, index=False)
        print(f"\n  Saved merged batch CSV -> {out.resolve()}")
    if e2e_df is not None:
        out = plots_dir / "master_e2e_metrics.csv"
        e2e_df.to_csv(out, index=False)
        print(f"  Saved merged e2e CSV   -> {out.resolve()}")

    output_dir = plots_dir

    # --- FF1 plots (batch metrics) ---
    if batch_df is not None and len(batch_df) > 0:
        print("\nGenerating FF1 diagrams...")
        use_agg = "run" in batch_df.columns and batch_df["run"].notna().any()

        plot_metric(
            batch_df, "batch_duration_s",
            ylabel="Batch-Dauer (s)",
            title="Batch-Verarbeitungsdauer nach Szenario",
            output_path=output_dir / "ff1_batch_duration.png",
            use_per_run_aggregation=use_agg,
        )
        plot_metric(
            batch_df, "chain_duration_s",
            ylabel="Chain-Dauer (s)",
            title="Blockchain-Transaktionsdauer nach Szenario",
            output_path=output_dir / "ff1_chain_duration.png",
            use_per_run_aggregation=use_agg,
        )
        plot_metric(
            batch_df, "ipfs_duration_s",
            ylabel="IPFS-Dauer (s)",
            title="IPFS-Upload-Dauer nach Szenario",
            output_path=output_dir / "ff1_ipfs_duration.png",
            use_per_run_aggregation=use_agg,
        )

        has_cpu = "cpu_percent" in batch_df.columns and batch_df["cpu_percent"].notna().any()
        has_ram = "memory_usage_bytes" in batch_df.columns and batch_df["memory_usage_bytes"].notna().any()

        if has_cpu:
            plot_line_with_errorbars(
                batch_df, "cpu_percent",
                ylabel="CPU-Auslastung (%)",
                title="CPU-Auslastung Middleware nach Last",
                output_path=output_dir / "ff1_cpu_vs_load.png",
            )
        else:
            print("  Skipping ff1_cpu_vs_load.png — cpu_percent column not available")

        if has_ram:
            batch_mb = batch_df.copy()
            batch_mb["memory_usage_mb"] = batch_mb["memory_usage_bytes"] / (1024 * 1024)
            plot_line_with_errorbars(
                batch_mb, "memory_usage_mb",
                ylabel="RAM-Nutzung (MB)",
                title="RAM-Nutzung Middleware nach Last",
                output_path=output_dir / "ff1_ram_vs_load.png",
            )
        else:
            print("  Skipping ff1_ram_vs_load.png — memory_usage_bytes column not available")

        plot_tx_comparison(
            batch_df,
            output_path=output_dir / "ff1_tx_comparison.png",
        )

        print_write_reduction_table(batch_df)
    else:
        print("\nSkipping FF1 diagrams — no batch data")

    # --- FF2 plots (e2e metrics) ---
    if e2e_df is not None and len(e2e_df) > 0:
        print("\nGenerating FF2 diagrams...")
        plot_metric(
            e2e_df, "e2e_finality_mean_s",
            ylabel="TTF (s)",
            title="End-to-End Time-to-Finality nach Szenario",
            output_path=output_dir / "ff2_e2e_finality.png",
        )
        plot_metric(
            e2e_df, "propagation_delay_s",
            ylabel="Propagierungsdauer (s)",
            title="Globale Propagierungsdauer nach Szenario",
            output_path=output_dir / "ff2_propagation.png",
        )
    else:
        print("\nSkipping FF2 diagrams — no e2e data")

    print(f"\nDone. Plots saved to: {plots_dir.resolve()}")


if __name__ == "__main__":
    main()
