#!/usr/bin/env python3
"""
Generate publication-quality boxplot diagrams from experiment CSV data.

Layout: one PNG per metric per scenario. Single axis, x-axis = scenarios, grouped boxes = msg/s rates.

Usage:
    python generate_boxplots.py --data-dir ./data --output-dir ./plots
    python generate_boxplots.py --data-dir ./data --output-dir ./plots --tex
"""

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# --- Constants ---

SCENARIO_ORDER = ["N", "V1", "V2", "V3", "Z1", "Z2", "Z3"]
RATE_ORDER = [1, 5, 10]


def setup_style(use_tex: bool):
    """Configure matplotlib for academic publication style."""
    plt.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Computer Modern Roman", "Times New Roman", "DejaVu Serif"],
        "font.size": 10,
        "axes.titlesize": 11,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "figure.dpi": 900,
        "savefig.dpi": 900,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.05,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "grid.linestyle": "--",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "boxplot.flierprops.markersize": 3,
        "boxplot.flierprops.marker": "o",
        "text.usetex": use_tex,
    })


def parse_label(label: str):
    """Parse experiment label into (scenario, rate, run).

    Supports two formats:
      - 'V1_5rps'       → ('V1', 5, None)   (legacy, no run suffix)
      - 'V1_5rps_r3'    → ('V1', 5, 3)      (queue scheduler format)

    Returns None on failure.
    """
    # Strip optional run suffix (_r<N>)
    run = None
    if "_r" in label:
        base, run_part = label.rsplit("_r", 1)
        try:
            run = int(run_part)
            label = base
        except ValueError:
            pass  # not a valid run suffix — treat as part of scenario name

    parts = label.rsplit("_", 1)
    if len(parts) != 2:
        return None
    scenario = parts[0]
    rate_str = parts[1].replace("rps", "")
    try:
        rate = int(rate_str)
    except ValueError:
        try:
            rate = int(float(rate_str))
        except ValueError:
            return None
    return scenario, rate, run


def aggregate_per_run(df, metric_col):
    """For FF1 metrics: compute per-run mean, then return those means.

    Groups by (scenario, rate, run) → mean of metric_col per group.
    Returns a DataFrame with columns: scenario, rate, run, <metric_col>_mean.
    Rows where run is None are treated as a single run (run=0).
    """
    df = df.copy()
    df["run"] = df["run"].fillna(0).astype(int)
    grouped = df.groupby(["scenario", "rate", "run"])[metric_col].mean().reset_index()
    grouped = grouped.rename(columns={metric_col: f"{metric_col}_mean"})
    return grouped


def load_data(data_dir: Path):
    """Load and validate CSV files."""
    batch_path = data_dir / "batch_metrics.csv"
    e2e_path = data_dir / "e2e_metrics.csv"

    batch_df = None
    e2e_df = None

    fmt_hint = "  Expected label format: {Scenario}_{Rate}rps, e.g. N_1rps, V1_5rps, Z2_10rps"

    if batch_path.exists():
        # Handle schema evolution: old files have 10-column header, new rows have 14 columns
        # (cpu_percent, memory_usage_bytes, memory_limit_bytes, throughput_msg_per_s added later).
        # Provide all known column names and skip the potentially stale file header.
        _batch_all_cols = [
            "timestamp", "scenario_label", "batching_strategy", "batch_param",
            "batch_size", "ipfs_duration_s", "chain_duration_s", "batch_duration_s",
            "tx_hash", "ipfs_cid",
            "cpu_percent", "memory_usage_bytes", "memory_limit_bytes", "throughput_msg_per_s",
        ]
        batch_df = pd.read_csv(batch_path, names=_batch_all_cols, skiprows=1, engine="python")
        total_rows = len(batch_df)
        unlabeled = (batch_df["scenario_label"] == "unlabeled").sum()
        batch_df = batch_df[batch_df["scenario_label"] != "unlabeled"].copy()
        if unlabeled > 0:
            print(f"  Filtered out {unlabeled} unlabeled batch rows (warmup data)")
        parsed = batch_df["scenario_label"].apply(parse_label)
        valid = parsed.notna()
        if not valid.all():
            invalid_labels = batch_df.loc[~valid, "scenario_label"].unique().tolist()
            print(f"  Warning: Skipping {(~valid).sum()} batch rows with unparseable labels: {invalid_labels}")
            print(fmt_hint)
        batch_df = batch_df[valid].copy()
        if len(batch_df) == 0 and total_rows > 0:
            print(f"  Error: All {total_rows} rows were filtered out. No valid scenario labels found.")
            print(fmt_hint)
        else:
            batch_df["scenario"] = parsed[valid].apply(lambda x: x[0])
            batch_df["rate"] = parsed[valid].apply(lambda x: x[1])
            batch_df["run"] = parsed[valid].apply(lambda x: x[2])
            for col in ["ipfs_duration_s", "chain_duration_s", "batch_duration_s"]:
                batch_df[col] = pd.to_numeric(batch_df[col], errors="coerce")
            for col in ["cpu_percent", "memory_usage_bytes", "memory_limit_bytes", "throughput_msg_per_s"]:
                if col in batch_df.columns:
                    batch_df[col] = pd.to_numeric(batch_df[col], errors="coerce")
            print(f"  Loaded {len(batch_df)} batch metric rows")
    else:
        print(f"  Warning: {batch_path} not found")

    if e2e_path.exists():
        e2e_df = pd.read_csv(e2e_path)
        total_rows = len(e2e_df)
        unlabeled = (e2e_df["scenario_label"] == "unlabeled").sum()
        e2e_df = e2e_df[e2e_df["scenario_label"] != "unlabeled"].copy()
        if unlabeled > 0:
            print(f"  Filtered out {unlabeled} unlabeled e2e rows (warmup data)")
        parsed = e2e_df["scenario_label"].apply(parse_label)
        valid = parsed.notna()
        if not valid.all():
            invalid_labels = e2e_df.loc[~valid, "scenario_label"].unique().tolist()
            print(f"  Warning: Skipping {(~valid).sum()} e2e rows with unparseable labels: {invalid_labels}")
            print(fmt_hint)
        e2e_df = e2e_df[valid].copy()
        if len(e2e_df) == 0 and total_rows > 0:
            print(f"  Error: All {total_rows} rows were filtered out. No valid scenario labels found.")
            print(fmt_hint)
        else:
            e2e_df["scenario"] = parsed[valid].apply(lambda x: x[0])
            e2e_df["rate"] = parsed[valid].apply(lambda x: x[1])
            # run column not needed for FF2 (all runs pooled together)
            for col in ["propagation_delay_s", "e2e_finality_min_s", "e2e_finality_max_s", "e2e_finality_mean_s"]:
                e2e_df[col] = pd.to_numeric(e2e_df[col], errors="coerce")
            print(f"  Loaded {len(e2e_df)} e2e metric rows")
    else:
        print(f"  Warning: {e2e_path} not found")

    return batch_df, e2e_df


def plot_metric(df, metric_col, ylabel, title, output_path,
                scenario_order=SCENARIO_ORDER, rate_order=RATE_ORDER,
                use_per_run_aggregation=False):
    """
    One PNG per metric per scenario.
    Single axis: x = msg/s rates, one box per rate.

    If use_per_run_aggregation=True (FF1 metrics): each box contains one value
    per repetition (mean of all batches in that run), giving N=repetitions boxes.
    If False (FF2 metrics): raw values are used directly.
    """
    available_rates = [r for r in rate_order if r in df["rate"].unique()]
    available_scenarios = [s for s in scenario_order if s in df["scenario"].unique()]

    if not available_rates or not available_scenarios:
        print(f"  Skipping {title}: no data")
        return

    if use_per_run_aggregation:
        agg_df = aggregate_per_run(df, metric_col)
        agg_col = f"{metric_col}_mean"
    else:
        agg_df = None

    base = Path(output_path).with_suffix("")
    saved = []

    for scenario in available_scenarios:
        data = []
        labels = []

        for rate in available_rates:
            if use_per_run_aggregation:
                values = agg_df[
                    (agg_df["scenario"] == scenario) & (agg_df["rate"] == rate)
                ][agg_col].dropna().values
            else:
                values = df[
                    (df["scenario"] == scenario) & (df["rate"] == rate)
                ][metric_col].dropna().values

            if len(values) > 0:
                data.append(values)
                labels.append(f"{rate}")

        if not data:
            continue

        fig, ax = plt.subplots(figsize=(1.5 * len(data) + 1.5, 3.8))

        bp = ax.boxplot(
            data,
            labels=labels,
            patch_artist=True,
            widths=0.5,
            showfliers=True,
            showmeans=True,
            medianprops=dict(color="black", linewidth=1.2),
            meanprops=dict(marker="D", markerfacecolor="crimson",
                           markeredgecolor="crimson", markersize=5),
            whiskerprops=dict(linewidth=0.8),
            capprops=dict(linewidth=0.8),
        )
        for patch in bp["boxes"]:
            patch.set_facecolor("#92c5de")
            patch.set_alpha(0.85)
            patch.set_edgecolor("black")
            patch.set_linewidth(0.6)

        ax.set_xlabel("Nachrichten pro Sekunde")
        ax.set_ylabel(ylabel)
        ax.set_title(f"{title} – Szenario {scenario}", pad=28)

        mean_handle = plt.Line2D([], [], marker="D", color="crimson",
                                 linestyle="None", markersize=5, label="Arithm. Mittel")
        ax.legend(handles=[mean_handle], loc="lower right", bbox_to_anchor=(1.0, 1.0),
                  fontsize=8, framealpha=0.7, borderaxespad=0)

        fig.tight_layout()
        out = Path(f"{base}_{scenario}.png")
        fig.savefig(out)
        plt.close(fig)
        saved.append(out.name)

    print(f"  Saved: {', '.join(saved)}")


def plot_line_with_errorbars(df, metric_col, ylabel, title, output_path,
                             scenario_order=SCENARIO_ORDER, rate_order=RATE_ORDER):
    """Line chart with error bars for FF1 metrics vs load level.

    X-axis = load (msg/s), Y-axis = metric value.
    One line per scenario. Data point = mean of per-run means; error bar = std.
    """
    available_scenarios = [s for s in scenario_order if s in df["scenario"].unique()]
    available_rates = sorted([r for r in rate_order if r in df["rate"].unique()])

    if not available_scenarios or not available_rates:
        print(f"  Skipping {title}: no data")
        return

    agg_df = aggregate_per_run(df, metric_col)
    agg_col = f"{metric_col}_mean"

    # Color palette for scenarios
    colors = plt.cm.tab10.colors

    fig, ax = plt.subplots(figsize=(6, 4))

    for idx, scenario in enumerate(available_scenarios):
        means = []
        stds = []
        x_vals = []
        for rate in available_rates:
            run_means = agg_df[
                (agg_df["scenario"] == scenario) & (agg_df["rate"] == rate)
            ][agg_col].dropna().values
            if len(run_means) > 0:
                means.append(np.mean(run_means))
                stds.append(np.std(run_means, ddof=1) if len(run_means) > 1 else 0.0)
                x_vals.append(rate)

        if x_vals:
            color = colors[idx % len(colors)]
            ax.errorbar(
                x_vals, means, yerr=stds,
                label=scenario, marker="o", linewidth=1.5, markersize=5,
                capsize=4, capthick=1.2, color=color,
            )

    ax.set_xlabel("Nachrichten pro Sekunde")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(available_rates)
    ax.legend(loc="upper left", fontsize=8, framealpha=0.7)

    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)
    print(f"  Saved: {output_path}")


def plot_tx_comparison(df, output_path,
                       scenario_order=SCENARIO_ORDER, rate_order=RATE_ORDER):
    """Bar chart showing mean blockchain transactions per run for each scenario/rate.

    Visualises the write reduction factor: N strategy = one TX per message;
    batching strategies = far fewer TXs for the same number of messages.
    """
    available_scenarios = [s for s in scenario_order if s in df["scenario"].unique()]
    available_rates = [r for r in rate_order if r in df["rate"].unique()]

    if not available_scenarios or not available_rates:
        print("  Skipping tx_comparison: no data")
        return

    # Aggregate: count batches (= TXs) per run, then mean/std across runs
    df2 = df.copy()
    df2["run"] = df2["run"].fillna(0).astype(int)
    tx_per_run = df2.groupby(["scenario", "rate", "run"]).size().reset_index(name="tx_count")

    n_rates = len(available_rates)
    n_scenarios = len(available_scenarios)
    bar_width = 0.8 / n_rates
    x = np.arange(n_scenarios)
    colors = plt.cm.Set2.colors

    fig, ax = plt.subplots(figsize=(max(6, n_scenarios * 1.2 + 2), 4))

    for ri, rate in enumerate(available_rates):
        means = []
        stds = []
        for scenario in available_scenarios:
            counts = tx_per_run[
                (tx_per_run["scenario"] == scenario) & (tx_per_run["rate"] == rate)
            ]["tx_count"].values
            means.append(np.mean(counts) if len(counts) > 0 else 0)
            stds.append(np.std(counts, ddof=1) if len(counts) > 1 else 0)

        offset = (ri - n_rates / 2 + 0.5) * bar_width
        ax.bar(
            x + offset, means, bar_width * 0.9,
            yerr=stds, label=f"{rate} msg/s",
            color=colors[ri % len(colors)], capsize=3, error_kw=dict(linewidth=0.8),
        )

    ax.set_xticks(x)
    ax.set_xticklabels(available_scenarios)
    ax.set_xlabel("Szenario")
    ax.set_ylabel("Blockchain-Transaktionen pro Run (Mittelwert)")
    ax.set_title("Blockchain-Transaktionen nach Szenario und Last")
    ax.legend(title="Last", fontsize=8)

    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)
    print(f"  Saved: {output_path}")


def print_write_reduction_table(batch_df):
    """Print write reduction factor per scenario and rate."""
    print("\n" + "=" * 60)
    print("Write Reduction Factor (requests / batches)")
    print("=" * 60)
    print(f"{'Scenario':<12} {'Rate':<10} {'Batches':<10} {'Total Items':<14} {'Factor':<10}")
    print("-" * 60)

    for scenario in SCENARIO_ORDER:
        for rate in RATE_ORDER:
            subset = batch_df[(batch_df["scenario"] == scenario) & (batch_df["rate"] == rate)]
            if len(subset) == 0:
                continue
            total_batches = len(subset)
            total_items = subset["batch_size"].sum()
            factor = total_items / total_batches if total_batches > 0 else 0
            print(f"{scenario:<12} {rate:<10} {total_batches:<10} {total_items:<14} {factor:<10.2f}")

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Generate publication-quality boxplots from experiment CSV data"
    )
    parser.add_argument(
        "--data-dir", type=str, required=True,
        help="Directory containing batch_metrics.csv and e2e_metrics.csv"
    )
    parser.add_argument(
        "--output-dir", type=str, default="./plots",
        help="Directory for output PNG files (default: ./plots)"
    )
    parser.add_argument(
        "--tex", action="store_true",
        help="Enable LaTeX rendering (requires texlive installation)"
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_style(args.tex)

    print("Loading data...")
    batch_df, e2e_df = load_data(data_dir)

    if batch_df is None and e2e_df is None:
        print("Error: No data files found. Exiting.")
        sys.exit(1)

    # --- FF1 Diagrams (batch metrics) ---
    if batch_df is not None and len(batch_df) > 0:
        print("\nGenerating FF1 diagrams...")

        has_run_col = "run" in batch_df.columns and batch_df["run"].notna().any()
        use_agg = has_run_col

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

        # New FF1 plots: CPU, RAM, TX comparison (require new CSV columns)
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
            print("  Skipping ff1_cpu_vs_load.png: cpu_percent column not available")

        if has_ram:
            # Convert bytes to MB for readability
            batch_df_mb = batch_df.copy()
            batch_df_mb["memory_usage_mb"] = batch_df_mb["memory_usage_bytes"] / (1024 * 1024)
            plot_line_with_errorbars(
                batch_df_mb, "memory_usage_mb",
                ylabel="RAM-Nutzung (MB)",
                title="RAM-Nutzung Middleware nach Last",
                output_path=output_dir / "ff1_ram_vs_load.png",
            )
        else:
            print("  Skipping ff1_ram_vs_load.png: memory_usage_bytes column not available")

        plot_tx_comparison(
            batch_df,
            output_path=output_dir / "ff1_tx_comparison.png",
        )

        print_write_reduction_table(batch_df)
    else:
        print("\nSkipping FF1 diagrams: no batch data available")

    # --- FF2 Diagrams (e2e metrics) ---
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
        print("\nSkipping FF2 diagrams: no e2e data available")

    print(f"\nDone. Output directory: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
