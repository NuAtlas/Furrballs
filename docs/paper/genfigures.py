#!/usr/bin/env python3
"""Generate ATC 2026 paper figures from Furrballs benchmark data.

Outputs .pgf files (LaTeX-native vector graphics) and .pdf previews.
All text is rendered by LaTeX at paper compile time — same font, selectable.

Usage:
    python3 genfigures.py [--preview] [--outdir DIR]

    --preview   Also render PNG previews for quick inspection
    --outdir    Output directory (default: docs/paper/figures)
"""

import json
import sys
import os
import argparse
from pathlib import Path
from collections import defaultdict

import matplotlib
matplotlib.use('pgf')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# --- LaTeX/ACM sigplan style ---
PGF_RC = {
    'pgf.texsystem': 'pdflatex',
    'text.usetex': True,
    'font.family': 'serif',
    'font.serif': ['Latin Modern Roman'],
    'font.size': 9,
    'axes.labelsize': 9,
    'axes.titlesize': 9,
    'legend.fontsize': 7,
    'xtick.labelsize': 7,
    'ytick.labelsize': 7,
    'figure.figsize': (3.33, 2.2),  # ACM sigplan single column ~3.33in
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.pad_inches': 0.02,
    'axes.linewidth': 0.5,
    'lines.linewidth': 1.0,
    'lines.markersize': 3,
    'hatch.linewidth': 0.5,
}

# --- Color palette (colorblind-safe) ---
COLORS = {
    'FurrBallTL': '#1b9e77',
    'FurrBallSN': '#d95f02',
    'CacheLib': '#e7298a',
    'RocksDB': '#7570b3',
    'TBB': '#66a61e',
}

MARKERS = {
    'FurrBallTL': 'o',
    'FurrBallSN': 's',
    'CacheLib': 'D',
    'RocksDB': '^',
    'TBB': 'v',
}

SHORT = {
    'FurrBallTL': 'FurrBall-TL',
    'FurrBallSN': 'FurrBall-SN',
    'CacheLib': 'CacheLib',
    'RocksDB': 'RocksDB',
    'TBB': 'TBB',
}

SYSTEM_ORDER = ['FurrBallTL', 'FurrBallSN', 'TBB', 'RocksDB', 'CacheLib']

COL = False  # set by --col flag; True = single-column ATC, False = wide whitepaper

# --- Data loading ---

def load_json(path):
    with open(path) as f:
        return json.load(f)['benchmarks']

def load_ycsb(data):
    """Parse YCSB benchmark data into structured dict."""
    wl_map = {'10': 'A', '11': 'B', '12': 'C'}
    records = []
    for b in data:
        parts = b['name'].split('/')
        sysname = parts[0].replace('YCSB_', '')
        threads = parts[2]
        cap = int(parts[3])
        wl = wl_map.get(parts[4], parts[4])
        vsz = int(parts[5])
        recs = int(parts[6]) if len(parts) > 6 else 0
        records.append({
            'system': sysname,
            'threads': int(threads),
            'workload': wl,
            'valuesize': vsz,
            'records': recs,
            'p50_get': b.get('p50_get_ns', 0),
            'p90_get': b.get('p90_get_ns', 0),
            'p99_get': b.get('p99_get_ns', 0),
            'p50_set': b.get('p50_set_ns', 0),
            'p99_set': b.get('p99_set_ns', 0),
            'ops_per_sec': b.get('ops_per_sec', 0),
            'hit_rate': b.get('hit_rate_pct', 0),
        })
    return records

def load_numabench(data):
    """Parse NUMABench data into structured dict."""
    wl_map = {'0': 'part', '1': 'shared', '2': 'trace', '3': 'read', '4': 'heavy'}
    records = []
    for b in data:
        parts = b['name'].split('/')
        sysname = parts[0].replace('NUMABench_', '')
        threads = parts[2]
        cap = int(parts[3])
        wl = wl_map.get(parts[4], parts[4])
        vsz = int(parts[5])
        records.append({
            'system': sysname,
            'threads': int(threads),
            'workload': wl,
            'valuesize': vsz,
            'p50_get': b.get('p50_get_ns', 0),
            'p90_get': b.get('p90_get_ns', 0),
            'p99_get': b.get('p99_get_ns', 0),
            'p50_set': b.get('p50_set_ns', 0),
            'p99_set': b.get('p99_set_ns', 0),
            'ops_per_sec': b.get('ops_per_sec', 0),
            'hit_rate': b.get('hit_rate_pct', 0),
        })
    return records

def aggregate(records, group_keys, value_key):
    """Group by keys, average the value."""
    groups = defaultdict(list)
    for r in records:
        key = tuple(r[k] for k in group_keys)
        groups[key].append(r[value_key])
    result = {}
    for key, vals in groups.items():
        result[key] = sum(vals) / len(vals) if vals else 0
    return result

# --- Figure: YCSB latency comparison ---

def fig_ycsb_latency(ycsb_data, outdir):
    """Bar chart: p50 GET latency across systems for YCSB A/B/C at 64B, 2T."""
    records = load_ycsb(ycsb_data)
    if COL:
        fig, axes = plt.subplots(3, 1, figsize=(3.33, 3.8), sharex=True)
    else:
        fig, axes = plt.subplots(1, 3, figsize=(6.5, 1.6), sharey=True)

    for idx, wl in enumerate(['A', 'B', 'C']):
        ax = axes[idx]
        data = aggregate(
            [r for r in records if r['workload'] == wl and r['threads'] == 2 and r['valuesize'] == 64],
            ['system'],
            'p50_get')

        systems = [s for s in SYSTEM_ORDER if (s,) in data]
        vals = [data[(s,)] for s in systems]
        colors = [COLORS[s] for s in systems]
        labels = [SHORT[s] for s in systems]

        bars = ax.bar(range(len(systems)), vals, color=colors, width=0.7, edgecolor='white', linewidth=0.5)
        ax.set_title(f'YCSB-{wl} (64B)')
        if COL:
            ax.set_ylabel('p50 GET (ns)')
            if idx == 0:
                ax.set_ylim(0, 2000)
            elif idx == 1:
                ax.set_ylim(0, 1000)
            if idx == 2:
                ax.set_xticks(range(len(systems)))
                ax.set_xticklabels(labels, rotation=30, ha='right')
            else:
                ax.set_xticks([])
        else:
            ax.set_xticks(range(len(systems)))
            ax.set_xticklabels(labels, rotation=30, ha='right')
            if idx == 0:
                ax.set_ylabel('p50 GET (ns)')

        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                        f'{v:.0f}', ha='center', va='bottom', fontsize=5)

    plt.tight_layout()
    save(fig, outdir, 'ycsb_latency_64b')
    plt.close(fig)

def fig_ycsb_latency_sweep(ycsb_data, outdir):
    """Grouped bar: p50 GET across value sizes for YCSB B, 2T."""
    records = load_ycsb(ycsb_data)
    fig, ax = plt.subplots(figsize=(3.33, 1.8))

    vsizes = [64, 256, 1024]
    x = np.arange(len(vsizes))
    width = 0.15

    for i, sysname in enumerate(SYSTEM_ORDER):
        data = aggregate(
            [r for r in records if r['system'] == sysname and r['workload'] == 'B' and r['threads'] == 2],
            ['valuesize'],
            'p50_get')
        vals = [data.get((v,), 0) for v in vsizes]
        ax.bar(x + i * width, vals, width, label=SHORT[sysname],
               color=COLORS[sysname], edgecolor='white', linewidth=0.3)

    ax.set_xlabel('Value size (bytes)')
    ax.set_ylabel('p50 GET (ns)')
    ax.set_title('YCSB-B, 2T, 32MB cache')
    ax.set_xticks(x + width * 2)
    ax.set_xticklabels(['64B', '256B', '1024B'])
    ax.legend(loc='upper left', ncol=2, framealpha=0.8)
    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())

    plt.tight_layout()
    save(fig, outdir, 'ycsb_latency_sweep')
    plt.close(fig)

def fig_ycsb_hitrate(ycsb_data, outdir):
    """Grouped bar: hit rate across value sizes for YCSB B, 2T."""
    records = load_ycsb(ycsb_data)
    fig, ax = plt.subplots(figsize=(3.33, 1.8))

    vsizes = [64, 256, 1024]
    x = np.arange(len(vsizes))
    width = 0.15

    for i, sysname in enumerate(SYSTEM_ORDER):
        data = aggregate(
            [r for r in records if r['system'] == sysname and r['workload'] == 'B' and r['threads'] == 2],
            ['valuesize'],
            'hit_rate')
        vals = [data.get((v,), 0) for v in vsizes]
        ax.bar(x + i * width, vals, width, label=SHORT[sysname],
               color=COLORS[sysname], edgecolor='white', linewidth=0.3)

    ax.set_xlabel('Value size (bytes)')
    ax.set_ylabel('Hit rate (%)')
    ax.set_title('YCSB-B, 2T, 32MB cache')
    ax.set_xticks(x + width * 2)
    ax.set_xticklabels(['64B', '256B', '1024B'])
    ax.legend(loc='lower left', ncol=2, framealpha=0.8)
    ax.set_ylim(0, 105)

    plt.tight_layout()
    save(fig, outdir, 'ycsb_hitrate')
    plt.close(fig)

# --- Figure: Thread scaling ---

def fig_thread_scaling(numabench_old, numabench_new, outdir):
    """Line plot: p50 GET vs thread count, before/after fix."""
    old_recs = load_numabench(numabench_old)
    new_recs = load_numabench(numabench_new)
    if COL:
        fig, axes = plt.subplots(2, 1, figsize=(3.33, 3.0))
    else:
        fig, axes = plt.subplots(1, 2, figsize=(6.5, 1.8))

    for ax_idx, (title, recs) in enumerate([
        ('Before fix', old_recs), ('After fix', new_recs)
    ]):
        ax = axes[ax_idx]
        for sysname in ['FurrBallTL', 'FurrBallSN', 'CacheLib', 'TBB']:
            data = aggregate(
                [r for r in recs if r['system'] == sysname and r['workload'] == 'part' and r['valuesize'] == 64],
                ['threads'],
                'p50_get')
            threads = sorted(data.keys())
            vals = [data[t] for t in threads]
            ax.plot([t[0] for t in threads], vals,
                    marker=MARKERS.get(sysname, 'o'),
                    color=COLORS.get(sysname, 'gray'),
                    label=SHORT.get(sysname, sysname))

        ax.set_xlabel('Threads')
        ax.set_ylabel('p50 GET (ns)')
        ax.set_title(title)
        ax.set_xscale('log', base=2)
        ax.set_xticks([1, 2, 4, 8, 16, 32])
        ax.set_xticklabels(['1', '2', '4', '8', '16', '32'])
        if ax_idx == 1:
            ax.legend(loc='upper left', fontsize=6, framealpha=0.8)

    plt.tight_layout()
    save(fig, outdir, 'thread_scaling_fix')
    plt.close(fig)

# --- Figure: YCSB ops/sec ---

def fig_ycsb_ops(ycsb_data, outdir):
    """Bar chart: throughput (ops/sec) across systems for YCSB A, 2T, 64B."""
    records = load_ycsb(ycsb_data)
    fig, ax = plt.subplots(figsize=(3.33, 1.8))

    data = aggregate(
        [r for r in records if r['workload'] == 'A' and r['threads'] == 2 and r['valuesize'] == 64],
        ['system'],
        'ops_per_sec')

    systems = [s for s in SYSTEM_ORDER if (s,) in data]
    vals = [data[(s,)] / 1e6 for s in systems]
    colors = [COLORS[s] for s in systems]
    labels = [SHORT[s] for s in systems]

    bars = ax.bar(range(len(systems)), vals, color=colors, width=0.7, edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(len(systems)))
    ax.set_xticklabels(labels, rotation=30, ha='right')
    ax.set_ylabel('Throughput (M ops/s)')
    ax.set_title('YCSB-A, 2T, 64B')

    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                f'{v:.1f}', ha='center', va='bottom', fontsize=5)

    plt.tight_layout()
    save(fig, outdir, 'ycsb_ops')
    plt.close(fig)

# --- Figure: YCSB comparison table as bar chart (TL vs CacheLib) ---

def fig_ycsb_tl_vs_cachelib(ycsb_data, outdir):
    """Paired bar: FurrBallTL vs CacheLib across YCSB A/B/C, 64B, 2T."""
    records = load_ycsb(ycsb_data)
    if COL:
        fig, axes = plt.subplots(2, 1, figsize=(3.33, 3.0))
    else:
        fig, axes = plt.subplots(1, 2, figsize=(6.0, 1.6))

    for ax_idx, metric, ylabel, title in [
        (0, 'p50_get', 'p50 GET (ns)', 'Latency'),
        (1, 'ops_per_sec', 'Throughput (M ops/s)', 'Throughput'),
    ]:
        ax = axes[ax_idx]
        workloads = ['A', 'B', 'C']
        x = np.arange(len(workloads))
        width = 0.35

        for i, sysname in enumerate(['FurrBallTL', 'CacheLib']):
            data = aggregate(
                [r for r in records if r['system'] == sysname and r['threads'] == 2 and r['valuesize'] == 64],
                ['workload'],
                metric)
            vals = [data.get((w,), 0) for w in workloads]
            if metric == 'ops_per_sec':
                vals = [v / 1e6 for v in vals]
            ax.bar(x + i * width, vals, width, label=SHORT[sysname],
                   color=COLORS[sysname], edgecolor='white', linewidth=0.3)

        ax.set_xticks(x + width / 2)
        ax.set_xticklabels([f'YCSB-{w}' for w in workloads])
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        if ax_idx == 0:
            ax.legend(framealpha=0.8)

    plt.tight_layout()
    save(fig, outdir, 'ycsb_tl_vs_cachelib')
    plt.close(fig)

# --- Utility ---

def save(fig, outdir, name):
    pgf_path = os.path.join(outdir, f'{name}.pgf')
    pdf_path = os.path.join(outdir, f'{name}.pdf')
    png_path = os.path.join(outdir, f'{name}.png')
    fig.savefig(pgf_path, bbox_inches='tight')
    fig.savefig(pdf_path, bbox_inches='tight')
    fig.savefig(png_path, bbox_inches='tight', dpi=200)
    print(f'  {name}.pgf + {name}.pdf + {name}.png')

# --- Main ---

def main():
    global COL
    parser = argparse.ArgumentParser(description='Generate ATC paper figures')
    parser.add_argument('--col', action='store_true', help='Single-column layout for ACM two-column papers')
    parser.add_argument('--preview', action='store_true', help='Also render PNG previews')
    parser.add_argument('--outdir', default='docs/paper/figures', help='Output directory')
    args = parser.parse_args()

    COL = args.col
    plt.rcParams.update(PGF_RC)
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    repo = Path(__file__).resolve().parent.parent.parent

    ycsb_fix = load_json(repo / 'data/ec2-c6a/ycsb-c6a-fix.json')
    numabench_old = load_json(repo / 'data/ec2-c6a/numabench-full-cachelib-v2.json')
    numabench_new = load_json(repo / 'data/ec2-c6a/numabench-full-v3-fix.json')

    print('Generating figures...')
    fig_ycsb_latency(ycsb_fix, outdir)
    fig_ycsb_latency_sweep(ycsb_fix, outdir)
    fig_ycsb_hitrate(ycsb_fix, outdir)
    fig_ycsb_ops(ycsb_fix, outdir)
    fig_ycsb_tl_vs_cachelib(ycsb_fix, outdir)
    fig_thread_scaling(numabench_old, numabench_new, outdir)

    if args.preview:
        print('Generating PNG previews...')
        matplotlib.use('Agg')
        plt.rcParams.update({'text.usetex': False, 'pgf.texsystem': 'xelatex'})
        for f in Path(outdir).glob('*.pdf'):
            os.system(f'convert -density 150 {f} {f.with_suffix(".png")} 2>/dev/null')
            if f.with_suffix('.png').exists():
                print(f'  {f.stem}.png')

    print('Done.')

if __name__ == '__main__':
    main()
