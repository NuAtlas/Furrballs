#!/usr/bin/env python3
"""Generate ATC 2026 paper figures from Furrballs benchmark data."""

import json, sys, os, argparse
from pathlib import Path
from collections import defaultdict

import matplotlib
matplotlib.use('pgf')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

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
    'figure.figsize': (3.33, 2.2),
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.pad_inches': 0.02,
    'axes.linewidth': 0.5,
    'lines.linewidth': 1.0,
    'lines.markersize': 3,
    'hatch.linewidth': 0.5,
}

COLORS = {
    'TopazTL': '#1b9e77', 'TopazSN': '#d95f02', 'CacheLib': '#e7298a',
    'RocksDB': '#7570b3', 'TBB': '#66a61e',
}
MARKERS = {
    'TopazTL': 'o', 'TopazSN': 's', 'CacheLib': 'D',
    'RocksDB': '^', 'TBB': 'v',
}
SHORT = {
    'TopazTL': 'Topaz-TL', 'TopazSN': 'Topaz-SN', 'CacheLib': 'CacheLib',
    'RocksDB': 'RocksDB', 'TBB': 'TBB',
}
SYSTEM_ORDER = ['TopazTL', 'TopazSN', 'TBB', 'RocksDB', 'CacheLib']
DATA_RENAME = {'FurrBallTL': 'TopazTL', 'FurrBallSN': 'TopazSN'}
COL = False

def load_json(path):
    with open(path) as f:
        content = f.read()
    decoder = json.JSONDecoder()
    data, idx = [], 0
    while idx < len(content):
        try:
            obj, end = decoder.raw_decode(content, idx)
            data.extend(obj.get('benchmarks', []))
            idx = end
        except json.JSONDecodeError:
            idx += 1
    return data

def load_ycsb(data):
    wl_map = {'10': 'A', '11': 'B', '12': 'C'}
    records = []
    for b in data:
        parts = b['name'].split('/')
        sn = DATA_RENAME.get(parts[0].replace('YCSB_', ''), parts[0].replace('YCSB_', ''))
        records.append({
            'system': sn, 'threads': int(parts[2]), 'capacity_kb': int(parts[3]),
            'workload': wl_map.get(parts[4], parts[4]), 'valuesize': int(parts[5]),
            'records': int(parts[6]) if len(parts) > 6 else 0,
            'p50_get': b.get('p50_get_ns', 0), 'p50_get_std': b.get('p50_get_ns_std', 0),
            'p90_get': b.get('p90_get_ns', 0), 'p90_get_std': b.get('p90_get_ns_std', 0),
            'p99_get': b.get('p99_get_ns', 0), 'p99_get_std': b.get('p99_get_ns_std', 0),
            'p50_set': b.get('p50_set_ns', 0), 'p99_set': b.get('p99_set_ns', 0),
            'ops_per_sec': b.get('ops_per_sec', 0), 'ops_per_sec_std': b.get('ops_per_sec_std', 0),
            'hit_rate': b.get('hit_rate_pct', 0), 'runs': int(b.get('runs', 1)),
        })
    return records

def load_numabench(data):
    wl_map = {'0': 'part', '1': 'shared', '2': 'trace', '3': 'read', '4': 'heavy'}
    records = []
    for b in data:
        parts = b['name'].split('/')
        sn = DATA_RENAME.get(parts[0].replace('NUMABench_', ''), parts[0].replace('NUMABench_', ''))
        records.append({
            'system': sn, 'threads': int(parts[2]), 'capacity_kb': int(parts[3]),
            'workload': wl_map.get(parts[4], parts[4]), 'valuesize': int(parts[5]),
            'p50_get': b.get('p50_get_ns', 0), 'p50_get_std': b.get('p50_get_ns_std', 0),
            'p90_get': b.get('p90_get_ns', 0), 'p99_get': b.get('p99_get_ns', 0),
            'p50_set': b.get('p50_set_ns', 0), 'p99_set': b.get('p99_set_ns', 0),
            'ops_per_sec': b.get('ops_per_sec', 0), 'ops_per_sec_std': b.get('ops_per_sec_std', 0),
            'hit_rate': b.get('hit_rate_pct', 0), 'runs': int(b.get('runs', 1)),
        })
    return records

def aggregate(records, group_keys, value_key, std_key=None):
    groups = defaultdict(list)
    std_groups = defaultdict(list)
    for r in records:
        key = tuple(r[k] for k in group_keys)
        groups[key].append(r[value_key])
        if std_key and r.get(std_key, 0) > 0:
            std_groups[key].append(r[std_key])
    result = {}
    for key, vals in groups.items():
        mean = sum(vals) / len(vals) if vals else 0
        if key in std_groups:
            std_vals = std_groups[key]
            std = sum(std_vals) / len(std_vals) if std_vals else 0
        else:
            std = 0
        result[key] = (mean, std)
    return result


def fig_capacity_sweep(ycsb_data, outdir):
    """Line plot: p50 GET vs cache capacity, 4T and 2T, YCSB-B 64B."""
    records = load_ycsb(ycsb_data)
    caps_mb = [64, 128, 512]
    caps_kb = [65536, 131072, 524288]
    x_pos = [0, 1, 2]

    if COL:
        fig, axes = plt.subplots(2, 1, figsize=(3.33, 3.2), sharex=True)
    else:
        fig, axes = plt.subplots(1, 2, figsize=(6.5, 2.0), sharey=True)

    for ax_idx, (title, nthreads) in enumerate([('4 threads', 4), ('2 threads', 2)]):
        ax = axes[ax_idx]
        for sysname in SYSTEM_ORDER:
            data = aggregate(
                [r for r in records
                 if r['system'] == sysname and r['workload'] == 'B'
                 and r['threads'] == nthreads and r['valuesize'] == 64
                 and r['capacity_kb'] in caps_kb],
                ['capacity_kb'], 'p50_get', 'p50_get_std')
            vals = [data.get((c,), (0, 0))[0] for c in caps_kb]
            errs = [data.get((c,), (0, 0))[1] for c in caps_kb]
            ax.errorbar(x_pos, vals, yerr=errs,
                        marker=MARKERS[sysname], color=COLORS[sysname],
                        label=SHORT[sysname], linewidth=1.0, capsize=2, markersize=4)

        ax.set_xlabel('Cache capacity (MB)')
        if ax_idx == 0:
            ax.set_ylabel('p50 GET (ns)')
        ax.set_title(title)
        ax.set_xticks(x_pos)
        ax.set_xticklabels([f'{c}' for c in caps_mb])

    axes[0].legend(loc='upper left', fontsize=5, framealpha=0.9, ncol=1)

    plt.tight_layout()
    save(fig, outdir, 'capacity_sweep')
    plt.close(fig)


def fig_ycsb_latency(ycsb_data, outdir):
    """Bar chart: p50 (solid) + p99 (hatched) GET for YCSB A/B/C at 64B, 4T, 64MB."""
    records = load_ycsb(ycsb_data)
    if COL:
        fig, axes = plt.subplots(3, 1, figsize=(3.33, 3.8), sharex=True)
    else:
        fig, axes = plt.subplots(1, 3, figsize=(6.5, 1.8), sharey=True)

    for idx, wl in enumerate(['A', 'B', 'C']):
        ax = axes[idx]
        p50_data = aggregate(
            [r for r in records
             if r['workload'] == wl and r['threads'] == 4
             and r['valuesize'] == 64 and r['capacity_kb'] == 65536],
            ['system'], 'p50_get', 'p50_get_std')
        p99_data = aggregate(
            [r for r in records
             if r['workload'] == wl and r['threads'] == 4
             and r['valuesize'] == 64 and r['capacity_kb'] == 65536],
            ['system'], 'p99_get', 'p99_get_std')

        systems = [s for s in SYSTEM_ORDER if (s,) in p50_data]
        x = np.arange(len(systems))
        width = 0.35

        p50_vals = [p50_data[(s,)][0] for s in systems]
        p50_errs = [p50_data[(s,)][1] for s in systems]
        p99_vals = [p99_data.get((s,), (0, 0))[0] for s in systems]
        p99_errs = [p99_data.get((s,), (0, 0))[1] for s in systems]
        colors = [COLORS[s] for s in systems]
        labels = [SHORT[s] for s in systems]

        bars50 = ax.bar(x - width/2, p50_vals, width, yerr=p50_errs, color=colors,
                        edgecolor='white', linewidth=0.5, capsize=2, error_kw={'linewidth': 0.8},
                        label='p50')
        bars99 = ax.bar(x + width/2, p99_vals, width, yerr=p99_errs, color=colors,
                        edgecolor='white', linewidth=0.5, capsize=2, error_kw={'linewidth': 0.8},
                        hatch='///', alpha=0.6, label='p99')

        ax.set_title(f'YCSB-{wl} (64B)')
        if COL:
            ax.set_ylabel('GET latency (ns)')
            if idx == 2:
                ax.set_xticks(x)
                ax.set_xticklabels(labels, rotation=30, ha='right')
            else:
                ax.set_xticks([])
        else:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, rotation=30, ha='right')
            if idx == 0:
                ax.set_ylabel('GET latency (ns)')
                ax.legend(['p50', 'p99'], loc='upper right', fontsize=5, framealpha=0.8)

        for bar, v, e in zip(bars50, p50_vals, p50_errs):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + e + 15,
                        f'{v:.0f}', ha='center', va='bottom', fontsize=5)

        for bar, v, e in zip(bars99, p99_vals, p99_errs):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + e + 15,
                        f'{v/1000:.1f}k', ha='center', va='bottom', fontsize=5)

    plt.tight_layout()
    save(fig, outdir, 'ycsb_latency_64b')
    plt.close(fig)


def fig_ycsb_latency_sweep(ycsb_data, outdir):
    """Grouped bar: p50 GET across value sizes for YCSB B, 2T, 32MB."""
    records = load_ycsb(ycsb_data)
    fig, ax = plt.subplots(figsize=(3.33, 2.0))

    vsizes = [64, 256, 1024]
    x = np.arange(len(vsizes))
    width = 0.15

    for i, sysname in enumerate(SYSTEM_ORDER):
        data = aggregate(
            [r for r in records if r['system'] == sysname and r['workload'] == 'B'
             and r['threads'] == 2 and r['capacity_kb'] == 32768],
            ['valuesize'], 'p50_get', 'p50_get_std')
        vals = [data[(v,)][0] for v in vsizes]
        errs = [data[(v,)][1] for v in vsizes]
        ax.bar(x + i * width, vals, width, yerr=errs, label=SHORT[sysname],
               color=COLORS[sysname], edgecolor='white', linewidth=0.3, capsize=2,
               error_kw={'linewidth': 0.6})

    ax.set_xlabel('Value size (bytes)')
    ax.set_ylabel('p50 GET (ns)')
    ax.set_title('YCSB-B, 2T, 32MB cache')
    ax.set_xticks(x + width * 2)
    ax.set_xticklabels(['64B', '256B', '1024B'])
    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.28)
    ax.legend(loc='upper center', ncol=3, framealpha=0.8, fontsize=5,
              bbox_to_anchor=(0.5, -0.35))

    save(fig, outdir, 'ycsb_latency_sweep')
    plt.close(fig)


def fig_ycsb_hitrate(ycsb_data, outdir):
    """Grouped bar: hit rate across value sizes for YCSB B, 2T, 32MB."""
    records = load_ycsb(ycsb_data)
    fig, ax = plt.subplots(figsize=(3.33, 1.8))

    vsizes = [64, 256, 1024]
    x = np.arange(len(vsizes))
    width = 0.15

    for i, sysname in enumerate(SYSTEM_ORDER):
        data = aggregate(
            [r for r in records if r['system'] == sysname and r['workload'] == 'B'
             and r['threads'] == 2 and r['capacity_kb'] == 32768],
            ['valuesize'], 'hit_rate')
        vals = [data[(v,)][0] for v in vsizes]
        ax.bar(x + i * width, vals, width, label=SHORT[sysname],
               color=COLORS[sysname], edgecolor='white', linewidth=0.3)

    ax.set_xlabel('Value size (bytes)')
    ax.set_ylabel('Hit rate (%)')
    ax.set_title('YCSB-B, 2T, 32MB cache')
    ax.set_xticks(x + width * 2)
    ax.set_xticklabels(['64B', '256B', '1024B'])
    ax.legend(loc='lower left', ncol=2, framealpha=0.8, fontsize=5)
    ax.set_ylim(0, 105)

    plt.tight_layout()
    save(fig, outdir, 'ycsb_hitrate')
    plt.close(fig)


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
        for sysname in ['TopazTL', 'TopazSN', 'CacheLib', 'TBB']:
            data = aggregate(
                [r for r in recs if r['system'] == sysname
                 and r['workload'] == 'part' and r['valuesize'] == 64],
                ['threads'], 'p50_get', 'p50_get_std')
            threads = sorted(data.keys())
            vals = [data[t][0] for t in threads]
            errs = [data[t][1] for t in threads]
            ax.errorbar([t[0] for t in threads], vals, yerr=errs,
                         fmt='none', linewidth=0.8, alpha=0.3, color=COLORS[sysname])
            ax.plot([t[0] for t in threads], vals,
                    marker=MARKERS[sysname], color=COLORS[sysname],
                    label=SHORT[sysname])

        ax.set_xlabel('Threads')
        ax.set_ylabel('p50 GET (ns)')
        ax.set_title(title)
        ax.set_xscale('log', base=2)
        ax.set_xticks([1, 2, 4, 8, 16, 32])
        ax.set_xticklabels(['1', '2', '4', '8', '16', '32'])
        if ax_idx == 0:
            ax.legend(loc='center right', fontsize=5, framealpha=0.9)

    plt.tight_layout()
    save(fig, outdir, 'thread_scaling_fix')
    plt.close(fig)


def fig_ycsb_tl_vs_cachelib(ycsb_data, outdir):
    """Paired bar: TopazTL vs CacheLib across YCSB A/B/C, 64B, 4T, 64MB."""
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

        std_metric = metric + '_std'
        for i, sysname in enumerate(['TopazTL', 'CacheLib']):
            data = aggregate(
                [r for r in records
                 if r['system'] == sysname and r['threads'] == 4
                 and r['valuesize'] == 64 and r['capacity_kb'] == 65536],
                ['workload'], metric, std_metric)
            vals = [data[(w,)][0] for w in workloads]
            errs = [data[(w,)][1] for w in workloads]
            if metric == 'ops_per_sec':
                vals = [v / 1e6 for v in vals]
                errs = [e / 1e6 for e in errs]
            ax.bar(x + i * width, vals, width, yerr=errs, label=SHORT[sysname],
                   color=COLORS[sysname], edgecolor='white', linewidth=0.3, capsize=2,
                   error_kw={'linewidth': 0.8})

        ax.set_xticks(x + width / 2)
        ax.set_xticklabels([f'YCSB-{w}' for w in workloads])
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        if ax_idx == 0:
            ax.legend(framealpha=0.8)

    plt.tight_layout()
    save(fig, outdir, 'ycsb_tl_vs_cachelib')
    plt.close(fig)


def save(fig, outdir, name):
    for ext in ['.pgf', '.pdf', '.png']:
        fig.savefig(os.path.join(outdir, f'{name}{ext}'),
                    bbox_inches='tight', dpi=200 if ext == '.png' else 300)
    print(f'  {name}.pgf + {name}.pdf + {name}.png')


def main():
    global COL
    parser = argparse.ArgumentParser()
    parser.add_argument('--col', action='store_true')
    parser.add_argument('--outdir', default='docs/paper/figures')
    args = parser.parse_args()

    COL = args.col
    matplotlib.use('pgf')
    plt.rcParams.update(PGF_RC)
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    repo = Path(__file__).resolve().parent.parent.parent
    ycsb_v3 = load_json(repo / 'data/ec2-c6a/ycsb-c6a-v3.json')
    ycsb_cap = load_json(repo / 'data/ec2-c6a/ycsb-c6a-capacity-sweep.json')
    ycsb_all = ycsb_v3 + ycsb_cap
    nb_old = load_json(repo / 'data/ec2-c6a/numabench-full-v3-fix.json')
    nb_new = load_json(repo / 'data/ec2-c6a/numabench-full-v4-stats.json')

    print('Generating figures...')
    fig_capacity_sweep(ycsb_all, outdir)
    fig_ycsb_latency(ycsb_all, outdir)
    fig_ycsb_tl_vs_cachelib(ycsb_all, outdir)
    fig_ycsb_latency_sweep(ycsb_v3, outdir)
    fig_ycsb_hitrate(ycsb_v3, outdir)
    fig_thread_scaling(nb_old, nb_new, outdir)
    print('Done.')


if __name__ == '__main__':
    main()
