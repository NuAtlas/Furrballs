#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np

workloads = ["Zipfian", "Temporal", "ScanResistant", "Looping"]

variants = {
    "Float64\n(Factored)":      [24.64, 19.88, 48.15,  0.00],
    "4-bit int\n(REMARC-4b)":   [24.30,  9.87, 90.42, 49.45],
    "8-bit int\n(FI-Mult)":     [24.48, 12.04, 90.43, 48.70],
    "Log8 int\n(tuned)":        [26.61, 12.35, 90.41,  3.02],
}

colors = {
    "Float64\n(Factored)":      "#e74c3c",
    "4-bit int\n(REMARC-4b)":   "#3498db",
    "8-bit int\n(FI-Mult)":     "#2ecc71",
    "Log8 int\n(tuned)":        "#f39c12",
}

x = np.arange(len(workloads))
width = 0.18
n = len(variants)

fig, ax = plt.subplots(figsize=(9, 4.5))

for i, (label, vals) in enumerate(variants.items()):
    offset = (i - (n - 1) / 2) * width
    bars = ax.bar(x + offset, vals, width, label=label, color=colors[label],
                  edgecolor="white", linewidth=0.5)
    for bar, v in zip(bars, vals):
        if v >= 5:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.8,
                    f"{v:.1f}", ha="center", va="bottom", fontsize=6.5)

ax.set_ylabel("Hit Rate (%)", fontsize=11)
ax.set_xticks(x)
ax.set_xticklabels(workloads, fontsize=10)
ax.set_ylim(0, 105)
ax.legend(fontsize=8, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.12),
          frameon=False)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(axis="y", linestyle=":", alpha=0.4)

fig.tight_layout()
fig.subplots_adjust(bottom=0.22)
fig.savefig("/home/ubuntu/source/repos/Furrballs/docs/fig-quantization.pdf",
            format="pdf", dpi=150, bbox_inches="tight")
fig.savefig("/home/ubuntu/source/repos/Furrballs/docs/fig-quantization.svg",
            format="svg", dpi=150, bbox_inches="tight")
print("OK")
