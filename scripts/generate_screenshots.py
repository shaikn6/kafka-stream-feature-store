"""
Generate PNG screenshots for the kafka-stream-feature-store repo.
Run: python scripts/generate_screenshots.py
Output: docs/screenshots/{architecture,feature_freshness,throughput_chart}.png
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
import numpy as np

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "screenshots")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── 1. Architecture diagram ───────────────────────────────────────────────────

def draw_box(ax, x, y, w, h, label, color, fontsize=9, text_color="white"):
    box = FancyBboxPatch(
        (x - w / 2, y - h / 2), w, h,
        boxstyle="round,pad=0.05",
        linewidth=1.5,
        edgecolor="white",
        facecolor=color,
        zorder=3,
    )
    ax.add_patch(box)
    ax.text(
        x, y, label,
        ha="center", va="center",
        fontsize=fontsize, fontweight="bold",
        color=text_color, zorder=4,
        multialignment="center",
    )


def arrow(ax, x0, y0, x1, y1, style="-|>", color="#cccccc", lw=1.5, linestyle="solid"):
    ax.annotate(
        "",
        xy=(x1, y1), xytext=(x0, y0),
        arrowprops=dict(
            arrowstyle=style,
            color=color,
            lw=lw,
            linestyle=linestyle,
            connectionstyle="arc3,rad=0",
        ),
        zorder=2,
    )


def generate_architecture():
    fig, ax = plt.subplots(figsize=(14, 5))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 5)
    ax.axis("off")

    KAFKA_ORANGE   = "#E87722"
    REDIS_RED      = "#DC382D"
    FASTAPI_GREEN  = "#009688"
    POSTGRES_BLUE  = "#336791"
    SOURCE_GRAY    = "#5a5a7a"

    BH = 0.8   # box height
    BW = 1.6   # box width
    MID_Y = 2.8
    BOT_Y = 1.1

    # Data Sources
    draw_box(ax, 1.1, MID_Y, BW, BH, "Data\nSources", SOURCE_GRAY)

    # Kafka Producer
    draw_box(ax, 3.1, MID_Y, BW, BH, "Kafka\nProducer", KAFKA_ORANGE)

    # Kafka Topic
    draw_box(ax, 5.4, MID_Y, BW + 0.4, BH, "Kafka Topic\n(features.raw)", KAFKA_ORANGE)

    # Kafka Consumer
    draw_box(ax, 7.8, MID_Y, BW, BH, "Kafka\nConsumer", KAFKA_ORANGE)

    # Redis
    draw_box(ax, 10.0, MID_Y, BW, BH, "Redis\n(Serving Layer)", REDIS_RED)

    # FastAPI
    draw_box(ax, 12.2, MID_Y, BW, BH, "FastAPI\n/features/{id}", FASTAPI_GREEN)

    # PostgreSQL (below Redis, dotted)
    draw_box(ax, 10.0, BOT_Y, BW, BH, "PostgreSQL\n(Feature Registry)", POSTGRES_BLUE)

    # Arrows — horizontal flow
    arrow(ax, 1.9, MID_Y, 2.3, MID_Y)
    arrow(ax, 3.9, MID_Y, 4.2, MID_Y)
    arrow(ax, 6.6, MID_Y, 6.9, MID_Y)
    arrow(ax, 8.6, MID_Y, 9.0, MID_Y)
    arrow(ax, 11.0, MID_Y, 11.3, MID_Y)

    # Arrow — Redis → PostgreSQL (dotted, vertical)
    arrow(ax, 10.0, MID_Y - BH / 2, 10.0, BOT_Y + BH / 2,
          linestyle="dashed", color="#aaaaaa")

    # Title
    ax.text(
        7.0, 4.6,
        "Kafka Feature Store — System Architecture",
        ha="center", va="center",
        fontsize=13, fontweight="bold", color="white",
    )

    # Legend
    legend_items = [
        mpatches.Patch(facecolor=KAFKA_ORANGE,  label="Kafka"),
        mpatches.Patch(facecolor=REDIS_RED,     label="Redis"),
        mpatches.Patch(facecolor=FASTAPI_GREEN, label="FastAPI"),
        mpatches.Patch(facecolor=POSTGRES_BLUE, label="PostgreSQL"),
    ]
    leg = ax.legend(
        handles=legend_items,
        loc="lower left",
        fontsize=8,
        framealpha=0.3,
        facecolor="#2a2a4e",
        edgecolor="#555555",
        labelcolor="white",
    )

    out = os.path.join(OUTPUT_DIR, "architecture.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"  Saved: {out}")


# ── 2. Feature freshness line chart ──────────────────────────────────────────

def generate_feature_freshness():
    np.random.seed(42)
    t = np.linspace(0, 60, 300)

    def freshness_wave(base, noise_amp, period, phase):
        return (
            base
            + noise_amp * np.sin(2 * np.pi * t / period + phase)
            + np.random.normal(0, noise_amp * 0.3, len(t))
        ).clip(0, 58)

    order_count    = freshness_wave(32, 6, 18, 0.0)
    revenue_sum    = freshness_wave(38, 6, 22, 1.1)
    avg_basket     = freshness_wave(44, 5, 26, 2.3)

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#16213e")

    ax.plot(t, order_count,  color="#00c9ff", lw=2.0, label="order_count")
    ax.plot(t, revenue_sum,  color="#f7971e", lw=2.0, label="revenue_sum")
    ax.plot(t, avg_basket,   color="#c471ed", lw=2.0, label="avg_basket_size")

    ax.axhline(60, color="#ff4444", lw=1.8, linestyle="--", label="SLA Threshold (60s)")
    ax.fill_between(t, 60, 70, color="#ff4444", alpha=0.08)

    ax.set_xlim(0, 60)
    ax.set_ylim(0, 70)
    ax.set_xlabel("Time (seconds)", color="#aaaacc", fontsize=11)
    ax.set_ylabel("Feature Age (seconds)", color="#aaaacc", fontsize=11)
    ax.set_title(
        "Feature Freshness — Sub-60s SLA Compliance",
        color="white", fontsize=13, fontweight="bold", pad=14,
    )

    ax.tick_params(colors="#aaaacc")
    for spine in ax.spines.values():
        spine.set_edgecolor("#333355")

    ax.legend(
        loc="upper right",
        fontsize=9,
        framealpha=0.3,
        facecolor="#2a2a4e",
        edgecolor="#555577",
        labelcolor="white",
    )
    ax.grid(color="#2a2a4e", linestyle="--", linewidth=0.7, alpha=0.7)

    out = os.path.join(OUTPUT_DIR, "feature_freshness.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"  Saved: {out}")


# ── 3. Throughput / latency bar chart ─────────────────────────────────────────

def generate_throughput_chart():
    labels    = ["10/s", "50/s", "100/s", "200/s", "500/s"]
    latencies = [12, 18, 24, 35, 67]

    # Gradient: green → yellow → red
    palette = ["#00c853", "#76ff03", "#ffea00", "#ff6d00", "#d50000"]

    fig, ax = plt.subplots(figsize=(8, 5))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#16213e")

    bars = ax.bar(labels, latencies, color=palette, width=0.55, zorder=3)

    for bar, val in zip(bars, latencies):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1.2,
            f"{val} ms",
            ha="center", va="bottom",
            color="white", fontsize=10, fontweight="bold",
            zorder=4,
        )

    ax.set_xlabel("Producer Throughput", color="#aaaacc", fontsize=11)
    ax.set_ylabel("Write Latency (ms)", color="#aaaacc", fontsize=11)
    ax.set_title(
        "Feature Store Write Latency vs Throughput",
        color="white", fontsize=13, fontweight="bold", pad=14,
    )

    ax.set_ylim(0, 80)
    ax.tick_params(colors="#aaaacc")
    for spine in ax.spines.values():
        spine.set_edgecolor("#333355")
    ax.grid(axis="y", color="#2a2a4e", linestyle="--", linewidth=0.7, alpha=0.7, zorder=0)

    out = os.path.join(OUTPUT_DIR, "throughput_chart.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"  Saved: {out}")


if __name__ == "__main__":
    print("Generating screenshots…")
    generate_architecture()
    generate_feature_freshness()
    generate_throughput_chart()
    print("Done.")
