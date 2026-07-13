import math
import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PASTURE_DATA_PATH = Path("outputs", "projected_pasture_scenarios_gdp_TB.csv")


GROUPBY_COL = "Sub-region Name"
REGION = "Sub-Saharan Africa"
# REGION = "Latin America and the Caribbean"

# Each scenario gets its own panel, alongside a panel for demand.
SCENARIOS = ["base_projection_capped", "base_projection_uncapped",
             "full_gap_closure_by_2075_capped", "full_gap_closure_by_2075_uncapped"]
ITEMS = ["beef", "milk", "mutton"]
BASE_YEAR = 2025

METADATA_COLS = ["Sub-region Name", "iso3", "Country or Area", "Income group", "source"]
COLOURS = ["#D81B60", "#1E88E5", "#FFC107"]

def load_region_totals(
    data_path,
    region,
    groupby_col=GROUPBY_COL,
    metadata_cols=METADATA_COLS,
):
    """Load pasture projections and sum all value columns for one region, by year."""
    df = pd.read_csv(data_path)
    df = df[df[groupby_col] == region]

    drop_cols = [col for col in metadata_cols if col != groupby_col]
    sums = df.drop(columns=drop_cols).groupby([groupby_col, "year"]).sum()

    return sums.reset_index()


def plot_stacked_area(ax, scenario, items, region_data, base_year=BASE_YEAR, color_map=None):
    """Plot a stacked area chart of absolute pasture area by item, for one scenario."""
    cols = [f"{scenario}_{item}_pasture_area_m2" for item in items]
    colors = [color_map[item] for item in items] if color_map else None
    ax.stackplot(region_data["year"], *(region_data[col] for col in cols), labels=items, colors=colors)

    ax.set_title(scenario)
    ax.legend(loc="upper left")
    ax.set_xlim(base_year, region_data["year"].max())
    ax.set_ylabel("Pasture area (m2)")


def plot_demand(ax, items, region_data, base_year=BASE_YEAR, color_map=None, alpha=1.0):
    """Plot a stacked area chart of absolute protein demand by item."""
    cols = [f"{item} protein demand (ton per year)" for item in items]
    colors = [color_map[item] for item in items] if color_map else None
    ax.stackplot(region_data["year"], *(region_data[col] for col in cols), labels=items, colors=colors, alpha=alpha)

    ax.set_title("Protein demand")
    ax.legend(loc="upper left")
    ax.set_xlim(base_year, region_data["year"].max())
    ax.set_ylabel("Demand (ton per year)")


def plot_region_scenarios(region_data, region, scenarios=SCENARIOS, items=ITEMS, base_year=BASE_YEAR,
                          colors=None):
    """Plot stacked pasture area for each scenario as its own panel, plus a demand panel."""
    panels = list(scenarios) + ["demand"]
    ncols = math.ceil(math.sqrt(len(panels)))
    nrows = math.ceil(len(panels) / ncols)

    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
    axes = np.atleast_1d(axes).flatten()

    color_map = {item: colors[i] for i, item in enumerate(items)}
    
    
    for ax, scenario in zip(axes, scenarios):
        plot_stacked_area(ax, scenario, items, region_data, base_year, color_map=color_map)
    plot_demand(axes[len(scenarios)], items, region_data, base_year, color_map=color_map, alpha = 0.5)

    for ax in axes[len(panels):]:
        ax.axis("off")

    # Share a common y-scale across the pasture-area panels (same units), but leave
    # the demand panel, which is on a different scale, to autoscale independently.
    area_axes = axes[:len(scenarios)]
    ymax = max(
        region_data[[f"{scenario}_{item}_pasture_area_m2" for item in items]].sum(axis=1).max()
        for scenario in scenarios
    )
    for ax in area_axes:
        ax.set_ylim(0, ymax * 1.05)

    fig.suptitle(region)
    fig.supxlabel("Year")
    fig.tight_layout()

    return fig


def main(
    data_path=PASTURE_DATA_PATH,
    groupby_col=GROUPBY_COL,
    region=REGION,
    scenarios=SCENARIOS,
    items=ITEMS,
    base_year=BASE_YEAR,
    colors = COLOURS,
    show=True,
):
    """Build the diagnostic plot for a single region, one panel per scenario plus demand."""
    region_data = load_region_totals(data_path, region, groupby_col)
    fig = plot_region_scenarios(region_data, region, scenarios, items, base_year, colors=colors)

    if show:
        plt.show()

    return fig


if __name__ == "__main__":
    fig = main()
    os.makedirs("outputs/diagnostics", exist_ok=True)
    fig.savefig(f"outputs/diagnostics/pasture_area_demand_{REGION.replace(' ', '_')}.png", dpi=300)
