import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PASTURE_DATA_PATH = Path("outputs", "projected_pasture_scenarios_gdp_TB_test.csv")

# Column to group/aggregate by, e.g. "Sub-region Name" or "Country or Area"
GROUPBY_COL = "Sub-region Name"
COL_FILTER = "total"  # only value columns containing this substring are plotted
BASE_YEAR = 2025

METADATA_COLS = ["Sub-region Name", "iso3", "Country or Area", "Income group", "source"]

REGION_SELECT = []  # restrict input rows to these regions before aggregating; empty = all
REGION_FILTER = []  # restrict which aggregated regions get plotted; empty = all

def load_regional_totals(
    data_path,
    groupby_col=GROUPBY_COL,
    col_filter=COL_FILTER,
    region_select=REGION_SELECT,
    metadata_cols=METADATA_COLS,
):
    """Load pasture projections and sum value columns matching `col_filter` by group/year."""
    df = pd.read_csv(data_path)

    if region_select:
        df = df[df[groupby_col].isin(region_select)]

    drop_cols = [col for col in metadata_cols if col != groupby_col]
    sums = df.drop(columns=drop_cols).groupby([groupby_col, "year"]).sum()

    return sums[[col for col in sums.columns if col_filter in col]].reset_index()


def plot_region(ax, region, region_data, groupby_col=GROUPBY_COL, base_year=BASE_YEAR, legend=True):
    """Plot each value column for one region, normalised to its base_year value."""
    for col in region_data.columns:
        if col in (groupby_col, "year"):
            continue
        base_value = region_data.loc[region_data["year"] == base_year, col].values[0]
        ax.plot(region_data["year"], region_data[col] / base_value, label=col)

    ax.set_title(region)
    if legend:
        ax.legend()
    ax.set_xlim(base_year, None)
    ax.axhline(y=1, color="k", linestyle="--", alpha=0.5)


def plot_regions_grid(totals, regions, groupby_col=GROUPBY_COL, base_year=BASE_YEAR):
    """Plot all regions as subplots of a single multipanel figure."""
    ncols = math.ceil(math.sqrt(len(regions)))
    nrows = math.ceil(len(regions) / ncols)
    figsize = (3 * ncols, 2 * nrows)

    fig, axes = plt.subplots(nrows, ncols, figsize=figsize, sharey=True)
    axes = np.atleast_1d(axes).flatten()

    for ax, region in zip(axes, regions):
        region_data = totals.loc[totals[groupby_col] == region]
        plot_region(ax, region, region_data, groupby_col, base_year, legend=False)

    for ax in axes[len(regions):]:
        ax.axis("off")

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower right", ncol=math.ceil(len(labels) / 2), bbox_to_anchor=(1, 0.2))

    fig.supylabel("Proportional change")
    fig.supxlabel("Year")
    fig.tight_layout(rect=(0, 0.05, 1, 1))

    return fig


def plot_regions_individually(totals, regions, groupby_col=GROUPBY_COL, base_year=BASE_YEAR):
    """Plot each region as its own figure. Returns the list of figures."""
    figs = []
    for region in regions:
        fig, ax = plt.subplots(figsize=(10, 6))
        region_data = totals.loc[totals[groupby_col] == region]
        plot_region(ax, region, region_data, groupby_col, base_year)
        fig.tight_layout()
        figs.append(fig)
    return figs


def main(
    data_path=PASTURE_DATA_PATH,
    groupby_col=GROUPBY_COL,
    col_filter=COL_FILTER,
    region_select=REGION_SELECT,
    region_filter=REGION_FILTER,
    base_year=BASE_YEAR,
    combine_plots=True,
    show=True,
):
    """Build the diagnostic plot(s) and return the resulting figure(s)."""
    totals = load_regional_totals(data_path, groupby_col, col_filter, region_select)

    regions = [
        region for region in totals[groupby_col].unique()
        if not region_filter or region in region_filter
    ]

    if combine_plots:
        result = plot_regions_grid(totals, regions, groupby_col, base_year)
    else:
        result = plot_regions_individually(totals, regions, groupby_col, base_year)

    if show:
        plt.show()

    return result


if __name__ == "__main__":
    main()
