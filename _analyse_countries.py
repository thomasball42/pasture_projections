import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pathlib import Path

projections_path = Path(__file__).parent / "outputs" / "projected_pasture_scenarios_gdp_TB.csv"

data_path = Path(__file__).parent / "data"

outputs = Path(__file__).parent / "outputs" / "figs" / "projected_areas_countries"
outputs.mkdir(exist_ok=True)

COUNTRIES = ["USA", "GBR", "BRA", "TZA", 
             "NGA", "UGA", "BWA", "IND",
             "ZAF", "RUS", "AUS", 
             "ARG",
             ]  

ONEPLOT = True

df = pd.read_csv(projections_path)

# col_filt = "no_gap_closure"
col_filt = "full_gap_closure_by_2075"

if ONEPLOT:
    fig, ax = plt.subplots(figsize=(12, 6))  # slightly wider to give labels room

for country in COUNTRIES:

    if not ONEPLOT:
        fig, ax = plt.subplots(figsize=(10, 6))

    df_country = df[(df["iso3"] == country) & (df["year"] >= 2023)]

    if df_country.empty:
        print(f"No data found for {country}, skipping.")
        if not ONEPLOT:
            plt.close(fig)
        continue

    df_country = df_country.loc[:, ["iso3", "year"] + df_country.columns[df_country.columns.str.contains(col_filt)].tolist()]

    sums = df_country.groupby(["iso3", "year"]).sum().reset_index()

    # print(f"{country} max: {sums['no_gap_closure_closure_MAX_pasture_efficiency'].max()}")

    conv = 0.0001 / 1e6
    
    if not ONEPLOT:
        for col in sums.columns[2:]:
            if "m2" in col:
                label = col.split("_")[3].upper()
                ax.plot(sums["year"], sums[col] * conv, label=label)
        totals = sums.loc[:, sums.columns[sums.columns.str.contains("m2")].tolist()].sum(axis=1)
        ax.plot(sums["year"], totals * conv, label="Total", color="k", linestyle="--")
        ax.set_title(country)
        ax.legend()

    if ONEPLOT:
        totals = sums.loc[:, sums.columns[sums.columns.str.contains("m2")].tolist()].sum(axis=1)
        line, = ax.plot(sums["year"], totals * conv)
        
        # Label at the end of the line
        ax.annotate(
            country,
            xy=(sums["year"].iloc[-1], totals.iloc[-1] * conv),
            xytext=(4, 0),
            textcoords="offset points",
            va="center",
            fontsize=8,
            color=line.get_color(),
        )

    ax.set_ylabel("Projected pasture area (Mha)")

    if not ONEPLOT:
        fig.tight_layout()
        fig.savefig(outputs / f"projected_pasture_area_{country}_{col_filt}.png", dpi=300)

if ONEPLOT:
    ax.set_title("Selected countries")
    ax.set_ylabel("Projected pasture area (Mha)")
    # Extend x-axis slightly so labels aren't clipped
    xmin, xmax = ax.get_xlim()
    ax.set_xlim(xmin, xmax + 2)
    fig.tight_layout()
    fig.savefig(outputs / f"projected_pasture_area_selected_countries_{col_filt}.png", dpi=300)
    plt.show()