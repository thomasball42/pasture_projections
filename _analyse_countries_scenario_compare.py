import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pathlib import Path

projections_path = Path(__file__).parent / "outputs" / "projected_pasture_scenarios_gdp_TB.csv"

outputs = Path(__file__).parent / "outputs" / "figs" / "projected_areas_countries"
outputs.mkdir(exist_ok=True)

COUNTRIES = ["USA", 
             "GBR", 
             "BRA", "TZA",
             "NGA"
            #  "NGA", "UGA", "BWA", "IND",
            #  "ZAF", "RUS", "AUS", 
            #  "ARG",
             ]

SCENARIOS = {
    "no_gap_closure": {"linestyle": "-", "label": "No gap closure"},
    "full_gap_closure_by_2075": {"linestyle": "--", "label": "Full gap closure by 2075"},
}

df = pd.read_csv(projections_path)

conv = 0.0001 / 1e6

color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]
country_colors = {country: color_cycle[i % len(color_cycle)] for i, country in enumerate(COUNTRIES)}

fig, ax = plt.subplots(figsize=(8, 5))

for country in COUNTRIES:
    for col_filt, scenario_props in SCENARIOS.items():
        df_country = df[(df["iso3"] == country) & (df["year"] >= 2023)]

        if df_country.empty:
            print(f"No data found for {country}, skipping.")
            continue

        df_country = df_country.loc[:, ["iso3", "year"] + df_country.columns[df_country.columns.str.contains(col_filt)].tolist()]
        sums = df_country.groupby(["iso3", "year"]).sum().reset_index()

        totals = sums.loc[:, sums.columns[sums.columns.str.contains("m2")].tolist()].sum(axis=1)

        line, = ax.plot(
            sums["year"],
            totals * conv,
            color=country_colors[country],
            linestyle=scenario_props["linestyle"],
            alpha = 0.8
        )

    # Annotate once per country (after both scenarios plotted) using the last scenario's line position
    ax.annotate(
        country,
        xy=(sums["year"].iloc[-1], totals.iloc[-1] * conv),
        xytext=(4, 0),
        textcoords="offset points",
        va="center",
        fontsize=8,
        color=country_colors[country],
    )

ax.set_title("Projected pasture area — scenario comparison")
ax.set_ylabel("Projected pasture area (Mha)")
ax.yaxis.get_major_formatter().set_useOffset(False)
ax.yaxis.get_major_formatter().set_scientific(False)

xmin, xmax = ax.get_xlim()
ax.set_xlim(xmin, xmax + 2)

# Legend for scenarios only
from matplotlib.lines import Line2D
legend_handles = [Line2D([0], [0], color="k", linestyle=props["linestyle"], label=props["label"])
                  for props in SCENARIOS.values()]

ax.legend(handles=legend_handles)

fig.tight_layout()
fig.savefig(outputs / "projected_pasture_area_scenario_comparison.png", dpi=300)
plt.show()