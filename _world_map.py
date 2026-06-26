import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pathlib import Path
import geopandas as gpd
from matplotlib.colors import BoundaryNorm, ListedColormap

# projections_path = Path(__file__).parent / "outputs" / "projected_pasture_scenarios_TB_LDN.csv"

projections_path = Path(__file__).parent / "outputs" / "projected_pasture_scenarios_gdp_TB.csv"

col_filt = "no_gap_closure"

year1 = 2025
year2 = 2075

outputs = Path(__file__).parent / "outputs" / "processed_gdp"
outputs.mkdir(exist_ok=True)

df = pd.read_csv(projections_path)

countries = df["Country or Area"].unique()
country_isos = df["iso3"].unique()

scenario_cols = [_ for _ in df.columns[df.columns.str.contains(col_filt)] if "m2" in _ or "total" in _]

output_df = pd.DataFrame()

conv = 0.0001 / 1e6

for icol, col in enumerate(scenario_cols):
    
    col_name = col.split("_")[3].upper()
    print(col_name)
    output_df = pd.DataFrame()

    for icountry, country in enumerate(countries):

        df_c = df[df["Country or Area"] == country]
        df_vals = df_c.loc[:, ["year"] + [col]]

        v1 = df_vals[df_vals["year"] == year1][col].sum() * conv
        v2 = df_vals[df_vals["year"] == year2][col].sum() * conv

        data  = {
                "country_iso" : df_c["iso3"].iloc[0],
                "country_name" : country,
                "scenario" : col,
                f"pasture_area_Mha_{year1}" : v1,
                f"pasture_area_Mha_{year2}" : v2,
                # f"pasture_area_change_Mha" : v2 - v1,
                "ratio" : v2 / v1 if v1 > 0 else np.nan
                }
        
        output_df = pd.concat([output_df, pd.DataFrame(data, index=[0])], ignore_index=True)
    
    output_df.to_csv(outputs / f"projected_pasture_area_{col_name}_BAU.csv", index=False)

# world_filt map
# Pick one scenario to plot (e.g. the first one)
plot_col = scenario_cols[3]
col_name = plot_col.split("_")[3].upper()

# Build the output_df for that scenario (reuse your loop logic)
map_df = pd.DataFrame()
for country in countries:
    df_c = df[df["Country or Area"] == country]
    v1 = df_c[df_c["year"] == year1][plot_col].sum() * conv
    v2 = df_c[df_c["year"] == year2][plot_col].sum() * conv
    map_df = pd.concat([map_df, pd.DataFrame({
                            "iso3": df_c["iso3"].iloc[0],
                            "ratio": v2 / v1 if v1 > 0 else np.nan,
                        }, index=[0])], ignore_index=True)

world = gpd.read_file(Path(__file__).parent / "data" / "boundaries" / "geoBoundariesCGAZ_ADM0" / "geoBoundariesCGAZ_ADM0.shp")


world_filt = world.merge(map_df, left_on="shapeGroup", right_on="iso3", how="left")

bounds = [0, 1, 2, 3, 4, 5, 
          world_filt["ratio"].max() if world_filt["ratio"].max() > 5 else 6]

norm = BoundaryNorm(bounds, ncolors=len(bounds) - 1)
cmap = plt.cm.get_cmap("Oranges", len(bounds) - 1)

fig, ax = plt.subplots(1, 1, figsize=(16, 8))

world_filt.plot(
    column="ratio",
    ax=ax,
    legend=True,
    cmap=cmap,
    norm=norm,
    missing_kwds={"color": "lightgrey", "label": "No data"},
    legend_kwds={"label": f"Pasture area ratio", "shrink": 0.5},
    edgecolor="black",
    linewidth=0.2
)

cbar = fig.axes[-1]

# cbar.set_yticks([0.5, 1.5, 2.5, 3.5, 4.5, 5.5])
cbar.set_yticks([0.5, 1.5, 2.5, 3.5, 4.5, (5 + bounds[-1]) / 2])
# cbar.set_yticks([0.25, 0.75, 1.5, 2.5, 3.5, 4.5, 5.5])

cbar.set_yticklabels(["<1", "1–2", "2–3", "3–4", "4–5", ">5"])
# cbar.set_yticklabels(["0-0.5", "0.5-1", "1–2", "2–3", "3–4", "4–5", ">5"])

ax.set_title(f"Projected pasture area ratio {year2}/{year1} ({col_name}, BAU projection)", fontsize=14)
ax.set_axis_off()
ax.set_ylim(-60, 90)
fig.tight_layout()
fig.savefig(outputs.parent / "figs" / "world_map" / f"world_map_pasture_area_ratio_{col_name}_BAU.png", dpi=300)
plt.show()