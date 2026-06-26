import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pathlib import Path

# projections_path = Path(__file__).parent / "outputs" / "projected_pasture_scenarios_TB_LDN.csv"
projections_path = Path(__file__).parent / "outputs" / "projected_pasture_scenarios_gdp_TB.csv"

data_path = Path(__file__).parent / "data"

outputs = Path(__file__).parent / "outputs" / "figs" / "projected_areas_regions"
outputs.mkdir(exist_ok=True)

df = pd.read_csv(projections_path)

col_filt = "no_gap_closure"

regions = df["Sub-region Name"].unique()

for r, region in enumerate(regions):
    
    fig, axs = plt.subplots(
                        # len(regions), 
                        figsize=(10, 6),
                        sharex=True,
                    )
    
    ax = axs

    df_region = df[(df["Sub-region Name"] == region) & (df["year"] >= 2023)]

    df_region = df_region.loc[:, ["Sub-region Name", "year"
                                
                                ] + df_region.columns[df_region.columns.str.contains(col_filt)].tolist()]

    sums = df_region.groupby(["Sub-region Name", "year"]).sum().reset_index()

    print(sums["no_gap_closure_closure_MAX_pasture_efficiency"].max())

    conv = 0.0001 / 1e6
    # conv = 1

    for col in sums.columns[2:]:
        if "m2" in col:
            
            label = col.split("_")[3].upper()
            ax.plot(sums["year"], sums[col] * conv, label=label)
    
    totals = sums.loc[:, sums.columns[sums.columns.str.contains("m2")].tolist()].sum(axis = 1)

    ax.plot(sums["year"], totals * conv, label = "Total", color="k", linestyle="--")

    ax.title.set_text(region)
    ax.legend()
    ax.set_ylabel("Projected pasture area (Mha)")

    fig.tight_layout()
    # plt.show()
    fig.savefig(outputs / f"projected_pasture_area_{region}_{col_filt}.png", dpi=300)


