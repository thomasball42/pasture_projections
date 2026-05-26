import pandas as pd
import numpy as np 
from scipy import stats
from pathlib import Path
import matplotlib.pyplot as plt

historic_yields_dir = Path("data") / "historic_pasture_yields"
output_dir = Path("data") / "processed_pasture_yields_GDP"

figs_dir = Path("outputs", "figs", "gdp_yield_relationships")

figs_dir.mkdir(exist_ok=True)
output_dir.mkdir(exist_ok=True)

gdp_df = pd.read_csv(Path("data", "GDP_data", "FAOSTAT_data_en_5-26-2026.csv"))
country_codes = pd.read_excel(Path("data", "animal_source_food_demand&production_1961_2100_03302026.xlsx"), sheet_name="contry code")

gdp_df = gdp_df.merge(country_codes, left_on="Area", right_on="Country or Area", how="inner")

files = sorted(historic_yields_dir.glob("*.csv"))

output_df = pd.DataFrame()
regression_results = []  # collect per-file, per-region regression stats


def generate_log_log_fits(overwrite=False):
    
    if (output_dir / "log_log_regression_by_region.csv").exists() and not overwrite:
        print(f"Loading existing regression results from {output_dir / 'log_log_regression_by_region.csv'}")
        return pd.read_csv(output_dir / "log_log_regression_by_region.csv")
    
    else:
        pasture_items = ["Buffalo_Meat", "Buffalo_Milk", "Cow_Meat", "Cow_Milk", "Goat_Meat", "Goat_Milk", "Horse_Meat", "Sheep_Meat", "Sheep_Milk"]
        classes =       ["bvmeat",      "bvmilk",           "bvmeat", "bvmilk",     "sgmeat", "bvmilk",     "sgmeat",       "sgmeat",   "bvmilk"]

        for file in files:
            
            print(f"\nProcessing: {file.name}")
            df = pd.read_csv(file)
            
            df_long = df.melt(
                        id_vars='Country_ISO',
                        var_name='Year',
                        value_name='pasture_yield_kg_per_m2'
                        )
            
            df_long['Year'] = df_long['Year'].astype(int)

            merged = pd.merge(
                    gdp_df[['iso3', 'Year', 'Value', 'Sub-region Name']].rename(columns={'Value': 'gdp_per_capita'}),
                    df_long,
                    left_on=['iso3', 'Year'],
                    right_on=['Country_ISO', 'Year'],
                    how='inner'
                            ).dropna(subset=['gdp_per_capita', 'pasture_yield_kg_per_m2'])

            # --- Log-log regression per region ---
            merged['log_gdp'] = np.log(merged['gdp_per_capita'])
            merged['log_yield'] = np.log(merged['pasture_yield_kg_per_m2'])

            log_merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=['log_gdp', 'log_yield'])

            for region, group in log_merged.groupby('Sub-region Name'):
                if len(group) < 3:
                    print(f"  Skipping {region}: only {len(group)} observations")
                    continue

                slope, intercept, r_value, p_value, std_err = stats.linregress(
                    group['log_gdp'], group['log_yield']
                )

                most_recent_year = group['Year'].max()

                for country_iso, country_group in group.groupby('iso3'):
                    country_most_recent_year = country_group['Year'].max()
                    country_current_yield = country_group.loc[
                        country_group['Year'] == country_most_recent_year, 'pasture_yield_kg_per_m2'
                    ].mean()

                    regression_results.append({
                        'file':                     file.stem,
                        'region':                   region,
                        'iso3':                     country_iso,
                        'n':                        len(group),       # region-level n, consistent with the fit
                        'slope':                    slope,
                        'intercept':                intercept,
                        'r_squared':                r_value ** 2,
                        'p_value':                  p_value,
                        'std_err':                  std_err,
                        'current_year':             most_recent_year, # region most recent year (for fit consistency)
                        'current_yield_kg_per_m2':  country_current_yield,
                    })

            regions = merged['Sub-region Name'].unique()
            colours = plt.cm.tab20(np.linspace(0, 1, len(regions)))
            region_colour_map = dict(zip(regions, colours))

            fig, axes = plt.subplots(1, 2, figsize=(14, 6))
            fig.suptitle(file.stem, fontsize=12)

            for region, colour in region_colour_map.items():
                subset = merged[merged['Sub-region Name'] == region]

                subset.replace([np.inf, -np.inf], np.nan, inplace=True)

                for ax in axes:
                    ax.scatter(
                        subset['gdp_per_capita'],
                        subset['pasture_yield_kg_per_m2'],
                        alpha=0.5, s=15,
                        color=colour,
                        label=region
                    )

            for ax, scale, title in zip(
                axes,
                [('linear', 'linear'), ('log', 'log')],
                ['Linear scale', 'Log-log scale']
            ):
                ax.set_xscale(scale[0])
                ax.set_yscale(scale[1])
                ax.set_xlabel('GDP per capita (2015 USD)')
                ax.set_ylabel('Pasture yield (kg/m²)')
                ax.set_title(title)

            handles, labels = axes[0].get_legend_handles_labels()
            unique = dict(zip(labels, handles))
            fig.legend(unique.values(), unique.keys(), loc='lower center', ncol=3, bbox_to_anchor=(0.5, 0.0), fontsize=8)
            fig.subplots_adjust(bottom=0.25)
            fig.savefig(figs_dir / f"{file.stem}_gdp_yield_scatter.png", dpi=300)

        regression_df = pd.DataFrame(regression_results)

        regression_df["item"] = regression_df["file"].apply(lambda x: f"{x.split('_')[-2]}_{x.split('_')[-1]}")
        regression_df["class"] = regression_df["item"].apply(lambda x: classes[pasture_items.index(x)] if x in pasture_items else None)

        regression_df = regression_df[["file", "region", "iso3", "n", "slope", "intercept", "r_squared", "p_value", "std_err", "current_year", "current_yield_kg_per_m2", "class"]]

        regression_df.to_csv(output_dir / "log_log_regression_by_region.csv", index=False)
        print(f"\nRegression results saved to {output_dir / 'log_log_regression_by_region.csv'}")

        return regression_df


def get_yield_val(gdp_per_capita, slope, intercept):
    log_gdp = np.log(gdp_per_capita)
    log_yield = intercept + slope * log_gdp
    return np.exp(log_yield)

def get_region(country_iso):
    region = country_codes[country_codes["iso3"] == country_iso]["Sub-region Name"].values
    return region[0] if len(region) > 0 else None