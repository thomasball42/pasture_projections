import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import _project_pasture_yields
import _project_pasture_yields_GDP_region as _project_pasture_yields_GDP

import os

output_file = Path("outputs", "projected_pasture_scenarios_gdp_TB.csv")
# countries_list = ["GBR", "TZA"]
countries_list = None  # set to None to process all countries

scenarios = {

            "base_projection_capped" : {
                    "target_year" : 2100,
                    "closure_perc" : 0.0,
                    "hist_proj" : True,
                    "cap" : True
                    },

            "base_projection_uncapped" : {
                "target_year" : 2100,
                "closure_perc" : 0.0,
                "hist_proj" : True,
                "cap" : False
                },

            "full_gap_closure_by_2075_capped" : {
                    "target_year" : 2075,
                    "closure_perc" : 1.0,
                    "hist_proj" : True,
                    "cap" : True
                    },

            "full_gap_closure_by_2075_uncapped" : {
                    "target_year" : 2075,
                    "closure_perc" : 1.0,
                    "hist_proj" : True,
                    "cap" : False
                    },
        }

data_dir = "data"
start_year = 2022

df = pd.read_excel(Path(data_dir) / 'animal_source_food_demand&production_1961_2100_03302026.xlsx', sheet_name='country-level absolute')
yield_gaps = pd.read_csv(Path(data_dir) / "pasture_yield_gaps_v2.csv").rename(columns={"iso_a3": "iso3", "mean_gap": "current_yield_gap"})[["iso3", "current_yield_gap", "physical_area_km2"]]
yield_gaps["region"] = yield_gaps["iso3"].apply(_project_pasture_yields_GDP.get_region)


def get_yield_gap(iso3):

    gap_data = yield_gaps[yield_gaps["iso3"] == iso3]

    if len(gap_data) > 0 and gap_data["current_yield_gap"].notna().any():
        return gap_data["current_yield_gap"].values[0]
    else:
        region = _project_pasture_yields_GDP.get_region(iso3)
        if region is None:
            return np.nan
        else:
            region_data = yield_gaps[yield_gaps.region == region]
            return np.nansum(region_data["current_yield_gap"] * region_data["physical_area_km2"]) / np.nansum(region_data["physical_area_km2"])


fit_stats_file = Path("outputs", "gdp_yield_fit_stats.csv")

gdp_log_fits = _project_pasture_yields_GDP.generate_log_log_fits(overwrite=False)

pasture_df = pd.read_csv(Path(data_dir) / "Pasture_calc.csv")

pasture_items = [867, 882, 947, 951, 977, 982, 1017, 1020, 1097]
classes = ["bvmeat", "bvmilk", "bvmeat", "bvmilk", "sgmeat", "bvmilk", "sgmeat", "bvmilk", "sgmeat"]

pasture_df["class"] = pasture_df["Item_Code"].apply(lambda x: classes[pasture_items.index(x)] if x in pasture_items else None)
pasture_df = pasture_df.groupby(["Country_ISO", "class"])["fp_m2"].sum(min_count=1).reset_index()
pasture_df["year"] = start_year

beef_pasture = pasture_df[pasture_df["class"] == "bvmeat"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3","fp_m2": "beef_m2"})
mutton_pasture = pasture_df[pasture_df["class"] == "sgmeat"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3", "fp_m2": "mutton_m2"})
milk_pasture = pasture_df[pasture_df["class"] == "bvmilk"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3", "fp_m2": "milk_m2"})

def weighted_mean_group(g):
    weights = g["n"]
    cols = g.drop(columns=["n"])
    return pd.Series(np.average(cols, weights=weights, axis=0), index=cols.columns)

gdp_log_fits = gdp_log_fits.drop(columns=["file", "region"])

n_totals = gdp_log_fits.groupby(["iso3", "class"])["n"].sum().rename("n_total")

gdp_log_fits = (
    gdp_log_fits
    .groupby(["iso3", "class"])
    .apply(weighted_mean_group)
    .reset_index()
    .merge(n_totals, on=["iso3", "class"], how="left")
)

gdp_log_fits.to_csv(fit_stats_file, index=False)

# countries with a negative fitted slope are generally already near their feasible yield
# ceiling (developed economies) rather than genuinely losing yield as GDP rises, so treat
# these as flat (no further GDP-driven yield change) instead of projecting a decline
gdp_log_fits.loc[gdp_log_fits["slope"] < 0, "slope"] = 0

# process yield data a bit
df = df.merge(beef_pasture, on=["iso3", "year"], how="left").merge(mutton_pasture, on=["iso3", "year"], how="left").merge(milk_pasture, on=["iso3", "year"], how="left")
df["current_beef_yield"] = df["beef protein demand (ton per year)"] / df["beef_m2"]
df["current_mutton_yield"] = df["mutton protein demand (ton per year)"] / df["mutton_m2"]
df["current_milk_yield"] = df["milk protein demand (ton per year)"] / df["milk_m2"]

countries = df["iso3"].unique() if countries_list is None else countries_list

# make baseline projection based on GDP
for c, country in enumerate(countries):

    print("calculating projections: ", country, round(c / len(countries), 3), end="\r")

    data = df[(df["iso3"] == country)&(df["year"] == start_year)]
    
    # baseline growth
    for item in ["beef", "mutton", "milk"]:

        region = _project_pasture_yields_GDP.get_region(country)
        gdpf = gdp_log_fits[(gdp_log_fits["iso3"] == country) & (gdp_log_fits["class"] == item.replace("beef", "bvmeat").replace("mutton", "sgmeat").replace("milk", "bvmilk"))]

        start_val = data[f"current_{item}_yield"].values[0]

        if len(gdpf) > 0:
            slope = gdpf["slope"].values[0]
            intercept = gdpf["intercept"].values[0]
            gdp_dat = df[(df["iso3"] == country)][["year", "GDP per capita (constant 2015 US$)"]]
            projected_yield = gdp_dat["GDP per capita (constant 2015 US$)"].apply(lambda x: np.exp(intercept + slope * np.log(x)))
            gdp_dat["projected_gdpyield_proportion"] = projected_yield / projected_yield[gdp_dat.year == start_year].values[0]
            df.loc[(df["iso3"] == country) & (df["year"] == start_year), f"current_{item}_yield"] = start_val
            df.loc[(df["iso3"] == country) & (df["year"] > start_year), f"current_{item}_yield"] = start_val * gdp_dat[gdp_dat.year > start_year]["projected_gdpyield_proportion"].values


# modify projection for gap closure if applicable
    for scenario_name, scenario_data in scenarios.items():

        closure_perc = scenario_data["closure_perc"]
        
        target_year = scenario_data["target_year"]

        current_gap = get_yield_gap(country)
        scenario_max_efficiency = (1 - (1-closure_perc) * current_gap) / (1 - current_gap)

        # df.loc[(df["iso3"] == country), "scenario_max_efficiency_cap"] = scenario_max_efficiency

        scenario_modifier_efficiency_interp = np.linspace(1, scenario_max_efficiency, target_year-start_year+1)

        df.loc[(df["iso3"] == country) & (df["year"] < start_year),
                f"{scenario_name}_pasture_efficiency_modifier"] = np.nan
        df.loc[(df["iso3"] == country) & (df["year"] >= start_year) & (df["year"] <= target_year),
                f"{scenario_name}_pasture_efficiency_modifier"] = scenario_modifier_efficiency_interp
        df.loc[(df["iso3"] == country) & (df["year"] > target_year), f"{scenario_name}_pasture_efficiency_modifier"] = scenario_max_efficiency

print(" ")

for scenario_name, scenario_data in scenarios.items():

    closure_perc = scenario_data["closure_perc"]
    
    for item in ["beef", "mutton", "milk"]:

        col = f"{scenario_name}_{item}_yield_tonnes/m2"

        if scenario_data["hist_proj"]:
            df[col] = np.where(
                df["year"] < start_year,
                np.nan,  # historic
                df[f"current_{item}_yield"] * df[f"{scenario_name}_pasture_efficiency_modifier"]
                                )
        else:

            start_yields = (
                df[df["year"] == start_year]
                .set_index("iso3")[f"current_{item}_yield"]
            )
            df[col] = np.where(
                df["year"] < start_year,
                np.nan,
                df["iso3"].map(start_yields) * df[f"{scenario_name}_pasture_efficiency_modifier"]
            )
        
        # cap yield data if cap is set
        if scenario_data.get("cap", False):
            for i, country in enumerate(countries):

                print("calculating landuse: ", country, round(i / len(countries), 3), end="\r")
                current_gap = get_yield_gap(iso3=country)

                max_ratio = (1 - (1-closure_perc) * current_gap) / (1 - current_gap)

                mask = df["iso3"] == country
                country_df = df[mask]

                start_val = country_df.loc[country_df["year"] == start_year, col]
                if start_val.empty or start_val.values[0] == 0:
                    continue

                yield_ratio = country_df[col] / start_val.values[0]
                cap_year = country_df.loc[(yield_ratio > max_ratio) & (country_df["year"] >= start_year), "year"].min()

                if not np.isnan(cap_year):
                    cap_val = country_df.loc[country_df["year"] == cap_year, col].values[0]
                    df.loc[mask, col] = np.where(yield_ratio < max_ratio, country_df[col], cap_val)

        df[f"{scenario_name}_{item}_pasture_area_m2"] = df[f"{item} protein demand (ton per year)"] / df[col]
        df = df.drop(columns=[f"{scenario_name}_{item}_yield_tonnes/m2"])

    df[f"{scenario_name}_total_pasture_area"] = df[[
                                                    f"{scenario_name}_beef_pasture_area_m2", 
                                                    f"{scenario_name}_mutton_pasture_area_m2", 
                                                    f"{scenario_name}_milk_pasture_area_m2"]
                                                    ].sum(axis=1).replace(0, np.nan)

for item in ["beef", "mutton", "milk"]:
    df = df.drop(columns=[f"current_{item}_yield", f"{item}_m2"])

df.to_csv(output_file, index=False)
