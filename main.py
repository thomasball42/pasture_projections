import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import _project_pasture_yields
import os

scenarios = {
            
            # "no_gap_closure_fixed_efficiency" : {"target_year" : 2100,
            #                                     "closure_perc" : 0.0,
            #                                     "hist_proj" : False},

            "no_gap_closure" : {"target_year" : 2100,
                                                "closure_perc" : 0.0,
                                                "hist_proj" : True},

            "quarter_gap_closure_by_2100" : {"target_year" : 2100,
                                        "closure_perc" : 0.25,
                                        "hist_proj" : True},

            "half_gap_closure_by_2100" : {"target_year" : 2100,
                                        "closure_perc" : 0.5,
                                        "hist_proj" : True},
                        
            "full_gap_closure_by_2100" : {"target_year" : 2100, # this scenario has to exist
                                       "closure_perc" : 1.0,
                                       "hist_proj" : True},
            
            "quarter_gap_closure_by_2075" : {"target_year" : 2075,
                                       "closure_perc" : 0.25,
                                       "hist_proj" : True},

            "half_gap_closure_by_2075" : {"target_year" : 2075,
                                       "closure_perc" : 0.5,
                                       "hist_proj" : True},

            "full_gap_closure_by_2075" : {"target_year" : 2075,
                                       "closure_perc" : 1.0,
                                       "hist_proj" : True},

            "quarter_gap_closure_by_2050" : {"target_year" : 2050,
                                            "closure_perc" : 0.25,
                                            "hist_proj" : True},

            "half_gap_closure_by_2050" : {"target_year" : 2050,
                                            "closure_perc" : 0.5,
                                            "hist_proj" : True},

            "full_gap_closure_by_2050" : {"target_year" : 2050,
                                       "closure_perc" : 1.0,
                                       "hist_proj" : True},
             
        }

data_dir = "data"
output_dir = "outputs"
start_year = 2022

df = pd.read_excel(Path(data_dir) / 'animal_source_food_demand&production_1961_2100_03302026.xlsx', sheet_name='country-level absolute')

linear_fits = _project_pasture_yields.generate_linear_fits()

pasture_df = pd.read_csv(Path(data_dir) / "Pasture_calc.csv")

pasture_items = [867, 882, 947, 951, 977, 982, 1017, 1020, 1097]
classes = ["bvmeat", "bvmilk", "bvmeat", "bvmilk", "sgmeat", "bvmilk", "sgmeat", "bvmilk", "sgmeat"]

pasture_df["class"] = pasture_df["Item_Code"].apply(lambda x: classes[pasture_items.index(x)] if x in pasture_items else None)
pasture_df = pasture_df.groupby(["Country_ISO", "class"])["fp_m2"].sum().reset_index()
pasture_df["year"] = start_year

beef_pasture = pasture_df[pasture_df["class"] == "bvmeat"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3","fp_m2": "beef_m2"})
mutton_pasture = pasture_df[pasture_df["class"] == "sgmeat"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3", "fp_m2": "mutton_m2"})
milk_pasture = pasture_df[pasture_df["class"] == "bvmilk"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3", "fp_m2": "milk_m2"})

linear_fits = linear_fits.groupby(["Country_ISO", "class"]).mean().reset_index()

df = df.merge(beef_pasture, on=["iso3", "year"], how="left").merge(mutton_pasture, on=["iso3", "year"], how="left").merge(milk_pasture, on=["iso3", "year"], how="left")

df["current_beef_yield"] = df["beef protein demand (ton per year)"] / df["beef_m2"]
df["current_mutton_yield"] = df["mutton protein demand (ton per year)"] / df["mutton_m2"]
df["current_milk_yield"] = df["milk protein demand (ton per year)"] / df["milk_m2"]

yield_gaps = pd.read_csv(Path(data_dir) / "pasture_yield_gaps_v2.csv").rename(columns={"iso_a3": "iso3", "mean_gap": "current_yield_gap"})[["iso3", "current_yield_gap"]]
df = df.merge(yield_gaps, on="iso3", how="left")

df["current_yield_efficiency"] = 1 - df["current_yield_gap"]

for scenario_name, scenario_data in scenarios.items():

    closure_perc = scenario_data["closure_perc"]

    df[f"{scenario_name}_closure_MAX_pasture_efficiency"] = (1 - (1-closure_perc) * df["current_yield_gap"]) / df["current_yield_efficiency"]

df[f"current_total_yield_efficiency"] = 1

for c, country in enumerate(df["iso3"].unique()):

    print("calculating projections: ", country, round(c / len(df["iso3"].unique()), 3), end="\r")

    data = df[(df["iso3"] == country)&(df["year"] == start_year)]
    
    # baseline growth
    for item in ["beef", "mutton", "milk"]:
        lf = linear_fits[(linear_fits["Country_ISO"] == country) & (linear_fits["class"] == item.replace("beef", "bvmeat").replace("mutton", "sgmeat").replace("milk", "bvmilk"))]
        
        start_val = data[f"current_{item}_yield"].values[0]

        if len(lf) > 0:
            slope = lf["Slope"].values[0]
            intercept = lf["Intercept"].values[0]
            val_current = intercept + slope * (start_year)
            val_2100 = intercept + slope * (2100)
            perc_growth_rate = ((val_2100 - val_current)/val_current) / (2100-start_year)

            df.loc[(df["iso3"] == country) & (df["year"] == start_year), f"current_{item}_yield"] = start_val
            df.loc[(df["iso3"] == country) & (df["year"] > start_year), f"current_{item}_yield"] = start_val + start_val *perc_growth_rate * (df.loc[(df["iso3"] == country) & (df["year"] > start_year), "year"] - start_year)

        else:
            df.loc[(df["iso3"] == country) & (df["year"] >= start_year), f"current_{item}_yield"] = start_val

    # projection of closure
    for scenario_name, scenario_data in scenarios.items():
        target_year = scenario_data["target_year"]
        current_yield_efficiency = data["current_total_yield_efficiency"].values[0]
        scenario_efficiency = data[f"{scenario_name}_closure_MAX_pasture_efficiency"].values[0]
        scenario_efficiency_interp = np.linspace(current_yield_efficiency, scenario_efficiency, target_year-start_year+1)

        df.loc[(df["iso3"] == country) & (df["year"] < start_year),
                f"{scenario_name}_relative_pasture_efficiency"] = np.nan
        df.loc[(df["iso3"] == country) & (df["year"] >= start_year) & (df["year"] <= target_year),
                f"{scenario_name}_relative_pasture_efficiency"] = scenario_efficiency_interp
        df.loc[(df["iso3"] == country) & (df["year"] > target_year), f"{scenario_name}_relative_pasture_efficiency"] = scenario_efficiency

print(" ")

for scenario_name, scenario_data in scenarios.items():

    for item in ["beef", "mutton", "milk"]:

        max_ratio = df[df.columns[df.columns.str.contains("full_gap_closure")]].max().max()

        col = f"{scenario_name}_{item}_yield_tonnes/m2"

        if scenario_data["hist_proj"]:
            df[col] = np.where(
                df["year"] < start_year,
                np.nan,  # historic
                df[f"current_{item}_yield"] * df[f"{scenario_name}_relative_pasture_efficiency"]
                                )
        else:
            start_yields = (
                df[df["year"] == start_year]
                .set_index("iso3")[f"current_{item}_yield"]
            )
            df[col] = np.where(
                df["year"] < start_year,
                np.nan,
                df["iso3"].map(start_yields) * df[f"{scenario_name}_relative_pasture_efficiency"]
            )
            
            
        for country in df["iso3"].unique():

            print("calculating landuse: ", country, round(c / len(df["iso3"].unique()), 3), end="\r")
            

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
df = df.drop(columns=["current_yield_gap", "current_yield_efficiency"])

df.to_csv(Path(output_dir) / "projected_pasture_scenarios_TB_LDN.csv", index=False)

# countries = ["GBR", "TZA"]

# scenarios_to_plot = ["no_gap_closure", "full_gap_closure_by_2100", "full_gap_closure_by_2050"]
# scenarios_to_plot = list(scenarios.keys())

# for iso3 in countries:
#     fig, ax = plt.subplots(figsize=(10, 6))
#     for scenario_name in scenarios_to_plot:
#         scenario_data = scenarios[scenario_name]
        
#         country_data = df[df["iso3"] == iso3]
#         ax.plot(country_data["year"], country_data[f"{scenario_name}_total_pasture_area"], 
#                 # label=f"{iso3}_{scenario_data['closure_perc']}_{scenario_data['target_year']}"
#                 label = scenario_name
#                 )
#         ax.set_xlabel("Year")
#         ax.set_ylabel("Total Pasture Area (m2)")
#         ax.set_ylim(0, None)
#         ax.legend()

# plt.show()