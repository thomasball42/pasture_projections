import pandas as pd
import numpy as np

df = pd.read_excel('animal_source_food_demand&production_1961_2100_03302026.xlsx', sheet_name='country-level absolute')
print(df.columns)
pasture_df = pd.read_csv("Pasture_calc.csv")

pasture_items = [867, 882, 947, 951, 977, 982, 1017, 1020, 1097]
classes = ["bvmeat", "bvmilk", "bvmeat", "bvmilk", "sgmeat", "bvmilk", "sgmeat", "bvmilk", "sgmeat"]

pasture_df["class"] = pasture_df["Item_Code"].apply(lambda x: classes[pasture_items.index(x)] if x in pasture_items else None)
pasture_df = pasture_df.groupby(["Country_ISO", "class"])["fp_m2"].sum().reset_index()
pasture_df["year"] = 2021
beef_pasture = pasture_df[pasture_df["class"] == "bvmeat"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3","fp_m2": "beef_m2"})
mutton_pasture = pasture_df[pasture_df["class"] == "sgmeat"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3", "fp_m2": "mutton_m2"})
milk_pasture = pasture_df[pasture_df["class"] == "bvmilk"][["Country_ISO", "fp_m2", "year"]].rename(columns={"Country_ISO": "iso3", "fp_m2": "milk_m2"})

df = df.merge(beef_pasture, on=["iso3", "year"], how="left").merge(mutton_pasture, on=["iso3", "year"], how="left").merge(milk_pasture, on=["iso3", "year"], how="left")

df["current_beef_yield"] = df["beef protein demand (ton per year)"] / df["beef_m2"]
df["current_mutton_yield"] = df["mutton protein demand (ton per year)"] / df["mutton_m2"]
df["current_milk_yield"] = df["milk protein demand (ton per year)"] / df["milk_m2"]

yield_gaps = pd.read_csv("pasture_yield_gaps.csv").rename(columns={"iso_a3": "iso3", "mean_gap": "current_yield_gap"})[["iso3", "current_yield_gap"]]
df = df.merge(yield_gaps, on="iso3", how="left")
df["current_yield_efficiency"] = 1 - df["current_yield_gap"]
df["full_closure_efficiency"] = (1 / df["current_yield_efficiency"])
df["50%_closure_efficiency"] = (1 - 0.5 * df["current_yield_gap"]) / df["current_yield_efficiency"]
df["25%_closure_efficiency"] = (1 - 0.75 * df["current_yield_gap"]) / df["current_yield_efficiency"]
df["current_yield_efficiency"] = 1

for country in df["iso3"].unique():
    data = df[(df["iso3"] == country)&(df["year"] == 2021)]
    full_closure = np.linspace(data["current_yield_efficiency"].values[0], data["full_closure_efficiency"].values[0], 2100-2021)
    half_closure = np.linspace(data["current_yield_efficiency"].values[0], data["50%_closure_efficiency"].values[0], 2100-2021)
    quarter_closure = np.linspace(data["current_yield_efficiency"].values[0], data["25%_closure_efficiency"].values[0], 2100-2021)
    df.loc[(df["iso3"] == country) & (df["year"] > 2021), "full_closure_efficiency"] = full_closure
    df.loc[(df["iso3"] == country) & (df["year"] > 2021), "50%_closure_efficiency"] = half_closure
    df.loc[(df["iso3"] == country) & (df["year"] > 2021), "25%_closure_efficiency"] = quarter_closure
    df.loc[(df["iso3"] == country) & (df["year"] > 2021), "current_beef_yield"] = data["current_beef_yield"].values[0]
    df.loc[(df["iso3"] == country) & (df["year"] > 2021), "current_mutton_yield"] = data["current_mutton_yield"].values[0]
    df.loc[(df["iso3"] == country) & (df["year"] > 2021), "current_milk_yield"] = data["current_milk_yield"].values[0]

for x in ["full_closure", "50%_closure", "25%_closure"]:
    for y in ["beef", "mutton", "milk"]:
        df[f"{x}_{y}_yield"] = df[f"current_{y}_yield"] * df[f"{x}_efficiency"]
        df[f"{x}_{y}_pasture_area"] = df[f"{y} protein demand (ton per year)"] / df[f"{x}_{y}_yield"]
        df = df.drop(columns=[f"{x}_{y}_yield"])
    df[f"{x}_total_pasture_area"] = df[[f"{x}_beef_pasture_area", f"{x}_mutton_pasture_area", f"{x}_milk_pasture_area"]].sum(axis=1)

for y in ["beef", "mutton", "milk"]:
    df = df.drop(columns=[f"current_{y}_yield", f"{y}_m2"])
df = df.drop(columns=["current_yield_gap", "current_yield_efficiency"])




print(df[df["year"] == 2021])
df.to_csv("pasture_efficiency_projections.csv", index=False)