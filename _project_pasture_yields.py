import pandas as pd
import numpy as np 
from scipy import stats
from pathlib import Path

historic_yields_dir = Path("data") / "historic_pasture_yields"
output_dir = Path("data") / "processed_pasture_yields"

output_dir.mkdir(exist_ok=True)



def linear_projection_with_r2(df):
    results = []
    id_col = df.columns[0]
    numeric_cols = df.columns[1:]
    x_values = np.arange(len(numeric_cols))
    for idx, row in df.iterrows():
        country_id = row[id_col]
        y_values = row[numeric_cols].values.astype(float)

        idx = ~np.isnan(y_values) & ~np.isnan(x_values)

        slope, intercept, r_value, p_value, std_err = stats.linregress(x_values[idx], y_values[idx])
        r2 = r_value ** 2
        
        results.append({
            'Country_ISO': country_id,
            'Slope': slope,
            'Intercept': intercept,
            'R2': r2,
            'P_value': p_value
        })
    return pd.DataFrame(results)


def generate_linear_fits():

    pasture_items = ["Buffalo_Meat", "Buffalo_Milk", "Cow_Meat", "Cow_Milk", "Goat_Meat", "Goat_Milk", "Horse_Meat", "Sheep_Meat", "Sheep_Milk"]
    classes =       ["bvmeat",      "bvmilk",           "bvmeat", "bvmilk",     "sgmeat", "bvmilk",     "sgmeat",       "sgmeat",   "bvmilk"]

    files = sorted(historic_yields_dir.glob("*.csv"))

    output_df = pd.DataFrame()

    for file in files:
        
        print(f"\nProcessing: {file.name}")
        df = pd.read_csv(file)
        r2_results = linear_projection_with_r2(df)

       
        # Get linear projection results
        r2_results = linear_projection_with_r2(df)
        
        r2_results["item"] = "_".join(file.stem.split("_")[-2:])
        r2_results["class"] = r2_results["item"].apply(lambda x: classes[pasture_items.index(x)] if x in pasture_items else None)
        r2_results.drop(columns = ["item"], inplace=True)
        # Save results
        
        output_df = pd.concat([output_df, r2_results])

        # r2_results.to_csv(output_file, index=False)
        # print(f"  Saved to: {output_file}")

    output_file = output_dir / f"pasture_linear_fits.csv"

    output_df.to_csv(output_file, index=False)
    return output_df


