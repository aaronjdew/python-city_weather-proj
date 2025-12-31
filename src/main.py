import json
import pandas as pd
from weather_data import weather_data


loc_rec: list[str] = ["New York City"]
json_file: str = "src/locations.json"

with open(json_file, "r", encoding='utf-8') as file:
    loc_list = json.load(file)

wd_list = []

for loc_lookup in loc_rec:
    get_loc = next(
        (loc for loc in loc_list if loc.get('loc') == loc_lookup), None)

    # Location exists check
    if not get_loc:
        print(f"No location found with loc name of {loc_lookup}")
        continue

    df_wd = weather_data(get_loc["loc"], get_loc["lat"],
                         get_loc["lng"], "2025-11-01", "2025-11-30")

    wd_list.append(df_wd)

    # Empty data check
    if df_wd.isnull().any().any():
        print(
            f'There is empty data in the {get_loc["loc"]} data. Please check')

# Create all weather data frame'
df_all_wd = pd.concat(wd_list, ignore_index=True)

print(df_all_wd)
