from weather_data import weather_data
import sys

df_wd = weather_data("New York City", 40.7128, -
                     74.006, "2025-11-01", "2025-11-30")

'Empty data check'
if df_wd.isnull().any().any():
    print('There is empty data in the dataframe. Please check')
    sys.exit()
