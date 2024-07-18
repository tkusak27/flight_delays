import pandas as pd
import numpy as np
import json, os
import time
from concurrent.futures import ProcessPoolExecutor

# Testing with subset (10,000 rows) why? 2009 has 6.4 MILLION rows (we're fucked)
# Processing at about 340k rows/sec
# Took 19 seconds to load the entire 2009.csv file into a dataFrame

CONFIG_PATH="/Users/tomkusak/Desktop/School/data_science/flight_delays/config/config.json"
YEARS=["2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018"]

# Load configuration
with open(CONFIG_PATH) as fp:
    config = json.load(fp)

def get_df(year):
    file_path = f'{config["data_path"]}/{year}.csv'
    yield pd.read_csv(file_path)

def flatten(sequence):
    for item in sequence:
         print(item)


def load_dfs():
        generator_years = (year for year in YEARS)
        with ProcessPoolExecutor(1) as executor:
                return flatten(executor.map(get_df, generator_years))
        
if __name__ == "__main__":
    dicts = load_dfs()
    
    
          