import sys
import os

from datetime import datetime

import re

import numpy as np
import pandas as pd

import sqlite3

start = datetime.now()

def shorten_zip(zipcode):
    #check for candian
    if re.search('[A-Za-z]', zipcode):
        return zipcode
    else:
        return zipcode[:5]

data_iterator = pd.read_csv("files/renthub_data.csv",
    iterator = True, chunksize = 10**6,
    usecols = ['state', 'city', 'zip', 'address', 'property_type', 'beds', 'baths', 'sqft', 'price', 'posted_at'],
    parse_dates = ['posted_at'],
    dtype = {
        'state': str,
        'city': str,
        'zip': str,
        'address': str,
        'property_type': str,
        'beds': int,
        'baths': float,
        'sqft': str,
        'price': str
    },
)

for current_frame in data_iterator:
    current_frame.dropna(subset=['state', 'city', 'zip', 'address'], inplace=True)
    current_frame['price'] = current_frame['price'].replace(r'[\$\,]', '').astype(int)
    current_frame['zip'].apply(shorten_zip)

print("Program run took: " + str(datetime.now() - start))
