import sys
import os

from datetime import datetime

import re

import numpy as np
import pandas as pd

import sqlite3

start = datetime.now()

database = sqlite3.connect('files/sql.db')
cursor = database.cursor()

def shorten_zip(zipcode):
    #check for candian
    if re.search('[A-Za-z]', zipcode):
        return zipcode
    else:
        return zipcode[:5]

def create_table():
    cursor.execute("DROP TABLE IF EXISTS real_estate_data")
    cursor.execute("CREATE TABLE real_estate_data(state text, city text, zip text, address text, property_type text, beds text, baths text, sqft text, price int, posted_at text)")

columns_list = ['state', 'city', 'zip', 'address', 'property_type', 'beds', 'baths', 'sqft', 'price', 'posted_at']

data_iterator = pd.read_csv("files/renthub_data.csv",
    iterator = True, chunksize = 10**6,
    usecols = columns_list,
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

create_table()

for current_frame in data_iterator:
    current_frame.dropna(subset=['state', 'city', 'zip', 'address'], inplace=True)
    current_frame['price'] = current_frame['price'].replace(r'[\$\,]', '').astype(int).round()
    current_frame['zip'] = current_frame['zip'].apply(shorten_zip)
    for row in current_frame.itertuples():
        row_dict = row._asdict()
        sql_row = [str(row_dict[field]) for field in columns_list]
        cursor.execute("INSERT INTO real_estate_data VALUES(?,?,?,?,?,?,?,?,?,?);",sql_row)

database.commit()

print("Program run took: " + str(datetime.now() - start))
