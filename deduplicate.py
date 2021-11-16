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

cursor.execute("UPDATE real_estate_data SET property_type = 'unknown' WHERE property_type = 'nan' ")

cursor.execute("UPDATE real_estate_data SET state = 'AB' WHERE state = 'ALBERTA' ")
cursor.execute("UPDATE real_estate_data SET state = 'UT' WHERE state = 'UTAH' ")
cursor.execute("UPDATE real_estate_data SET state = 'CA' WHERE state = 'HOLLYWOOD' ")
cursor.execute("UPDATE real_estate_data SET state = 'AB' WHERE state = 'Minnesota' ")
cursor.execute("UPDATE real_estate_data SET state = 'FL' WHERE state = '30' ")
cursor.execute("UPDATE real_estate_data SET state = 'TX' WHERE state = '44' ")

cursor.execute("CREATE TABLE deduped AS SELECT address, city, state, zip, avg(price) as price, min(posted_at) as posted_at from real_estate_data GROUP BY address, city, state, zip, substr(posted_at, 0, 7)")

print("Program run took: " + str(datetime.now() - start))
