import sys
import os

from datetime import datetime

import re

import numpy as np
import pandas as pd

start = datetime.now()

data_iterator = pd.read_csv("files/renthub_data.csv",
    iterator = True, chunksize = 10**6)

for current_frame in data_iterator:
    print(current_frame.head())

print("Program run took: " + str(datetime.now() - start))
