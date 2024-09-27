# Using the unzipped wedge files, this scipt cleans and uploads the data to Google Big Query.

import numpy as np
import pandas as pd
from google.cloud import bigquery
import os

dir = 'data/WedgeZipOfZips_small/extracted/'
for file in os.listdir(dir):
    print(file)
    
df = pd.read_csv(dir + file) # Load file to df
df = df.replace(['\\N', r'\N'], 'NULL') # replace 
# Clean the data
