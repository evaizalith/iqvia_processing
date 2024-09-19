import pandas as pd 
import sys
from os import path, walk

input = sys.argv[1] 

rows = 0
for (root, dirnames, filenames) in walk(input):
    for filename in filenames:
        if filename.endswith('.csv'):
            df = pd.read_csv(input + "/" + filename)
            rowcount, column_count = df.shape
            rows += rowcount
            print("File ", filename, ":", rowcount, ";", column_count)

print("Number of rows: ", rows)

