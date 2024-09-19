# pre-process IQVIA data
# Eva Powlison
# Last modified: September 17, 2024

import pandas as pd 
import sys
import os
from os import path, walk, makedirs
import multiprocessing
from multiprocessing import Pool

#############################################################

single_file_out = True # If true, will compile all data into one single .csv file, else keeps data separate
n_threads = 2 # Number of worker threads
categorical_columns = [3, 4, 6, 7, 9, 11, 14, 15, 16, 54, 56, 60,] # Contain categorical data 
id_columns = [0, 1, 2, 5] # These columns provide id information for the patient and the specific claim, used for connecting Claims, Enroll, and Enroll2 files. These columns are not relevant for model training purposes and thus should be removed

############################################################

input = ""
output = "processed_data"

total_to_process = 0
done_processing = 0

semaphore = multiprocessing.Semaphore(1)

# Fancy percentage indicator, not important 
def update_status():
    global done_processing
    done_processing += 1
    percentage: int = int((done_processing / total_to_process) * 100)
    sys.stdout.write("\r")
    sys.stdout.write(f"Processed {percentage}%")
    sys.stdout.flush()

# Handles actual processing for one individual file
def process(filename):
    df = pd.read_csv(f"{input}/{filename}", delimiter="|", header=None, low_memory=False)

    # one hot encode categorical data
    df = pd.get_dummies(df, columns=categorical_columns, dtype=int)

    # remove id columns
    df = df.drop(columns=id_columns)

    # fill in missing values with 0 
    df = df.fillna(0)

    # write to file
    if single_file_out == True:
        semaphore.acquire()
        df.to_csv(f"processed_data/processed_data.csv",sep="|", index=False, header=None, mode = 'a')
        update_status()
        semaphore.release()
    else:
        df.to_csv(f"processed_data/{filename}", sep="|", index=False, header=None)
        semaphore.acquire()
        update_status()
        semaphore.release()


def main():
    if len(sys.argv) < 2:
        print("Error: not enough arguments")
        print("Please pass data path as an argument")
        exit()

    global input
    input = sys.argv[1] 

    if not path.exists(output):
        os.makedirs(output)

    files = []
    for (root, dirnames, filenames) in walk(input):
        for filename in filenames:
            if filename.endswith('.csv'):
                files.append(filename)

    global total_to_process
    total_to_process = len(files)

    print(f"Processing {total_to_process} files using {n_threads} workers")

    with Pool(n_threads) as p:
        p.map(process, files)

    print("\nAll done! <3")

if __name__ == "__main__":
    main()

