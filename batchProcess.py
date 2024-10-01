import pandas as pd 
import sys
import os
from os import path, walk, makedirs

output = "output"
delim = "|"

labels = ["pat_id", "claimno", "linenum", "rectype", "tos_flag", "pos", "conf_num", "patstat", "billtype", "ndc", "daw", "formulary", "dayssup", "quan", "proc_cde", "cpt_mod", "rev_code", "srv_unit", "from_dt", "to_dt", "diagprc_ind", "diag_admit", "diag1", "diag2", "diag3", "diag4", "diag5", "diag6", "diag7", "diag8", "diag9", "diag10", "diag11", "diag12", "icdprc1", "icdprc2", "icdprc3", "icdprc4", "icdprc5", "icdprc6", "icdprc7", "icdprc8", "icdprc9", "icdprc10", "icdprc11", "icdprc12", "allowed", "paid", "deductible", "copay", "coinsamt", "cobamt", "dispense_fee", "bill_id", "bill_spec", "rend_id", "rend_spec", "prscbr_id", "prscbr_spec", "ptypefig", "sub_tp_cd", "paid_dt"]

categorical_columns = [3, 4, 6, 7, 9, 11, 14, 15, 16, 54, 56, 60,] # Contain categorical data 
id_columns = [0, 1, 2, 5] # These columns provide id information for the patient and the specific claim, used for connecting Claims, Enroll, and Enroll2 files. These columns are not relevant for model training purposes and thus should be removed

# Append a column onto an existing csv file 
def appendCols(file, cols, n_batches, batch_size):
    temp = f"{output}/temp.csv"
    createdOutput = False
    if path.exists(file):
        os.rename(file, f"{output}/temp.csv")
    else:
        createdOutput = True

    for batch in range(0, n_batches):
        start = batch * batch_size
        end = start + batch_size

        if createdOutput == False:
            df = pd.read_csv(temp, delimiter=delim, header=None, low_memory=False, skiprows=start, nrows=batch_size)

        sub_col = cols.iloc[start:end]
    
        if createdOutput == False:
            concat = df.join(sub_col, how="outer")
        else:
            concat = sub_col

        concat.to_csv(f"{file}", sep=delim, index=False, header=None, mode='a')

    if createdOutput == False:
        os.remove(temp)

# Main processing function
def process(file, input, n_batches, batch_size):
    out_file = f"{output}/output.csv"

    processed = 0
    n_labels = len(labels)
    for col in labels:
        if labels.index(col) not in id_columns:
            df = pd.read_csv(f"{input}/{file}", delimiter=delim, header=None, low_memory=True, usecols=[labels.index(col)])

            df = df.fillna(0)

            if labels.index(col) in categorical_columns:
                df = pd.get_dummies(df, dtype=int)

            appendCols(out_file, df, n_batches, batch_size)
            
            processed = processed + 1
            printProgressPercent(processed, n_labels)

# Prints an updating percentage completion bar to stdout
def printProgressPercent(current, max):
    percentage: int = int((current / max) * 100)
    sys.stdout.write("\r")
    sys.stdout.write(f"{percentage}%")
    sys.stdout.flush()

# Merges all files into one intermediate 
def consolidate(input, files):
    n_files = len(files)
    print(f"Consolidating {n_files} files into 1...")
    
    n_done = 0
    for file in files:
        df = pd.read_csv(f"{input}/{file}", delimiter=delim, header=None, low_memory=False)

        df.to_csv(f"{output}/intermediateFile.csv", sep=delim, index=False, header=None, mode='a')

        n_done = n_done + 1
        printProgressPercent(n_done, n_files)

    print("")

# Counts number of rows in a file 
def countData(input, filename):
    df = pd.read_csv(input + "/" + filename, usecols=[0])
    n_rows, n_cols = df.shape
    return n_rows

def main():
    if len(sys.argv) < 2:
        print("Error: not enough arguments")
        print("Please pass data path as an argument")
        exit()

    input = sys.argv[1]

    if not path.exists(output):
        os.makedirs(output)
        print(f"Created new output dir {output}")


    print("Counting rows...")
    files = []
    n_rows = 0
    i = 0
    for (root, dirnames, filenames) in walk(input):
        n_files = len(filenames)
        for filename in filenames:
            if filename.endswith('.csv'):
                files.append(filename)
                n_rows += countData(input, filename)
            i = i + 1
            printProgressPercent(i, n_files)

    print(f"\nCounted {n_rows} rows")

    intermediateCreated = False
    if (len(files) > 1):
        consolidate(input, files)
        file = f"{output}/intermediateFile.csv"
        intermediateCreated = True
    else:
        file = files[0]

    batch_size = 4096
    n_batches = int(n_rows / batch_size) 
    print(f"Processing {n_rows} in {n_batches} batches")

    process(file, input, n_batches, batch_size)

    if intermediateCreated == True:
        os.remove(f"{output}/intermediateFile.csv")

    print("\nAll done! <3")

if __name__ == "__main__":
    main()
