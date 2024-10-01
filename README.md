This repository contains scripts for processing the IQVIA dataset. 

## Contents

1. batchProcess.py - counts all rows in the dataset, consolidates them into one file, and then processes them in many batches. This takes a long time to run but it can process larger than memory datasets. 
2. countData.py - counts number of columns in the dataset across an arbitrary number of .csv files
3. SBMT-preprocess.py - multithreaded script that processes all data in a single batch. Do not use for larger than memory datasets! 
