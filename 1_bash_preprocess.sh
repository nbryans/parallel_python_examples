#!/bin/bash

# filename="/mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01.csv"
# total_lines=$(wc -l < "$filename" | awk '{print $1}')  # Get total lines, removing filename
# lines_per_file=$(( (total_lines + 3) / 4 ))  # Calculate lines per file, including header + rounding up

# # Extract the header
# header=$(head -n 1 "$filename")

# # Split the file and add the header to each
# head -n "$lines_per_file" "$filename" > /mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01-1.csv
# echo "$header" > /mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01-2.csv && head -n "$((lines_per_file * 2))" "$filename" | tail -n "$lines_per_file" >> /mnt/c/Users/natha/Documents/yellow_tripdata_2015-01-2.csv
# echo "$header" > /mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01-3.csv && head -n "$((lines_per_file * 3))" "$filename" | tail -n "$lines_per_file" >> /mnt/c/Users/natha/Documents/yellow_tripdata_2015-01-3.csv
# echo "$header" > /mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01-4.csv && tail -n +$((lines_per_file * 3 + 1)) "$filename" >> /mnt/c/Users/natha/Documents/yellow_tripdata_2015-01-4.csv

echo "Starting preprocessing"

# Process each file with 1_bash_preprocess.py
python3 1_bash_preprocess.py /mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01-1.csv &
python3 1_bash_preprocess.py /mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01-2.csv &
python3 1_bash_preprocess.py /mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01-3.csv &
python3 1_bash_preprocess.py /mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01-4.csv &