#!/bin/bash

source_folder="/home/bdm/P2_data/idealista"  # Replace with the path to the 'idealista' folder on your local machine
target_folder="/user/bdm/idealista"  # Replace with the desired target folder in HDFS

# Create the target folder in HDFS
~/BDM_Software/hadoop/bin/hdfs dfs -mkdir -p "$target_folder"

# Recursively iterate over subfolders in the source folder
for subfolder in "$source_folder"/*; do
    if [[ -d "$subfolder" ]]; then  # Check if it's a directory
        subfolder_name=$(basename "$subfolder")  # Get the subfolder name
        target_subfolder="$target_folder/$subfolder_name"  # Construct the corresponding target subfolder path in HDFS
        ~/BDM_Software/hadoop/bin/hdfs dfs -mkdir -p "$target_subfolder"  # Create the target subfolder in HDFS

        # Copy files from the source subfolder to the target subfolder in HDFS
        ~/BDM_Software/hadoop/bin/hdfs dfs -put "$subfolder"/* "$target_subfolder"
    fi
done

