#!/bin/bash

echo "==========STARTING HDFS IMPORT=========="
catalogue_path="/vagrant/TPA_13/data/Catalogue.csv"
co2_path="/vagrant/TPA_13/data/CO2.csv"
immatriculation_path="/vagrant/TPA_13/data/Immatriculations.csv"
hdfs_destination_path="tpa_13"

folder_exists() {
    hdfs dfs -test -d $1
}

if folder_exists $hdfs_destination_path; then
    hdfs dfs -rm -r $hdfs_destination_path
    if [ $? -ne 0 ]; then
        echo "Error: Failed to delete folder in HDFS."
        exit 1
    fi
    echo "Folder already exists in HDFS. Deleting it..."
fi

hdfs dfs -mkdir -p $hdfs_destination_path/immatriculation
if [ $? -ne 0 ]; then
    echo "Error: Failed to create immatriculation folder in HDFS."
    exit 1
fi

local_files=("$catalogue_path" "$co2_path" "$immatriculation_path")

for local_file_path in "${local_files[@]}"; do
    if [[ $local_file_path == *Immatriculations.csv ]]; then
        hdfs dfs -put $local_file_path $hdfs_destination_path/immatriculation
    else
        hdfs dfs -put $local_file_path $hdfs_destination_path
    fi
    if [ $? -eq 0 ]; then
        echo "File $local_file_path successfully copied to HDFS."
    else
        echo "Error: Failed to copy file $local_file_path to HDFS."
        exit 1
    fi
done

echo "==========END HDFS IMPORT=========="
exit 0

