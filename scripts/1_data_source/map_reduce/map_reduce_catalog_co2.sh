#!/bin/bash

resutl_path="tpa_13/transformed_catalog"

folder_exists() {
    hdfs dfs -test -d $1
}

if folder_exists $resutl_path; then
    hdfs dfs -rm -r $resutl_path
    if [ $? -ne 0 ]; then
        echo "Error: Failed to delete folder in HDFS."
        exit 1
    fi
    echo "Folder already exists in HDFS.Deleting it..."
fi

spark-submit /vagrant/TPA_13/scripts/1_data_source/map_reduce/map_reduce_catalog_co2.py


