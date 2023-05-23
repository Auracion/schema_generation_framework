#!/bin/bash

method=$1


data_out='gendata_wossp/aug_'${method}'_0.2/train.json'
table_out='gendata_wossp/aug_'${method}'_0.2/tables.json'
proc_data='gendata_wossp/aug_'${method}'_0.2/train.bin'
proc_table='gendata_wossp/aug_'${method}'_0.2/tables.bin'
output_path='gendata_wossp/aug_'${method}'_0.2/train.rgatsql.bin'


#echo 'Generate hardness oriented '${method}' data for train set'
#python3 -u sampling_system.py --rate 1.0 --method ${method} --augmentation

echo "Start to preprocess the original train dataset ..."
python3 -u preprocess/process_dataset.py --dataset_path ${data_out} \
                                         --raw_table_path ${table_out} \
                                         --table_path ${proc_table} \
                                         --output_path ${proc_data} \
                                         --skip_large \
                                         --db_content

echo "Start to construct graphs for the dataset using rgat ..."
python3 -u preprocess/process_graphs.py --dataset_path ${proc_data} \
                                        --table_path ${proc_table} \
                                        --method 'rgatsql' \
                                        --output_path ${output_path}