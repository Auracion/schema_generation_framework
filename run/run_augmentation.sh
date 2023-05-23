#!/bin/bash

rate=$1
method=$2


data_out='gendata/aug_'${method}'_'${rate}'/train.json'
table_out='gendata/aug_'${method}'_'${rate}'/tables.json'
proc_data='gendata/aug_'${method}'_'${rate}'/train.bin'
proc_table='gendata/aug_'${method}'_'${rate}'/tables.bin'
output_path='gendata/aug_'${method}'_'${rate}'/train.rgatsql.bin'
lge_output_path='gendata/aug_'${method}'_'${rate}'/train.lgesql.bin'


echo 'Generate hardness oriented '${method}' data for train set with '${rate}' times generated data'
python3 -u sampling_system.py --rate ${rate} --method ${method} --augmentation

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

#echo "Start to construct graphs for the dataset using lge ..."
#python3 -u preprocess/process_graphs.py --dataset_path ${proc_data} \
#                                        --table_path ${proc_table} \
#                                        --method 'lgesql' \
#                                        --output_path ${lge_output_path}