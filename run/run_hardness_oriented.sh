#!/bin/bash

mode=$1
method=$2

data_out='gendata/hardness_'${method}'_'${mode}'_1.0/'${mode}'.json'
table_out='gendata/hardness_'${method}'_'${mode}'_1.0/tables.json'
proc_data='gendata/hardness_'${method}'_'${mode}'_1.0/'${mode}'.bin'
proc_table='gendata/hardness_'${method}'_'${mode}'_1.0/tables.bin'
output_path='gendata/hardness_'${method}'_'${mode}'_1.0/'${mode}'.rgatsql.bin'
lge_output_path='gendata/hardness_'${method}'_'${mode}'_1.0/'${mode}'.lgesql.bin'


#echo 'Generate hardness oriented '${method}' data for '${mode}' set'
#python3 -u sampling_system.py --mode ${mode} --method ${method}

echo "Start to preprocess the original '${mode}' dataset ..."
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

echo "Start to construct graphs for the dataset using lge ..."
python3 -u preprocess/process_graphs.py --dataset_path ${proc_data} \
                                        --table_path ${proc_table} \
                                        --method 'lgesql' \
                                        --output_path ${lge_output_path}





