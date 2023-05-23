#!/bin/bash

dataset=$1

data_path='data/'${dataset}'.json'
data_out='gendata/eq_imprevious_'${dataset}'/'${dataset}'.json'
table_out='gendata/eq_imprevious_'${dataset}'/tables.json'
proc_data='gendata/eq_imprevious_'${dataset}'/'${dataset}'.bin'
proc_table='gendata/eq_imprevious_'${dataset}'/tables.bin'
output_path='gendata/eq_imprevious_'${dataset}'/'${dataset}'.rgatsql.bin'
lge_output_path='gendata/eq_imprevious_'${dataset}'/'${dataset}'.lgesql.bin'
#vocab_glove='../text2sql-lgesql/pretrained_models/glove.42b.300d/vocab_glove.txt'
#vocab='../text2sql-lgesql/pretrained_models/glove.42b.300d/vocab.txt'

echo 'Generate imprevious data for '${dataset}' set'
python3 -u aug_system.py --data_path ${data_path} \
                         --table_path 'data/tables_with_tags.json' \
                         --data_out ${data_out} \
                         --table_out ${table_out} \
                         --num_steps 1 \
                         --keep_original \
                         --only_aug

echo "Start to preprocess the original '${dataset}' dataset ..."
python3 -u preprocess/process_dataset.py --dataset_path ${data_out} \
                                         --raw_table_path ${table_out} \
                                         --table_path ${proc_table} \
                                         --output_path ${proc_data} \
                                         --skip_large

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
