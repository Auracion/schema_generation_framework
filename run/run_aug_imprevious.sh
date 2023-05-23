#!/bin/bash

data_path='data/train.json'
data_out='gendata/aug_imprevious_train/train.json'
table_out='gendata/aug_imprevious_train/tables.json'
proc_data='gendata/aug_imprevious_train/train.bin'
proc_table='gendata/aug_imprevious_train/tables.bin'
output_path='gendata/aug_imprevious_train/train.rgatsql.bin'
#vocab_glove='../text2sql-lgesql/pretrained_models/glove.42b.300d/vocab_glove.txt'
#vocab='../text2sql-lgesql/pretrained_models/glove.42b.300d/vocab.txt'

echo 'Generate imprevious data for train set augmentation'
python3 -u aug_system.py --data_path ${data_path} \
                         --table_path 'data/tables_with_tags.json' \
                         --data_out ${data_out} \
                         --table_out ${table_out} \
                         --num_steps 1 \

echo "Start to preprocess the original train dataset ..."
python3 -u preprocess/process_dataset.py --dataset_path ${data_out} \
                                         --raw_table_path ${table_out} \
                                         --table_path ${proc_table} \
                                         --output_path ${proc_data} \
                                         --skip_large \
                                         --db_content

#echo "Start to build word vocab for the dataset ..."
#python3 -u preprocess/build_glove_vocab.py --data_paths ${proc_data} \
#                                           --table_path ${proc_table} \
#                                           --reference_file ${vocab_glove} \
#                                           --mwf 4 \
#                                           --output_path ${vocab}

echo "Start to construct graphs for the dataset using rgat ..."
python3 -u preprocess/process_graphs.py --dataset_path ${proc_data} \
                                        --table_path ${proc_table} \
                                        --method 'rgatsql' \
                                        --output_path ${output_path}