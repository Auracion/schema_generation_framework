#!/bin/bash

train_path='data/train.json'
dev_path='data/dev.json'
table_path='data/tables_with_tags.json'
affected_root='gendata/all_generated_affected'
imprevious_root='gendata/all_generated_imprevious'

mkdir -p ${affected_root}
mkdir -p ${imprevious_root}

echo 'Generate all affected data for train set'
python3 -u aug_system.py --data_path ${train_path} \
                         --table_path ${table_path} \
                         --data_out ${affected_root}'/train.json' \
                         --table_out ${affected_root}'/train_tables.json' \
                         --num_steps 1 \
                         --affected \
                         --gen_all

echo 'Generate all affected data for dev set'
python3 -u aug_system.py --data_path ${dev_path} \
                         --table_path ${table_path} \
                         --data_out ${affected_root}'/dev.json' \
                         --table_out ${affected_root}'/dev_tables.json' \
                         --num_steps 1 \
                         --affected \
                         --gen_all

echo 'Generate all imprevious data for train set'
python3 -u aug_system.py --data_path ${train_path} \
                         --table_path ${table_path} \
                         --data_out ${imprevious_root}'/train.json' \
                         --table_out ${imprevious_root}'/train_tables.json' \
                         --num_steps 1 \
                         --gen_all

echo 'Generate all imprevious data for dev set'
python3 -u aug_system.py --data_path ${dev_path} \
                         --table_path ${table_path} \
                         --data_out ${imprevious_root}'/dev.json' \
                         --table_out ${imprevious_root}'/dev_tables.json' \
                         --num_steps 1 \
                         --gen_all