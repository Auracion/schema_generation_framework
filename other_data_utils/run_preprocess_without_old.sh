#!/bin/bash

root='gendata_wossp/original/'
train_data='gendata_wossp/original/train.json'
dev_data='gendata_wossp/original/dev.json'
table_data='gendata_wossp/original/tables.json'
train_rgat_out='gendata_wossp/original/train.rgatsql.bin'
dev_rgat_out='gendata_wossp/original/dev.rgatsql.bin'
train_lge_out='gendata_wossp/original/train.lgesql.bin'
dev_lge_out='gendata_wossp/original/dev.lgesql.bin'
table_out='gendata_wossp/original/tables.bin'

#echo "Start to preprocess the original train dataset ..."
#python3 -u preprocess/process_dataset.py --dataset_path ${train_data} --raw_table_path ${table_data} --table_path ${table_out} --output_path 'gendata_wossp/original/train.bin' --skip_large --db_content #--verbose > train.log
#echo "Start to construct graphs for the train dataset with method rgat ..."
#python3 -u preprocess/process_graphs.py --dataset_path 'gendata_wossp/original/train.bin' --table_path ${table_out} --method 'rgatsql' --output_path ${train_rgat_out}
#echo "Start to construct graphs for the train dataset with method lge ..."
#python3 -u preprocess/process_graphs.py --dataset_path 'gendata_wossp/original/train.bin' --table_path ${table_out} --method 'lgesql' --output_path ${train_lge_out}

for data in 'dev' 'academic' 'geo' 'imdb' 'restaurants' 'scholar' 'yelp'
do
  echo "Start to preprocess the original '${data}' dataset ..."#python3 -u preprocess/process_dataset.py --dataset_path ${dev_data} --table_path ${table_out} --output_path 'data/dev.bin' #--verbose > dev.log
  python3 -u preprocess/process_dataset.py --dataset_path ${root}${data}'.json' --table_path ${table_out} --output_path ${root}${data}'.bin' --db_content #--verbose > dev.log
  echo "Start to construct graphs for the '${data}' dataset with method rgat ..."
  python3 -u preprocess/process_graphs.py --dataset_path ${root}${data}'.bin' --table_path ${table_out} --method 'rgatsql' --output_path ${root}${data}'.rgatsql.bin'
  echo "Start to construct graphs for the '${data}' dataset with method lge ..."
  python3 -u preprocess/process_graphs.py --dataset_path ${root}${data}'.bin' --table_path ${table_out} --method 'lgesql' --output_path ${root}${data}'.lgesql.bin'
done


