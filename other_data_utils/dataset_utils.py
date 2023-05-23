import sys, os, json
from collections import defaultdict
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from eval.spider.evaluation import Evaluator


def collect_spider_without_old_dataset():
    ori_train = json.load(open('data/train.json', 'r'))
    ori_dev = json.load(open('data/dev.json', 'r'))

    dbs = ['academic', 'geo', 'imdb', 'restaurants', 'scholar', 'yelp']
    new_devs = defaultdict(list)
    new_train = []
    for data in ori_train:
        db_id = data['db_id']
        if db_id in dbs:
            new_devs[db_id].append(data)
        else:
            new_train.append(data)

    if not os.path.exists('data/without_old'):
        os.makedirs('data/without_old')
    json.dump(new_train, open('data/without_old/train.json', 'w'), indent=4)
    json.dump(ori_dev, open('data/without_old/dev.json', 'w'), indent=4)
    for domain in new_devs:
        dev = new_devs[domain]
        json.dump(dev, open(f'data/without_old/{domain}.json', 'w'), indent=4)


def select_from_aug(method):
    assert method in ['affected', 'imprevious', 'mixed']
    if not os.path.exists(f'gendata_wossp/aug_{method}_0.2'):
        os.mkdir(f'gendata_wossp/aug_{method}_0.2')
    ori_data = json.load(open(f'gendata_wossp/original/train.json', 'r'))
    ori_tables = json.load(open('gendata_wossp/original/tables.json', 'r'))
    if method == 'mixed':
        aff_gen = json.load(open(f'gendata/all_generated_affected/train.json', 'r'))
        aff_tables = json.load(open(f'gendata/all_generated_affected/train_tables.json', 'r'))
        imp_gen = json.load(open(f'gendata/all_generated_imprevious/train.json', 'r'))
        imp_tables = json.load(open(f'gendata/all_generated_imprevious/train_tables.json', 'r'))
        random.shuffle(imp_gen)
        all_gen = aff_gen + imp_gen[:len(aff_gen)]
        random.shuffle(all_gen)
        gen_tables = {db['db_id']: db for db in aff_tables}
        imp_tables = {db['db_id']: db for db in imp_tables}
        gen_tables.update(imp_tables)
        gen_tables = list(gen_tables.values())
    else:
        all_gen = json.load(open(f'gendata/all_generated_{method}/train.json', 'r'))
        gen_tables = json.load(open(f'gendata/all_generated_{method}/train_tables.json', 'r'))
    data_out = f'gendata_wossp/aug_{method}_0.2/train.json'
    table_out = f'gendata_wossp/aug_{method}_0.2/tables.json'

    all_tables = {db['db_id']: db for db in ori_tables + gen_tables}
    evaluator = Evaluator()
    ori_dist = {'easy': 0, 'medium': 0, 'hard': 0, 'extra': 0}
    for data in ori_data:
        sql = data['sql']
        hardness = evaluator.eval_hardness(sql)
        data['hardness'] = hardness
        ori_dist[hardness] += 1

    qes2idx = defaultdict(list)
    idx2hdn = {}
    for idx, data in enumerate(all_gen):
        question = data['question']
        sql = data['sql']
        hardness = evaluator.eval_hardness(sql)
        data['hardness'] = hardness
        qes2idx[question].append(idx)
        idx2hdn[idx] = hardness

    new_dataset, new_tables = [], {}
    gen_dist = {'easy': 0, 'medium': 0, 'hard': 0, 'extra': 0}
    chosen = []
    for data in ori_data:
        question = data['question']
        hardness = data['hardness']
        if question in qes2idx:
            gen_idx = qes2idx[question]
            candidate, others = [], []
            for i in gen_idx:
                hdn = idx2hdn[i]
                if hdn == hardness:
                    candidate.append(i)
                else:
                    others.append(i)
            if len(candidate) > 0 and gen_dist[hardness] < ori_dist[hardness] * 0.2:
                random.shuffle(candidate)
                db_id = all_gen[candidate[0]]['db_id']
                new_dataset.append(all_gen[candidate[0]])
                new_tables[db_id] = all_tables[db_id]
                gen_dist[hardness] += 1
                chosen.append(candidate[0])
            elif len(others) > 0:
                random.shuffle(others)
                sample = all_gen[others[0]]
                h = sample['hardness']
                if gen_dist[h] < ori_dist[h] * 0.2:
                    db_id = sample['db_id']
                    new_dataset.append(sample)
                    new_tables[db_id] = all_tables[db_id]
                    gen_dist[h] += 1
                    chosen.append(others[0])

        index_list = list(range(len(all_gen)))
        random.shuffle(index_list)
        for idx in index_list:
            data = all_gen[idx]
            db_id = data['db_id']
            if idx in chosen or \
                    db_id.startswith('academic_') or \
                    db_id.startswith('geo_') or \
                    db_id.startswith('imdb_') or \
                    db_id.startswith('restaurants_') or \
                    db_id.startswith('scholar_') or \
                    db_id.startswith('yelp'):
                continue
            hardness = data['hardness']
            if gen_dist[hardness] < ori_dist[hardness] * 0.2:
                db_id = data['db_id']
                gen_dist[hardness] += 1
                new_dataset.append(data)
                new_tables[db_id] = all_tables[db_id]

    print(f"Totally generate {len(new_dataset)} data and {len(new_tables)} databases")
    print(ori_dist)
    print(gen_dist)

    new_dataset = ori_data + new_dataset
    ori_tables = {db["db_id"]: db for db in ori_tables}
    new_tables.update(ori_tables)
    json.dump(new_dataset, open(data_out, 'w'), indent=4)
    json.dump(list(new_tables.values()), open(table_out, 'w'), indent=4)


def gen_distribution(method):
    ori_data = json.load(open(f'data/train.json', 'r'))
    all_gen = json.load(open(f'gendata/all_generated_{method}/train.json', 'r'))
    dist = {}
    for data in ori_data:
        db_id = data['db_id']
        dist[db_id] = dist.get(db_id, 0) + 1
    for db_id in dist:
        print(db_id, dist[db_id])



if __name__ == '__main__':
    # collect_spider_without_old_dataset()
    select_from_aug('affected')
    select_from_aug('imprevious')
    select_from_aug('mixed')
    # ori_dev = json.load(open('data/dev.json', 'r'))
    # statistics = {}
    # for data in ori_dev:
    #     db_id = data['db_id']
    #     statistics[db_id] = statistics.get(db_id, 0) + 1
    #
    # for domain in statistics:
    #     print(domain, statistics[domain])
    # gen_distribution('affected')