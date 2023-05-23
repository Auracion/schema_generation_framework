import os, sys, json
import random
import argparse
from collections import defaultdict

sys.path.append(os.path.dirname(__file__))

from eval.spider.evaluation import Evaluator

random.seed(42)


def sample_by_hardness(mode, method):
    assert mode in ['train', 'dev'] and method in ['affected', 'imprevious']
    if not os.path.exists(f'gendata/hardness_{method}_{mode}_1.0'):
        os.mkdir(f'gendata/hardness_{method}_{mode}_1.0')
    ori_data = json.load(open(f'data/{mode}.json', 'r'))
    ori_tables = json.load(open('data/tables_with_tags.json', 'r'))
    all_gen = json.load(open(f'gendata/all_generated_{method}/{mode}.json', 'r'))
    gen_tables = json.load(open(f'gendata/all_generated_{method}/{mode}_tables.json', 'r'))
    data_out = f'gendata/hardness_{method}_{mode}_1.0/{mode}.json'
    table_out = f'gendata/hardness_{method}_{mode}_1.0/tables.json'

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
        qes2idx[question].append(idx)
        idx2hdn[idx] = hardness

    new_dataset, new_tables = [], {}
    gen_dist = {'easy': 0, 'medium': 0, 'hard': 0, 'extra': 0}
    residue = []
    for data in ori_data:
        question = data['question']
        hardness = data['hardness']
        if question in qes2idx:
            gen_idx = qes2idx[question]
            candidate, gen_hardness = [], []
            for i in gen_idx:
                hdn = idx2hdn[i]
                gen_hardness.append(hdn)
                if hdn == hardness:
                    candidate.append(i)
            if len(candidate) > 0:
                random.shuffle(candidate)
                db_id = all_gen[candidate[0]]['db_id']
                new_dataset.append(all_gen[candidate[0]])
                new_tables[db_id] = all_tables[db_id]
                gen_dist[hardness] += 1
            else:
                residue.append(question)
        else:
            db_id = data['db_id']
            new_dataset.append(data)
            new_tables[db_id] = all_tables[db_id]
            gen_dist[hardness] += 1

    diff_dist = {k: ori_dist[k] - gen_dist[k] for k in ori_dist}
    targets = list(diff_dist.keys())
    done = []
    for i in range(4):
        order = update_order(residue, qes2idx, idx2hdn, targets)
        residue = sort_generated(residue, qes2idx, idx2hdn, order + done)
        threshold = 0
        target = order[0]
        for qes in residue:
            if diff_dist[target] == 0:
                break
            gen_idx = qes2idx[qes]
            candidate = []
            for idx in gen_idx:
                hdn = idx2hdn[idx]
                if hdn == target:
                    candidate.append(idx)
            if len(candidate) == 0:
                break
            random.shuffle(candidate)
            data = all_gen[candidate[0]]
            db_id = data['db_id']
            new_dataset.append(data)
            new_tables[db_id] = all_tables[db_id]
            diff_dist[target] -= 1
            gen_dist[target] += 1
            threshold += 1
        done.append(target)
        residue = residue[threshold:]
        targets = order[1:]

    for qes in residue:
        gen_idx = qes2idx[qes]
        candidate = {'easy': [], 'medium': [], 'hard': [], 'extra': []}
        for idx in gen_idx:
            hdn = idx2hdn[idx]
            candidate[hdn].append(idx)
        if len(candidate['easy']) > 0:
            candidate = candidate['easy']
            hardness = 'easy'
        elif len(candidate['medium']) > 0:
            candidate = candidate['medium']
            hardness = 'medium'
        elif len(candidate['hard']) > 0:
            candidate = candidate['hard']
            hardness = 'hard'
        else:
            candidate = candidate['extra']
            hardness = 'extra'
        random.shuffle(candidate)
        data = all_gen[candidate[0]]
        db_id = data['db_id']
        new_dataset.append(data)
        new_tables[db_id] = all_tables[db_id]
        gen_dist[hardness] += 1

    print(ori_dist)
    print(gen_dist)

    json.dump(new_dataset, open(data_out, 'w'), indent=4)
    json.dump(list(new_tables.values()), open(table_out, 'w'), indent=4)


def update_order(residue, qes2idx, idx2hdn, targets):
    order = {hdn: 0 for hdn in targets}
    for qes in residue:
        gen_idx = qes2idx[qes]
        hardness = set([idx2hdn[i] for i in gen_idx])
        for hdn in hardness:
            if hdn in order:
                order[hdn] += 1
    order = sorted(order, key=lambda x: order[x])
    return order


def sort_generated(residue, qes2idx, idx2hdn, order):
    scores = []
    score_list = [8, 1, 2, 4]
    for qes in residue:
        gen_idx = qes2idx[qes]
        hardness = set([idx2hdn[i] for i in gen_idx])
        score = 0
        for hdn in hardness:
            if hdn in order:
                score += score_list[order.index(hdn)]
        scores.append((qes, score))
    sorted_qes = sorted(scores, key=lambda x: x[1], reverse=True)
    residue = [item[0] for item in sorted_qes]
    return residue


def sampling_for_augmentation(rate, method):
    assert method in ['affected', 'imprevious', 'mixed']
    if not os.path.exists(f'gendata/aug_{method}_{rate}'):
        os.mkdir(f'gendata/aug_{method}_{rate}')
    ori_data = json.load(open(f'data/train.json', 'r'))
    ori_tables = json.load(open('data/tables_with_tags.json', 'r'))
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
    data_out = f'gendata/aug_{method}_{rate}/train.json'
    table_out = f'gendata/aug_{method}_{rate}/tables.json'

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
            if len(candidate) > 0 and gen_dist[hardness] < ori_dist[hardness] * rate:
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
                if gen_dist[h] < ori_dist[h] * rate:
                    db_id = sample['db_id']
                    new_dataset.append(sample)
                    new_tables[db_id] = all_tables[db_id]
                    gen_dist[h] += 1
                    chosen.append(others[0])

        index_list = list(range(len(all_gen)))
        random.shuffle(index_list)
        for idx in index_list:
            if idx in chosen:
                continue
            data = all_gen[idx]
            hardness = data['hardness']
            if gen_dist[hardness] < ori_dist[hardness] * rate:
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


def sampling_with_minimal_schema(rate, method):
    assert method in ['affected', 'imprevious', 'mixed']
    if not os.path.exists(f'gendata_minschema/aug_{method}_{rate}'):
        os.mkdir(f'gendata_minschema/aug_{method}_{rate}')
    ori_data = json.load(open(f'data/train.json', 'r'))
    ori_tables = json.load(open('data/tables_with_tags.json', 'r'))
    data_out = f'gendata_minschema/aug_{method}_{rate}/train.json'
    table_out = f'gendata_minschema/aug_{method}_{rate}/tables.json'

    if method == 'mixed':
        aff_gen = json.load(open(f'gendata/all_generated_affected/train.json', 'r'))
        aff_tables = json.load(open(f'gendata/all_generated_affected/train_tables.json', 'r'))
        imp_gen = json.load(open(f'gendata/all_generated_imprevious/train.json', 'r'))
        imp_tables = json.load(open(f'gendata/all_generated_imprevious/train_tables.json', 'r'))

        gen_tables = {db['db_id']: db for db in aff_tables}
        imp_tables = {db['db_id']: db for db in imp_tables}
        gen_tables.update(imp_tables)
        all_src_db = [db['db_id'] for db in ori_tables]
        schema_data = defaultdict(dict)
        for data in aff_gen:
            db_id = data['db_id']
            src = db_id.split('[')[0][:-4]
            assert src in all_src_db
            if db_id not in schema_data[src]:
                schema_data[src][db_id] = {'affected': [], 'imprevious': []}
            schema_data[src][db_id]['affected'].append(data)
        for data in imp_gen:
            db_id = data['db_id']
            data['source'] = 'imprevious'
            src = db_id.split('[')[0][:-4]
            assert src in all_src_db
            if db_id not in schema_data[src]:
                schema_data[src][db_id] = {'affected': [], 'imprevious': []}
            schema_data[src][db_id]['imprevious'].append(data)

        train_db_id = set()
        for data in ori_data:
            db_id = data['db_id']
            if db_id == 'baseball_1':
                continue
            train_db_id.add(db_id)

        aug_data = []
        aug_tables = []
        chosen = []
        num_aff, num_imp, num_db = 0, 0, 0
        for db_id in train_db_id:
            order = sorted(schema_data[db_id], key=lambda x: len(schema_data[db_id][x]['affected']), reverse=True)[:10]
            _order = list(filter(lambda x: len(schema_data[db_id][x]['affected']) >= 20 and len(schema_data[db_id][x]['affected']) + len(schema_data[db_id][x]['imprevious']) >= 50, order))
            if len(_order) != 0:
                random.shuffle(_order)
                tgt = _order[0]
                extend = schema_data[db_id][tgt]['affected'] + schema_data[db_id][tgt]['imprevious']
                aug_data.extend(extend[:100])
                aug_tables.append(gen_tables[tgt])
                chosen.append(tgt)
                num_db += 1
                num_aff += min(len(schema_data[db_id][tgt]['affected']), 100)
                if len(schema_data[db_id][tgt]['affected']) < 100:
                    if len(schema_data[db_id][tgt]['affected']) + len(schema_data[db_id][tgt]['imprevious']) < 100:
                        num_imp += len(schema_data[db_id][tgt]['imprevious'])
                    else:
                        num_imp += 100 - len(schema_data[db_id][tgt]['affected'])


        if len(aug_data) <= int(rate * len(ori_data)):
            others = []
            for src in schema_data:
                order = sorted(schema_data[src], key=lambda x: len(schema_data[src][x]['affected']), reverse=True)
                for db_id in order:
                    if len(schema_data[src][db_id]['affected']) >= 20 and len(schema_data[src][db_id]['affected']) + len(schema_data[src][db_id]['imprevious']) >= 50 and db_id not in chosen:
                        others.append((src, db_id))
            # random.shuffle(others)
            for src, db_id in others:
                extend = schema_data[src][db_id]['affected'] + schema_data[src][db_id]['imprevious']
                aug_data.extend(extend[:100])
                aug_tables.append(db_id)
                num_db += 1
                num_aff += min(len(schema_data[src][db_id]['affected']), 100)
                if len(schema_data[src][db_id]['affected']) < 100:
                    if len(schema_data[src][db_id]['affected']) + len(schema_data[src][db_id]['imprevious']) < 100:
                        num_imp += len(schema_data[src][db_id]['imprevious'])
                    else:
                        num_imp += 100 - len(schema_data[src][db_id]['affected'])
                if len(aug_data) >= int(rate * len(ori_data)):
                    break
        else:
            random.shuffle(aug_data)
            aug_data = aug_data[:int(rate * len(ori_data))]
        print(len(aug_data))
        print(len(aug_tables))
        print(num_aff, num_imp, num_db)

    else:
        all_gen = json.load(open(f'gendata/all_generated_{method}/train.json', 'r'))
        gen_tables = json.load(open(f'gendata/all_generated_{method}/train_tables.json', 'r'))

        gen_tables = {db['db_id']: db for db in gen_tables}
        all_src_db = [db['db_id'] for db in ori_tables]
        schema_data = defaultdict(dict)
        for data in all_gen:
            db_id = data['db_id']
            src = db_id.split('[')[0][:-4]
            assert src in all_src_db
            if db_id not in schema_data[src]:
                schema_data[src][db_id] = []
            schema_data[src][db_id].append(data)

        train_db_id = set()
        for data in ori_data:
            db_id = data['db_id']
            if db_id == 'baseball_1':
                continue
            train_db_id.add(db_id)

        aug_data = []
        aug_tables = []
        chosen = []
        for db_id in train_db_id:
            order = sorted(schema_data[db_id], key=lambda x: len(schema_data[db_id][x]), reverse=True)[:10]
            _order = list(filter(lambda x: len(schema_data[db_id][x]) > 20, order))
            if len(_order) != 0:
                random.shuffle(_order)
                tgt = _order[0]
                aug_data.extend(schema_data[db_id][tgt])
                aug_tables.append(gen_tables[tgt])
                chosen.append(tgt)

        if len(aug_data) <= int(rate * len(ori_data)):
            others = []
            for src in schema_data:
                for db_id in schema_data[src]:
                    if len(schema_data[src][db_id]) > 20 and db_id not in chosen:
                        others.append((src, db_id))
            random.shuffle(others)
            for src, db_id in others:
                aug_data.extend(schema_data[src][db_id])
                aug_tables.append(db_id)
                if len(aug_data) >= int(rate * len(ori_data)):
                    break
        else:
            random.shuffle(aug_data)
            aug_data = aug_data[:int(rate * len(ori_data))]
        print(len(aug_data))
        print(len(aug_tables))










if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--mode', type=str, default='dev', help='train | dev', choices=['train', 'dev'])
    arg_parser.add_argument('--method', type=str, required=True, help='affected | imprevious | mixed', choices=['affected', 'imprevious', 'mixed'])
    arg_parser.add_argument('--augmentation', action='store_true', help='Do data augmentation if true')
    arg_parser.add_argument('--rate', type=float, default=1.0, help='rate of generation')
    args = arg_parser.parse_args()

    if args.augmentation:
        sampling_for_augmentation(args.rate, args.method)
    else:
        sample_by_hardness(args.mode, args.method)
    # sampling_with_minimal_schema(1.0, 'mixed')