import os, sys, json, random
from copy import deepcopy
import argparse
from tqdm import tqdm
import gc

sys.path.append(os.path.dirname(__file__))

from database_utils import Column, Table, Value, Database
from sql_parser import SQLEncoder
from sql_unparser import SQLDecoder, Unparser
from entity_relation_graph import ERG, update_schema_linking
from qualifier import *
from eval.spider.process_sql import get_sql

random.seed(42)


class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, db):
        self.db = db
        self._schema = get_schema(db)
        # self._idMap = self._map(self._schema)
        self._idMap = {}

        for tid, tab in enumerate(self.db["table_names_original"]):
            self._idMap[tab.lower()] = tid

        for cid, col in enumerate(self.db["column_names_original"]):
            if cid == 0:
                self._idMap["*"] = 0
            else:
                tab = self.db["table_names_original"][col[0]]
                self._idMap[tab.lower() + '.' + col[1].lower()] = cid

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema):
        idMap = {'*': "__all__"}
        id = 1
        for key, vals in schema.items():
            for val in vals:
                idMap[key.lower() + "." + val.lower()] = "__" + key.lower() + "." + val.lower() + "__"
                id += 1

        for key in schema:
            idMap[key.lower()] = "__" + key.lower() + "__"
            id += 1

        return idMap


def get_schema(db):
    """
    Get database's schema, which is a dict with table name as key
    and list of column names as value
    :param db: database path
    :return: schema dict
    """

    schema = defaultdict(list)

    for col in db["column_names_original"]:
        if col[0] == -1:
            continue
        table = db["table_names_original"][col[0]].lower()
        schema[table].append(col[1].lower())

    return schema


class Generator:

    BANQES = ["What are the lot details of lots associated with transactions with share count smaller than 50?",
              'What are the lot details of lots associated with transactions whose share count is bigger than 100 and whose type code is "PUR"?']

    def __init__(self, n_step=1, affected=True, keep_original=False):
        self.n_step = n_step
        self.affected = affected
        self.keep_original = keep_original
        self.statistic = {"e2a": 0, "c2a": 0, "r2u": 0, "u2r": 0}
        self.seed = {"e2a": 0, "c2a": 0, "r2u": 0, "u2r": 0, "total": 0}

    def generate_one_step(self, erg: ERG, steps):
        methods = ["e2a", "c2a", "r2u", "u2r"]
        random.shuffle(methods)
        filters = {"e2a": filter_for_e2a,
                   "c2a": filter_for_c2a,
                   "r2u": filter_for_r2u,
                   "u2r": filter_for_u2r}
        transformers = {"e2a": erg.convert_entity_to_attribute,
                        "c2a": erg.convert_concept_to_attribute,
                        "r2u": erg.convert_relation_to_unk,
                        "u2r": erg.convert_unk_to_relation}
        for method in methods:
            if len(steps) > 0 and method == "c2a" and steps[-1].split('[')[0] == "c2a":
                continue
            whitelist = filters[method](erg, self.affected)
            if len(whitelist) > 0:
                transformers[method](*whitelist[0])
                update_schema_linking(erg)
                self.statistic[method] += 1
                return method + f"[{','.join(whitelist[0])}]"
        return None

    def generate_single(self, db: Database, data):
        if data["question"] in self.BANQES:
            return None
        steps = []
        sql = deepcopy(data["sql"])
        encoder = SQLEncoder(db)
        for i in range(self.n_step):
            acdb, values = encoder.encode(sql)
            erg = ERG(acdb, values)
            method = self.generate_one_step(erg, steps)
            if method is None:
                break
            else:
                steps.append(method)
            decoder = SQLDecoder(erg.acdb, values)
            ast = decoder.tree
            unparser = Unparser(ast)
            new_query = unparser.get_face_code()

            new_db = acdb.to_dict()
            schema = Schema(new_db)
            sql = get_sql(schema, new_query)
            encoder = SQLEncoder(Database(new_db))

            new_data = {
                "question": data["question"],
                "question_toks": data["question_toks"],
                "query": new_query,
                "query_toks": new_query.split(" "),
                "query_toks_no_value": new_query.split(" "),
                "sql": sql
            }
        if len(steps) > 0:
            return new_data, new_db, steps
        else:
            return None

    def generate(self, dbs, dataset):
        aug_dataset = []
        aug_dbs = {db_id: {} for db_id in dbs}
        aug_databases = []
        for data in dataset:
            # if data["question"] != 'Find the name of product that is produced by both companies Creative Labs and Sony.':
            #     continue
            db_id = data["db_id"]
            if db_id == "baseball_1":
                continue
            db = Database(dbs[db_id])
            res = self.generate_single(db, data)
            if res is None:
                if self.keep_original:
                    aug_dataset.append(data)
                    aug_databases.append(db.to_dict())
            else:
                new_data, new_db, steps = res
                steps = '|'.join(steps)
                if steps in aug_dbs[db_id]:
                    new_data["db_id"] = db_id + '_' + str(list(aug_dbs[db_id].keys()).index(steps))
                else:
                    new_data["db_id"] = db_id + '_' + str(len(aug_dbs[db_id]))
                    aug_dbs[db_id][steps] = new_db
                aug_dataset.append(new_data)
            gc.collect()

        for db_id in aug_dbs:
            for i, db in enumerate(aug_dbs[db_id]):
                aug_dbs[db_id][db]["db_id"] += '_' + str(i)
                aug_databases.append(aug_dbs[db_id][db])
        return aug_dataset, aug_databases

    def generate_all(self, dbs, dataset):
        filters = {"e2a": filter_for_e2a,
                   "c2a": filter_for_c2a,
                   "r2u": filter_for_r2u,
                   "u2r": filter_for_u2r}
        aug_dataset = []
        aug_db_ids = []
        aug_databases = []
        for data in dataset:
            db_id = data["db_id"]
            if db_id == "baseball_1":
                continue
            db = Database(dbs[db_id])
            if data["question"] in self.BANQES:
                continue
            else:
                sql = data["sql"]
                ori_encoder = SQLEncoder(db.copy())
                ori_acdb, ori_values = ori_encoder.encode(sql)
                ori_erg = ERG(ori_acdb, ori_values)
                candidates = []
                for method in ["e2a", "c2a", "r2u", "u2r"]:
                    whitelist = filters[method](ori_erg, self.affected)
                    for elements in whitelist:
                        candidates.append([method, *elements])
                flags = {"e2a": False, "c2a": False, "r2u": False, "u2r": False, "total": False}
                for trans in candidates:
                    method, *elements = trans
                    sql = data["sql"]
                    encoder = SQLEncoder(db.copy())
                    acdb, values = encoder.encode(sql)
                    erg = ERG(acdb, values)
                    if method == "e2a":
                        erg.convert_entity_to_attribute(*elements)
                    elif method == "c2a":
                        erg.convert_concept_to_attribute(*elements)
                    elif method == "r2u":
                        erg.convert_relation_to_unk(*elements)
                    elif method == "u2r":
                        erg.convert_unk_to_relation(*elements)
                    self.statistic[method] += 1
                    flags[method] = True
                    flags["total"] = True
                    update_schema_linking(erg)
                    decoder = SQLDecoder(erg.acdb, values)
                    ast = decoder.tree
                    unparser = Unparser(ast)
                    new_query = unparser.get_face_code()

                    new_db = acdb.to_dict()
                    schema = Schema(new_db)
                    new_sql = get_sql(schema, new_query)

                    gen_log = method + f"[{','.join(elements)}]"
                    new_data = {
                        "question": data["question"],
                        "question_toks": data["question_toks"],
                        "query": new_query,
                        "query_toks": new_query.split(" "),
                        "query_toks_no_value": new_query.split(" "),
                        "sql": new_sql,
                        "gen_log": gen_log
                    }
                    new_db_id = db_id + '_' + gen_log
                    new_data["db_id"] = new_db_id
                    if new_db_id not in aug_db_ids:
                        new_db["db_id"] = new_db_id
                        aug_db_ids.append(new_db_id)
                        aug_databases.append(new_db)
                    aug_dataset.append(new_data)
                for k in self.seed:
                    self.seed[k] += flags[k]
            gc.collect()

        return aug_dataset, aug_databases


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--data_path', type=str, required=True, help='dataset path')
    arg_parser.add_argument('--table_path', type=str, required=True, help='table path')
    arg_parser.add_argument('--data_out', type=str, required=True, help='output dataset path')
    arg_parser.add_argument('--table_out', type=str, required=True, help='output table path')
    arg_parser.add_argument('--num_steps', type=int, default=1, help='number of transformation steps')
    arg_parser.add_argument('--affected', action='store_true', help='whether generate affected data')
    arg_parser.add_argument('--keep_original', action='store_true', help='whether keep original data')
    arg_parser.add_argument('--only_aug', action='store_true', help='only store augment data')
    arg_parser.add_argument('--gen_all', action='store_true', help='whether generate all possible data')
    args = arg_parser.parse_args()

    generator = Generator(n_step=args.num_steps, affected=args.affected, keep_original=args.keep_original)
    dbs = json.load(open(args.table_path, 'r'))
    dbs = {db["db_id"]: db for db in dbs}
    dataset = json.load(open(args.data_path, 'r'))
    if args.gen_all:
        aug_dataset, aug_databases = generator.generate_all(dbs, dataset)
        args.only_aug = True
    else:
        aug_dataset, aug_databases = generator.generate(dbs, dataset)
    print(f"Totally generate {len(aug_dataset)} data and {len(aug_databases)} databases")
    print(generator.statistic)
    if args.gen_all:
        print(generator.seed)

    if not args.only_aug:
        aug_dataset += dataset
        all_db_id = [db['db_id'] for db in aug_databases]
        for db_id in dbs:
            if db_id not in all_db_id:
                aug_databases.append(dbs[db_id])

    json.dump(aug_dataset, open(args.data_out, 'w'), indent=4)
    json.dump(aug_databases, open(args.table_out, 'w'), indent=4)
