import sys, os, json
import sqlite3
from collections import defaultdict
# import stanza

sys.path.append(os.path.dirname(__file__))

from eval.spider.process_sql import get_sql


class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, db):
        self.db = db
        self._schema = get_schema(db)
        self._idMap = self._map(self._schema)
        # self._idMap = {}
        #
        # for tid, tab in enumerate(self.db["table_names_original"]):
        #     self._idMap[tab.lower()] = tid
        #
        # for cid, col in enumerate(self.db["column_names_original"]):
        #     if cid == 0:
        #         self._idMap["*"] = 0
        #     else:
        #         tab = self.db["table_names_original"][col[0]]
        #         self._idMap[tab.lower() + '.' + col[1].lower()] = cid

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


def create_raw_annotation_json():
    dbs = json.load(open('data/tables.json', 'r'))
    dbs = {db['db_id']: db for db in dbs}

    annotations = {}
    for db_id in dbs:
        db = dbs[db_id]
        db_tables = db["table_names_original"]
        annos = {tab: None for tab in db_tables}
        annotations[db_id] = annos

    json.dump(annotations, open('data/annotations.json', 'w'), indent=4)


def create_agent_finding_json():
    dbs = json.load(open('data/tables.json', 'r'))
    dbs = {db['db_id']: db for db in dbs}
    annotations = json.load(open('data/annotations.json', 'r'))

    new_data = []
    for db_id in dbs:
        db = dbs[db_id]
        annot = annotations[db_id]
        for i, table in enumerate(db["table_names"]):
            if annot[db['table_names_original'][i]] != "Entity":
                continue
            question = f"Show me all the {table} ."
            query = f"SELECT * FROM {db['table_names_original'][i]}"
            sql = {
            "except": None,
            "from": {
                "conds": [],
                "table_units": []
            },
            "groupBy": [],
            "having": [],
            "intersect": None,
            "limit": None,
            "orderBy": [],
            "select": [
                False,
                []
            ],
            "union": None,
            "where": []
            }
            new_data.append({
                "db_id": db_id,
                "question": question,
                "question_toks": question.split(' '),
                "query": query,
                "query_toks": query.split(" "),
                "query_toks_no_value": query.split(" "),
                "sql": sql
            })

    print(len(new_data))
    json.dump(new_data, open('data/agents.json', 'w'), indent=4)


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def restore_cell_values():
    dbs = json.load(open('data/tables.json', 'r'))
    dbs_wcv = []
    for db in dbs:
        db_id = db["db_id"]
        db_file = os.path.join('./data/database', db_id, db_id + '.sqlite')
        db["cell_values"] = [[] for _ in range(len(db['column_names_original']))]
        if not os.path.exists(db_file):
            raise ValueError('[ERROR]: database file %s not found ...' % (db_file))
        conn = sqlite3.connect(db_file)
        conn.text_factory = lambda b: b.decode(errors='ignore')
        conn.execute('pragma foreign_keys=ON')
        for i, (tab_id, col_name) in enumerate(db['column_names_original']):
            if i == 0 or 'id' in db['column_names'][i][1].lower().split(' '):  # ignore * and special token 'id'
                db["cell_values"][i] = []
                continue
            tab_name = db['table_names_original'][tab_id]
            try:
                cursor = conn.execute("SELECT DISTINCT \"%s\" FROM \"%s\";" % (col_name, tab_name))
                cell_values = cursor.fetchall()
                cell_values = [str(each[0]) for each in cell_values]
                cell_values = [[str(float(each))] if is_number(each) else each.lower().split() for each in cell_values]
                db["cell_values"][i] = cell_values
            except Exception as e:
                print(e)
            dbs_wcv.append(db)
        conn.close()

    json.dump(dbs_wcv, open('data/tables_with_cell_values.json', 'w'), indent=4)


def merge_annotations():
    dbs = json.load(open('data/tables_with_cell_values.json', 'r'))
    dbs = {db['db_id']: db for db in dbs}
    annotations = json.load(open('data/annotations.json', 'r'))
    for db_id in dbs:
        db = dbs[db_id]
        annots = annotations[db_id]
        db["table_type"] = []
        for tab in db["table_names_original"]:
            db["table_type"].append(annots[tab])

    dbs = list(dbs.values())
    json.dump(dbs, open('data/tables_with_annots.json', 'w'), indent=4)


def get_new_concept(table, processor):
    tag_vots = {}
    for column in table.agents:
        if column.dtype != 'text':
            continue
        for value in column.cell_values[:10]:
            if value is None:
                continue
            doc = processor(' '.join(value))
            for sent in doc.sentences:
                for token in sent.tokens:
                    if token.ner != "O":
                        tag = token.ner.split('-')[-1]
                        tag_vots[tag] = tag_vots.get(tag, 0) + 1

    if len(tag_vots) == 0:
        return None
    else:
        sorted(tag_vots, key=lambda x: tag_vots[x], reverse=True)
        tag = list(tag_vots.keys())[-1]
        return tag
        # if tag == "PERSON":
        #     new_concept = "people"
        #     concept_column = "identity"
        # elif tag == "ORG":
        #     new_concept = "organization"
        #     concept_column = "type"
        # elif tag == "GPE":
        #     new_concept = "location"
        #     concept_column = "level"
        # else:
        #     return None
        #
        # return new_concept, concept_column


def table_tagging():
    from database_utils import Database
    processor = stanza.Pipeline(lang="en", processors="tokenize,ner")
    dbs = json.load(open('data/tables_with_annots.json', 'r'))
    dbs = {db["db_id"]: db for db in dbs}
    databases = {db_id: Database(dbs[db_id]) for db_id in dbs}
    for db_id in databases:
        db = databases[db_id]
        dbs[db_id]["tag"] = [None for _ in range(len(db.tables))]
        for table in db.tables:
            tag = get_new_concept(table, processor)
            tid = table.tid
            dbs[db_id]["tag"][tid] = tag

    dbs = list(dbs.values())
    json.dump(dbs, open('data/tables_with_tags.json', 'w'), indent=4)


def correct_primary_keys():
    import sqlite3

    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: db for db in dbs}

    for db_id in dbs:
        # print(os.path.join('./data', db_id, db_id + '.sqlite'))
        db = dbs[db_id]
        conn = sqlite3.connect(os.path.join('./data/database', db_id, db_id + '.sqlite'))
        cursor = conn.cursor()
        columns = [f"{db['table_names_original'][c[0]]}.{c[1]}" for c in db["column_names_original"]]
        pkeys = []
        for table in db["table_names_original"]:
            cursor.execute(f"PRAGMA table_info({table})")
            res = cursor.fetchall()
            for value in res:
                cid = columns.index(f"{table}.{value[1]}")
                if value[5] != 0:
                    pkeys.append(cid)
        db["primary_keys"] = sorted(pkeys)

    dbs = list(dbs.values())
    json.dump(dbs, open('data/tables_with_tags.json', 'w'), indent=4)


def correct_foreign_keys():
    import sqlite3

    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: db for db in dbs}

    for db_id in dbs:
        if db_id == 'baseball_1':
            continue
        # print(os.path.join('./data', db_id, db_id + '.sqlite'))
        db = dbs[db_id]
        conn = sqlite3.connect(os.path.join('./data/database', db_id, db_id + '.sqlite'))
        cursor = conn.cursor()
        columns = [f"{db['table_names_original'][c[0]]}.{c[1]}".lower() for c in db["column_names_original"]]
        fkeys = []
        for table in db["table_names_original"]:
            cursor.execute(f"PRAGMA foreign_key_list({table})")
            res = cursor.fetchall()
            for value in res:
                pk = f"{value[2]}.{value[4]}".lower()
                pid = columns.index(pk)
                fk = f"{table}.{value[3]}".lower()
                fid = columns.index(fk)
                fkeys.append([pid, fid])
        db["foreign_keys"] = fkeys

    dbs = list(dbs.values())
    json.dump(dbs, open('data/tables_with_tags.json', 'w'), indent=4)


def sample_fks_from_sql():
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: db for db in dbs}

    trains = json.load(open('data/train.json', 'r'))
    devs = json.load(open('data/dev.json', 'r'))
    data = trains + devs

    for example in data:
        db_id = example['db_id']
        if db_id == 'baseball_1':
            continue
        db = dbs[db_id]
        sql = example['sql']
        on_clause = sql['from']['conds']
        for i, cond in enumerate(on_clause):
            if i % 2 == 1:
                continue
            cid1 = cond[2][1][1]
            cid2 = cond[3][1]
            # if cid1 in db["primary_keys"] and cid2 in db["primary_keys"]:
            #     print("Both Primary Key in ", db_id)
            #     print(db["column_names_original"][cid1][1], cid1)
            #     print(db["column_names_original"][cid2][1], cid2)
            #     print()
            if cid1 not in db["primary_keys"] and cid2 not in db["primary_keys"]:
                print("Neither Primary Key in ", db_id)
                print(db["table_names_original"][db["column_names_original"][cid1][0]], db["column_names_original"][cid1][1])
                print(db["table_names_original"][db["column_names_original"][cid2][0]], db["column_names_original"][cid2][1])
                print()


def edit_table_type():
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: db for db in dbs}

    db = dbs['shop_membership']
    tid = db["table_names_original"].index('purchase')
    db["table_type"][tid] = "Entity"

    json.dump(list(dbs.values()), open('data/tables_with_tags.json', 'w'), indent=4)

    num_relation, total = 0, 0
    for db_id in dbs:
        db = dbs[db_id]
        total += len(db["table_names_original"])
        num_relation += sum([t == "Relation" for t in db["table_type"]])

    print(num_relation/total)


def correct_sample(dataset, dbs, db_id, query, new_query):
    for data in dataset:
        if data["db_id"] != db_id:
            continue
        if data["query"] == query:
            print(data["question"])
            print(query)
            print(new_query)
            print()
            schema = Schema(dbs[db_id])
            data["query"] = new_query
            data["query_toks"] = new_query.split(" ")
            data["query_toks_no_value"] = new_query.split(" ")
            data["sql"] = get_sql(schema, new_query)


def correct_dataset():
    trains = json.load(open('data/train.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: db for db in dbs}
    with open('correct_sql.txt', 'r') as f:
        for line in f:
            db_id, query, new_query = line.strip('\n').split(' <=> ')
            correct_sample(trains, dbs, db_id, query, new_query)

    json.dump(trains, open('data/new_train.json', 'w'), indent=4)


def check_schema():
    from sql_parser import SQLEncoder
    from sql_unparser import SQLDecoder, Unparser
    from database_utils import Database
    from entity_relation_graph import ERG
    import tempfile
    from eval.spider.evaluation import build_foreign_key_map_from_json, evaluate

    dataset = json.load(open('data/dev_aug.json', 'r'))
    dbs = json.load(open('data/table_aug.json', 'r'))
    dbs = {db["db_id"]: db for db in dbs}

    golden, preds = [], []
    for data in dataset:
        db_id = data['db_id']
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        sql = data['sql']
        db = dbs[db_id]

        encoder = SQLEncoder(Database(db.copy()))
        acdb, values = encoder.encode(sql)
        erg = ERG(acdb, values)
        decoder = SQLDecoder(erg.acdb, values)
        ast = decoder.tree
        unparser = Unparser(ast)
        new_query = unparser.get_face_code()
        golden.append(query)
        preds.append(new_query)

    kmaps = build_foreign_key_map_from_json('data/table_aug.json')
    schemas = {db_id: Schema(dbs[db_id]) for db_id in dbs}
    with open('data/golden', 'w') as f:
        for gold in golden:
            f.write(gold + '\n')
    with open('data/preds', 'w') as f:
        for pred in preds:
            f.write(pred + '\n')
    all_exact_acc = evaluate('data/golden', 'data/preds', 'data/database', 'match', kmaps, schemas=schemas)[1]

    print(all_exact_acc)


def construct_dbqa():
    root = 'data/kaggle-dbqa'
    db_path = os.path.join(root, 'databases')
    ex_path = os.path.join(root, 'examples')
    tab_path = os.path.join(root, 'tables.json')
    db_names = []
    for _, dirs, _ in os.walk(db_path):
        db_names = dirs
        break
    trainset = []
    devset = []
    for name in db_names:
        train = json.load(open(os.path.join(ex_path, name + '_fewshot.json'), 'r'))
        dev = json.load(open(os.path.join(ex_path, name + '_test.json'), 'r'))
        trainset += train
        devset += dev
    tables = json.load(open(tab_path, 'r'))
    for table in tables:
        table["column_names"] = table["column_names_manually_normalized_alternative"]
    json.dump(tables, open(tab_path, 'w'), indent=4)
    json.dump(trainset, open(os.path.join(root, 'train.json'), 'w'), indent=4)
    json.dump(devset, open(os.path.join(root, 'dev.json'), 'w'), indent=4)


def correct_value():
    tables = json.load(open(f'gendata/ets_affected_train/tables.json', 'r'))
    for did, db in enumerate(tables):
        cell_values = db['cell_values']
        for i, col_val in enumerate(cell_values):
            if col_val is None:
                tables[did]['cell_values'][i] = []
                continue
            for j, value in enumerate(col_val):
                for k, token in enumerate(value):
                    tables[did]['cell_values'][i][j][k] = token.replace('\"', '')
    json.dump(tables, open(f'gendata/ets_affected_train/tables.json', 'w'), indent=4)


def split_method():
    if not os.path.exists('gendata/methods'):
        os.mkdir('gendata/methods')
        os.mkdir('gendata/methods/e2a')
        os.mkdir('gendata/methods/c2a')
        os.mkdir('gendata/methods/u2r')
        os.mkdir('gendata/methods/r2u')

    all_data = json.load(open('gendata/all_generated_affected/dev.json', 'r'))
    all_tables = json.load(open('gendata/all_generated_affected/dev_tables.json', 'r'))
    e2a, c2a, u2r, r2u = [], [], [], []
    e2a_tab, c2a_tab, u2r_tab, r2u_tab = [], [], [], []
    all_tables = {db['db_id']: db for db in all_tables}
    for data in all_data:
        db_id = data['db_id']
        if 'e2a' in db_id:
            e2a.append(data)
            e2a_tab.append(all_tables[db_id])
        elif 'c2a' in db_id:
            c2a.append(data)
            c2a_tab.append(all_tables[db_id])
        elif 'u2r' in db_id:
            u2r.append(data)
            u2r_tab.append(all_tables[db_id])
        elif 'r2u' in db_id:
            r2u.append(data)
            r2u_tab.append(all_tables[db_id])
        else:
            raise ValueError

    json.dump(e2a, open('gendata/methods/e2a/dev.json', 'w'), indent=4)
    json.dump(c2a, open('gendata/methods/c2a/dev.json', 'w'), indent=4)
    json.dump(u2r, open('gendata/methods/u2r/dev.json', 'w'), indent=4)
    json.dump(r2u, open('gendata/methods/r2u/dev.json', 'w'), indent=4)
    json.dump(e2a_tab, open('gendata/methods/e2a/tables.json', 'w'), indent=4)
    json.dump(c2a_tab, open('gendata/methods/c2a/tables.json', 'w'), indent=4)
    json.dump(u2r_tab, open('gendata/methods/u2r/tables.json', 'w'), indent=4)
    json.dump(r2u_tab, open('gendata/methods/r2u/tables.json', 'w'), indent=4)



if __name__ == '__main__':
    # create_raw_annotation_json()
    # create_agent_finding_json()
    # restore_cell_values()
    # merge_annotations()
    # table_tagging()
    # correct_primary_keys()
    # correct_foreign_keys()
    # sample_fks_from_sql()
    # edit_table_type()
    # correct_dataset()
    # import nltk
    # nltk.download('punkt')
    # check_schema()
    # construct_dbqa()
    correct_value()
    # split_method()