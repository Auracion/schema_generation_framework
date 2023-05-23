import os, sys, json, gc
from shutil import copy
import tempfile
import sqlite3
from tqdm import tqdm

sys.path.append(os.path.dirname(__file__))

from eval.spider.evaluation import evaluate, build_foreign_key_map_from_json
from eval.spider.process_sql import get_sql
from database_utils import Database, Schema
from sql_parser import SQLEncoder
from sql_unparser import SQLDecoder, Unparser
from entity_relation_graph import ERG, update_schema_linking
from qualifier import filter_for_c2a, filter_for_r2u, filter_for_u2r


def eval_exec_match(p_db, g_db, p_str, g_str, pred, gold):
    """
    return 1 if the values between prediction and gold are matching
    in the corresponding index. Currently not support multiple col_unit(pairs).
    """
    pconn = sqlite3.connect(os.path.join('data/new_database', p_db, p_db + '.sqlite'))
    pcursor = pconn.cursor()
    gconn = sqlite3.connect(os.path.join('data/database', g_db, g_db + '.sqlite'))
    gcursor = gconn.cursor()
    try:
        gcursor.execute(g_str.replace("DISTINCT", '').replace("Distinct", '').replace("distinct", ''))
        q_res = gcursor.fetchall()
    except:
        return True

    try:
        pcursor.execute(p_str)
        p_res = pcursor.fetchall()
    except Exception as e:
        print(repr(e))
        print(g_str)
        print(p_str)
        print()
        return False

    # cursor.execute(g_str)
    # q_res = cursor.fetchall()

    def res_map(res, val_units):
        rmap = {}
        for idx, val_unit in enumerate(val_units):
            key = tuple(val_unit[1]) if not val_unit[2] else (val_unit[0], tuple(val_unit[1]), tuple(val_unit[2]))
            rmap[key] = [r[idx] for r in res]
        return rmap

    # p_val_units = [unit[1] for unit in pred['select'][1]]
    # q_val_units = [unit[1] for unit in gold['select'][1]]
    # return res_map(p_res, p_val_units) == res_map(q_res, q_val_units)
    # if p_res != q_res:
    #     print(p_db)
    #     print(g_str)
    #     print(p_str)
    #     print(q_res)
    #     print(p_res)
    #     print()

    return p_res == q_res


def build_single_for_c2a(db, new_db, ent, new_concept, concept_column, new_value):
    # print(db, new_db)
    db_path = os.path.join('data/database', db, db + '.sqlite')
    if not os.path.exists(os.path.join('data/new_database', new_db)):
        os.mkdir(os.path.join('data/new_database', new_db))
    new_db_path = os.path.join('data/new_database', new_db, new_db + '.sqlite')
    if os.path.exists(new_db_path):
        os.remove(new_db_path)
    copy(db_path, new_db_path)

    conn = sqlite3.connect(new_db_path)
    cursor = conn.cursor()

    sql = f"ALTER TABLE {ent} RENAME TO {new_concept};"
    cursor.execute(sql)
    sql = f"ALTER TABLE {new_concept} ADD {concept_column} varchar(30);"
    cursor.execute(sql)
    sql = f"UPDATE {new_concept} SET {concept_column} = '{new_value}';"
    cursor.execute(sql)

    conn.commit()

    cursor.close()


def build_single_for_e2a():
    pass


def build_single_for_u2r(db, new_db, src, tgt):
    db_path = os.path.join('data/database', db, db + '.sqlite')
    if not os.path.exists(os.path.join('data/new_database', new_db)):
        os.mkdir(os.path.join('data/new_database', new_db))
    new_db_path = os.path.join('data/new_database', new_db, new_db + '.sqlite')
    if os.path.exists(new_db_path):
        os.remove(new_db_path)
    copy(db_path, new_db_path)

    conn = sqlite3.connect(new_db_path)
    cursor = conn.cursor()

    sql = f"PRAGMA FOREIGN_KEY_LIST({tgt});"
    fks = cursor.execute(sql).fetchall()
    rel_columns, ori_columns = [], []
    on_conds = []
    for fk in fks:
        if fk[2] == src:
            f_name = fk[3]
            new_name = f_name
            if f_name.lower() == tgt.lower()[0] + 'id' or f_name.lower() == tgt.lower()[0] + '_id':
                new_name = src + '_id'
            rel_columns.append(new_name)
            ori_columns.append(f_name)
            on_conds.append(f"\"{src}\".\"{fk[4]}\" = \"{tgt}\".\"{fk[3]}\"")

    sql = f"PRAGMA TABLE_INFO({tgt});"
    tgt_columns = cursor.execute(sql).fetchall()
    flag = True
    for col in tgt_columns:
        if col[-1] != 0:
            flag = False
            if col[1].lower() == 'id':
                rel_columns.append(tgt + '_id')
            else:
                rel_columns.append(col[1])
            ori_columns.append(col[1])
    if flag:
        add_new_pk(cursor, tgt)
        rel_columns.append(tgt + '_id')
        ori_columns.append(tgt + '_id')

    new_columns = ', '.join([f"\"{col}\"" for col in rel_columns])
    select_columns = ', '.join([f"\"{tgt}\".\"{col}\"" for col in ori_columns])
    conds = " AND ".join(on_conds)
    sql = f"CREATE VIEW {src}_{tgt} ({new_columns}) " \
          f"AS " \
          f"SELECT {select_columns} " \
          f"FROM \"{src}\" JOIN \"{tgt}\" ON {conds}"
    # print()
    # print(sql)
    cursor.execute(sql)
    conn.commit()

    cursor.close()


def add_new_pk(cursor, table_name):
    sql = f"SELECT * FROM {table_name};"
    all_values = cursor.execute(sql).fetchall()

    sql = f"PRAGMA TABLE_INFO({table_name});"
    all_columns = cursor.execute(sql).fetchall()

    sql = f"ALTER TABLE {table_name} ADD COLUMN {table_name}_id INT;"
    cursor.execute(sql)

    for i in range(len(all_values)):
        values = all_values[i]
        conds = []
        for cid in range(len(all_columns)):
            conds.append(f"\"{all_columns[cid]}\" = \"{values[cid]}\"")
        conds = " AND ".join(conds)
        sql = f"UPDATE {table_name} SET {table_name}_id = {i} WHERE {conds};"
        cursor.execute(sql)


def build_single_for_r2u(db, new_db, src, tgt, rel):
    db_path = os.path.join('data/database', db, db + '.sqlite')
    if not os.path.exists(os.path.join('data/new_database', new_db)):
        os.mkdir(os.path.join('data/new_database', new_db))
    new_db_path = os.path.join('data/new_database', new_db, new_db + '.sqlite')
    if os.path.exists(new_db_path):
        os.remove(new_db_path)
    copy(db_path, new_db_path)

    if rel.lower().replace(src.lower(), 'T').replace(tgt.lower(), 'T') in ['T_T', 'TT']:
        rel_prefix = ''
    else:
        rel_prefix = rel + '_'

    conn = sqlite3.connect(new_db_path)
    cursor = conn.cursor()

    sql = f"PRAGMA TABLE_INFO({tgt});"
    all_columns = list(list(zip(*cursor.execute(sql).fetchall()))[1])

    sql = f"PRAGMA TABLE_INFO({rel});"
    rel_columns = cursor.execute(sql).fetchall()

    sql = f"ALTER TABLE {tgt} RENAME TO ori_{tgt};"
    cursor.execute(sql)
    conn.commit()

    sql = f"PRAGMA FOREIGN_KEY_LIST({rel});"
    fks = cursor.execute(sql).fetchall()
    rel_fks = [v[3] for v in fks]
    all_fks = [(f"\"{v[2] if v[2] != tgt else 'ori_' + v[2]}\".\"{v[4]}\" = \"{rel}\".\"{v[3]}\"") for v in fks]
    ori_pks, all_pks = [], []
    for fk in fks:
        if fk[2] == src:
            ori_pks.append(fk[3])
            new_name = fk[3]
            if new_name in all_columns:
                new_name = rel_prefix + fk[3]
            if new_name in all_columns:
                new_name = rel_prefix + src + '_' + fk[3]
            all_pks.append(new_name)

    tmp = []
    for value in rel_columns:
        if value[-1] == 0 and value[1] not in rel_fks:
            tmp.append(value[1])
    rel_columns = tmp
    select_columns = ', '.join(
        [f'\"ori_{tgt}\".\"{col}\"' for col in all_columns] +
        [f'\"{rel}\".\"{col}\"' for col in ori_pks] +
        [f'\"{rel}\".\"{col}\"' for col in rel_columns])
    new_columns = ', '.join(
        [f'\"{col}\"' for col in all_columns + all_pks] + [f'\"{rel}_{col}\"' for col in rel_columns])
    sql = f"CREATE VIEW {tgt} ({new_columns}) " \
          f"AS " \
          f"SELECT {select_columns} " \
          f"FROM \"{src}\" JOIN \"ori_{tgt}\" JOIN \"{rel}\" ON {' AND '.join(all_fks)}"
    # print()
    # print(sql)
    cursor.execute(sql)
    conn.commit()

    cursor.close()


def get_samples_for_c2a():
    trains = json.load(open('data/train.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}

    corr, total = 0, 0
    golden, preds = [], []
    golden_sql, pred_sql = [], []
    for i, data in tqdm(enumerate(trains)):
        sql = data["sql"]
        db_id = data["db_id"]
        if db_id == "baseball_1":
            continue
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        question = data['question']
        total += 1
        db = dbs[db_id]
        encoder = SQLEncoder(db.copy())
        acdb, values = encoder.encode(sql)
        erg = ERG(acdb, values)

        whitelist = filter_for_c2a(erg, used=True)
        for ent, new_concept, concept_column in whitelist:
            _acdb = acdb.copy()
            _values = [v.copy() for v in values]
            _erg = ERG(_acdb, _values)
            new_value = _erg.entities[ent].table.nature
            _erg.convert_concept_to_attribute(ent, new_concept, concept_column)

            update_schema_linking(_erg)
            decoder = SQLDecoder(_erg.acdb, _values)
            ast = decoder.tree
            unparser = Unparser(ast)
            new_query = unparser.get_face_code()
            new_db = _acdb.to_dict()
            schema = Schema(new_db)
            new_sql = get_sql(schema, new_query)
            golden.append(query)
            preds.append(' <=> '.join([new_query, db_id, ent, new_concept, concept_column, new_value]))
            golden_sql.append(sql)
            pred_sql.append(new_sql)

            # print(question)
            # print(query)
            # print(ent, new_concept, concept_column)
            # print(new_query)
            # print()
            gc.collect()
    return golden, preds, golden_sql, pred_sql


def get_samples_for_e2a():
    trains = json.load(open('data/train.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}

    corr, total = 0, 0
    golden, preds = [], []
    golden_sql, pred_sql = [], []
    for i, data in tqdm(enumerate(trains)):
        sql = data["sql"]
        db_id = data["db_id"]
        if db_id == "baseball_1":
            continue
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        question = data['question']
        total += 1
        db = dbs[db_id]
        encoder = SQLEncoder(db.copy())
        acdb, values = encoder.encode(sql)
        erg = ERG(acdb, values)

        whitelist = filter_for_r2u(erg, used=True)
        for direction, rel in whitelist:
            total += 1
            _acdb = acdb.copy()
            _values = [v.copy() for v in values]
            _erg = ERG(_acdb, _values)

            relation = _erg.relations[rel]

            if direction == 'l2r':
                src = relation.left.table.name
                tgt = relation.right.table.name
            else:
                src = relation.right.table.name
                tgt = relation.left.table.name

            _erg.convert_relation_to_unk(direction, rel)

            update_schema_linking(_erg)
            decoder = SQLDecoder(_erg.acdb, _values)
            ast = decoder.tree
            unparser = Unparser(ast)
            new_query = unparser.get_face_code()
            new_db = _acdb.to_dict()
            schema = Schema(new_db)
            new_sql = get_sql(schema, new_query)
            golden.append(query)
            preds.append(' <=> '.join([new_query, db_id, src, tgt, rel]))
            golden_sql.append(sql)
            pred_sql.append(new_sql)

            # print(question)
            # print(query)
            # print(ent, new_concept, concept_column)
            # print(new_query)
            # print()
            gc.collect()
    return golden, preds, golden_sql, pred_sql


def get_samples_for_u2r():
    trains = json.load(open('data/dev.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}

    corr, total = 0, 0
    golden, preds = [], []
    golden_sql, pred_sql = [], []
    for i, data in tqdm(enumerate(trains)):
        sql = data["sql"]
        db_id = data["db_id"]
        if db_id == "baseball_1":
            continue
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        question = data['question']
        total += 1
        db = dbs[db_id]
        encoder = SQLEncoder(db.copy())
        acdb, values = encoder.encode(sql)
        erg = ERG(acdb, values)

        whitelist = filter_for_u2r(erg, used=True)
        for src, tgt in whitelist:
            total += 1
            _acdb = acdb.copy()
            _values = [v.copy() for v in values]
            _erg = ERG(_acdb, _values)

            _erg.convert_unk_to_relation(src, tgt)

            update_schema_linking(_erg)
            decoder = SQLDecoder(_erg.acdb, _values)
            ast = decoder.tree
            unparser = Unparser(ast)
            new_query = unparser.get_face_code()
            new_db = _acdb.to_dict()
            schema = Schema(new_db)
            new_sql = get_sql(schema, new_query)
            golden.append(query)
            preds.append(' <=> '.join([new_query, db_id, src, tgt]))
            golden_sql.append(sql)
            pred_sql.append(new_sql)

            # print(question)
            # print(query)
            # print(ent, new_concept, concept_column)
            # print(new_query)
            # print()
            gc.collect()
    return golden, preds, golden_sql, pred_sql


def get_samples_for_r2u():
    trains = json.load(open('data/train.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}

    corr, total = 0, 0
    golden, preds = [], []
    golden_sql, pred_sql = [], []
    for i, data in tqdm(enumerate(trains)):
        sql = data["sql"]
        db_id = data["db_id"]
        if db_id == "baseball_1":
            continue
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        question = data['question']
        total += 1
        db = dbs[db_id]
        encoder = SQLEncoder(db.copy())
        acdb, values = encoder.encode(sql)
        erg = ERG(acdb, values)

        whitelist = filter_for_r2u(erg, used=True)
        for direction, rel in whitelist:
            total += 1
            _acdb = acdb.copy()
            _values = [v.copy() for v in values]
            _erg = ERG(_acdb, _values)

            relation = _erg.relations[rel]

            if direction == 'l2r':
                src = relation.left.table.name
                tgt = relation.right.table.name
            else:
                src = relation.right.table.name
                tgt = relation.left.table.name

            _erg.convert_relation_to_unk(direction, rel)

            update_schema_linking(_erg)
            decoder = SQLDecoder(_erg.acdb, _values)
            ast = decoder.tree
            unparser = Unparser(ast)
            new_query = unparser.get_face_code()
            new_db = _acdb.to_dict()
            schema = Schema(new_db)
            new_sql = get_sql(schema, new_query)
            golden.append(query)
            preds.append(' <=> '.join([new_query, db_id, src, tgt, rel]))
            golden_sql.append(sql)
            pred_sql.append(new_sql)

            # print(question)
            # print(query)
            # print(ent, new_concept, concept_column)
            # print(new_query)
            # print()
            gc.collect()
    return golden, preds, golden_sql, pred_sql


def eval_for_c2a():
    goldens, preds, golden_sql, pred_sql = get_samples_for_c2a()
    corr, total = 0, 0
    for i in range(len(goldens)):
        golden = goldens[i]
        pred = preds[i]
        g_sql = golden_sql[i]
        p_sql = pred_sql[i]
        g_str, g_db = golden.split('\t')
        p_str, p_db, ent, new_concept, concept_column, new_value = pred.split(' <=> ')
        build_single_for_c2a(g_db, p_db, ent, new_concept, concept_column, new_value)
        total += 1
        corr += eval_exec_match(p_db, g_db, p_str, g_str, p_sql, g_sql)

    print(corr, total)


def eval_for_e2a():
    goldens, preds, golden_sql, pred_sql = get_samples_for_r2u()
    corr, total = 0, 0
    for i in range(len(goldens)):
        golden = goldens[i]
        pred = preds[i]
        g_sql = golden_sql[i]
        p_sql = pred_sql[i]
        g_str, g_db = golden.split('\t')
        p_str, p_db, src, tgt, rel = pred.split(' <=> ')
        build_single_for_r2u(g_db, p_db, src, tgt, rel)
        total += 1
        corr += eval_exec_match(p_db, g_db, p_str, g_str, p_sql, g_sql)

    print(corr, total)


def eval_for_u2r():
    goldens, preds, golden_sql, pred_sql = get_samples_for_u2r()
    corr, total = 0, 0
    for i in range(len(goldens)):
        golden = goldens[i]
        pred = preds[i]
        g_sql = golden_sql[i]
        p_sql = pred_sql[i]
        g_str, g_db = golden.split('\t')
        p_str, p_db, src, tgt = pred.split(' <=> ')
        build_single_for_u2r(g_db, p_db, src, tgt)
        total += 1
        corr += eval_exec_match(p_db, g_db, p_str, g_str, p_sql, g_sql)

    print(corr, total)


def eval_for_r2u():
    goldens, preds, golden_sql, pred_sql = get_samples_for_r2u()
    corr, total = 0, 0
    for i in range(len(goldens)):
        golden = goldens[i]
        pred = preds[i]
        g_sql = golden_sql[i]
        p_sql = pred_sql[i]
        g_str, g_db = golden.split('\t')
        p_str, p_db, src, tgt, rel = pred.split(' <=> ')
        build_single_for_r2u(g_db, p_db, src, tgt, rel)
        total += 1
        corr += eval_exec_match(p_db, g_db, p_str, g_str, p_sql, g_sql)

    print(corr, total)


if __name__ == '__main__':
    # eval_for_c2a()
    # eval_for_e2a()
    eval_for_u2r()
    # eval_for_r2u()