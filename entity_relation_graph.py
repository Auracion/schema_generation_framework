import os, sys, json
from collections import defaultdict
import tempfile
import gc

sys.path.append(os.path.dirname(__file__))

from sql_parser import SQLEncoder
from database_utils import *
from code_utils import *
from erg_utils import *
from qualifier import *
from eval.spider.evaluation import evaluate, build_foreign_key_map_from_json
from sql_unparser import SQLDecoder, Unparser
from eval.spider.process_sql import get_sql


class ERG:

    def __init__(self, acdb: Database, values=None):
        self.acdb = acdb
        self.star = acdb.columns[0]
        self.entities = {}
        self.relations = {}
        self.directed_edges = []
        self.fault_tables = []
        self.fault_columns = []
        self.values = values

        self.build_graph()

    def build_graph(self):
        for table in self.acdb.tables:
            if table.dtype == "Entity":
                entity = Entity(table)
                for column in table.columns:
                    if column in table.pks or column in table.agents or column.fk is not None:
                        continue
                    else:
                        entity.add_attribute(column)
                self.entities[table.name] = entity

        for table in self.acdb.tables:
            if table.dtype == "Relation":
                relation = Relation(table)
                succ = relation.build_edges(self.entities)
                if succ:
                    for column in table.columns:
                        # if not column.is_pk and column.fk is None:
                        if column.fk is None:
                            relation.add_attribute(column)
                    left = relation.left
                    right = relation.right
                    left.add_adjacent(right, relation)
                    right.add_adjacent(left, relation)
                    self.relations[table.name] = relation
                else:
                    self.fault_tables.append(table)
                    for column in table.columns:
                        self.fault_columns.append(column)

        for ent in self.entities:
            entity = self.entities[ent]
            for primary_column, foreign_column in entity.table.fks:
                assert primary_column.table == entity.table or foreign_column.table == entity.table
                if (primary_column.table == entity.table and foreign_column.table.dtype == "Entity") or \
                        (foreign_column.table == entity.table and primary_column.table.dtype == "Entity"):
                    fkey = FKey(start_joint=primary_column, end_joint=foreign_column)
                    if fkey not in self.directed_edges:
                        self.directed_edges.append(fkey)
                    if primary_column.table == entity.table:
                        other = self.entities[foreign_column.table.name]
                    else:
                        other = self.entities[primary_column.table.name]
                    entity.add_adjacent(other, fkey)

    def convert_concept_to_attribute(self, concept: str, new_concept: str, concept_column: str):
        entity = self.entities[concept]
        new_value = f"\"{entity.table.nature}\""

        entity.name = new_concept
        entity.table.name = new_concept
        entity.table.nature = new_concept.replace('_', ' ')
        entity.table.tag = None

        new_attribute = Column(concept_column, -2)
        new_attribute.table = entity.table
        new_attribute.nature = concept_column.replace('_', ' ')
        new_attribute.cell_values = [[new_value]]
        new_attribute.dtype = "text"
        new_attribute.is_pk = False
        new_attribute.fk = None

        # if len(list(entity.table.codes)) > 0:
        #     for path, branch in self.star.codes:
        #         new_attribute.add_path(path)
        #         new_attribute.add_branch(branch)

        codes = self.codes()
        for from_code in entity.table.codes:
            cond_path_code, cond_branch_code, cond_value = add_condition_to_where(codes, from_code, new_value)
            new_attribute.add_path(cond_path_code)
            new_attribute.add_branch(cond_branch_code)
            self.values.append(cond_value)
            codes.append((cond_path_code, cond_branch_code))
            codes.append(cond_value.codes)
        entity.add_attribute(new_attribute)
        self.entities.pop(concept)
        self.entities[new_concept] = entity

    def convert_entity_to_attribute(self, src, tgt):
        source_entity = self.entities[src]
        target_entity = self.entities[tgt]

        new_target_entity = Entity(target_entity.table)
        new_target_entity.attributes = target_entity.attributes
        new_target_entity.adjacent = target_entity.adjacent

        # self.move_relation_between_entities_to_where(source_entity, new_target_entity, values)
        remove_relation_between_entities(source_entity, new_target_entity)

        # 处理source.table
        tgt_codes = []
        for tgt_path_code, tgt_branch_code in target_entity.table.codes:
            assert tgt_path_code[-1] == "from"
            code = ''
            for i in range(len(tgt_path_code) - 1):
                code += tgt_path_code[i] + tgt_branch_code[i]
            code += tgt_path_code[-1]
            tgt_codes.append(code)

        for src_path_code, src_branch_code in source_entity.table.codes:
            assert src_path_code[-1] == "from"
            code = ''
            for i in range(len(src_path_code) - 1):
                code += src_path_code[i] + src_branch_code[i]
            code += src_path_code[-1]
            if code not in tgt_codes:
                new_target_entity.table.add_path(src_path_code)
                new_target_entity.table.add_branch(src_branch_code)
            else:
                reduce_from_branch(self.codes(), (src_path_code, src_branch_code))

        edges = source_entity.get_relation(target_entity)["forward"]
        if len(edges) == 1:
            fk_cache = edges[0].end_joint
        else:
            fk_cache = None
        source_agents = source_entity.agents
        if len(source_agents) == 1:
            new_attribute = source_agents[0]
            new_attribute.name = src
            new_attribute.nature = target_entity.table.nature + ' ' + source_entity.table.nature
            new_attribute.table = target_entity.table
            new_attribute.is_pk = False
            add_new_attribute(new_attribute, new_target_entity)
            if fk_cache is not None:
                for pcode, bcode in fk_cache.codes:
                    new_attribute.add_path(pcode)
                    new_attribute.add_branch(bcode)
            if len(source_entity.primary) == 1 and source_entity.primary[0] != source_agents[0]:
                for pcode, bcode in source_entity.primary[0].codes:
                    new_attribute.add_path(pcode)
                    new_attribute.add_branch(bcode)
        elif len(source_agents) > 1:
            for primary_column in source_agents:
                new_attribute = primary_column
                new_attribute.name = src + '_' + primary_column.name
                new_attribute.nature = source_entity.table.nature + ' ' + primary_column.nature
                new_attribute.table = target_entity.table
                new_attribute.is_pk = False
                new_target_entity.add_attribute(new_attribute)
                new_target_entity.table.add_column(new_attribute)
        elif fk_cache is not None:
            new_attribute = fk_cache
            new_attribute.name = src
            new_attribute.nature = target_entity.table.nature + ' ' + source_entity.table.nature
            new_attribute.table = target_entity.table
            new_attribute.is_pk = False
            new_target_entity.add_attribute(new_attribute)
            new_target_entity.table.add_column(new_attribute)

        # 移除source在其它点中的邻接关系，同步其他entity和relation
        for adj_entity in source_entity.adjacent:
            edges = source_entity.get_relation(adj_entity)
            if adj_entity == new_target_entity:
                edge_codes = []
                for edge in edges["forward"] + edges["backward"]:
                    for code in edge.codes:
                        edge_codes.append(code)
                    self.directed_edges.remove(edge)
                    new_attribute = edge.end_joint
                    new_attribute.table = target_entity.table
                    new_attribute.is_pk = False
                    new_target_entity.add_attribute(new_attribute)
                    new_target_entity.table.add_column(new_attribute)
                edge_codes = sorted(edge_codes, key=lambda x: x[1][-3], reverse=True)
                for code in edge_codes:
                    all_codes = self.codes()
                    reduce_on_branch(all_codes, code)

            # forward最多一个，且指向target
            adj_entity.delete_adjacent(source_entity)
            for edge in edges["backward"]:
                self.directed_edges.remove(edge)
                edge.end_joint.table = new_target_entity.table
                edge.end_joint.name = edge.end.name + '_' + edge.end_joint.name
                edge.end_joint.nature = edge.end.nature + ' ' + edge.end_joint.nature
                new_edge = FKey(start_joint=edge.start_joint, end_joint=edge.end_joint)
                self.directed_edges.append(edge)
                new_target_entity.add_adjacent(adj_entity, new_edge)
                adj_entity.add_adjacent(new_target_entity, new_edge)

            for edge in edges["relation"]:
                new_edge = Relation(edge.table)
                new_edge.attributes = edge.attributes
                if edge.left == adj_entity:
                    new_edge.left = adj_entity
                    new_edge.right = new_target_entity
                    new_edge.left_fks = edge.left_fks
                    for fks in new_edge.right_fks:
                        fks.start = new_target_entity.table
                    self.relations[edge.table.name] = new_edge
                    new_target_entity.add_adjacent(adj_entity, new_edge)
                    adj_entity.add_adjacent(new_target_entity, new_edge)
                elif edge.right == adj_entity:
                    new_edge.right = adj_entity
                    new_edge.left = new_target_entity
                    new_edge.right_fks = edge.right_fks
                    for fks in new_edge.left_fks:
                        fks.start = new_target_entity.table
                    self.relations[edge.table.name] = new_edge
                    new_target_entity.add_adjacent(adj_entity, new_edge)
                    adj_entity.add_adjacent(new_target_entity, new_edge)
                else:
                    raise ValueError

        # 迁移attribute
        for attr in source_entity.attributes:
            attr.table = target_entity.table
            attr.name = src + '_' + attr.name
            attr.nature = source_entity.table.nature + ' ' + attr.nature
            new_target_entity.add_attribute(attr)

        new_target_entity.delete_adjacent(source_entity)
        new_target_entity.hash_id = target_entity.hash_id

        self.entities.pop(src)
        self.entities.pop(tgt)
        self.entities[tgt] = new_target_entity

    def convert_relation_to_unk(self, direction, rel):
        relation = self.relations[rel]

        if direction == 'l2r':
            start_entity = relation.left
            end_entity = relation.right
            start_edges = relation.left_fks
            end_edges = relation.right_fks
        else:
            start_entity = relation.right
            end_entity = relation.left
            start_edges = relation.right_fks
            end_edges = relation.left_fks

        new_edges = []
        if rel.lower().replace(start_entity.table.name.lower(), 'T').replace(end_entity.table.name.lower(), 'T') in ['T_T','TT']:
            rel_prefix = ''
            rel_prefix_nature = ''
        else:
            rel_prefix = rel + '_'
            rel_prefix_nature = relation.table.nature + ' '
        end_names = [col.name for col in end_entity.table.columns]
        for edge in start_edges:
            new_name = edge.end_joint.name
            new_nature = edge.end_joint.nature
            if new_name in end_names:
                new_name = rel_prefix + edge.end_joint.name
                new_nature = rel_prefix_nature + edge.end_joint.nature
            if new_name in end_names:
                new_name = rel_prefix + start_entity.table.name + '_' + edge.end_joint.name
                new_nature = rel_prefix_nature + start_entity.table.nature + ' ' + edge.end_joint.nature
            new_column = Column(new_name, -2)
            new_column.table = end_entity.table
            new_column.nature = new_nature
            new_column.dtype = edge.end_joint.dtype
            new_column.cell_values = edge.end_joint.cell_values
            new_column.is_pk = False
            new_column.fk = edge.start_joint
            new_column.path_code = edge.end_joint.path_code
            new_column.branch_code = edge.end_joint.branch_code
            new_edge = FKey(edge.start_joint, new_column)
            new_edges.append(new_edge)

        start_entity.adjacent[end_entity]["relation"].remove(relation)
        end_entity.adjacent[start_entity]["relation"].remove(relation)
        for edge in new_edges:
            start_entity.add_adjacent(end_entity, edge)
            end_entity.add_adjacent(start_entity, edge)

        reduce_codes = []
        for edge in end_edges:
            remove_list = []
            copy_codes = []
            for efk_path, efk_branch in edge.end_joint.codes:
                flag = True
                for i, (epk_path, epk_branch) in enumerate(edge.start_joint.codes):
                    if epk_path.string == efk_path.string and epk_branch.string[:-2] == efk_branch.string[:-2]:
                        remove_list.append(i)
                        reduce_codes.append((epk_path, epk_branch))
                        flag = False
                        break
                if flag:
                    copy_codes.append((efk_path, efk_branch))
            for remove_idx in remove_list[::-1]:
                edge.start_joint.pop_code(remove_idx)
            for p_code, b_code in copy_codes:
                edge.start_joint.add_path(p_code)
                edge.start_joint.add_branch(b_code)
        all_codes = self.codes()
        reduce_codes = sorted(reduce_codes, key=lambda x: x[1][-3], reverse=True)
        for code in reduce_codes:
            reduce_on_branch(all_codes, code)

        for attr in relation.attributes:
            attr.name = relation.table.name + '_' + attr.name
            attr.table = end_entity.table
            attr.nature = relation.table.nature + ' ' + attr.nature
            end_entity.add_attribute(attr)

        for (path, branch) in relation.table.codes:
            prefix = '-'.join([path[i] + branch[i] for i in range(len(path) - 1)])
            f = True
            for (e_path, e_branch) in end_entity.table.codes:
                if '-'.join([e_path[i] + e_branch[i] for i in range(len(e_path) - 1)]) == prefix:
                    f = False
                    all_codes = self.codes()
                    reduce_from_branch(all_codes, (path, branch))
                    break
            if f:
                end_entity.table.add_path(path)
                end_entity.table.add_branch(branch)

        self.directed_edges.extend(new_edges)
        self.relations.pop(rel)

    def convert_unk_to_relation(self, src, tgt):
        source_entity = self.entities[src]
        target_entity = self.entities[tgt]

        edges = source_entity.get_relation(target_entity)["forward"]

        new_table = Table(source_entity.name + '_' + target_entity.name, -2)
        new_table.nature = source_entity.table.nature + ' ' + target_entity.table.nature
        new_table.dtype = "Relation"
        relation = Relation(new_table)
        relation.left = source_entity
        relation.right = target_entity

        all_code = self.codes()
        add_from_path, add_from_branch = [], []
        add_on_path, add_on_branch0, add_on_branch1 = [], [], []
        mode = "NoChange"
        remove_idx = []
        for edge in edges:
            remove_idx.append(self.directed_edges.index(edge))
            if edge.end_joint.name.lower() == edge.end_joint.table.name.lower()[0] + 'id' or \
                edge.end_joint.name.lower() == edge.end_joint.table.name.lower()[0] + '_id':
                edge.end_joint.name = src + '_id'
            edge.end_joint.table = new_table
            relation.left_fks.append(edge)
            if len(list(edge.end_joint.codes)) > 0 and mode != "Add":
                mode = "Replace"
            for path_code, branch_code in edge.end_joint.codes:
                if "on" in path_code.path:
                    mode = "Add"
                    from_path, from_branch = add_from_code(all_code, (path_code, branch_code))
                    add_from_path.append(from_path)
                    add_from_branch.append(from_branch)
                    on_path, on_branch0, on_branch1 = add_on_code(all_code, (path_code, branch_code))
                    add_on_path.append(on_path)
                    add_on_branch0.append(on_branch0)
                    add_on_branch1.append(on_branch1)

        if mode == "Add":
            prefix = {}
            for i in range(len(add_from_path)):
                path = add_from_path[i]
                branch = add_from_branch[i]
                key = path.string + branch.string[:-1]
                if key not in prefix:
                    prefix[key] = (path, branch)
                else:
                    if prefix[key][1][-1] > branch[-1]:
                        prefix[key] = (path, branch)
            for path, branch in prefix.values():
                new_table.add_path(path)
                new_table.add_branch(branch)
        elif mode == "Replace":
            new_table.path_code = target_entity.table.path_code
            new_table.branch_code = target_entity.table.branch_code
            target_entity.table.path_code = []
            target_entity.table.branch_code = []

        if len(target_entity.primary) == 0:
            new_pk = Column(target_entity.name + '_id', -2)
            new_pk.table = target_entity.table
            new_pk.nature = target_entity.table.nature + ' ' + 'id'
            new_pk.dtype = "number"
            new_pk.is_pk = True
            target_entity.table.pks.append(new_pk)

        for pi, pk in enumerate(target_entity.primary):
            if pk.name.lower() == 'id':
                new_column = Column(target_entity.name + '_' + pk.name, -2)
                new_column.nature = target_entity.table.nature + ' ' + pk.nature
            else:
                new_column = Column(pk.name, -2)
                new_column.nature = pk.nature
            new_column.table = new_table
            new_column.dtype = pk.dtype
            new_column.cell_values = pk.cell_values
            new_column.fk = pk
            if mode == "Add":
                for i in range(len(add_on_path)):
                    pk.add_path(add_on_path[i].copy())
                    b0 = add_on_branch0[i].copy()
                    b0.edit(-3, str(int(b0[-3]) + pi))
                    pk.add_branch(b0)
                    new_column.add_path(add_on_path[i].copy())
                    b1 = add_on_branch1[i].copy()
                    b1.edit(-3, str(int(b1[-3]) + pi))
                    new_column.add_branch(b1)
            edge = FKey(pk, new_column)
            relation.right_fks.append(edge)

        source_entity.adjacent[target_entity]["forward"] = []
        target_entity.adjacent[source_entity]["backward"] = []
        source_entity.add_adjacent(target_entity, relation)
        target_entity.add_adjacent(source_entity, relation)
        remove_idx = sorted(remove_idx, reverse=True)
        for i in remove_idx:
            self.directed_edges.pop(i)
        self.relations[relation.table.name] = relation

    def codes(self):
        codes = collect_codes(self)
        for value in self.values:
            codes.append(value.codes)
        return codes


def collect_codes(erg: ERG):
    codes = []

    codes.extend(erg.star.codes)

    for table in erg.fault_tables:
        codes.extend(table.codes)
    for column in erg.fault_columns:
        codes.extend(column.codes)

    for ent in erg.entities:
        entity = erg.entities[ent]

        table = entity.table
        codes.extend(table.codes)
        for column in table.columns:
            codes.extend(column.codes)

    for rel in erg.relations:
        relation = erg.relations[rel]
        table = relation.table
        codes.extend(table.codes)
        for column in table.columns:
            codes.extend(column.codes)

    return codes


def update_schema_linking(erg: ERG):
    erg.acdb.clear()
    erg.acdb.add_column(erg.star)

    for ent in erg.entities:
        entity = erg.entities[ent]
        table = entity.table
        table.fks = []
        table.columns = []
        erg.acdb.add_table(table)
        for column in entity.primary + entity.agents:
            column.fk = None
            column.table = table
            # table.add_column(column)
            erg.acdb.add_column(column)

        for column in entity.attributes:
            column.is_pk = False
            column.fk = None
            column.table = table
            # table.add_column(column)
            erg.acdb.add_column(column)

        for edge_dict in entity.adjacent.values():
            forward = edge_dict["forward"]
            backward = edge_dict["backward"]
            # print('f', forward)
            # print('b', backward)

            for fedge in forward:
                fedge.end_joint.table = fedge.end
                fedge.end_joint.fk = fedge.start_joint
                # fedge.end.add_column(fedge.end_joint)
                erg.acdb.add_column(fedge.end_joint)
                table.fks.append((fedge.start_joint, fedge.end_joint))

            for bedge in backward:
                bedge.start_joint.table = bedge.start
                bedge.end_joint.fk = bedge.start_joint
                # bedge.start.add_column(bedge.start_joint)
                erg.acdb.add_column(bedge.start_joint)
                table.fks.append((bedge.start_joint, bedge.end_joint))

    for rel in erg.relations:
        relation = erg.relations[rel]
        table = relation.table
        table.fks = []
        table.columns = []
        erg.acdb.add_table(table)
        for column in table.pks + table.agents:
            column.table = table
            # table.add_column(column)
            erg.acdb.add_column(column)
        for column in relation.attributes:
            column.table = table
            # table.add_column(column)
            erg.acdb.add_column(column)

        for edge in relation.left_fks + relation.right_fks:
            column = edge.end_joint
            column.table = table
            column.fk = edge.start_joint
            # table.add_column(column)
            erg.acdb.add_column(column)
            table.fks.append((edge.start_joint, edge.end_joint))
            edge.start.fks.append((edge.start_joint, edge.end_joint))

    for table in erg.fault_tables:
        erg.acdb.add_table(table)
        for primary_column, foreign_column in table.fks:
            primary_column.is_pk = True
            foreign_column.fk = primary_column
            tmp_fks = [p.full_name + f.full_name for (p, f) in foreign_column.table.fks]
            if primary_column.full_name + foreign_column.full_name not in tmp_fks:
                foreign_column.table.fks.append((primary_column, foreign_column))
            tmp_fks = [p.full_name + f.full_name for (p, f) in primary_column.table.fks]
            if primary_column.full_name + foreign_column.full_name not in tmp_fks:
                primary_column.table.fks.append((primary_column, foreign_column))
    for column in erg.fault_columns:
        erg.acdb.add_column(column)

    for column in erg.acdb.columns:
        if column.name != '*':
            column.table.add_column(column)


def check_e2a(target):
    trains = json.load(open('data/dev.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}

    corr, total = 0, 0
    golden, preds = [], []
    for i, data in enumerate(trains):
        sql = data["sql"]
        db_id = data["db_id"]
        if db_id == "baseball_1":
            continue
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        question = data['question']
        if target is not None and question != target:
            continue
        golden.append(query)
        db = dbs[db_id]
        encoder = SQLEncoder(db.copy())
        acdb, values = encoder.encode(sql)
        erg = ERG(acdb, values)

        whitelist = filter_for_e2a(erg, used=True)
        # whitelist_c2a = filter_for_c2a(erg.entities, acdb, used=True)
        # whitelist_r2u = filter_for_r2u(erg.relations, acdb, used=True)
        # whitelist_u2r = filter_for_u2r(erg.dircted_edges, acdb, used=True)
        for src, tgt in whitelist:
            _acdb = acdb.copy()
            _values = [v.copy() for v in values]
            _erg = ERG(_acdb, _values)
            _erg.convert_entity_to_attribute(src, tgt)

            update_schema_linking(_erg)
            decoder = SQLDecoder(_erg.acdb, _values)
            ast = decoder.tree
            unparser = Unparser(ast)
            total += 1
            new_query = unparser.get_face_code()
            new_db = _acdb.to_dict()
            schema = Schema(new_db)
            new_sql = get_sql(schema, new_query)
            if target is not None:
                print(question)
                print(query)
                print(new_query)
                print(src, tgt)
                print()
            # try:
            #     new_query = unparser.get_face_code()
            #     new_db = _acdb.to_dict()
            #     schema = Schema(new_db)
            #     new_sql = get_sql(schema, new_query)
            #     preds.append(new_sql)
            #     corr += 1
            # except:
            #     print(question)
            #     print(query)
            #     print(src, tgt)
            #     # print(new_sql)
            #     print()
            gc.collect()
    print(corr, total)


def check_c2a(target):
    trains = json.load(open('data/dev.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}

    corr, total = 0, 0
    golden, preds = [], []
    for i, data in enumerate(trains):
        sql = data["sql"]
        db_id = data["db_id"]
        if db_id == "baseball_1":
            continue
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        question = data['question']
        if question != target:
            continue
        golden.append(query)
        total += 1
        db = dbs[db_id]
        encoder = SQLEncoder(db.copy())
        acdb, values = encoder.encode(sql)
        erg = ERG(acdb, values)

        whitelist = filter_for_c2a(erg, used=False)
        for ent, new_concept, concept_column in whitelist:
            _acdb = acdb.copy()
            _values = [v.copy() for v in values]
            _erg = ERG(_acdb, _values)
            _erg.convert_concept_to_attribute(ent, new_concept, concept_column)

            update_schema_linking(_erg)
            decoder = SQLDecoder(_erg.acdb, _values)
            ast = decoder.tree
            unparser = Unparser(ast)
            new_query = unparser.get_face_code()
            new_db = _acdb.to_dict()
            schema = Schema(new_db)
            new_sql = get_sql(schema, new_query)
            preds.append(new_sql)

            print(question)
            print(query)
            print(ent, new_concept, concept_column)
            print(new_query)
            print()
            gc.collect()


def check_r2u(target):
    trains = json.load(open('data/train.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}

    corr, total = 0, 0
    golden, preds = [], []
    for i, data in enumerate(trains):
        sql = data["sql"]
        db_id = data["db_id"]
        if db_id == "baseball_1":
            continue
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        question = data['question']
        if question != target:
            continue
        golden.append(query)
        # total += 1
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
            _erg.convert_relation_to_unk(direction, rel)
            update_schema_linking(_erg)
            decoder = SQLDecoder(_erg.acdb, _values)
            ast = decoder.tree
            unparser = Unparser(ast)
            new_query = unparser.get_face_code()
            new_db = _acdb.to_dict()
            schema = Schema(new_db)
            new_sql = get_sql(schema, new_query)
            preds.append(new_sql)
            print(question)
            print(query)
            print(direction, rel)
            print(new_query)
            print()
            gc.collect()
    print(corr, total)


def check_u2r(target=None):
    trains = json.load(open('data/train.json', 'r'))
    dbs = json.load(open('data/tables_with_tags.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}

    corr, total = 0, 0
    golden, preds = [], []
    for i, data in enumerate(trains):
        sql = data["sql"]
        db_id = data["db_id"]
        if db_id == "baseball_1":
            continue
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        question = data['question']
        if target is not None and question != target:
            continue
        golden.append(query)
        total += 1
        db = dbs[db_id]
        encoder = SQLEncoder(db.copy())
        acdb, values = encoder.encode(sql)
        erg = ERG(acdb, values)

        whitelist = filter_for_u2r(erg, used=True)
        for src, tgt in whitelist:
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
            # preds.append(new_sql)

            print(question)
            print(query)
            print(src, tgt)
            print(new_query)
            print()
            gc.collect()
    # print(total, corr)


if __name__ == '__main__':
    # check_u2r()
    check_e2a('Show the stadium name and the number of concerts in each stadium.')
    # check_e2a(None)
    # dbs = json.load(open('data/tables_with_tags.json', 'r'))
    # dbs = {db["db_id"]: db for db in dbs}
    # corr, total = 0, 0
    # for db_id in dbs:
    #     db = Database(dbs[db_id])
    #     erg = ERG(db)
    #     update_schema_linking(erg)
    #     new_db = erg.acdb
    #     total += 1
    #     corr += check(db, new_db)
    # print(corr, total)


