import os, sys, json
from collections import defaultdict

sys.path.append(os.path.dirname(__file__))

from sql_parser import SQLEncoder
from database_utils import Database, Column, Table
from code_utils import *


class Entity:

    def __init__(self, table: Table):
        self.table = table
        self.name = table.name
        self.primary = table.pks  # list of Column
        self.agents = table.agents  # list of Column
        self.attributes = []
        self.adjacent = {}  # node: edge
        self.hash_id = hash(self.table) ^ hash("Entity")

    def add_attribute(self, attr: Column):
        if attr not in self.attributes:
            self.attributes.append(attr)

    def delete_attribute(self, attr: Column):
        self.attributes.remove(attr)

    def add_adjacent(self, adjacent, edge):
        if adjacent not in self.adjacent:
            self.adjacent[adjacent] = {"forward": [], "backward": [], "relation": []}
        if isinstance(edge, Relation):
            self.adjacent[adjacent]["relation"].append(edge)
        else:
            if edge.start == self.table:
                self.adjacent[adjacent]["forward"].append(edge)
            else:
                assert edge.end == self.table
                self.adjacent[adjacent]["backward"].append(edge)

    def delete_adjacent(self, adjacent):
        if adjacent in self.adjacent:
            self.adjacent.pop(adjacent)

    def has_adjacent(self, node):
        return node in self.adjacent

    def get_relation(self, node):
        '''
        :param node: Entity
        :return: List[DirEdge/Relation]
        '''
        return self.adjacent[node]

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return f"Entity: {self.name}"

    def __hash__(self):
        return self.hash_id


class FKey:

    def __init__(self, start_joint: Column, end_joint: Column):
        self.start = start_joint.table
        self.end = end_joint.table
        self.start_joint = start_joint
        self.end_joint = end_joint
        self.codes = self.get_code()

    def get_code(self):
        codes = []
        for s_path, s_branch in self.start_joint.codes:
            s_code = s_path.string + s_branch.string[:-2]
            for e_path, e_branch in self.end_joint.codes:
                e_code = e_path.string + e_branch.string[:-2]
                if s_code == e_code:
                    codes.append((s_path, s_branch))
        return codes

    def __hash__(self):
        return hash(self.start_joint) ^ hash(self.end_joint) ^ hash("FKey")

    def __eq__(self, other):
        return self.start_joint == other.start_joint and self.end_joint == other.end_joint

    def __repr__(self):
        return f"{self.start_joint.full_name} -> {self.end_joint.full_name}"


class Relation:

    def __init__(self, table: Table):
        self.table = table
        self.left = None  # Entity
        self.right = None  # Entity
        self.left_fks = []  # list of FKey
        self.right_fks = []  # list of FKey
        self.attributes = []  # list of Column

    def add_attribute(self, attr: Column):
        if attr not in self.attributes:
            self.attributes.append(attr)

    def build_edges(self, entities: dict):
        edges = []
        for column in self.table.columns:
            if column.fk is not None:
                pk = column.fk
                edge = FKey(start_joint=pk, end_joint=column)
                edges.append(edge)

        for edge in edges:
            start = edge.start
            entity = entities[start.name]
            if self.left is None:
                self.left = entity
                self.left_fks.append(edge)
            elif self.left == entity:
                self.left_fks.append(edge)
            elif self.right is None:
                self.right = entity
                self.right_fks.append(edge)
            elif self.right == entity:
                self.right_fks.append(edge)
            else:
                return False
        return self.right is not None

    def __hash__(self):
        return hash(self.table) ^ hash("ERD Relation")

    def __eq__(self, other):
        return self.table == other.table

    def __repr__(self):
        return f"{self.left.name} <= {self.table.name} => {self.right.name}"


def check(db1, db2):
    tables1 = db1.tables
    tables2 = db2.tables

    if len(tables1) != len(tables2):
        print("Num of table")
        return False

    tables1 = sorted(tables1, key=lambda x: x.name)
    tables2 = sorted(tables2, key=lambda x: x.name)

    for i in range(len(tables1)):
        tab1 = tables1[i]
        tab2 = tables2[i]
        if tab1.name != tab2.name:
            print(tab1.name, tab2.name, "table name")
            return False
        columns1 = tab1.columns
        columns2 = tab2.columns
        if len(columns1) != len(columns2):
            print(tab1.name, tab2.name, "Num of columns")
            return False
        columns1 = sorted(columns1, key=lambda x: x.name)
        columns2 = sorted(columns2, key=lambda x: x.name)
        for j in range(len(columns1)):
            col1 = columns1[j]
            col2 = columns2[j]
            if col1.full_name != col2.full_name:
                print(col1.full_name, col2.full_name, "column name")
                return False
            if col1.is_pk != col2.is_pk:
                print(col1.full_name, col2.full_name, "pk")
                return False
            if col1.fk is None and col2.fk is not None or col1.fk is not None and col2.fk is None:
                print(col1.full_name, col2.full_name, "fk None")
                return False
            if col1.fk != col2.fk:
                print(col1.full_name, col2.full_name, "fk")
                return False

        columns1 = tab1.pks
        columns2 = tab2.pks
        if len(columns1) != len(columns2):
            print(tab1.name, tab2.name, "Num of pks")
            return False
        columns1 = sorted(columns1, key=lambda x: x.name)
        columns2 = sorted(columns2, key=lambda x: x.name)
        for j in range(len(columns1)):
            col1 = columns1[j]
            col2 = columns2[j]
            if col1.full_name != col2.full_name:
                print(col1.full_name, col2.full_name, "column name of pk")
                return False
            if col1.is_pk != col2.is_pk:
                print(col1.full_name, col2.full_name, "pk of pk")
                return False
            if col1.fk is None and col2.fk is not None or col1.fk is not None and col2.fk is None:
                print(col1.full_name, col2.full_name, "fk of pk None")
                return False
            if col1.fk != col2.fk:
                print(col1.full_name, col2.full_name, "fk of pk")
                return False

        fks1 = [fk[0].full_name + fk[1].full_name for fk in tab1.fks]
        fks2 = [fk[0].full_name + fk[1].full_name for fk in tab2.fks]
        if len(fks1) != len(fks2):
            print(tab1.name, tab2.name, "Num of fks")
            return False
        for fk in fks1:
            if fk not in fks2:
                print(tab1.name, tab2.name, "fk not in")
                return False
        for fk in fks2:
            if fk not in fks1:
                print(tab1.name, tab2.name, "fk not in")
                return False
    return True


def remove_relation_between_entities(source: Entity, target: Entity):
    # src -> tgt
    edges = source.get_relation(target)
    assert len(edges["backward"]) == 0 and len(edges["relation"]) == 0
    for edge in edges["forward"]:
        src_codes = list(edge.start_joint.codes)
        tgt_codes = list(edge.end_joint.codes)
        for s in range(len(src_codes) - 1, -1, -1):
            s_path, s_branch = src_codes[s]
            for t in range(len(tgt_codes) - 1, -1, -1):
                t_path, t_branch = tgt_codes[t]
                if 'on' in s_path.path and 'on' in t_path.path and s_path == t_path and s_branch.string[:-2] == t_branch.string[:-2]:
                    edge.start_joint.pop_code(s)
                    edge.end_joint.pop_code(t)


def add_new_attribute(new_attribute, new_target_entity):
    if new_attribute in new_target_entity.agents:
        aid = new_target_entity.agents.index(new_attribute)
        agent = new_target_entity.agents[aid]
        for pcode, bcode in new_attribute.codes:
            agent.add_path(pcode)
            agent.add_branch(bcode)
    elif new_attribute in new_target_entity.primary:
        pid = new_target_entity.primary.index(new_attribute)
        primary = new_target_entity.primary[pid]
        for pcode, bcode in new_attribute.codes:
            primary.add_path(pcode)
            primary.add_branch(bcode)
    elif new_attribute in new_target_entity.attributes:
        aid = new_target_entity.attributes.index(new_attribute)
        attr = new_target_entity.attributes[aid]
        for pcode, bcode in new_attribute.codes:
            attr.add_path(pcode)
            attr.add_branch(bcode)
    else:
        new_target_entity.add_attribute(new_attribute)
        new_target_entity.table.add_column(new_attribute)

