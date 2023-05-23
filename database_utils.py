import os, sys, json
from collections import defaultdict
from itertools import product
from copy import deepcopy

sys.path.append(os.path.dirname(__file__))

from code_utils import PathCode, BranchCode


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


class Column:

    def __init__(self, col, cid):
        self.name = col  # original
        self.cid = cid
        self.table = None  # Table
        self.nature = None
        self.dtype = None
        self.cell_values = None
        self.is_pk = False
        self.fk = None  # pk Column or None
        self.path_code = []
        self.branch_code = []
        self.cond_value = {}

    def add_path(self, path: PathCode):
        self.path_code.append(path)

    def add_branch(self, branch: BranchCode):
        self.branch_code.append(branch)

    def add_value(self, path, branch, value):
        self.cond_value[hash(path) ^ hash(branch)] = value

    def find_value(self, path, branch):
        return self.cond_value[hash(path) ^ hash(branch)]

    @property
    def codes(self):
        return zip(self.path_code, self.branch_code)

    def codes_with_values(self):
        return zip(self.path_code, self.branch_code, self.cond_value)

    def pop_code(self, index):
        self.path_code.pop(index)
        self.branch_code.pop(index)

    @property
    def full_name(self):
        if self.name == '*':
            return '*'
        else:
            return f"{self.table.name}.{self.name}"

    def copy(self):
        #  在外面补table,fk,cond_value
        new = Column(self.name, self.cid)
        new.nature = self.nature
        new.dtype = self.dtype
        new.cell_values = self.cell_values
        new.is_pk = self.is_pk
        new.path_code = [p.copy() for p in self.path_code]
        new.branch_code = [b.copy() for b in self.branch_code]
        return new

    def __hash__(self):
        return hash(self.name) ^ hash(self.table.name) ^ hash("Column")

    def __eq__(self, other):
        return other.name == self.name and other.table == self.table

    def __repr__(self):
        if self.name == "*":
            return f"Column: {self.name}"
        else:
            return f"Column: {self.table.name}.{self.name}"


class Table:

    def __init__(self, tab, tid):
        self.name = tab
        self.tid = tid
        self.nature = None
        self.dtype = None
        self.columns = []  # type Column
        self.pks = []  # type Column
        self.fks = []  # (Column, Column)
        self.agents = []  # list of Column
        self.path_code = []
        self.branch_code = []
        self.tag = None

    def add_column(self, column: Column):
        if column not in self.columns:
            self.columns.append(column)

    def add_path(self, path: PathCode):
        self.path_code.append(path)

    def add_branch(self, branch: BranchCode):
        self.branch_code.append(branch)

    @property
    def codes(self):
        return zip(self.path_code, self.branch_code)

    def pop_code(self, index):
        self.path_code.pop(index)
        self.branch_code.pop(index)

    def copy(self):
        #  在外面补columns,pks,fks,agents
        new = Table(self.name, self.tid)
        new.nature = self.nature
        new.dtype = self.dtype
        new.path_code = [p.copy() for p in self.path_code]
        new.branch_code = [b.copy() for b in self.branch_code]
        new.tag = self.tag
        return new

    def __hash__(self):
        return hash(self.name) ^ hash("Table")

    def __eq__(self, other):
        return other.name == self.name

    def __repr__(self):
        return f"Table: {self.name}"


class Value:

    def __init__(self, value, path_code, branch_code):
        self.value = value
        self.path_code = path_code
        self.branch_code = branch_code

    def __eq__(self, other):
        return self.path_code.string == other.path_code.string and \
               self.branch_code.value == other.branch_code.value

    @property
    def codes(self):
        return (self.path_code, self.branch_code)

    def __hash__(self):
        return hash(self.value) ^ hash("Value")

    def __repr__(self):
        return f"Value: {self.value}"

    def copy(self):
        path_code = self.path_code.copy()
        branch_code = self.branch_code.copy()
        return Value(self.value, path_code, branch_code)


class Database:

    def __init__(self, db=None):
        self.tables = []
        self.columns = []

        if db is None:
            return

        self.db_id = db["db_id"]

        table_names = db["table_names"]
        table_names_original = db["table_names_original"]
        table_types = db["table_type"]
        table_tags = db["tag"]
        for tid, tab in enumerate(table_names_original):
            table = Table(tab, tid)
            table.nature = table_names[tid]
            table.dtype = table_types[tid]
            table.tag = table_tags[tid]
            self.tables.append(table)

        column_names = db["column_names"]
        column_names_original = db["column_names_original"]
        column_type = db["column_types"]
        for cid, col in enumerate(column_names_original):
            column = Column(col[1], cid)
            column.nature = column_names[cid][1]
            column.dtype = column_type[cid]
            column.cell_values = db["cell_values"][cid]
            if col[1] == '*':
                column.table = None
            else:
                table = self.tables[col[0]]
                column.table = table
                table.columns.append(column)
            self.columns.append(column)

        foreign_keys = [tuple(k) for k in db["foreign_keys"]]
        primary_keys = db["primary_keys"]
        for pk in primary_keys:
            column = self.columns[pk]
            column.is_pk = True
            column.table.pks.append(column)

        for fk in set(foreign_keys):
            s, e = fk
            primary_column = self.columns[s]
            foreign_column = self.columns[e]
            foreign_column.fk = primary_column
            primary_column.table.fks.append((primary_column, foreign_column))
            foreign_column.table.fks.append((primary_column, foreign_column))

        for table in self.tables:
            for column in table.pks:
                if column.dtype != "number":
                    table.agents.append(column)
            if len(table.agents) == 0:
                for column in table.columns:
                    if column.name.lower().replace('_', '') == table.name.lower().replace('_', ''):
                        table.agents.append(column)
                        break
            if len(table.agents) == 0:
                for column in table.columns:
                    if column.name.lower().replace('_', '') in ["name", "firstname", "lastname", "fname", "lname", "title"]:
                        table.agents.append(column)
            if len(table.agents) == 0:
                for column in table.columns:
                    if "name" in column.name.lower() or "title" in column.name.lower():
                        table.agents.append(column)

    def clear(self):
        self.tables = []
        self.columns = []

    def add_column(self, column):
        if column not in self.columns:
            column.cid = len(self.columns)
            self.columns.append(column)

    def add_table(self, table):
        if table not in self.tables:
            table.tid = len(self.tables)
            self.tables.append(table)

    def copy(self):
        new = Database()
        new.db_id = self.db_id
        tables = {}
        for table in self.tables:
            tables[table.name] = table.copy()

        columns = {}
        for column in self.columns:
            columns[column.full_name] = column.copy()

        for column in self.columns:
            if column.name == "*":
                continue
            new_column = columns[column.full_name]
            new_column.table = tables[column.table.name]
            new_column.fk = columns[column.fk.full_name] if column.fk is not None else None

        for table in self.tables:
            new_table = tables[table.name]
            for column in table.columns:
                new_table.columns.append(columns[column.full_name])
            for pk in table.pks:
                new_table.pks.append(columns[pk.full_name])
            for pk, fk in table.fks:
                new_table.fks.append((columns[pk.full_name], columns[fk.full_name]))
            for agent in table.agents:
                new_table.agents.append(columns[agent.full_name])

        new.tables = list(tables.values())
        new.columns = list(columns.values())
        return new

    def to_dict(self):
        db_dict = {'db_id': self.db_id,
                   'table_names_original': [],
                   'table_names': [],
                   'column_names_original': [],
                   'column_names': [],
                   'column_types': [],
                   'table_type': [],
                   'primary_keys': [],
                   'foreign_keys': [],
                   'cell_values': [],
                   'tag': []}

        self.tables = sorted(self.tables, key=lambda x: x.tid)
        for table in self.tables:
            db_dict["table_names_original"].append(table.name)
            db_dict["table_names"].append(table.nature)
            db_dict["table_type"].append(table.dtype)
            db_dict["tag"].append(table.tag)
            for pk in table.pks:
                db_dict["primary_keys"].append(pk.cid)
            for fk in table.fks:
                if [fk[0].cid, fk[1].cid] not in db_dict["foreign_keys"]:
                    db_dict["foreign_keys"].append([fk[0].cid, fk[1].cid])

        self.columns = sorted(self.columns, key=lambda x: x.cid)
        for column in self.columns:
            db_dict["cell_values"].append(column.cell_values)
            if column.name == "*":
                db_dict["column_names_original"].append([-1, "*"])
                db_dict["column_names"].append([-1, "*"])
            else:
                db_dict["column_names_original"].append([column.table.tid, column.name])
                db_dict["column_names"].append([column.table.tid, column.nature])
            db_dict["column_types"].append(column.dtype)

        return db_dict


if __name__ == '__main__':
    dbs = json.load(open('data/tables_with_tags.json', 'r'))

    total, error = 0, 0
    for db in dbs:
        database = Database(db)
        recover = database.to_dict()
        # original = json.dumps(db)
        # new = json.dumps(recover)
        total += 1
        for k in db:
            if db[k] != recover[k]:
                if k != "foreign_keys":
                    if set([json.dumps(s) for s in db[k]]) == set([json.dumps(s) for s in recover[k]]):
                        continue
                    else:
                        error += 1
                break
        # if original != new:
        #     print(db)
        #     print(recover)
        #     break
        # corr += db == recover

    print(total, error)

