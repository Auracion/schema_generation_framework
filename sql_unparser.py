import os, sys, json, tempfile
from collections import defaultdict
from copy import deepcopy

sys.path.append(os.path.dirname(__file__))

from database_utils import Database
from sql_parser import SQLEncoder
from eval.spider.evaluation import evaluate, build_foreign_key_map_from_json


class ASTNode:
    def __init__(self, name=None):
        self.name = name
        self.isterminal = True
        self.named_fields = {}
        self.fields = Field()

    def add_field(self, branch):
        self.isterminal = False
        if len(self.fields) <= branch:
            for i in range(len(self.fields), branch + 1):
                self.fields.add_value(ASTNode())

    def add_named_field(self, node):
        self.isterminal = False
        if node not in self.named_fields:
            self.named_fields[node] = ASTNode(node)

    def set_name(self, name):
        self.name = name

    def exist(self, name):
        return name in self.named_fields

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.fields[item]
        else:
            return self.named_fields[item]


class Field:
    def __init__(self):
        self.values = []

    def add_value(self, value):
        self.values.append(value)

    def __len__(self):
        return len(self.values)

    def __getitem__(self, item):
        return self.values[item]


class SQLDecoder:
    def __init__(self, dsg, values):
        self.dsg = dsg
        self.values = values
        self.tree = ASTNode('root')
        self.decode()

    def decode(self):
        for column in self.dsg.columns:
            for code in column.codes:
                self.decode_node(column.full_name, code)

        for table in self.dsg.tables:
            for code in table.codes:
                self.decode_node(table.name, code)

        for value in self.values:
            self.decode_node(value.value, value.codes)

    def decode_node(self, name, code):
        path_code, branch_code = code
        if len(path_code) == 0:
            return
        cur = self.tree
        for i, (node, branch) in enumerate(zip(path_code, branch_code)):
            cur.set_name(node)
            if node == 'unit':
                cur.add_named_field(path_code[i + 1])
                cur = cur[path_code[i + 1]]
            else:
                cur.add_field(int(branch))
                cur = cur[int(branch)]
        cur.set_name(name)


class Unparser:

    def __init__(self, tree):
        self.tree = tree

    def get_face_code(self):
        return self.unparse_sql(self.tree)

    def unparse_sql(self, node):
        if node.name in ["union", "intersect", "except"]:
            return self.unparse_sql(node[0]) + f" {node.name.upper()} " + self.unparse_sql(node[1])
        else:
            return self.unparse_sql_unit(node)

    def unparse_sql_unit(self, node: ASTNode):
        select_clause = self.unparse_select(node["select"])
        from_clause = self.unparse_from(node["from"])
        fcode = select_clause + " " + from_clause
        if node.exist("on"):
            on_clause = self.unparse_on(node["on"])
            fcode += " " + on_clause
        if node.exist("where"):
            where_clause = self.unparse_conds(node["where"][0])
            fcode += " WHERE " + where_clause
        if node.exist("groupby"):
            groupby_clause = self.unparse_groupby(node["groupby"])
            fcode += " " + groupby_clause
        if node.exist("having"):
            where_clause = self.unparse_conds(node["having"][0])
            fcode += " HAVING " + where_clause
        if node.exist("orderby"):
            orderby_clause = self.unparse_orderby(node["orderby"][0])
            fcode += " " + orderby_clause
        return fcode

    def unparse_select(self, node: ASTNode):
        fcode_items = [self.unparse_value(item) for item in node.fields]
        return "SELECT " + ", ".join(fcode_items)

    def unparse_from(self, node: ASTNode):
        fcode_items = []
        for ast in node.fields:
            if ast.isterminal:
                fcode_items.append(ast.name)
            else:
                fcode_items.append(f"({self.unparse_sql(ast)})")
        return "FROM " + " JOIN ".join(fcode_items)

    def unparse_on(self, node: ASTNode):
        conds = [self.unparse_cond(item) for item in node.fields]
        return "ON " + " and ".join(conds)

    def unparse_groupby(self, node: ASTNode):
        fcode_items = [self.unparse_col(item) for item in node.fields]
        return "GROUP BY " + ", ".join(fcode_items)

    def unparse_orderby(self, node: ASTNode):
        if node.name in ["asc", "desc"]:
            fcode_items = [self.unparse_value(item) for item in node.fields]
            return "ORDER BY " + ", ".join(fcode_items) + f" {node.name.upper()}"
        else:
            order = " ASC " if node.name.startswith("asc") else " DESC "
            limit = int(node.name[3:]) if node.name.startswith("asc") else int(node.name[4:])
            fcode_items = [self.unparse_value(item) for item in node.fields]
            return "ORDER BY " + ", ".join(fcode_items) + order + f"LIMIT {limit}"

    def unparse_conds(self, node: ASTNode):
        if node.name in ['and', 'or']:
            left, right = node.fields
            left = self.unparse_conds(left)
            right = self.unparse_conds(right)
            return f"{left} {node.name} {right}"
        else:
            return self.unparse_cond(node)

    def unparse_cond(self, node: ASTNode):
        op = node.name
        if op == "NotIn":
            op = "NOT IN"
        if op == "NotLike":
            op = "NOT LIKE"
        if len(node.fields) == 3:
            left, val1, val2 = node.fields.values
            left = self.unparse_value(left)
            if val1.name in ['unit', 'union', 'intersect', 'except']:
                right = self.unparse_sql(val1)
                right = f"({right})"
            else:
                right = val1.name
            return f"{left} {op} {right} and {val2.name}"
        else:
            left, right = node.fields.values
            left = self.unparse_value(left)
            if right.name in ['unit', 'union', 'intersect', 'except']:
                right = self.unparse_sql(right)
                right = f"({right})"
            elif right.name in ["Unary", "Plus", "Minus", "Times", "Divide", "Count", "Avg", "Sum", "Min", "Max"]:
                right = self.unparse_value(right)
            else:
                right = right.name

            return f"{left} {op} {right}"

    def unparse_value(self, node: ASTNode):
        if node.name == "Unary":
            return self.unparse_col(node[0])
        elif node.name == "Plus":
            values = [self.unparse_col(val) for val in node.fields]
            return " + ".join(values)
        elif node.name == "Minus":
            values = [self.unparse_col(val) for val in node.fields]
            return " - ".join(values)
        elif node.name == "Times":
            values = [self.unparse_col(val) for val in node.fields]
            return " * ".join(values)
        elif node.name == "Divide":
            values = [self.unparse_col(val) for val in node.fields]
            return " / ".join(values)
        elif node.name in ["Count", "Avg", "Sum", "Min", "Max"]:
            return f"{node.name.lower()}({self.unparse_value(node[0])})"
        else:
            raise ValueError

    def unparse_col(self, node: ASTNode):
        if node.isterminal:
            return node.name
        else:
            agg = node.name
            col = node[0].name
            return f"{agg}({col})"


if __name__ == '__main__':
    trains = json.load(open('data/train.json', 'r'))
    dbs = json.load(open('data/tables_with_annots.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}
    count = 0
    golden, preds = [], []
    kmaps = build_foreign_key_map_from_json('data/tables.json')
    for i, data in enumerate(trains):
        sql = data["sql"]
        db_id = data["db_id"]
        query = data['query'].replace('\t', ' ') + '\t' + db_id
        golden.append(query)
        db = deepcopy(dbs[db_id])
        encoder = SQLEncoder(db)
        dsg, values = encoder.encode(sql)
        decoder = SQLDecoder(dsg, values)
        ast = decoder.tree
        unparser = Unparser(ast)
        new_sql = unparser.get_face_code()
        preds.append(new_sql)

    with tempfile.NamedTemporaryFile('w+t', encoding='utf8', suffix='.sql') as tmp_pred, \
            tempfile.NamedTemporaryFile('w+t', encoding='utf8', suffix='.sql') as tmp_ref:
        # write pred and ref sqls
        for s in preds:
            tmp_pred.write(s + '\n')
        tmp_pred.flush()
        for s in golden:
            tmp_ref.write(s + '\n')
        tmp_ref.flush()
        # calculate sql accuracy
        result_type = 'exact'
        all_exact_acc = evaluate(tmp_ref.name, tmp_pred.name, '../data/spider/database', 'match', kmaps)[1]

        print(all_exact_acc)
