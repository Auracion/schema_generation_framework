import os, sys, json
from copy import deepcopy

sys.path.append(os.path.dirname(__file__))

from database_utils import *


class TreeNode:
    def __init__(self, value, isterminal):
        self.value = value
        self.left = None
        self.right = None
        self.isterminal = isterminal
        self.pc_buffer = None
        self.bc_buffer = None


class SQLEncoder:
    AGG = [None, 'Max', 'Min', 'Count', 'Sum', 'Avg']
    UNITOP = ['Unary', 'Minus', 'Plus', 'Times', 'Divide']
    CMPOP = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')

    def __init__(self, dsg: Database, path_prefix: PathCode=None, branch_prefix: BranchCode=None):

        self.dsg = dsg
        self.path_prefix = path_prefix if path_prefix is not None else PathCode()
        self.branch_prefix = branch_prefix if branch_prefix is not None else BranchCode()
        self.tables = set()
        self.values = []

    def encode(self, sql):
        if sql["union"] is not None:
            parser = SQLEncoder(self.dsg,
                                path_prefix=self.path_prefix.copy("union"),
                                branch_prefix=self.branch_prefix.copy('1'))
            self.path_prefix.add("union")
            self.branch_prefix.add('0')
            dsg, values = parser.encode(sql["union"])
            self.values.extend(values)

        if sql["intersect"] is not None:
            parser = SQLEncoder(self.dsg,
                                path_prefix=self.path_prefix.copy("intersect"),
                                branch_prefix=self.branch_prefix.copy('1'))
            self.path_prefix.add("intersect")
            self.branch_prefix.add('0')
            dsg, values = parser.encode(sql["intersect"])
            self.values.extend(values)

        if sql["except"] is not None:
            parser = SQLEncoder(self.dsg,
                                path_prefix=self.path_prefix.copy("except"),
                                branch_prefix=self.branch_prefix.copy('1'))
            self.path_prefix.add("except")
            self.branch_prefix.add('0')
            dsg, values = parser.encode(sql["except"])
            self.values.extend(values)

        self.path_prefix.add("unit")
        self.branch_prefix.add('0')
        self.encode_sql_unit(sql)
        return self.dsg, self.values

    def encode_sql_unit(self, sql_unit):
        self.encode_from(sql_unit['from'])
        if sql_unit['where']:
            self.encode_where(sql_unit['where'])
        if sql_unit['having']:
            self.encode_having(sql_unit['having'])
        if sql_unit['groupBy']:
            self.encode_groupby(sql_unit['groupBy'])
        if sql_unit['orderBy']:
            self.encode_orderby(sql_unit['orderBy'], sql_unit['limit'])
        self.encode_select(sql_unit['select'])

    def encode_select(self, select_clause):
        select_clause = select_clause[1]
        for i, (agg_id, val_unit) in enumerate(select_clause):
            agg = self.AGG[agg_id]
            path_prefix = self.path_prefix.copy("select")
            branch_prefix = self.branch_prefix.copy(str(i))
            if agg is not None:
                path_prefix.add(agg)
                branch_prefix.add('0')
            self.encode_val_unit(val_unit, path_prefix, branch_prefix)

    def encode_from(self, from_clause):
        table_units = from_clause['table_units']
        t = table_units[0][0]
        if t == 'table_unit':
            for i, tab_id in enumerate(table_units):
                table = self.dsg.tables[tab_id[1]]
                path = self.path_prefix.copy("from")
                branch = self.branch_prefix.copy(str(i))
                table.add_path(path)
                table.add_branch(branch)
                self.tables.add(table.name)
        else:
            parser = SQLEncoder(self.dsg,
                                path_prefix=self.path_prefix.copy("from"),
                                branch_prefix=self.branch_prefix.copy('0'))
            dsg, values = parser.encode(table_units[0][1])
            self.values.extend(values)

        conds = from_clause['conds']
        if len(conds) > 0:
            cid = 0
            for cond in conds:
                if cond != "and":
                    path_prefix = self.path_prefix.copy("on")
                    branch_prefix = self.branch_prefix.copy(str(cid))
                    self.encode_cond(cond, path_prefix, branch_prefix)
                    cid += 1

    def encode_where(self, where_clause):
        path_prefix = self.path_prefix.copy("where")
        branch_prefix = self.branch_prefix.copy('0')
        self.encode_conds(where_clause, path_prefix, branch_prefix)

    def encode_having(self, having_clause):
        path_prefix = self.path_prefix.copy("having")
        branch_prefix = self.branch_prefix.copy('0')
        self.encode_conds(having_clause, path_prefix, branch_prefix)

    def encode_groupby(self, groupby_clause):
        for i, col_unit in enumerate(groupby_clause):
            path_prefix = self.path_prefix.copy("groupby")
            branch_prefix = self.branch_prefix.copy(str(i))
            self.encode_col_unit(col_unit, path_prefix, branch_prefix)

    def encode_orderby(self, orderby_clause, limit_clause):
        order_str = orderby_clause[0]
        limit_str = str(limit_clause) if limit_clause else ''
        for i, val_unit in enumerate(orderby_clause[1]):
            path_prefix = self.path_prefix.copy("orderby")
            path_prefix.add(order_str + limit_str)
            branch_prefix = self.branch_prefix.copy('0')
            branch_prefix.add(str(i))
            self.encode_val_unit(val_unit, path_prefix, branch_prefix)

    def encode_conds(self, conds, path_prefix, branch_prefix):
        assert len(conds) > 0
        cond = conds[0]
        root = TreeNode(cond, True)
        last = root
        for i in range(1, len(conds), 2):
            op, cond = conds[i], conds[i + 1]
            op_node = TreeNode(op, isterminal=False)
            cond_node = TreeNode(cond, isterminal=True)
            if op.lower() == 'and':
                if last.isterminal:
                    op_node.left = last
                    op_node.right = cond_node
                    root = op_node
                    last = root
                else:
                    op_node.left = last.right
                    op_node.right = cond_node
                    last.right = op_node
                    last = op_node
            elif op.lower() == 'or':
                op_node.left = root
                op_node.right = cond_node
                root = op_node
                last = root
            else:
                raise ValueError

        queue = [root]
        root.pc_buffer = path_prefix.copy()
        root.bc_buffer = branch_prefix.copy()
        while len(queue) > 0:
            node = queue.pop()
            if not node.isterminal:
                node.left.pc_buffer = node.pc_buffer.copy(node.value)
                node.left.bc_buffer = node.bc_buffer.copy('0')
                node.right.pc_buffer = node.pc_buffer.copy(node.value)
                node.right.bc_buffer = node.bc_buffer.copy('1')
                queue.append(node.left)
                queue.append(node.right)
            else:
                p_prefix = node.pc_buffer
                b_prefix = node.bc_buffer
                self.encode_cond(node.value, p_prefix, b_prefix)

    def encode_cond(self, cond, path_prefix, branch_prefix):
        not_op, cmp_op, val_unit, val1, val2 = cond
        op = self.CMPOP[cmp_op]
        if not_op:
            op = 'NotIn' if op == 'in' else 'NotLike'
        # SQL
        if isinstance(val1, dict):
            val_path_prefix = path_prefix.copy(op)
            val_branch_prefix = branch_prefix.copy('0')
            self.encode_val_unit(val_unit, val_path_prefix, val_branch_prefix)

            parser = SQLEncoder(self.dsg,
                                path_prefix=path_prefix.copy(op),
                                branch_prefix=branch_prefix.copy('1'))
            dsg, values = parser.encode(val1)
            self.values.extend(values)
            if val2 is not None:
                value = Value(val2, path_prefix.copy(op), branch_prefix.copy('2'))
                self.values.append(value)
        # col op col
        elif isinstance(val1, list) or isinstance(val1, tuple):
            col1_path_prefix = path_prefix.copy(op)
            col1_branch_prefix = branch_prefix.copy('0')
            self.encode_val_unit(val_unit, col1_path_prefix, col1_branch_prefix)
            col2_path_prefix = path_prefix.copy(op)
            col2_path_prefix.add("Unary")
            col2_branch_prefix = branch_prefix.copy('1')
            col2_branch_prefix.add('0')
            self.encode_col_unit(val1, col2_path_prefix, col2_branch_prefix)

        else:
            p_prefix = path_prefix.copy(op)
            b_prefix = branch_prefix.copy('0')
            value = Value(val1, path_prefix.copy(op), branch_prefix.copy('1'))
            self.values.append(value)
            if val2 is not None:
                value = Value(val2, path_prefix.copy(op), branch_prefix.copy('2'))
                self.values.append(value)
            self.encode_val_unit(val_unit, p_prefix, b_prefix)

    def encode_val_unit(self, val_unit, path_prefix, branch_prefix):
        unit_op, col_unit1, col_unit2 = val_unit
        if unit_op == 0:
            p_prefix = path_prefix.copy("Unary")
            b_prefix = branch_prefix.copy('0')
            self.encode_col_unit(col_unit1, p_prefix, b_prefix)
        else:
            op = self.UNITOP[unit_op]
            p_prefix1 = path_prefix.copy(op)
            b_prefix1 = branch_prefix.copy('0')
            p_prefix2 = path_prefix.copy(op)
            b_prefix2 = branch_prefix.copy('1')
            self.encode_col_unit(col_unit1, p_prefix1, b_prefix1)
            self.encode_col_unit(col_unit2, p_prefix2, b_prefix2)

    def encode_col_unit(self, col_unit, path_prefix, branch_prefix):
        agg_id, col_id, distinct_flag = col_unit
        agg = self.AGG[agg_id]
        if agg is None:
            path = path_prefix.copy()
            branch = branch_prefix.copy()
        else:
            path = path_prefix.copy(agg)
            branch = branch_prefix.copy('0')
        column = self.dsg.columns[col_id]
        column.add_path(path)
        column.add_branch(branch)


if __name__ == '__main__':
    trains = json.load(open('data/train.json', 'r'))
    dbs = json.load(open('data/tables_with_annots.json', 'r'))
    dbs = {db["db_id"]: Database(db) for db in dbs}
    count = 0
    for i, data in enumerate(trains):
        sql = data["sql"]
        db_id = data["db_id"]
        db = deepcopy(dbs[db_id])
        encoder = SQLEncoder(db)
        dsg, values = encoder.encode(sql)