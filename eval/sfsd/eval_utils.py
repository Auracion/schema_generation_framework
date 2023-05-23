import os, sys
import json
import sqlite3
from nltk import word_tokenize

CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'order', 'limit', 'intersect', 'union', 'except')
JOIN_KEYWORDS = ('join', 'on', 'as')

WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')
UNIT_OPS = ('none', '-', '+', "*", '/')
AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg')
TABLE_TYPE = {
    'sql': "sql",
    'table_unit': "table_unit",
}

COND_OPS = ('and', 'or')
SQL_OPS = ('intersect', 'union', 'except')
ORDER_OPS = ('desc', 'asc')


HARDNESS = {
    "component1": ('where', 'group', 'order', 'limit', 'join', 'or', 'like'),
    "component2": ('except', 'union', 'intersect')
}


class HardnessChecker:

    def get_hardness(self, sql):
        count_comp1_ = self.count_component1(sql)
        count_comp2_ = self.count_component2(sql)
        count_others_ = self.count_others(sql)

        if count_comp1_ <= 1 and count_others_ == 0 and count_comp2_ == 0:
            return "easy"
        elif (count_others_ <= 2 and count_comp1_ <= 1 and count_comp2_ == 0) or \
                (count_comp1_ <= 2 and count_others_ < 2 and count_comp2_ == 0):
            return "medium"
        elif (count_others_ > 2 and count_comp1_ <= 2 and count_comp2_ == 0) or \
                (2 < count_comp1_ <= 3 and count_others_ <= 2 and count_comp2_ == 0) or \
                (count_comp1_ <= 1 and count_others_ == 0 and count_comp2_ <= 1):
            return "hard"
        else:
            return "extra"

    @staticmethod
    def has_agg(unit):
        return unit[0] != AGG_OPS.index('none')

    def count_agg(self, units):
        return len([unit for unit in units if self.has_agg(unit)])

    @staticmethod
    def count_component1(sql):
        count = 0
        if len(sql['where']) > 0:
            count += 1
        if len(sql['groupBy']) > 0:
            count += 1
        if len(sql['orderBy']) > 0:
            count += 1
        if sql['limit'] is not None:
            count += 1
        if len(sql['from']['table_units']) > 0:  # JOIN
            count += len(sql['from']['table_units']) - 1

        ao = sql['from']['conds'][1::2] + sql['where'][1::2] + sql['having'][1::2]
        count += len([token for token in ao if token == 'or'])
        cond_units = sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]
        count += len([cond_unit for cond_unit in cond_units if cond_unit[1] == WHERE_OPS.index('like')])

        return count

    @staticmethod
    def get_nestedSQL(sql):
        nested = []
        for cond_unit in sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]:
            if type(cond_unit[3]) is dict:
                nested.append(cond_unit[3])
            if type(cond_unit[4]) is dict:
                nested.append(cond_unit[4])
        if sql['intersect'] is not None:
            nested.append(sql['intersect'])
        if sql['except'] is not None:
            nested.append(sql['except'])
        if sql['union'] is not None:
            nested.append(sql['union'])
        return nested

    def count_component2(self, sql):
        nested = self.get_nestedSQL(sql)
        return len(nested)

    def count_others(self, sql):
        count = 0
        # number of aggregation
        agg_count = self.count_agg(sql['select'][1])
        agg_count += self.count_agg(sql['where'][::2])
        agg_count += self.count_agg(sql['groupBy'])
        if len(sql['orderBy']) > 0:
            agg_count += self.count_agg([unit[1] for unit in sql['orderBy'][1] if unit[1]] +
                                   [unit[2] for unit in sql['orderBy'][1] if unit[2]])
        agg_count += self.count_agg(sql['having'])
        if agg_count > 1:
            count += 1

        # number of select columns
        if len(sql['select'][1]) > 1:
            count += 1

        # number of where conditions
        if len(sql['where']) > 1:
            count += 1

        # number of group by clauses
        if len(sql['groupBy']) > 1:
            count += 1

        return count


class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema):
        self._schema = schema
        self._idMap = self._map(self._schema)

    @property
    def schema(self): # map lowercased raw tab name to lowercased raw col name list
        return self._schema

    @property
    def idMap(self): # map tab name to __tab__, tab.col to __tab.col__, * to __all__, all lowercased
        return self._idMap

    def _map(self, schema):
        idMap = {'*': "__all__", '[TABLE]': "__tab__", '[COLUMN]':'__col__', '[VALUE]': '__val__'}
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

    schema = {}
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # fetch table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [str(table[0].lower()) for table in cursor.fetchall()]

    # fetch table info
    for table in tables:
        cursor.execute("PRAGMA table_info({})".format(table))
        schema[table] = [str(col[1].lower()) for col in cursor.fetchall()]

    return schema


def get_schema_from_json(fpath):
    with open(fpath) as f:
        data = json.load(f)

    schema = {}
    for entry in data:
        table = str(entry['table'].lower())
        cols = [str(col['column_name'].lower()) for col in entry['col_data']]
        schema[table] = cols

    return schema


def tokenize(string):
    string = str(string)
    string = string.replace("\'", "\"")  # ensures all string values wrapped by "" problem??
    quote_idxs = [idx for idx, char in enumerate(string) if char == '"']
    assert len(quote_idxs) % 2 == 0, "Unexpected quote"

    # keep string value as token
    vals = {}
    for i in range(len(quote_idxs)-1, -1, -2):
        qidx1 = quote_idxs[i-1]
        qidx2 = quote_idxs[i]
        val = string[qidx1: qidx2+1]
        key = "__val_{}_{}__".format(qidx1, qidx2)
        string = string[:qidx1] + key + string[qidx2+1:]
        vals[key] = val

    _toks = [word for word in word_tokenize(string)]
    toks = []
    for tok in _toks:
        if tok == '[' or tok == ']':
            continue
        elif tok in ['TABLE', 'COLUMN', 'VALUE']:
            toks.append('[' + tok + ']')
        else:
            toks.append(tok.lower())
    # replace with string value token
    for i in range(len(toks)):
        if toks[i] in vals:
            toks[i] = vals[toks[i]]

    # find if there exists !=, >=, <=
    eq_idxs = [idx for idx, tok in enumerate(toks) if tok == "="]
    eq_idxs.reverse()
    prefix = ('!', '>', '<')
    for eq_idx in eq_idxs:
        pre_tok = toks[eq_idx-1]
        if pre_tok in prefix:
            toks = toks[:eq_idx-1] + [pre_tok + "="] + toks[eq_idx+1: ]

    return toks


def scan_alias(toks):
    """Scan the index of 'as' and build the map for all alias"""
    as_idxs = [idx for idx, tok in enumerate(toks) if tok == 'as']
    alias = {}
    for idx in as_idxs:
        alias[toks[idx+1]] = toks[idx-1] # overwritten problem
    return alias


def check_contradiction(toks):
    as_idxs = [idx for idx, tok in enumerate(toks) if tok == 'as']
    alias = {}
    for idx in as_idxs:
        a = toks[idx+1]
        if a in alias and alias[a] != toks[idx-1]:
            return True
        alias[a] = toks[idx-1]
    return False


def toks2nested(toks):
    """
        Determine the scope for each sub-sql
        mapping [select, count, (, c1, ), from, (, select c1, c2, from, t, ), ... ] into
        [select, count, (, c1, ), from, [select, c1, c2, from, t], ... ]
    """
    def detect_sql(idx):
        count, sql_list = 0, []
        while idx < len(toks):
            if toks[idx] == '(':
                count += 1
                if toks[idx + 1] == 'select':
                    sub_sql_list, idx = detect_sql(idx + 1)
                    count -= 1
                    sql_list.append(sub_sql_list)
                else:
                    sql_list.append('(')
            elif toks[idx] == ')':
                count -= 1
                if count < 0:
                    return sql_list, idx
                else:
                    sql_list.append(')')
            else:
                sql_list.append(toks[idx])
            idx += 1
        return sql_list, idx

    def intersect_union_except(tok_list):
        for idx, tok in enumerate(tok_list):
            if type(tok) == list:
                new_tok = intersect_union_except(tok)
                tok_list[idx] = new_tok
        for op in ['intersect', 'union', 'except']:
            if op in tok_list:
                idx = tok_list.index(op)
                tok_list = [tok_list[:idx]] + [op] + [tok_list[idx+1:]]
                break
        return tok_list

    try:
        nested_toks, _ = detect_sql(0)
        # not all sqls are wrapped with (), e.g. sql1 intersect sql2
        # add wrapper for each sql on the left and right handside of intersect/union/except
        nested_toks = intersect_union_except(nested_toks)
        return nested_toks
    except:
        print('Something unknown happened when transforming %s' % (' '.join(toks)))
        return None


def reassign_table_alias(nested_toks, index):
    current_map = {} # map old alias in the current sql to new alias in global map
    as_idxs = [idx for idx, tok in enumerate(nested_toks) if tok == 'as']
    for idx in as_idxs:
        index += 1 # add 1 to global index for table alias before assignment
        assert nested_toks[idx+1] not in current_map
        current_map[nested_toks[idx+1]] = 't' + str(index)
        nested_toks[idx+1] = 't' + str(index)
    for j, tok in enumerate(nested_toks):
        if type(tok) == list:
            new_tok, index = reassign_table_alias(tok, index)
            nested_toks[j] = new_tok
        elif '.' in tok:
            for alias in current_map.keys():
                if tok.startswith(alias + '.'):
                    nested_toks[j] = current_map[alias] + '.' + tok[tok.index('.')+1:]
                    break
    return nested_toks, index


def normalize_table_alias(toks):
    """ Make sure that different table alias are assigned to different tables """
    if toks.count('select') == 1:
        return toks # no nested sql, don't worry
    elif toks.count('select') > 1:
        flag = check_contradiction(toks)
        if flag: # avoid unnecessary normalization process
            nested_toks = toks2nested(toks)
            index = 0 # global index for table alias
            nested_toks, _ = reassign_table_alias(nested_toks, index)
            def flatten(x):
                if type(x) == list and ('intersect' in x or 'union' in x or 'except' in x):
                    assert len(x) == 3 # sql1 union sql2
                    return ['('] + flatten(x[0])[1:-1] + [x[1]] + flatten(x[2])[1:-1] + [')']
                elif type(x) == list:
                    return ['('] + [y for l in x for y in flatten(l)] + [')']
                else:
                    return [x]
            toks = flatten(nested_toks)[1:-1]
        return toks
    else:
        raise ValueError('Something wrong in sql because no select clause is found!')


def get_tables_with_alias(schema, toks):
    toks = normalize_table_alias(toks)
    tables = scan_alias(toks)
    for key in schema:
        assert key not in tables, "Alias {} has the same name in table".format(key)
        tables[key] = key
    return tables, toks


def parse_col(toks, start_idx, tables_with_alias, schema, default_tables=None, sketch=False, *args, **kwargs):
    """
        :returns next idx, column id
    """
    tok = toks[start_idx]
    if tok == "*":
        return start_idx + 1, schema.idMap[tok]

    if '.' in tok:  # if token is a composite
        alias, col = tok.split('.')
        if sketch: return start_idx+1, schema.idMap['[COLUMN]']
        key = tables_with_alias[alias] + "." + col
        return start_idx+1, schema.idMap[key]

    # assert default_tables is not None and len(default_tables) > 0, "Default tables should not be None or empty"

    if sketch:
        return start_idx + 1, schema.idMap['[COLUMN]']
    for alias in default_tables:
        table = tables_with_alias[alias]
        if tok in schema.schema[table]:
            key = table + "." + tok
            return start_idx+1, schema.idMap[key]

    assert False, "Error col: {}".format(tok)


def parse_col_unit(toks, start_idx, tables_with_alias, schema, default_tables=None, *args, **kwargs):
    """
        :returns next idx, (agg_op id, col_id)
    """
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    isDistinct = False
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    if toks[idx] in AGG_OPS:
        agg_id = AGG_OPS.index(toks[idx])
        idx += 1
        assert idx < len_ and toks[idx] == '('
        idx += 1
        if toks[idx] == "distinct":
            idx += 1
            isDistinct = True
        idx, col_id = parse_col(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
        assert idx < len_ and toks[idx] == ')'
        idx += 1
        return idx, (agg_id, col_id, isDistinct)

    if toks[idx] == "distinct":
        idx += 1
        isDistinct = True
    agg_id = AGG_OPS.index("none")
    idx, col_id = parse_col(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)

    if isBlock:
        assert toks[idx] == ')'
        idx += 1  # skip ')'

    return idx, (agg_id, col_id, isDistinct)


def parse_val_unit(toks, start_idx, tables_with_alias, schema, default_tables=None, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    col_unit1 = None
    col_unit2 = None
    unit_op = UNIT_OPS.index('none')

    idx, col_unit1 = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
    if idx < len_ and toks[idx] in UNIT_OPS:
        unit_op = UNIT_OPS.index(toks[idx])
        idx += 1
        idx, col_unit2 = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)

    if isBlock:
        assert toks[idx] == ')'
        idx += 1  # skip ')'

    return idx, (unit_op, col_unit1, col_unit2)


def parse_table_unit(toks, start_idx, tables_with_alias, schema, sketch=False, *args, **kwargs):
    """
        :returns next idx, table id, table name
    """
    idx = start_idx
    len_ = len(toks)
    # print(sketch, toks[idx])
    if sketch and toks[idx] == '[TABLE]':
        key = '[TABLE]'
    else:
        key = tables_with_alias[toks[idx]]

    if idx + 1 < len_ and toks[idx+1] == "as":
        idx += 3
    else:
        idx += 1

    return idx, schema.idMap[key], key


def parse_value(toks, start_idx, tables_with_alias, schema, default_tables=None, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)

    isBlock = False
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    if toks[idx] == 'select':
        idx, val = parse_sql(toks, idx, tables_with_alias, schema, *args, **kwargs)
    elif "\"" in toks[idx] and toks[idx] not in ['[TABLE]', '[COLUMN]']:  # token is a string value
        val = toks[idx]
        idx += 1
    else:
        try:
            val = float(toks[idx])
            idx += 1
        except:
            end_idx = idx
            while end_idx < len_ and toks[end_idx] != ',' and toks[end_idx] != ')'\
                and toks[end_idx] != 'and' and toks[end_idx] not in CLAUSE_KEYWORDS and toks[end_idx] not in JOIN_KEYWORDS:
                    end_idx += 1

            idx, val = parse_col_unit(toks[start_idx: end_idx], 0, tables_with_alias, schema, default_tables, *args, **kwargs)
            idx = end_idx

    if isBlock:
        assert toks[idx] == ')'
        idx += 1

    return idx, val


def parse_condition(toks, start_idx, tables_with_alias, schema, default_tables=None, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)
    conds = []

    while idx < len_:
        idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
        not_op = False
        if toks[idx] == 'not':
            not_op = True
            idx += 1

        assert idx < len_ and toks[idx] in WHERE_OPS, "Error condition: idx: {}, tok: {}".format(idx, toks[idx])
        op_id = WHERE_OPS.index(toks[idx])
        idx += 1
        val1 = val2 = None
        if op_id == WHERE_OPS.index('between'):  # between..and... special case: dual values
            idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
            assert toks[idx] == 'and'
            idx += 1
            idx, val2 = parse_value(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
        else:  # normal case: single value
            idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
            val2 = None

        conds.append((not_op, op_id, val_unit, val1, val2))

        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";") or toks[idx] in JOIN_KEYWORDS):
            break

        if idx < len_ and toks[idx] in COND_OPS:
            conds.append(toks[idx])
            idx += 1  # skip and/or

    return idx, conds


def parse_select(toks, start_idx, tables_with_alias, schema, default_tables=None, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)

    assert toks[idx] == 'select', "'select' not found"
    idx += 1
    isDistinct = False
    if idx < len_ and toks[idx] == 'distinct':
        idx += 1
        isDistinct = True
    val_units = []

    while idx < len_ and toks[idx] not in CLAUSE_KEYWORDS:
        agg_id = AGG_OPS.index("none")
        if toks[idx] in AGG_OPS:
            agg_id = AGG_OPS.index(toks[idx])
            idx += 1
        idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
        val_units.append((agg_id, val_unit))
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','

    return idx, (isDistinct, val_units)


def parse_from(toks, start_idx, tables_with_alias, schema, *args, **kwargs):
    """
    Assume in the from clause, all table units are combined with join
    """
    assert 'from' in toks[start_idx:], "'from' not found"

    len_ = len(toks)
    idx = toks.index('from', start_idx) + 1
    default_tables = []
    table_units = []
    conds = []

    while idx < len_:
        isBlock = False
        if toks[idx] == '(':
            isBlock = True
            idx += 1

        if toks[idx] == 'select':
            idx, sql = parse_sql(toks, idx, tables_with_alias, schema, *args, **kwargs)
            table_units.append((TABLE_TYPE['sql'], sql))
        else:
            if idx < len_ and toks[idx] == 'join':
                idx += 1  # skip join
            idx, table_unit, table_name = parse_table_unit(toks, idx, tables_with_alias, schema, *args, **kwargs)
            table_units.append((TABLE_TYPE['table_unit'],table_unit))
            default_tables.append(table_name)
        if idx < len_ and toks[idx] == "on":
            idx += 1  # skip on
            idx, this_conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
            if len(conds) > 0:
                conds.append('and')
            conds.extend(this_conds)

        if isBlock:
            assert toks[idx] == ')'
            idx += 1
        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
            break

    return idx, table_units, conds, default_tables


def parse_where(toks, start_idx, tables_with_alias, schema, default_tables, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != 'where':
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
    return idx, conds


def parse_group_by(toks, start_idx, tables_with_alias, schema, default_tables, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)
    col_units = []

    if idx >= len_ or toks[idx] != 'group':
        return idx, col_units

    idx += 1
    assert toks[idx] == 'by'
    idx += 1

    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        idx, col_unit = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
        col_units.append(col_unit)
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','
        else:
            break

    return idx, col_units


def parse_order_by(toks, start_idx, tables_with_alias, schema, default_tables, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)
    val_units = []
    order_type = 'asc' # default type is 'asc'

    if idx >= len_ or toks[idx] != 'order':
        return idx, val_units

    idx += 1
    assert toks[idx] == 'by'
    idx += 1

    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
        val_units.append(val_unit)
        if idx < len_ and toks[idx] in ORDER_OPS:
            order_type = toks[idx]
            idx += 1
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','
        else:
            break

    return idx, (order_type, val_units)


def parse_having(toks, start_idx, tables_with_alias, schema, default_tables, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != 'having':
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
    return idx, conds


def parse_limit(toks, start_idx, sketch=False, *args, **kwargs):
    idx = start_idx
    len_ = len(toks)

    if idx < len_ and toks[idx] == 'limit':
        idx += 2
        return idx, 1 if sketch else int(toks[idx-1])

    return idx, None


def parse_sql(toks, start_idx, tables_with_alias, schema, *args, **kwargs):
    isBlock = False # indicate whether this is a block of sql/sub-sql
    len_ = len(toks)
    idx = start_idx

    sql = {}
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    # parse from clause in order to get default tables
    from_end_idx, table_units, conds, default_tables = parse_from(toks, start_idx, tables_with_alias, schema, *args, **kwargs)
    # if len(default_tables) == 0:
    #     print(toks)
    sql['from'] = {'table_units': table_units, 'conds': conds}
    # select clause
    _, select_col_units = parse_select(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
    idx = from_end_idx
    sql['select'] = select_col_units
    # where clause
    idx, where_conds = parse_where(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
    sql['where'] = where_conds
    # group by clause
    idx, group_col_units = parse_group_by(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
    sql['groupBy'] = group_col_units
    # having clause
    idx, having_conds = parse_having(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
    sql['having'] = having_conds
    # order by clause
    idx, order_col_units = parse_order_by(toks, idx, tables_with_alias, schema, default_tables, *args, **kwargs)
    sql['orderBy'] = order_col_units
    # limit clause
    idx, limit_val = parse_limit(toks, idx, *args, **kwargs)
    sql['limit'] = limit_val

    idx = skip_semicolon(toks, idx)
    if isBlock:
        assert toks[idx] == ')'
        idx += 1  # skip ')'
    idx = skip_semicolon(toks, idx)

    # intersect/union/except clause
    for op in SQL_OPS:  # initialize IUE
        sql[op] = None
    if idx < len_ and toks[idx] in SQL_OPS:
        sql_op = toks[idx]
        idx += 1
        idx, IUE_sql = parse_sql(toks, idx, tables_with_alias, schema, *args, **kwargs)
        sql[sql_op] = IUE_sql
    return idx, sql


def load_data(fpath):
    with open(fpath) as f:
        data = json.load(f)
    return data


def get_sql(schema, query, *args, **kwargs):
    toks = tokenize(query)
    tables_with_alias, toks = get_tables_with_alias(schema.schema, toks)
    _, sql = parse_sql(toks, 0, tables_with_alias, schema, *args, **kwargs)
    return sql


def skip_semicolon(toks, start_idx):
    idx = start_idx
    while idx < len(toks) and toks[idx] == ";":
        idx += 1
    return idx